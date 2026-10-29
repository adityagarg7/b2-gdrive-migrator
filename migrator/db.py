"""Thread-safe SQLite database for catalog, checkpoints, resumable sessions, and quota tracking."""

from __future__ import annotations

import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

from .config import DB_PATH


class StateDB:
    """Persistent state store for multi-day, multi-TB migrations."""

    def __init__(self, db_path: Path = DB_PATH) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._init_schema()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self._lock, self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS buckets (
                    bucket_id TEXT PRIMARY KEY,
                    bucket_name TEXT UNIQUE NOT NULL,
                    bucket_type TEXT,
                    file_count INTEGER DEFAULT 0,
                    total_bytes INTEGER DEFAULT 0,
                    last_scanned_at REAL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS files (
                    b2_file_id TEXT PRIMARY KEY,
                    bucket_id TEXT NOT NULL,
                    bucket_name TEXT NOT NULL,
                    file_name TEXT NOT NULL,
                    basename TEXT NOT NULL,
                    top_prefix TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    b2_sha1 TEXT,
                    content_type TEXT,
                    upload_timestamp INTEGER,
                    status TEXT NOT NULL DEFAULT 'PENDING',
                    -- PENDING | IN_PROGRESS | COMPLETED | SKIPPED_DEDUP | SKIPPED_EXISTS | FAILED
                    transfer_mode TEXT,
                    target_account TEXT,
                    dest_rel_path TEXT,
                    drive_file_id TEXT,
                    verified_sha1 TEXT,
                    bytes_transferred INTEGER DEFAULT 0,
                    resumable_uri TEXT,
                    resumable_dest TEXT,
                    resumable_updated_at REAL,
                    retry_count INTEGER DEFAULT 0,
                    last_error TEXT,
                    updated_at REAL NOT NULL,
                    completed_at REAL,
                    UNIQUE(bucket_name, file_name)
                );

                CREATE INDEX IF NOT EXISTS idx_files_bucket_status ON files(bucket_name, status);
                CREATE INDEX IF NOT EXISTS idx_files_sha1 ON files(b2_sha1) WHERE b2_sha1 IS NOT NULL AND b2_sha1 != '';
                CREATE INDEX IF NOT EXISTS idx_files_basename_size ON files(basename, size_bytes);

                CREATE TABLE IF NOT EXISTS drive_folders (
                    folder_key TEXT PRIMARY KEY,
                    drive_folder_id TEXT NOT NULL,
                    created_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS byte_ledger (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    bucket_name TEXT NOT NULL,
                    bytes_count INTEGER NOT NULL,
                    mode TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_ledger_ts ON byte_ledger(ts);

                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    level TEXT NOT NULL,
                    bucket_name TEXT,
                    message TEXT NOT NULL
                );
                """
            )
            # Reset any stale IN_PROGRESS statuses left over from a hard crash back to PENDING
            # (preserving resumable_uri so uploads resume from exact byte offset!)
            cols = {r["name"] for r in conn.execute("PRAGMA table_info(files)").fetchall()}
            if "target_account" not in cols:
                conn.execute("ALTER TABLE files ADD COLUMN target_account TEXT;")
                conn.execute(
                    "UPDATE files SET target_account = 'mounted:G:\\' WHERE target_account IS NULL AND transfer_mode = 'mounted'"
                )
            conn.execute(
                "UPDATE files SET status = 'PENDING', updated_at = ? WHERE status = 'IN_PROGRESS'",
                (time.time(),),
            )

    def log_event(self, level: str, message: str, bucket_name: Optional[str] = None) -> None:
        now = time.time()
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT INTO events (ts, level, bucket_name, message) VALUES (?, ?, ?, ?)",
                (now, level.upper(), bucket_name, message),
            )
            # Keep last 1000 events
            conn.execute("DELETE FROM events WHERE id NOT IN (SELECT id FROM events ORDER BY id DESC LIMIT 1000)")

    def get_recent_events(self, limit: int = 80) -> List[Dict[str, Any]]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT id, ts, level, bucket_name, message FROM events ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    def upsert_bucket_catalog(
        self,
        bucket_id: str,
        bucket_name: str,
        bucket_type: str,
        file_records: List[Dict[str, Any]],
    ) -> Tuple[int, int]:
        """Insert or refresh B2 files for a bucket while preserving COMPLETED/resumable state."""
        now = time.time()
        total_bytes = sum(int(f["size_bytes"]) for f in file_records)
        file_count = len(file_records)

        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO buckets (bucket_id, bucket_name, bucket_type, file_count, total_bytes, last_scanned_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(bucket_id) DO UPDATE SET
                    bucket_name=excluded.bucket_name,
                    bucket_type=excluded.bucket_type,
                    file_count=excluded.file_count,
                    total_bytes=excluded.total_bytes,
                    last_scanned_at=excluded.last_scanned_at
                """,
                (bucket_id, bucket_name, bucket_type, file_count, total_bytes, now),
            )

            conn.executemany(
                """
                INSERT INTO files (
                    b2_file_id, bucket_id, bucket_name, file_name, basename, top_prefix,
                    size_bytes, b2_sha1, content_type, upload_timestamp, status, updated_at
                ) VALUES (
                    :b2_file_id, :bucket_id, :bucket_name, :file_name, :basename, :top_prefix,
                    :size_bytes, :b2_sha1, :content_type, :upload_timestamp, 'PENDING', :updated_at
                )
                ON CONFLICT(bucket_name, file_name) DO UPDATE SET
                    b2_file_id=excluded.b2_file_id,
                    size_bytes=excluded.size_bytes,
                    b2_sha1=excluded.b2_sha1,
                    content_type=excluded.content_type,
                    upload_timestamp=excluded.upload_timestamp,
                    updated_at=excluded.updated_at
                """,
                [{**r, "updated_at": now} for r in file_records],
            )
        return file_count, total_bytes

    def get_bucket_summaries(self) -> List[Dict[str, Any]]:
        """Return per-bucket file counts and byte progress across statuses."""
        with self._lock, self._connect() as conn:
            buckets = conn.execute("SELECT * FROM buckets ORDER BY bucket_name").fetchall()
            stats_rows = conn.execute(
                """
                SELECT
                    bucket_name,
                    status,
                    COUNT(*) AS cnt,
                    COALESCE(SUM(size_bytes), 0) AS bytes_sum,
                    COALESCE(SUM(bytes_transferred), 0) AS transferred_sum
                FROM files
                GROUP BY bucket_name, status
                """
            ).fetchall()

        by_bucket: Dict[str, Dict[str, Any]] = {}
        for b in buckets:
            bname = b["bucket_name"]
            by_bucket[bname] = {
                "bucket_id": b["bucket_id"],
                "bucket_name": bname,
                "bucket_type": b["bucket_type"],
                "total_files": b["file_count"],
                "total_bytes": b["total_bytes"],
                "last_scanned_at": b["last_scanned_at"],
                "completed_files": 0,
                "completed_bytes": 0,
                "skipped_files": 0,
                "skipped_bytes": 0,
                "failed_files": 0,
                "failed_bytes": 0,
                "pending_files": 0,
                "pending_bytes": 0,
                "in_progress_files": 0,
                "in_progress_transferred_bytes": 0,
            }

        for r in stats_rows:
            bname = r["bucket_name"]
            if bname not in by_bucket:
                continue
            st = r["status"]
            cnt = int(r["cnt"])
            bsum = int(r["bytes_sum"])
            tsum = int(r["transferred_sum"])
            entry = by_bucket[bname]
            if st == "COMPLETED":
                entry["completed_files"] += cnt
                entry["completed_bytes"] += bsum
            elif st in ("SKIPPED_DEDUP", "SKIPPED_EXISTS"):
                entry["skipped_files"] += cnt
                entry["skipped_bytes"] += bsum
            elif st == "FAILED":
                entry["failed_files"] += cnt
                entry["failed_bytes"] += bsum
            elif st == "IN_PROGRESS":
                entry["in_progress_files"] += cnt
                entry["in_progress_transferred_bytes"] += tsum
                entry["pending_bytes"] += max(0, bsum - tsum)
            else:
                entry["pending_files"] += cnt
                entry["pending_bytes"] += bsum

        for entry in by_bucket.values():
            done_bytes = (
                entry["completed_bytes"]
                + entry["skipped_bytes"]
                + entry["in_progress_transferred_bytes"]
            )
            total = entry["total_bytes"]
            entry["done_bytes"] = done_bytes
            entry["done_files"] = entry["completed_files"] + entry["skipped_files"]
            entry["progress_pct"] = round((done_bytes / total) * 100.0, 2) if total > 0 else (
                100.0 if entry["total_files"] == 0 else 0.0
            )
        return list(by_bucket.values())

    def claim_next_file(
        self,
        bucket_name: str,
        prefix_filter: Optional[str] = None,
        include_failed: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """Atomically claim the next pending file in a bucket (smallest metadata/files or natural order)."""
        allowed_statuses = ("PENDING", "FAILED") if include_failed else ("PENDING",)
        placeholders = ",".join("?" for _ in allowed_statuses)
        params: List[Any] = [bucket_name, *allowed_statuses]
        prefix_clause = ""
        if prefix_filter:
            prefix_clause = " AND file_name LIKE ?"
            params.append(f"{prefix_filter.rstrip('/')}/%")

        with self._lock, self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT * FROM files
                WHERE bucket_name = ?
                  AND status IN ({placeholders})
                  {prefix_clause}
                ORDER BY
                  CASE WHEN resumable_uri IS NOT NULL AND resumable_uri != '' THEN 0 ELSE 1 END,
                  file_name ASC
                LIMIT 1
                """,
                params,
            ).fetchone()
            if not row:
                return None
            conn.execute(
                "UPDATE files SET status = 'IN_PROGRESS', updated_at = ? WHERE b2_file_id = ?",
                (time.time(), row["b2_file_id"]),
            )
            return dict(row)

    def find_completed_duplicate(
        self,
        exclude_bucket: str,
        b2_sha1: Optional[str],
        basename: str,
        size_bytes: int,
        target_account: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Check if an identical clip was already migrated to the SAME target Google account."""
        acct_clause = " AND target_account = ?" if target_account else ""
        with self._lock, self._connect() as conn:
            if b2_sha1:
                params_sha: List[Any] = [exclude_bucket, b2_sha1, size_bytes]
                if target_account:
                    params_sha.append(target_account)
                row = conn.execute(
                    f"""
                    SELECT bucket_name, file_name, dest_rel_path, drive_file_id, verified_sha1
                    FROM files
                    WHERE bucket_name != ?
                      AND status IN ('COMPLETED', 'SKIPPED_EXISTS')
                      AND b2_sha1 = ?
                      AND size_bytes = ?
                      {acct_clause}
                    LIMIT 1
                    """,
                    params_sha,
                ).fetchone()
                if row:
                    return dict(row)

            params_name: List[Any] = [exclude_bucket, basename, size_bytes]
            if target_account:
                params_name.append(target_account)
            row = conn.execute(
                f"""
                SELECT bucket_name, file_name, dest_rel_path, drive_file_id, verified_sha1
                FROM files
                WHERE bucket_name != ?
                  AND status IN ('COMPLETED', 'SKIPPED_EXISTS')
                  AND basename = ?
                  AND size_bytes = ?
                  {acct_clause}
                LIMIT 1
                """,
                params_name,
            ).fetchone()
            return dict(row) if row else None

    def save_resumable_session(
        self,
        b2_file_id: str,
        resumable_uri: Optional[str],
        resumable_dest: Optional[str],
        bytes_transferred: int = 0,
    ) -> None:
        now = time.time()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE files
                SET resumable_uri = ?,
                    resumable_dest = ?,
                    resumable_updated_at = ?,
                    bytes_transferred = ?,
                    updated_at = ?
                WHERE b2_file_id = ?
                """,
                (resumable_uri, resumable_dest, now, bytes_transferred, now, b2_file_id),
            )

    def update_file_progress(self, b2_file_id: str, bytes_transferred: int) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE files SET bytes_transferred = ?, updated_at = ? WHERE b2_file_id = ?",
                (bytes_transferred, time.time(), b2_file_id),
            )

    def mark_file_completed(
        self,
        b2_file_id: str,
        transfer_mode: str,
        dest_rel_path: str,
        size_bytes: int,
        verified_sha1: Optional[str] = None,
        drive_file_id: Optional[str] = None,
        status: str = "COMPLETED",
        target_account: Optional[str] = None,
    ) -> None:
        now = time.time()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE files
                SET status = ?,
                    transfer_mode = ?,
                    target_account = ?,
                    dest_rel_path = ?,
                    drive_file_id = ?,
                    verified_sha1 = ?,
                    bytes_transferred = ?,
                    resumable_uri = NULL,
                    last_error = NULL,
                    updated_at = ?,
                    completed_at = ?
                WHERE b2_file_id = ?
                """,
                (
                    status,
                    transfer_mode,
                    target_account,
                    dest_rel_path,
                    drive_file_id,
                    verified_sha1,
                    size_bytes,
                    now,
                    now,
                    b2_file_id,
                ),
            )

    def reset_bucket_progress(self, bucket_name: Optional[str] = None) -> int:
        """Reset completed/skipped/failed files back to PENDING (e.g. to re-migrate to a new Google Drive account)."""
        now = time.time()
        with self._lock, self._connect() as conn:
            if bucket_name:
                cur = conn.execute(
                    """
                    UPDATE files
                    SET status = 'PENDING',
                        transfer_mode = NULL,
                        target_account = NULL,
                        drive_file_id = NULL,
                        verified_sha1 = NULL,
                        bytes_transferred = 0,
                        resumable_uri = NULL,
                        resumable_dest = NULL,
                        last_error = NULL,
                        completed_at = NULL,
                        updated_at = ?
                    WHERE bucket_name = ?
                    """,
                    (now, bucket_name),
                )
            else:
                cur = conn.execute(
                    """
                    UPDATE files
                    SET status = 'PENDING',
                        transfer_mode = NULL,
                        target_account = NULL,
                        drive_file_id = NULL,
                        verified_sha1 = NULL,
                        bytes_transferred = 0,
                        resumable_uri = NULL,
                        resumable_dest = NULL,
                        last_error = NULL,
                        completed_at = NULL,
                        updated_at = ?
                    """,
                    (now,),
                )
            return cur.rowcount

    def mark_file_pending_paused(self, b2_file_id: str, bytes_transferred: int, reason: str = "") -> None:
        now = time.time()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE files
                SET status = 'PENDING',
                    bytes_transferred = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE b2_file_id = ?
                """,
                (bytes_transferred, reason or None, now, b2_file_id),
            )

    def mark_file_failed(self, b2_file_id: str, error_msg: str) -> None:
        now = time.time()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE files
                SET status = 'FAILED',
                    retry_count = retry_count + 1,
                    last_error = ?,
                    updated_at = ?
                WHERE b2_file_id = ?
                """,
                (error_msg[:1000], now, b2_file_id),
            )

    def reset_failed_files(self, bucket_name: Optional[str] = None) -> int:
        now = time.time()
        with self._lock, self._connect() as conn:
            if bucket_name:
                cur = conn.execute(
                    "UPDATE files SET status = 'PENDING', last_error = NULL, updated_at = ? WHERE bucket_name = ? AND status = 'FAILED'",
                    (now, bucket_name),
                )
            else:
                cur = conn.execute(
                    "UPDATE files SET status = 'PENDING', last_error = NULL, updated_at = ? WHERE status = 'FAILED'",
                    (now,),
                )
            return cur.rowcount

    def record_uploaded_bytes(self, bucket_name: str, bytes_count: int, mode: str) -> None:
        if bytes_count <= 0:
            return
        now = time.time()
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT INTO byte_ledger (ts, bucket_name, bytes_count, mode) VALUES (?, ?, ?, ?)",
                (now, bucket_name, int(bytes_count), mode),
            )
            # Prune entries older than 7 days
            conn.execute("DELETE FROM byte_ledger WHERE ts < ?", (now - 7 * 86400,))

    def get_rolling_24h_bytes(self) -> int:
        cutoff = time.time() - 86400.0
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT COALESCE(SUM(bytes_count), 0) AS total FROM byte_ledger WHERE ts >= ?",
                (cutoff,),
            ).fetchone()
            return int(row["total"]) if row else 0

    def get_cached_drive_folder(self, folder_key: str) -> Optional[str]:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT drive_folder_id FROM drive_folders WHERE folder_key = ?",
                (folder_key,),
            ).fetchone()
            return str(row["drive_folder_id"]) if row else None

    def set_cached_drive_folder(self, folder_key: str, drive_folder_id: str) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO drive_folders (folder_key, drive_folder_id, created_at)
                VALUES (?, ?, ?)
                ON CONFLICT(folder_key) DO UPDATE SET drive_folder_id=excluded.drive_folder_id
                """,
                (folder_key, drive_folder_id, time.time()),
            )

    def clear_cached_drive_folder(self, folder_key_prefix: str = "") -> None:
        with self._lock, self._connect() as conn:
            if folder_key_prefix:
                conn.execute("DELETE FROM drive_folders WHERE folder_key LIKE ?", (f"{folder_key_prefix}%",))
            else:
                conn.execute("DELETE FROM drive_folders")

    def list_bucket_files(
        self,
        bucket_name: str,
        status_filter: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        with self._lock, self._connect() as conn:
            if status_filter:
                rows = conn.execute(
                    """
                    SELECT file_name, size_bytes, b2_sha1, status, transfer_mode,
                           bytes_transferred, verified_sha1, drive_file_id, last_error, updated_at
                    FROM files
                    WHERE bucket_name = ? AND status = ?
                    ORDER BY updated_at DESC, file_name ASC
                    LIMIT ?
                    """,
                    (bucket_name, status_filter, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT file_name, size_bytes, b2_sha1, status, transfer_mode,
                           bytes_transferred, verified_sha1, drive_file_id, last_error, updated_at
                    FROM files
                    WHERE bucket_name = ?
                    ORDER BY
                        CASE status
                            WHEN 'IN_PROGRESS' THEN 0
                            WHEN 'FAILED' THEN 1
                            WHEN 'COMPLETED' THEN 2
                            WHEN 'SKIPPED_DEDUP' THEN 3
                            WHEN 'SKIPPED_EXISTS' THEN 4
                            ELSE 5
                        END,
                        updated_at DESC,
                        file_name ASC
                    LIMIT ?
                    """,
                    (bucket_name, limit),
                ).fetchall()
            return [dict(r) for r in rows]
