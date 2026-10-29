"""Migration Engine coordinating catalog scans, workers, telemetry, deduplication, and quota limits."""

from __future__ import annotations

import collections
import ctypes
import os
import threading
import time
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

from .b2_client import B2Client
from .config import (
    RECOMMENDED_BUCKET_ORDER,
    MigrationSettings,
    check_b2_credentials_status,
    compute_relative_dest_path,
    save_b2_credentials,
)
from .db import StateDB
from .gdrive_auth import GDriveAuthManager
from .gdrive_cloud import GDriveCloudStreamer, QuotaExceededError, TransferPausedError
from .gdrive_mounted import (
    GDriveMountedStreamer,
    get_c_drive_free_bytes,
    get_mounted_drive_base,
)


class MigrationEngine:
    """Thread-safe coordinator powering both the Web Dashboard and CLI."""

    def __init__(
        self,
        settings: Optional[MigrationSettings] = None,
        db: Optional[StateDB] = None,
    ) -> None:
        self.settings = settings or MigrationSettings.load()
        self.db = db or StateDB()
        self.b2 = B2Client(self.settings.b2_key_file)
        self.auth = GDriveAuthManager(on_account_connected=self._on_account_connected)
        self.cloud_streamer = GDriveCloudStreamer(self.auth, self.db, self.b2)
        self.mounted_streamer = GDriveMountedStreamer(self.b2)

        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._state: str = "IDLE"
        # IDLE | SCANNING | RUNNING | PAUSING | PAUSED | QUOTA_PAUSED | CACHE_WAIT | COMPLETED_BUCKET | ERROR
        self._state_detail: str = "Ready"
        self._workers: List[threading.Thread] = []
        self._scan_thread: Optional[threading.Thread] = None
        self._quota_timer_thread: Optional[threading.Thread] = None
        self._active_files: Dict[str, Dict[str, Any]] = {}
        self._speed_window: Deque[Tuple[float, int]] = collections.deque()
        self._session_uploaded_bytes: int = 0
        self._session_started_at: Optional[float] = None
        self._prefix_filter: Optional[str] = None

    def _on_account_connected(self, email: Optional[str]) -> None:
        self.db.clear_cached_drive_folder()
        self.update_settings({"transfer_mode": "cloud", "shared_drive_id": ""})
        who = email or "Google Account"
        self.db.log_event(
            "SUCCESS",
            f"Connected Google Drive account ({who}) — switched to Direct Cloud Stream mode!",
        )

    def get_current_target_account_key(self) -> str:
        if self.settings.transfer_mode == "cloud":
            return f"cloud:{self.auth.get_account_fingerprint()}"
        return f"mounted:{self.settings.mounted_drive_root}"

    @staticmethod
    def _set_system_keep_awake(keep_awake: bool) -> None:
        """Prevent Windows PC from idle-sleeping while an active migration is running."""
        if os.name != "nt":
            return
        try:
            es_continuous = 0x80000000
            es_system_required = 0x00000001
            flags = (es_continuous | es_system_required) if keep_awake else es_continuous
            ctypes.windll.kernel32.SetThreadExecutionState(flags)
        except Exception:
            pass

    def configure_b2_credentials(self, key_id: str, application_key: str) -> Dict[str, Any]:
        save_b2_credentials(key_id, application_key)
        self.b2 = B2Client(self.settings.b2_key_file)
        self.cloud_streamer = GDriveCloudStreamer(self.auth, self.db, self.b2)
        self.mounted_streamer = GDriveMountedStreamer(self.b2)
        auth_info = self.b2.authorize(force=True)
        self.db.log_event("SUCCESS", f"Configured Backblaze B2 key (Account ID: {auth_info.get('accountId')})")
        self.start_background_scan()
        return {"ok": True, "account_id": auth_info.get("accountId")}

    # ------------------------------------------------------------------
    # Settings management
    # ------------------------------------------------------------------
    def update_settings(self, updates: Dict[str, Any]) -> MigrationSettings:
        with self._lock:
            for k, v in updates.items():
                if hasattr(self.settings, k):
                    field_type = type(getattr(self.settings, k))
                    if field_type is bool and not isinstance(v, bool):
                        v = str(v).lower() in ("1", "true", "yes", "on")
                    elif field_type is int:
                        v = int(v)
                    elif field_type is float:
                        v = float(v)
                    elif field_type is str:
                        v = str(v)
                    setattr(self.settings, k, v)
            self.settings.max_workers = max(1, min(6, int(self.settings.max_workers)))
            self.settings.chunk_size_mib = max(4, min(64, int(self.settings.chunk_size_mib)))
            # Snap chunk_size_mib to whole MiB (always multiple of 256 KiB)
            self.settings.save()
            if "b2_key_file" in updates:
                self.b2 = B2Client(self.settings.b2_key_file)
                self.cloud_streamer = GDriveCloudStreamer(self.auth, self.db, self.b2)
                self.mounted_streamer = GDriveMountedStreamer(self.b2)
            return self.settings

    # ------------------------------------------------------------------
    # Backblaze B2 Bucket Scanning
    # ------------------------------------------------------------------
    def start_background_scan(self, bucket_name: Optional[str] = None) -> bool:
        with self._lock:
            if self._state in ("SCANNING", "RUNNING", "PAUSING", "CACHE_WAIT"):
                return False
            self._state = "SCANNING"
            self._state_detail = f"Scanning Backblaze B2 ({bucket_name or 'all buckets'})..."
            t = threading.Thread(
                target=self._run_scan_safe,
                args=(bucket_name,),
                daemon=True,
                name="b2-scanner",
            )
            self._scan_thread = t
            t.start()
            return True

    def _run_scan_safe(self, bucket_name: Optional[str] = None) -> None:
        try:
            self.scan_buckets_sync(bucket_name=bucket_name)
            with self._lock:
                if self._state == "SCANNING":
                    self._state = "IDLE"
                    self._state_detail = "Catalog up to date"
        except Exception as exc:
            self.db.log_event("ERROR", f"B2 catalog scan failed: {exc}", bucket_name)
            with self._lock:
                self._state = "ERROR"
                self._state_detail = f"Scan error: {exc}"

    def scan_buckets_sync(
        self,
        bucket_name: Optional[str] = None,
        on_progress: Optional[Callable[[str, int, int], None]] = None,
    ) -> List[Dict[str, Any]]:
        buckets = self.b2.list_buckets()
        results = []
        for b in buckets:
            bname = b["bucketName"]
            bid = b["bucketId"]
            btype = b.get("bucketType", "")
            if bucket_name and bname != bucket_name:
                continue
            if btype == "snapshot" and not bucket_name:
                # Still register empty/snapshot bucket quickly
                pass

            with self._lock:
                if self._state == "SCANNING":
                    self._state_detail = f"Scanning {bname}..."

            def _page_cb(cnt: int, sz: int, _bn: str = bname) -> None:
                with self._lock:
                    if self._state == "SCANNING":
                        self._state_detail = f"Scanning {_bn}: {cnt:,} files ({sz / (1024**3):.1f} GiB)..."
                if on_progress:
                    on_progress(_bn, cnt, sz)

            records = self.b2.scan_bucket_files(bid, bname, on_page=_page_cb)
            f_cnt, f_bytes = self.db.upsert_bucket_catalog(bid, bname, btype, records)
            self.db.log_event(
                "INFO",
                f"Scanned bucket '{bname}': {f_cnt:,} files ({f_bytes / (1024**3):.2f} GiB)",
                bname,
            )
            results.append({"bucket_name": bname, "files": f_cnt, "bytes": f_bytes})
        return results

    # ------------------------------------------------------------------
    # Ordered Bucket Summaries & Dry-Run Plan Preview
    # ------------------------------------------------------------------
    def get_ordered_buckets(self) -> List[Dict[str, Any]]:
        summaries = self.db.get_bucket_summaries()
        order_idx = {name: i for i, name in enumerate(RECOMMENDED_BUCKET_ORDER)}
        summaries.sort(
            key=lambda x: (
                0 if x["total_files"] > 0 else 1,
                order_idx.get(x["bucket_name"], 99),
                x["bucket_name"],
            )
        )
        for idx, s in enumerate(summaries):
            if s["bucket_name"] in order_idx:
                s["step_number"] = order_idx[s["bucket_name"]] + 1
            else:
                s["step_number"] = None
        return summaries

    def preview_bucket_plan(self, bucket_name: Optional[str] = None) -> Dict[str, Any]:
        target_bucket = bucket_name or self.settings.active_bucket
        summaries = {b["bucket_name"]: b for b in self.db.get_bucket_summaries()}
        b_info = summaries.get(target_bucket)
        if not b_info:
            return {"bucket_name": target_bucket, "scanned": False}

        # Sample pending files & calculate cross-bucket duplicate savings against ALREADY completed OR archive catalog
        with self.db._lock, self.db._connect() as conn:
            pending_rows = conn.execute(
                """
                SELECT file_name, basename, size_bytes, b2_sha1, status
                FROM files
                WHERE bucket_name = ?
                ORDER BY file_name ASC
                """,
                (target_bucket,),
            ).fetchall()

            completed_other_sha1 = set()
            completed_other_name_size = set()
            catalog_archive_sha1 = set()
            catalog_archive_name_size = set()
            cur_acct = self.get_current_target_account_key()
            if self.settings.skip_cross_bucket_duplicates:
                for r in conn.execute(
                    """
                    SELECT b2_sha1, basename, size_bytes
                    FROM files
                    WHERE bucket_name != ?
                      AND status IN ('COMPLETED', 'SKIPPED_EXISTS')
                      AND target_account = ?
                    """,
                    (target_bucket, cur_acct),
                ):
                    if r["b2_sha1"]:
                        completed_other_sha1.add((r["b2_sha1"], int(r["size_bytes"])))
                    completed_other_name_size.add((r["basename"], int(r["size_bytes"])))

                if target_bucket == "fourvectors-16-sep-27":
                    for r in conn.execute(
                        "SELECT b2_sha1, basename, size_bytes FROM files WHERE bucket_name = 'fourvectors-archive'"
                    ):
                        if r["b2_sha1"]:
                            catalog_archive_sha1.add((r["b2_sha1"], int(r["size_bytes"])))
                        catalog_archive_name_size.add((r["basename"], int(r["size_bytes"])))

        would_dedup_now_files = 0
        would_dedup_now_bytes = 0
        archive_overlap_files = 0
        archive_overlap_bytes = 0
        net_transfer_files = 0
        net_transfer_bytes = 0
        sample_mappings = []

        for r in pending_rows:
            sz = int(r["size_bytes"])
            rel_dest = compute_relative_dest_path(
                bucket_name=target_bucket,
                b2_file_name=r["file_name"],
                include_bucket_folder=self.settings.include_bucket_folder,
                normalize_categories=self.settings.normalize_categories,
            )
            if len(sample_mappings) < 8:
                sample_mappings.append(
                    {
                        "source": f"{target_bucket}/{r['file_name']}",
                        "destination": f"{self.settings.dest_root_folder}/{rel_dest}",
                        "size_bytes": sz,
                        "status": r["status"],
                    }
                )

            if r["status"] in ("COMPLETED", "SKIPPED_DEDUP", "SKIPPED_EXISTS"):
                continue

            # Mirrors StateDB.find_completed_duplicate: SHA-1 + size when B2 has a SHA-1, basename + size otherwise.
            is_done_dup = self.settings.skip_cross_bucket_duplicates and (
                (r["b2_sha1"] and (r["b2_sha1"], sz) in completed_other_sha1)
                or (not r["b2_sha1"] and (r["basename"], sz) in completed_other_name_size)
            )
            if is_done_dup:
                would_dedup_now_files += 1
                would_dedup_now_bytes += sz
            else:
                net_transfer_files += 1
                net_transfer_bytes += sz

            if (r["b2_sha1"] and (r["b2_sha1"], sz) in catalog_archive_sha1) or (
                not r["b2_sha1"] and (r["basename"], sz) in catalog_archive_name_size
            ):
                archive_overlap_files += 1
                archive_overlap_bytes += sz

        daily_cap_bytes = max(1.0, self.settings.daily_upload_limit_gib) * (1024 ** 3)
        est_days_quota = round(net_transfer_bytes / daily_cap_bytes, 1) if net_transfer_bytes > 0 else 0.0

        return {
            "bucket_name": target_bucket,
            "scanned": True,
            "summary": b_info,
            "net_transfer_files": net_transfer_files,
            "net_transfer_bytes": net_transfer_bytes,
            "would_dedup_now_files": would_dedup_now_files,
            "would_dedup_now_bytes": would_dedup_now_bytes,
            "archive_overlap_files": archive_overlap_files,
            "archive_overlap_bytes": archive_overlap_bytes,
            "estimated_min_days_at_750gb_cap": est_days_quota,
            "sample_mappings": sample_mappings,
        }

    # ------------------------------------------------------------------
    # Transfer Control (Start / Pause / Retry Failed)
    # ------------------------------------------------------------------
    def start_migration(
        self,
        bucket_name: Optional[str] = None,
        prefix_filter: Optional[str] = None,
        retry_failed: bool = True,
    ) -> Tuple[bool, str]:
        with self._lock:
            if self._state in ("RUNNING", "SCANNING", "PAUSING", "CACHE_WAIT"):
                return False, f"Engine is currently {self._state}"
            if any(w.is_alive() for w in self._workers):
                return False, "Previous workers are still finishing their current chunk; try again in a moment"

            if bucket_name:
                self.settings.active_bucket = bucket_name
                self.settings.save()

            target_bucket = self.settings.active_bucket
            mode = self.settings.transfer_mode

            # Preflight checks before launching workers
            if mode == "cloud":
                if not self.auth.is_connected():
                    return (
                        False,
                        "Direct Cloud Streaming requires Google OAuth sign-in first (click '1-Click Sign In with Google' or switch to Mounted G: mode).",
                    )
                if self.settings.drive_location == "shared_drive":
                    try:
                        sd_id = self.cloud_streamer.resolve_shared_drive_id(
                            self.settings.shared_drive_name,
                            self.settings.shared_drive_id,
                        )
                        self.settings.shared_drive_id = sd_id
                        self.settings.save()
                    except Exception as exc:
                        return False, str(exc)
            else:
                ok, detail = self.mounted_streamer.check_mounted_available(
                    self.settings.mounted_drive_root,
                    self.settings.drive_location,
                    self.settings.shared_drive_name,
                )
                if not ok:
                    return False, detail

            # No worker of this engine is alive here, so any IN_PROGRESS row is left over from a crash or hard stop.
            stale_cnt = self.db.reset_stale_in_progress()
            if stale_cnt:
                self.db.log_event("INFO", f"Re-queued {stale_cnt} interrupted file(s) from a previous run", target_bucket)

            if retry_failed:
                reset_cnt = self.db.reset_failed_files(target_bucket)
                if reset_cnt:
                    self.db.log_event("INFO", f"Re-queued {reset_cnt} previously failed files", target_bucket)

            self._stop_event.clear()
            self._prefix_filter = prefix_filter
            self._state = "RUNNING"
            self._state_detail = f"Migrating {target_bucket} via {'Cloud API Stream' if mode == 'cloud' else 'Mounted G: Drive'}"
            self._session_started_at = time.time()
            self._session_uploaded_bytes = 0
            self._speed_window.clear()
            self._active_files.clear()

            num_workers = max(1, int(self.settings.max_workers))
            if mode == "mounted":
                # Keep mounted writes at 1-2 sequential/parallel streams so DriveFS cache flushes smoothly
                num_workers = min(num_workers, 2)

            self._workers = []
            self._set_system_keep_awake(True)
            for idx in range(num_workers):
                t = threading.Thread(
                    target=self._worker_loop,
                    args=(idx + 1, target_bucket, prefix_filter),
                    daemon=True,
                    name=f"migrator-worker-{idx + 1}",
                )
                self._workers.append(t)
                t.start()

            self.db.log_event(
                "INFO",
                f"Started migration for '{target_bucket}' ({num_workers} worker(s), mode={mode})",
                target_bucket,
            )
            return True, f"Started migrating {target_bucket}"

    def pause_migration(self, reason: str = "Paused by user") -> None:
        with self._lock:
            if self._state not in ("RUNNING", "CACHE_WAIT", "QUOTA_PAUSED"):
                return
            self._state = "PAUSING"
            self._state_detail = "Saving chunk checkpoints and pausing..."
            self._stop_event.set()
        self.db.log_event("INFO", reason, self.settings.active_bucket)

    def wait_until_done(self) -> str:
        """Block until active workers finish (used by CLI)."""
        while True:
            with self._lock:
                alive = [w for w in self._workers if w.is_alive()]
                st = self._state
            if not alive and st != "SCANNING":
                return st
            time.sleep(0.5)

    # ------------------------------------------------------------------
    # Worker Loop
    # ------------------------------------------------------------------
    def _record_speed_delta(self, byte_delta: int, bucket_name: str, mode: str) -> None:
        now = time.time()
        with self._lock:
            self._session_uploaded_bytes += byte_delta
            self._speed_window.append((now, byte_delta))
            cutoff = now - 15.0
            while self._speed_window and self._speed_window[0][0] < cutoff:
                self._speed_window.popleft()
        self.db.record_uploaded_bytes(bucket_name, byte_delta, mode)

    def get_current_speed_bps(self) -> float:
        now = time.time()
        with self._lock:
            cutoff = now - 15.0
            while self._speed_window and self._speed_window[0][0] < cutoff:
                self._speed_window.popleft()
            if not self._speed_window:
                return 0.0
            earliest = self._speed_window[0][0]
            span = max(1.0, now - earliest)
            total = sum(b for _, b in self._speed_window)
            return total / span

    def _check_daily_quota_reached(self) -> bool:
        limit_bytes = int(max(1.0, float(self.settings.daily_upload_limit_gib)) * (1024 ** 3))
        used_24h = self.db.get_rolling_24h_bytes()
        return used_24h >= limit_bytes

    def _worker_loop(
        self,
        worker_id: int,
        bucket_name: str,
        prefix_filter: Optional[str],
    ) -> None:
        worker_key = f"W{worker_id}"
        try:
            while not self._stop_event.is_set():
                if self._check_daily_quota_reached():
                    self._trigger_quota_pause(
                        f"Daily upload limit ({self.settings.daily_upload_limit_gib:.0f} GiB / 24h) reached; auto-paused safely."
                    )
                    break

                file_rec = self.db.claim_next_file(bucket_name=bucket_name, prefix_filter=prefix_filter)
                if file_rec is None:
                    break

                b2_file_id = file_rec["b2_file_id"]
                fname = file_rec["file_name"]
                fsize = int(file_rec["size_bytes"])
                mode = self.settings.transfer_mode

                dest_rel_path = compute_relative_dest_path(
                    bucket_name=bucket_name,
                    b2_file_name=fname,
                    include_bucket_folder=self.settings.include_bucket_folder,
                    normalize_categories=self.settings.normalize_categories,
                )

                # Cross-bucket deduplication check (strictly scoped to the SAME target Google account)
                target_acct = self.get_current_target_account_key()
                if self.settings.skip_cross_bucket_duplicates:
                    dup = self.db.find_completed_duplicate(
                        exclude_bucket=bucket_name,
                        b2_sha1=file_rec.get("b2_sha1"),
                        basename=file_rec["basename"],
                        size_bytes=fsize,
                        target_account=target_acct,
                    )
                    if dup:
                        self.db.mark_file_completed(
                            b2_file_id=b2_file_id,
                            transfer_mode=mode,
                            dest_rel_path=dest_rel_path,
                            size_bytes=fsize,
                            verified_sha1=dup.get("verified_sha1") or file_rec.get("b2_sha1"),
                            drive_file_id=dup.get("drive_file_id"),
                            status="SKIPPED_DEDUP",
                            target_account=target_acct,
                        )
                        self.db.log_event(
                            "INFO",
                            f"Skipped duplicate '{fname}' (already migrated from {dup['bucket_name']})",
                            bucket_name,
                        )
                        continue

                with self._lock:
                    self._active_files[worker_key] = {
                        "worker": worker_key,
                        "file_name": fname,
                        "size_bytes": fsize,
                        "bytes_done": int(file_rec.get("bytes_transferred") or 0),
                        "started_at": time.time(),
                    }

                def _on_chunk(delta: int, offset_now: int, _wk: str = worker_key) -> None:
                    with self._lock:
                        if _wk in self._active_files:
                            self._active_files[_wk]["bytes_done"] = offset_now
                        if self._state == "CACHE_WAIT":
                            self._state = "RUNNING"
                            self._state_detail = f"Migrating {bucket_name}"
                    self._record_speed_delta(delta, bucket_name, mode)

                def _on_cache_wait(free_gib: float, min_gib: float) -> None:
                    with self._lock:
                        if self._state == "RUNNING":
                            self._state = "CACHE_WAIT"
                        self._state_detail = (
                            f"Waiting for Google Drive Desktop cache on C: to flush "
                            f"(C: free {free_gib:.1f} GiB < {min_gib:.1f} GiB guard)..."
                        )

                def _should_stop() -> bool:
                    return self._stop_event.is_set() or self._check_daily_quota_reached()

                try:
                    if mode == "cloud":
                        res = self.cloud_streamer.stream_file(
                            file_rec=file_rec,
                            dest_rel_path=dest_rel_path,
                            dest_root_folder=self.settings.dest_root_folder,
                            drive_location=self.settings.drive_location,
                            shared_drive_id=self.settings.shared_drive_id,
                            chunk_size_mib=self.settings.chunk_size_mib,
                            should_stop=_should_stop,
                            on_chunk_uploaded=_on_chunk,
                        )
                    else:
                        res = self.mounted_streamer.stream_file(
                            file_rec=file_rec,
                            dest_rel_path=dest_rel_path,
                            mounted_root=self.settings.mounted_drive_root,
                            drive_location=self.settings.drive_location,
                            shared_drive_name=self.settings.shared_drive_name,
                            dest_root_folder=self.settings.dest_root_folder,
                            min_c_free_gib=self.settings.min_c_free_gib,
                            chunk_size_mib=self.settings.chunk_size_mib,
                            should_stop=_should_stop,
                            on_chunk_written=_on_chunk,
                            on_cache_wait=_on_cache_wait,
                        )

                    final_status = res["status"]
                    self.db.mark_file_completed(
                        b2_file_id=b2_file_id,
                        transfer_mode=mode,
                        dest_rel_path=dest_rel_path,
                        size_bytes=fsize,
                        verified_sha1=res.get("verified_sha1"),
                        drive_file_id=res.get("drive_file_id"),
                        status=final_status,
                        target_account=target_acct,
                    )
                    tag = "Verified SHA-1" if final_status == "COMPLETED" else "Already exists on Drive"
                    self.db.log_event(
                        "SUCCESS",
                        f"[{tag}] {fname} ({fsize / (1024**2):.2f} MiB)",
                        bucket_name,
                    )

                except TransferPausedError:
                    cur_done = 0
                    with self._lock:
                        if worker_key in self._active_files:
                            cur_done = int(self._active_files[worker_key]["bytes_done"])
                    if self._check_daily_quota_reached() and not self._stop_event.is_set():
                        self.db.mark_file_pending_paused(b2_file_id, cur_done, "Paused at daily 750 GB limit")
                        self._trigger_quota_pause(
                            f"Daily upload limit ({self.settings.daily_upload_limit_gib:.0f} GiB / 24h) reached; checkpoint saved."
                        )
                    else:
                        self.db.mark_file_pending_paused(b2_file_id, cur_done, "Paused by user")
                    break

                except QuotaExceededError as qexc:
                    cur_done = 0
                    with self._lock:
                        if worker_key in self._active_files:
                            cur_done = int(self._active_files[worker_key]["bytes_done"])
                    self.db.mark_file_pending_paused(b2_file_id, cur_done, str(qexc))
                    self._trigger_quota_pause(str(qexc))
                    break

                except Exception as exc:
                    self.db.mark_file_failed(b2_file_id, str(exc))
                    self.db.log_event("ERROR", f"Failed '{fname}': {exc}", bucket_name)

                finally:
                    with self._lock:
                        self._active_files.pop(worker_key, None)

        finally:
            self._on_worker_exit(bucket_name)

    def _trigger_quota_pause(self, detail: str) -> None:
        with self._lock:
            self._stop_event.set()
            self._state = "QUOTA_PAUSED"
            self._state_detail = detail
        self.db.log_event("WARN", detail, self.settings.active_bucket)

        # Spawn auto-resume watcher (checks rolling 24h window every 15 min)
        with self._lock:
            if self._quota_timer_thread is None or not self._quota_timer_thread.is_alive():
                t = threading.Thread(target=self._quota_auto_resume_loop, daemon=True, name="quota-watcher")
                self._quota_timer_thread = t
                t.start()

    def _quota_auto_resume_loop(self) -> None:
        while True:
            time.sleep(900)  # Check every 15 minutes
            with self._lock:
                if self._state != "QUOTA_PAUSED":
                    return
            if not self._check_daily_quota_reached():
                self.db.log_event("INFO", "24h quota window freed space; auto-resuming migration!", self.settings.active_bucket)
                with self._lock:
                    self._state = "IDLE"
                self.start_migration(retry_failed=False)
                return

    def _on_worker_exit(self, bucket_name: str) -> None:
        with self._lock:
            current = threading.current_thread()
            still_alive = [w for w in self._workers if w is not current and w.is_alive()]
            if still_alive:
                return

            self._active_files.clear()
            self._set_system_keep_awake(False)
            if self._state == "QUOTA_PAUSED":
                return
            if self._stop_event.is_set() or self._state == "PAUSING":
                self._state = "PAUSED"
                self._state_detail = f"Paused ({bucket_name}) — resumable checkpoints saved"
                return

            # Check if bucket is completely done
            summaries = {b["bucket_name"]: b for b in self.db.get_bucket_summaries()}
            b_sum = summaries.get(bucket_name)
            if b_sum and b_sum["pending_files"] == 0 and b_sum["failed_files"] == 0 and b_sum["total_files"] > 0:
                self._state = "COMPLETED_BUCKET"
                self._state_detail = (
                    f"Completed '{bucket_name}'! ({b_sum['done_files']:,}/{b_sum['total_files']:,} files verified)"
                )
                self.db.log_event(
                    "SUCCESS",
                    f"Bucket '{bucket_name}' migration finished ({b_sum['done_files']:,} files, "
                    f"{b_sum['done_bytes'] / (1024**3):.2f} GiB).",
                    bucket_name,
                )
            elif b_sum and b_sum["failed_files"] > 0:
                self._state = "ERROR"
                self._state_detail = f"Finished pass on '{bucket_name}' with {b_sum['failed_files']} failed file(s) — click Retry Failed"
            else:
                self._state = "IDLE"
                self._state_detail = "Ready"

    # ------------------------------------------------------------------
    # Full Live Status Snapshot for Web UI & CLI
    # ------------------------------------------------------------------
    def get_status_snapshot(self, include_cloud_quota: bool = False) -> Dict[str, Any]:
        buckets = self.get_ordered_buckets()
        active_bname = self.settings.active_bucket
        active_summary = next((b for b in buckets if b["bucket_name"] == active_bname), None)

        speed_bps = self.get_current_speed_bps()
        eta_seconds: Optional[int] = None
        if active_summary and speed_bps > 1024 and self._state == "RUNNING":
            rem = max(0, int(active_summary["total_bytes"]) - int(active_summary["done_bytes"]))
            eta_seconds = int(rem / speed_bps)

        c_free_bytes = get_c_drive_free_bytes()
        mounted_ok, mounted_path = self.mounted_streamer.check_mounted_available(
            self.settings.mounted_drive_root,
            self.settings.drive_location,
            self.settings.shared_drive_name,
        )
        mounted_dest_preview = str(
            get_mounted_drive_base(
                self.settings.mounted_drive_root,
                self.settings.drive_location,
                self.settings.shared_drive_name,
                self.settings.dest_root_folder,
            )
        )

        auth_status = self.auth.get_auth_status()
        cloud_info: Dict[str, Any] = {"connected": auth_status["connected"]}
        if include_cloud_quota and auth_status["connected"]:
            try:
                cloud_info = self.auth.get_cloud_drive_info(force=False)
            except Exception as exc:
                cloud_info = {"connected": True, "error": str(exc)}

        used_24h = self.db.get_rolling_24h_bytes()
        limit_24h = int(self.settings.daily_upload_limit_gib * (1024 ** 3))

        with self._lock:
            active_workers = list(self._active_files.values())
            state = self._state
            detail = self._state_detail

        b2_ok, b2_hint = check_b2_credentials_status(self.settings.b2_key_file)

        return {
            "state": state,
            "state_detail": detail,
            "b2": {"configured": b2_ok, "key_hint": b2_hint},
            "settings": {
                "transfer_mode": self.settings.transfer_mode,
                "active_bucket": self.settings.active_bucket,
                "drive_location": self.settings.drive_location,
                "shared_drive_name": self.settings.shared_drive_name,
                "shared_drive_id": self.settings.shared_drive_id,
                "dest_root_folder": self.settings.dest_root_folder,
                "mounted_drive_root": self.settings.mounted_drive_root,
                "include_bucket_folder": self.settings.include_bucket_folder,
                "normalize_categories": self.settings.normalize_categories,
                "skip_cross_bucket_duplicates": self.settings.skip_cross_bucket_duplicates,
                "min_c_free_gib": self.settings.min_c_free_gib,
                "daily_upload_limit_gib": self.settings.daily_upload_limit_gib,
                "chunk_size_mib": self.settings.chunk_size_mib,
                "max_workers": self.settings.max_workers,
                "b2_key_file": self.settings.b2_key_file,
            },
            "telemetry": {
                "speed_bps": speed_bps,
                "eta_seconds": eta_seconds,
                "session_uploaded_bytes": self._session_uploaded_bytes,
                "rolling_24h_bytes": used_24h,
                "daily_limit_bytes": limit_24h,
                "c_drive_free_bytes": c_free_bytes,
                "c_drive_free_gib": round(c_free_bytes / (1024 ** 3), 2),
                "mounted_available": mounted_ok,
                "mounted_path": mounted_path,
                "mounted_dest_preview": mounted_dest_preview,
            },
            "auth": auth_status,
            "cloud_drive": cloud_info,
            "buckets": buckets,
            "active_workers": active_workers,
            "events": self.db.get_recent_events(limit=60),
        }

