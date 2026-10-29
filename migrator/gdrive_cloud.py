"""Direct Cloud-to-Cloud RAM Streamer: Backblaze B2 HTTP Range -> Google Drive API v3 Resumable Upload."""

from __future__ import annotations

import hashlib
import http.client
import random
import threading
import time
from typing import Any, Callable, Dict, Optional, Tuple

import requests
import urllib3.exceptions

from .b2_client import B2Client, iter_exact_chunks
from .db import StateDB
from .gdrive_auth import GDriveAuthManager


class QuotaExceededError(Exception):
    """Raised when Google Drive daily 750 GB upload limit or cloud storage quota is reached."""


class TransferPausedError(Exception):
    """Raised when the user pauses or stops an active transfer."""


STREAM_READ_EXCEPTIONS = (
    requests.RequestException,
    urllib3.exceptions.HTTPError,
    http.client.HTTPException,
    OSError,
)


class GDriveCloudStreamer:
    """
    Streams files directly from Backblaze B2 HTTP Range streams to Google Drive v3 Resumable Uploads
    entirely in RAM (0 bytes written to C:), with 7-day resumable session persistence and SHA-1 verification.
    """

    DRIVE_API = "https://www.googleapis.com/drive/v3"
    DRIVE_UPLOAD_API = "https://www.googleapis.com/upload/drive/v3"
    FOLDER_MIME = "application/vnd.google-apps.folder"

    def __init__(self, auth: GDriveAuthManager, db: StateDB, b2: B2Client) -> None:
        self.auth = auth
        self.db = db
        self.b2 = b2
        self._folder_lock = threading.RLock()
        self._local = threading.local()

    def _session(self) -> requests.Session:
        sess = getattr(self._local, "session", None)
        if sess is None:
            sess = requests.Session()
            adapter = requests.adapters.HTTPAdapter(pool_connections=8, pool_maxsize=8)
            sess.mount("https://", adapter)
            self._local.session = sess
        return sess

    def _headers(self, force_refresh: bool = False) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.auth.get_valid_access_token(force_refresh=force_refresh)}"}

    @staticmethod
    def _is_transient_rate_limit(status_code: int, resp_text: str) -> bool:
        """
        Detect per-minute / per-second rate limits (e.g. rclone shared project_number 'Requests per minute')
        that clear automatically within seconds—distinct from daily 750 GB cap or full storage quota.
        """
        if status_code == 429:
            return True
        if status_code != 403:
            return False
        lower = resp_text.lower()
        if "storagequotaexceeded" in lower or "teamdrivefilelimitexceeded" in lower:
            return False
        return any(
            kw in lower
            for kw in (
                "requests per minute",
                "queries per minute",
                "quota metric 'queries'",
                '"reason": "ratelimitexceeded"',
                '"reason":"ratelimitexceeded"',
                "ratelimitexceeded",
            )
        ) and "userratelimitexceeded" not in lower

    @staticmethod
    def _is_daily_or_storage_quota_error(resp_text: str) -> bool:
        lower = resp_text.lower()
        return any(
            kw in lower
            for kw in (
                "userratelimitexceeded",
                "dailylimitexceeded",
                "storagequotaexceeded",
                "teamdrivefilelimitexceeded",
            )
        )

    @staticmethod
    def _rate_limit_sleep(attempt: int, should_stop: Optional[Callable[[], bool]] = None) -> None:
        # Backoff schedule spanning >2 minute boundaries: 4s, 8s, 15s, 25s, 35s, 45s, 60s + jitter
        schedule = [4.0, 8.0, 15.0, 25.0, 35.0, 45.0, 60.0]
        delay = schedule[min(attempt, len(schedule) - 1)] + random.uniform(0.5, 3.5)
        end_ts = time.time() + delay
        while time.time() < end_ts:
            if should_stop and should_stop():
                raise TransferPausedError("Transfer paused by user")
            time.sleep(min(0.5, max(0.05, end_ts - time.time())))

    def _drive_request_with_backoff(
        self,
        method: str,
        url: str,
        should_stop: Optional[Callable[[], bool]] = None,
        max_attempts: int = 8,
        **kwargs: Any,
    ) -> requests.Response:
        """Execute a Drive metadata/session HTTP call with automatic token refresh, 5xx, and 403/429 rate-limit backoff."""
        extra_headers = dict(kwargs.pop("headers", None) or {})
        last_exc: Optional[Exception] = None

        for attempt in range(max_attempts):
            if should_stop and should_stop():
                raise TransferPausedError("Transfer paused by user")
            headers = {**self._headers(force_refresh=False), **extra_headers}
            try:
                resp = self._session().request(method, url, headers=headers, **kwargs)
            except STREAM_READ_EXCEPTIONS as exc:
                last_exc = exc
                if attempt + 1 >= max_attempts:
                    raise
                self._rate_limit_sleep(attempt, should_stop)
                continue

            if resp.status_code == 401 and attempt + 1 < max_attempts:
                self.auth.get_valid_access_token(force_refresh=True)
                continue

            if self._is_transient_rate_limit(resp.status_code, resp.text) and attempt + 1 < max_attempts:
                self._rate_limit_sleep(attempt, should_stop)
                continue

            if resp.status_code in (500, 502, 503, 504) and attempt + 1 < max_attempts:
                self._rate_limit_sleep(attempt, should_stop)
                continue

            return resp

        if last_exc:
            raise last_exc
        raise RuntimeError(f"Drive request {method} {url} exhausted retries")

    def resolve_shared_drive_id(self, shared_drive_name: str, shared_drive_id: str = "") -> str:
        if shared_drive_id:
            return shared_drive_id
        info = self.auth.get_cloud_drive_info(force=True)
        for d in info.get("shared_drives", []):
            if d.get("name", "").lower() == shared_drive_name.strip().lower():
                return d["id"]
        raise RuntimeError(f"Shared Drive '{shared_drive_name}' not found on this Google account.")

    def ensure_folder_path(
        self,
        folder_parts: list[str],
        drive_location: str = "my_drive",
        shared_drive_id: str = "",
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> str:
        """Thread-safely find or create nested folders in Google Drive, caching IDs in SQLite per account."""
        acct = self.auth.get_account_fingerprint()
        root_id = shared_drive_id if (drive_location == "shared_drive" and shared_drive_id) else "root"
        scope_prefix = f"{acct}:{drive_location}:{root_id}"
        parent_id = root_id
        built_parts: list[str] = []

        with self._folder_lock:
            for part in folder_parts:
                if not part:
                    continue
                built_parts.append(part)
                cache_key = f"{scope_prefix}:{'/'.join(built_parts)}"
                cached = self.db.get_cached_drive_folder(cache_key)
                if cached:
                    parent_id = cached
                    continue

                parent_id = self._find_or_create_single_folder(
                    name=part,
                    parent_id=parent_id,
                    drive_location=drive_location,
                    shared_drive_id=shared_drive_id,
                    should_stop=should_stop,
                )
                self.db.set_cached_drive_folder(cache_key, parent_id)
        return parent_id

    def _find_or_create_single_folder(
        self,
        name: str,
        parent_id: str,
        drive_location: str,
        shared_drive_id: str,
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> str:
        escaped = name.replace("\\", "\\\\").replace("'", "\\'")
        q = (
            f"'{parent_id}' in parents and name = '{escaped}' "
            f"and mimeType = '{self.FOLDER_MIME}' and trashed = false"
        )
        params: Dict[str, Any] = {
            "q": q,
            "fields": "files(id, name)",
            "pageSize": 5,
            "supportsAllDrives": "true",
            "includeItemsFromAllDrives": "true",
        }
        if drive_location == "shared_drive" and shared_drive_id:
            params["corpora"] = "drive"
            params["driveId"] = shared_drive_id

        resp = self._drive_request_with_backoff(
            "GET",
            f"{self.DRIVE_API}/files",
            should_stop=should_stop,
            params=params,
            timeout=30,
        )
        if resp.status_code == 200:
            files = resp.json().get("files", [])
            if files:
                return files[0]["id"]

        body: Dict[str, Any] = {
            "name": name,
            "mimeType": self.FOLDER_MIME,
            "parents": [parent_id],
        }
        c_resp = self._drive_request_with_backoff(
            "POST",
            f"{self.DRIVE_API}/files",
            should_stop=should_stop,
            params={"supportsAllDrives": "true", "fields": "id"},
            headers={"Content-Type": "application/json"},
            json=body,
            timeout=30,
        )
        if c_resp.status_code not in (200, 201):
            raise RuntimeError(f"Drive mkdir '{name}' failed ({c_resp.status_code}): {c_resp.text[:300]}")
        return c_resp.json()["id"]

    def find_existing_remote_file(
        self,
        parent_id: str,
        filename: str,
        expected_size: int,
        expected_sha1: Optional[str],
        drive_location: str = "my_drive",
        shared_drive_id: str = "",
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Check if an identical file (matching size and SHA-1) already exists in the target Drive folder."""
        escaped = filename.replace("\\", "\\\\").replace("'", "\\'")
        q = f"'{parent_id}' in parents and name = '{escaped}' and trashed = false"
        params: Dict[str, Any] = {
            "q": q,
            "fields": "files(id, name, size, sha1Checksum, md5Checksum)",
            "pageSize": 10,
            "supportsAllDrives": "true",
            "includeItemsFromAllDrives": "true",
        }
        if drive_location == "shared_drive" and shared_drive_id:
            params["corpora"] = "drive"
            params["driveId"] = shared_drive_id

        resp = self._drive_request_with_backoff(
            "GET",
            f"{self.DRIVE_API}/files",
            should_stop=should_stop,
            params=params,
            timeout=30,
        )
        if resp.status_code != 200:
            return None

        for f in resp.json().get("files", []):
            remote_size = int(f.get("size") or -1)
            remote_sha1 = (f.get("sha1Checksum") or "").lower()
            if remote_size == expected_size:
                if not expected_sha1 or not remote_sha1 or remote_sha1 == expected_sha1.lower():
                    return f
        return None

    def _create_resumable_session(
        self,
        parent_id: str,
        filename: str,
        content_type: str,
        total_size: int,
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> str:
        extra_headers = {
            "Content-Type": "application/json; charset=UTF-8",
            "X-Upload-Content-Type": content_type or "application/octet-stream",
            "X-Upload-Content-Length": str(total_size),
        }
        metadata = {"name": filename, "parents": [parent_id]}
        resp = self._drive_request_with_backoff(
            "POST",
            f"{self.DRIVE_UPLOAD_API}/files",
            should_stop=should_stop,
            params={
                "uploadType": "resumable",
                "supportsAllDrives": "true",
                "fields": "id,name,size,sha1Checksum,md5Checksum",
            },
            headers=extra_headers,
            json=metadata,
            timeout=30,
        )
        if resp.status_code == 403 and self._is_daily_or_storage_quota_error(resp.text):
            raise QuotaExceededError(f"Google Drive upload quota reached: {resp.text[:250]}")
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"Failed to start Drive resumable upload ({resp.status_code}): {resp.text[:300]}")
        loc = resp.headers.get("Location")
        if not loc:
            raise RuntimeError("Drive resumable upload response missing Location header")
        return loc

    def _query_session_offset(
        self,
        session_uri: str,
        total_size: int,
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> Tuple[str, int, Optional[Dict[str, Any]]]:
        """
        Query Google Drive for how many bytes were committed on an existing resumable session URI.
        Returns (state, committed_bytes, completed_json_or_None)
        where state is 'RESUME' | 'COMPLETED' | 'EXPIRED'.
        """
        for attempt in range(6):
            if should_stop and should_stop():
                raise TransferPausedError("Transfer paused by user")
            try:
                resp = self._session().put(
                    session_uri,
                    headers={
                        "Content-Length": "0",
                        "Content-Range": f"bytes */{total_size}",
                    },
                    allow_redirects=False,
                    timeout=30,
                )
            except STREAM_READ_EXCEPTIONS:
                if attempt == 5:
                    raise
                self._rate_limit_sleep(attempt, should_stop)
                continue

            if resp.status_code in (200, 201):
                return "COMPLETED", total_size, resp.json()
            if resp.status_code == 308:
                rng = resp.headers.get("Range", "")
                if "-" in rng:
                    last_byte = int(rng.split("-")[-1].strip())
                    return "RESUME", last_byte + 1, None
                return "RESUME", 0, None
            if resp.status_code in (404, 410):
                return "EXPIRED", 0, None
            if (
                self._is_transient_rate_limit(resp.status_code, resp.text)
                or resp.status_code in (500, 502, 503, 504)
            ) and attempt < 5:
                self._rate_limit_sleep(attempt, should_stop)
                continue
            raise RuntimeError(f"Unexpected status querying resumable session ({resp.status_code}): {resp.text[:200]}")

        raise RuntimeError("Exhausted retries querying resumable session offset")

    def _put_single_chunk_with_retry(
        self,
        session_uri: str,
        chunk: bytes,
        offset: int,
        total_size: int,
        should_stop: Callable[[], bool],
    ) -> Tuple[str, int, Optional[Dict[str, Any]]]:
        """
        Upload a single in-RAM chunk to Google Drive without re-downloading from B2 on rate-limit/5xx spikes!
        Returns:
          - ('CONT', offset + len(chunk), None) on HTTP 308
          - ('DONE', total_size, completed_json) on HTTP 200/201
          - ('RECONNECT', new_offset, None) if partial byte synchronization with B2 stream is needed
        """
        chunk_len = len(chunk)
        end_byte = offset + chunk_len - 1
        headers = {
            "Content-Length": str(chunk_len),
            "Content-Range": f"bytes {offset}-{end_byte}/{total_size}",
        }

        max_chunk_attempts = 8
        for attempt in range(max_chunk_attempts):
            if should_stop():
                raise TransferPausedError("Transfer paused by user")

            try:
                put_resp = self._session().put(
                    session_uri,
                    data=chunk,
                    headers=headers,
                    allow_redirects=False,
                    timeout=(20, 180),
                )
            except STREAM_READ_EXCEPTIONS as exc:
                if attempt + 1 >= max_chunk_attempts:
                    raise RuntimeError(f"Network error uploading chunk at {offset}: {exc}") from exc
                self._rate_limit_sleep(attempt, should_stop)
                state, committed, done_json = self._query_session_offset(session_uri, total_size, should_stop)
                if state == "COMPLETED" and done_json:
                    return "DONE", total_size, done_json
                if state == "RESUME":
                    if committed == offset:
                        # Exact same offset — retry sending this in-RAM chunk directly!
                        continue
                    return "RECONNECT", committed, None
                raise RuntimeError("Resumable session expired during upload") from exc

            if put_resp.status_code == 308:
                return "CONT", offset + chunk_len, None

            if put_resp.status_code in (200, 201):
                return "DONE", offset + chunk_len, put_resp.json()

            # Per-minute shared project rate limit (e.g. Queries / Requests per minute) -> sleep & retry IN PLACE!
            if self._is_transient_rate_limit(put_resp.status_code, put_resp.text):
                if attempt + 1 >= max_chunk_attempts:
                    raise RuntimeError(
                        f"Drive rate limit persisted after {max_chunk_attempts} retries: {put_resp.text[:250]}"
                    )
                self._rate_limit_sleep(attempt, should_stop)
                state, committed, done_json = self._query_session_offset(session_uri, total_size, should_stop)
                if state == "COMPLETED" and done_json:
                    return "DONE", total_size, done_json
                if state == "RESUME" and committed != offset:
                    return "RECONNECT", committed, None
                continue

            # Daily 750 GB per-user upload cap or storage full
            if put_resp.status_code == 403 and self._is_daily_or_storage_quota_error(put_resp.text):
                if attempt < 2 and "storagequotaexceeded" not in put_resp.text.lower():
                    # Brief check in case userRateLimitExceeded was a 1-second burst
                    self._rate_limit_sleep(attempt, should_stop)
                    state, committed, done_json = self._query_session_offset(session_uri, total_size, should_stop)
                    if state == "COMPLETED" and done_json:
                        return "DONE", total_size, done_json
                    if state == "RESUME" and committed != offset:
                        return "RECONNECT", committed, None
                    continue
                raise QuotaExceededError(f"Google Drive upload limit reached ({put_resp.text[:200]})")

            if put_resp.status_code in (500, 502, 503, 504):
                if attempt + 1 >= max_chunk_attempts:
                    raise RuntimeError(f"Drive 5xx error ({put_resp.status_code}): {put_resp.text[:200]}")
                self._rate_limit_sleep(attempt, should_stop)
                state, committed, done_json = self._query_session_offset(session_uri, total_size, should_stop)
                if state == "COMPLETED" and done_json:
                    return "DONE", total_size, done_json
                if state == "RESUME":
                    if committed == offset:
                        continue
                    return "RECONNECT", committed, None

            raise RuntimeError(
                f"Drive chunk PUT failed ({put_resp.status_code}) at offset {offset}: {put_resp.text[:300]}"
            )

        raise RuntimeError(f"Exhausted retries on chunk at offset {offset}")

    def stream_file(
        self,
        file_rec: Dict[str, Any],
        dest_rel_path: str,
        dest_root_folder: str,
        drive_location: str,
        shared_drive_id: str,
        chunk_size_mib: int,
        should_stop: Callable[[], bool],
        on_chunk_uploaded: Callable[[int, int], None],
    ) -> Dict[str, Any]:
        """
        Stream a single file from B2 to Google Drive in RAM chunks with full resumability and SHA-1 verification.
        """
        b2_file_id = file_rec["b2_file_id"]
        total_size = int(file_rec["size_bytes"])
        expected_sha1 = file_rec.get("b2_sha1")
        content_type = file_rec.get("content_type") or "application/octet-stream"

        chunk_size = max(1, int(chunk_size_mib)) * 1024 * 1024
        if chunk_size % (256 * 1024) != 0:
            chunk_size = 32 * 1024 * 1024

        rel_parts = [p for p in dest_rel_path.split("/") if p]
        filename = rel_parts[-1]
        folder_parts = [p for p in [dest_root_folder, *rel_parts[:-1]] if p]

        parent_id = self.ensure_folder_path(
            folder_parts=folder_parts,
            drive_location=drive_location,
            shared_drive_id=shared_drive_id,
            should_stop=should_stop,
        )

        existing = self.find_existing_remote_file(
            parent_id=parent_id,
            filename=filename,
            expected_size=total_size,
            expected_sha1=expected_sha1,
            drive_location=drive_location,
            shared_drive_id=shared_drive_id,
            should_stop=should_stop,
        )
        if existing:
            return {
                "status": "SKIPPED_EXISTS",
                "drive_file_id": existing["id"],
                "verified_sha1": (existing.get("sha1Checksum") or expected_sha1 or "").lower() or None,
                "uploaded_this_run": 0,
            }

        acct = self.auth.get_account_fingerprint()
        dest_suffix = f":{drive_location}:{shared_drive_id}:{dest_root_folder}/{dest_rel_path}"
        dest_key = f"{acct}{dest_suffix}"
        session_uri = file_rec.get("resumable_uri")
        saved_dest = str(file_rec.get("resumable_dest") or "")
        offset = 0
        completed_meta: Optional[Dict[str, Any]] = None

        dest_matches = saved_dest == dest_key or (
            saved_dest.endswith(dest_suffix)
            and (saved_dest.startswith("default:") or "@" in saved_dest.split(":", 1)[0])
        )
        if session_uri and dest_matches and total_size > 0:
            try:
                state, committed, done_json = self._query_session_offset(session_uri, total_size, should_stop)
                if state == "COMPLETED" and done_json:
                    completed_meta = done_json
                    offset = total_size
                elif state == "RESUME":
                    offset = committed
                else:
                    session_uri = None
            except TransferPausedError:
                raise
            except Exception:
                session_uri = None

        if not session_uri and completed_meta is None:
            session_uri = self._create_resumable_session(
                parent_id=parent_id,
                filename=filename,
                content_type=content_type,
                total_size=total_size,
                should_stop=should_stop,
            )
            offset = 0
            self.db.save_resumable_session(b2_file_id, session_uri, dest_key, 0)

        uploaded_this_run = 0

        if total_size == 0 and completed_meta is None:
            assert session_uri is not None
            _, _, completed_meta = self._put_single_chunk_with_retry(
                session_uri=session_uri,
                chunk=b"",
                offset=0,
                total_size=0,
                should_stop=should_stop,
            )

        stream_hasher = hashlib.sha1() if offset == 0 else None
        consecutive_stream_drops = 0
        max_stream_drops = 10

        while completed_meta is None and offset < total_size:
            if should_stop():
                self.db.save_resumable_session(b2_file_id, session_uri, dest_key, offset)
                raise TransferPausedError("Transfer paused by user")

            assert session_uri is not None
            offset_at_stream_open = offset

            try:
                b2_resp = self.b2.open_download_stream(
                    b2_file_id=b2_file_id,
                    start_byte=offset,
                    total_size=total_size,
                )

                for chunk in iter_exact_chunks(b2_resp, chunk_size=chunk_size):
                    if should_stop():
                        self.db.save_resumable_session(b2_file_id, session_uri, dest_key, offset)
                        raise TransferPausedError("Transfer paused by user")

                    try:
                        action, new_offset, done_json = self._put_single_chunk_with_retry(
                            session_uri=session_uri,
                            chunk=chunk,
                            offset=offset,
                            total_size=total_size,
                            should_stop=should_stop,
                        )
                    except (TransferPausedError, QuotaExceededError):
                        self.db.save_resumable_session(b2_file_id, session_uri, dest_key, offset)
                        raise

                    if action == "CONT":
                        if stream_hasher is not None:
                            stream_hasher.update(chunk)
                        delta = new_offset - offset
                        offset = new_offset
                        uploaded_this_run += delta
                        consecutive_stream_drops = 0  # Forward progress made!
                        self.db.update_file_progress(b2_file_id, offset)
                        on_chunk_uploaded(delta, offset)
                        continue

                    if action == "DONE" and done_json is not None:
                        if stream_hasher is not None:
                            stream_hasher.update(chunk)
                        delta = max(0, new_offset - offset)
                        offset = new_offset
                        uploaded_this_run += delta
                        completed_meta = done_json
                        self.db.update_file_progress(b2_file_id, offset)
                        on_chunk_uploaded(delta, offset)
                        break

                    if action == "RECONNECT":
                        stream_hasher = None
                        offset = new_offset
                        self.db.update_file_progress(b2_file_id, offset)
                        break

            except STREAM_READ_EXCEPTIONS as exc:
                # Catches B2 mid-stream IncompleteRead / ProtocolError / ConnectionResetError!
                stream_hasher = None
                if offset > offset_at_stream_open:
                    consecutive_stream_drops = 0
                else:
                    consecutive_stream_drops += 1
                if consecutive_stream_drops > max_stream_drops:
                    self.db.save_resumable_session(b2_file_id, session_uri, dest_key, offset)
                    raise RuntimeError(
                        f"B2 download connection dropped repeatedly at {offset}/{total_size} bytes: {exc}"
                    ) from exc

                self._rate_limit_sleep(min(consecutive_stream_drops, 4), should_stop)
                state, committed, done_json = self._query_session_offset(session_uri, total_size, should_stop)
                if state == "COMPLETED" and done_json:
                    completed_meta = done_json
                    offset = total_size
                elif state == "RESUME":
                    offset = committed
                else:
                    session_uri = self._create_resumable_session(
                        parent_id=parent_id,
                        filename=filename,
                        content_type=content_type,
                        total_size=total_size,
                        should_stop=should_stop,
                    )
                    offset = 0
                    self.db.save_resumable_session(b2_file_id, session_uri, dest_key, 0)
                continue

            if completed_meta is None and offset < total_size:
                if offset > offset_at_stream_open:
                    consecutive_stream_drops = 0
                else:
                    consecutive_stream_drops += 1
                if consecutive_stream_drops > max_stream_drops:
                    self.db.save_resumable_session(b2_file_id, session_uri, dest_key, offset)
                    raise RuntimeError(f"B2 stream ended early at {offset}/{total_size} bytes")
                stream_hasher = None
                self._rate_limit_sleep(min(consecutive_stream_drops, 3), should_stop)

        if not completed_meta:
            raise RuntimeError("Upload finished loop without Drive completion metadata")

        drive_sha1 = (completed_meta.get("sha1Checksum") or "").lower()
        drive_size = int(completed_meta.get("size") or total_size)
        streamed_sha1 = stream_hasher.hexdigest().lower() if stream_hasher is not None else None

        if drive_size != total_size:
            raise RuntimeError(f"Size mismatch on Drive: expected {total_size}, got {drive_size}")

        ref_sha1 = (expected_sha1 or streamed_sha1 or "").lower()
        if ref_sha1 and drive_sha1 and ref_sha1 != drive_sha1:
            raise RuntimeError(
                f"SHA-1 mismatch! B2={ref_sha1} vs Google Drive={drive_sha1}"
            )

        return {
            "status": "COMPLETED",
            "drive_file_id": completed_meta.get("id"),
            "verified_sha1": drive_sha1 or ref_sha1 or None,
            "uploaded_this_run": uploaded_this_run,
        }
