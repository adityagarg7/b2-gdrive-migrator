"""Backblaze B2 Native API v2 client with automatic token refresh and HTTP Range streaming."""

from __future__ import annotations

import base64
import re
import threading
import time
from typing import Any, Callable, Dict, Generator, Iterator, List, Optional

import requests

from .config import parse_b2_key_file

HEX40_RE = re.compile(r"^[0-9a-fA-F]{40}$")


def extract_b2_sha1(file_obj: Dict[str, Any]) -> Optional[str]:
    """Extract authoritative 40-char lowercase SHA-1 from B2 file metadata (small or large multipart)."""
    raw = (file_obj.get("contentSha1") or "").strip()
    if raw.lower().startswith("unverified:"):
        raw = raw.split(":", 1)[1].strip()
    if HEX40_RE.match(raw):
        return raw.lower()

    info = file_obj.get("fileInfo") or {}
    for key in ("large_file_sha1", "sha1", "src_sha1"):
        cand = str(info.get(key) or "").strip()
        if HEX40_RE.match(cand):
            return cand.lower()
    return None


class B2Client:
    """Thread-safe Backblaze B2 API client."""

    AUTH_URL = "https://api.backblazeb2.com/b2api/v2/b2_authorize_account"
    TOKEN_MAX_AGE_SEC = 12 * 3600  # Refresh proactively every 12h (tokens last 24h)

    def __init__(self, key_file_path: str) -> None:
        self.key_file_path = key_file_path
        self._lock = threading.RLock()
        self._auth_data: Optional[Dict[str, Any]] = None
        self._auth_ts: float = 0.0
        self._local = threading.local()

    def _session(self) -> requests.Session:
        sess = getattr(self._local, "session", None)
        if sess is None:
            sess = requests.Session()
            adapter = requests.adapters.HTTPAdapter(pool_connections=8, pool_maxsize=8, max_retries=2)
            sess.mount("https://", adapter)
            self._local.session = sess
        return sess

    def authorize(self, force: bool = False) -> Dict[str, Any]:
        with self._lock:
            now = time.time()
            if (
                not force
                and self._auth_data is not None
                and (now - self._auth_ts) < self.TOKEN_MAX_AGE_SEC
            ):
                return self._auth_data

            key_id, app_key, _ = parse_b2_key_file(self.key_file_path)
            creds = base64.b64encode(f"{key_id}:{app_key}".encode("utf-8")).decode("ascii")
            resp = requests.get(
                self.AUTH_URL,
                headers={"Authorization": f"Basic {creds}"},
                timeout=30,
            )
            if resp.status_code != 200:
                raise RuntimeError(f"Backblaze B2 auth failed ({resp.status_code}): {resp.text[:300]}")
            self._auth_data = resp.json()
            self._auth_ts = now
            return self._auth_data

    def _post(self, endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        for attempt in range(3):
            auth = self.authorize(force=(attempt > 0))
            url = f"{auth['apiUrl']}/b2api/v2/{endpoint}"
            resp = self._session().post(
                url,
                json=payload,
                headers={"Authorization": auth["authorizationToken"]},
                timeout=45,
            )
            if resp.status_code == 401 and attempt < 2:
                continue
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < 2:
                time.sleep(2 ** (attempt + 1))
                continue
            if resp.status_code != 200:
                raise RuntimeError(f"B2 {endpoint} failed ({resp.status_code}): {resp.text[:300]}")
            return resp.json()
        raise RuntimeError(f"B2 {endpoint} exhausted retries")

    def list_buckets(self) -> List[Dict[str, Any]]:
        auth = self.authorize()
        allowed = auth.get("allowed") or {}
        if allowed.get("bucketId"):
            return [
                {
                    "bucketId": allowed["bucketId"],
                    "bucketName": allowed.get("bucketName") or allowed["bucketId"],
                    "bucketType": "restricted",
                }
            ]
        res = self._post("b2_list_buckets", {"accountId": auth["accountId"]})
        return res.get("buckets", [])

    def scan_bucket_files(
        self,
        bucket_id: str,
        bucket_name: str,
        on_page: Optional[Callable[[int, int], None]] = None,
    ) -> List[Dict[str, Any]]:
        """List all active uploaded files in a B2 bucket."""
        records: List[Dict[str, Any]] = []
        start_file_name: Optional[str] = None
        total_bytes = 0

        while True:
            payload: Dict[str, Any] = {"bucketId": bucket_id, "maxFileCount": 1000}
            if start_file_name:
                payload["startFileName"] = start_file_name
            res = self._post("b2_list_file_names", payload)
            for f in res.get("files", []):
                if f.get("action") != "upload":
                    continue
                fname = f["fileName"]
                parts = fname.split("/")
                basename = parts[-1]
                top_prefix = parts[0] if len(parts) > 1 else "(root)"
                size_bytes = int(f.get("contentLength") or 0)
                total_bytes += size_bytes
                records.append(
                    {
                        "b2_file_id": f["fileId"],
                        "bucket_id": bucket_id,
                        "bucket_name": bucket_name,
                        "file_name": fname,
                        "basename": basename,
                        "top_prefix": top_prefix,
                        "size_bytes": size_bytes,
                        "b2_sha1": extract_b2_sha1(f),
                        "content_type": f.get("contentType") or "application/octet-stream",
                        "upload_timestamp": int(f.get("uploadTimestamp") or 0),
                    }
                )
            if on_page:
                on_page(len(records), total_bytes)
            start_file_name = res.get("nextFileName")
            if not start_file_name:
                break
        return records

    def open_download_stream(
        self,
        b2_file_id: str,
        start_byte: int = 0,
        total_size: Optional[int] = None,
    ) -> requests.Response:
        """Open a streaming HTTP response from B2 starting at start_byte."""
        for attempt in range(3):
            auth = self.authorize(force=(attempt > 0))
            url = f"{auth['downloadUrl']}/b2api/v2/b2_download_file_by_id"
            headers: Dict[str, str] = {"Authorization": auth["authorizationToken"]}
            if start_byte > 0:
                if total_size is not None and total_size > start_byte:
                    headers["Range"] = f"bytes={start_byte}-{total_size - 1}"
                else:
                    headers["Range"] = f"bytes={start_byte}-"

            resp = self._session().get(
                url,
                params={"fileId": b2_file_id},
                headers=headers,
                stream=True,
                timeout=(20, 120),
            )
            if resp.status_code == 401 and attempt < 2:
                resp.close()
                continue
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < 2:
                resp.close()
                time.sleep(2 ** (attempt + 1))
                continue
            if resp.status_code not in (200, 206):
                body = resp.text[:300]
                resp.close()
                raise RuntimeError(f"B2 download failed ({resp.status_code}): {body}")
            return resp
        raise RuntimeError("B2 download stream exhausted retries")


def iter_exact_chunks(
    resp: requests.Response,
    chunk_size: int,
    read_size: int = 1024 * 1024,
) -> Generator[bytes, None, None]:
    """
    Yield exact `chunk_size`-byte chunks from a streaming Response until EOF,
    where only the final chunk may be shorter than `chunk_size`.
    Required by Google Drive Resumable Upload API (chunks must be multiples of 256 KiB).
    """
    buf = bytearray()
    try:
        for piece in resp.iter_content(chunk_size=min(read_size, chunk_size)):
            if not piece:
                continue
            buf.extend(piece)
            while len(buf) >= chunk_size:
                out = bytes(buf[:chunk_size])
                del buf[:chunk_size]
                yield out
        if buf:
            yield bytes(buf)
    finally:
        resp.close()

