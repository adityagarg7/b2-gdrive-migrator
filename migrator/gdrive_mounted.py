"""Mounted Google Drive for Desktop (G:\\) engine with automatic C:\\ free-space cache guard."""

from __future__ import annotations

import hashlib
import http.client
import json
import os
import shutil
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

import requests
import urllib3.exceptions

from .b2_client import B2Client
from .gdrive_cloud import TransferPausedError

MOUNTED_STREAM_EXCEPTIONS = (
    requests.RequestException,
    urllib3.exceptions.HTTPError,
    http.client.HTTPException,
    OSError,
)


def get_mounted_drive_base(
    mounted_root: str,
    drive_location: str,
    shared_drive_name: str,
    dest_root_folder: str,
) -> Path:
    root = Path(mounted_root)
    if drive_location == "shared_drive":
        base = root / "Shared drives" / shared_drive_name
    else:
        base = root / "My Drive"
    if dest_root_folder.strip():
        base = base / dest_root_folder.strip().strip("/\\")
    return base


def get_c_drive_free_bytes() -> int:
    try:
        return shutil.disk_usage("C:\\").free
    except Exception:
        return 100 * (1024 ** 3)


class GDriveMountedStreamer:
    """
    Streams Backblaze B2 files into mounted G:\\ (Google Drive for Desktop) with:
    - Real-time C:\\ disk space guard so DriveFS cache never fills the OS drive
    - Byte-level .b2part resume across pauses/restarts
    - End-to-end streaming SHA-1 cryptographic verification before atomic rename
    """

    def __init__(self, b2: B2Client) -> None:
        self.b2 = b2

    def check_mounted_available(
        self,
        mounted_root: str,
        drive_location: str,
        shared_drive_name: str,
    ) -> Tuple[bool, str]:
        root = Path(mounted_root)
        if not root.exists():
            return False, f"Mounted drive {mounted_root} not found."
        sub = (root / "Shared drives" / shared_drive_name) if drive_location == "shared_drive" else (root / "My Drive")
        if not sub.exists():
            return False, f"Target path {sub} does not exist."
        return True, str(sub)

    def stream_file(
        self,
        file_rec: Dict[str, Any],
        dest_rel_path: str,
        mounted_root: str,
        drive_location: str,
        shared_drive_name: str,
        dest_root_folder: str,
        min_c_free_gib: float,
        chunk_size_mib: int,
        should_stop: Callable[[], bool],
        on_chunk_written: Callable[[int, int], None],
        on_cache_wait: Optional[Callable[[float, float], None]] = None,
    ) -> Dict[str, Any]:
        b2_file_id = file_rec["b2_file_id"]
        total_size = int(file_rec["size_bytes"])
        expected_sha1 = (file_rec.get("b2_sha1") or "").lower() or None

        base_dir = get_mounted_drive_base(
            mounted_root=mounted_root,
            drive_location=drive_location,
            shared_drive_name=shared_drive_name,
            dest_root_folder=dest_root_folder,
        )
        rel_parts = [p for p in dest_rel_path.split("/") if p]
        final_path = base_dir.joinpath(*rel_parts)

        # Skip if final file already exists on G:\ with exact byte size
        if final_path.exists():
            try:
                if final_path.stat().st_size == total_size:
                    return {
                        "status": "SKIPPED_EXISTS",
                        "drive_file_id": str(final_path),
                        "verified_sha1": expected_sha1,
                        "uploaded_this_run": 0,
                    }
            except OSError:
                pass

        final_path.parent.mkdir(parents=True, exist_ok=True)
        part_path = final_path.with_name(final_path.name + ".b2part")
        meta_path = final_path.with_name(final_path.name + ".b2part.json")

        # Determine safe resume offset on .b2part and rebuild SHA-1 prefix state
        offset = 0
        hasher = hashlib.sha1()

        if part_path.exists() and meta_path.exists():
            try:
                pmeta = json.loads(meta_path.read_text(encoding="utf-8"))
                existing_len = part_path.stat().st_size
                if (
                    pmeta.get("b2_file_id") == b2_file_id
                    and 0 < existing_len <= total_size
                ):
                    # Hash existing partial bytes so end-to-end SHA-1 verification remains 100% exact
                    with part_path.open("rb") as rf:
                        while True:
                            blk = rf.read(4 * 1024 * 1024)
                            if not blk:
                                break
                            hasher.update(blk)
                    offset = existing_len
                else:
                    part_path.unlink(missing_ok=True)
            except Exception:
                offset = 0
                hasher = hashlib.sha1()
                part_path.unlink(missing_ok=True)
        elif part_path.exists():
            part_path.unlink(missing_ok=True)

        meta_path.write_text(
            json.dumps({"b2_file_id": b2_file_id, "total_size": total_size, "expected_sha1": expected_sha1}),
            encoding="utf-8",
        )

        min_free_bytes = int(max(1.0, float(min_c_free_gib)) * (1024 ** 3))
        read_chunk = max(1, min(int(chunk_size_mib), 16)) * 1024 * 1024
        uploaded_this_run = 0

        if offset < total_size:
            consecutive_drops = 0
            while offset < total_size:
                mode = "ab" if offset > 0 else "wb"
                start_at_open = offset
                b2_resp = self.b2.open_download_stream(
                    b2_file_id=b2_file_id,
                    start_byte=offset,
                    total_size=total_size,
                )
                try:
                    with part_path.open(mode) as wf:
                        for piece in b2_resp.iter_content(chunk_size=read_chunk):
                            if not piece:
                                continue
                            if should_stop():
                                wf.flush()
                                raise TransferPausedError("Transfer paused by user")

                            # C:\ Free-Space Guard (protects OS drive while Google Drive Desktop caches uploads)
                            while True:
                                free_c = get_c_drive_free_bytes()
                                if (free_c - len(piece)) >= min_free_bytes:
                                    break
                                if should_stop():
                                    wf.flush()
                                    raise TransferPausedError("Transfer paused by user")
                                if on_cache_wait:
                                    on_cache_wait(free_c / (1024 ** 3), float(min_c_free_gib))
                                time.sleep(4.0)

                            wf.write(piece)
                            hasher.update(piece)
                            plen = len(piece)
                            offset += plen
                            uploaded_this_run += plen
                            consecutive_drops = 0
                            on_chunk_written(plen, offset)
                except MOUNTED_STREAM_EXCEPTIONS as exc:
                    if offset > start_at_open:
                        consecutive_drops = 0
                    else:
                        consecutive_drops += 1
                    if consecutive_drops > 8:
                        raise RuntimeError(
                            f"B2 stream interrupted repeatedly at {offset}/{total_size}: {exc}"
                        ) from exc
                    time.sleep(min(2 ** consecutive_drops, 15))
                finally:
                    b2_resp.close()
        elif total_size == 0:
            part_path.write_bytes(b"")

        if offset != total_size:
            raise RuntimeError(f"Incomplete write to {part_path}: {offset}/{total_size} bytes")

        computed_sha1 = hasher.hexdigest().lower()
        if expected_sha1 and computed_sha1 != expected_sha1:
            part_path.unlink(missing_ok=True)
            meta_path.unlink(missing_ok=True)
            raise RuntimeError(
                f"SHA-1 mismatch on {final_path.name}: expected {expected_sha1}, got {computed_sha1}"
            )

        part_path.replace(final_path)
        meta_path.unlink(missing_ok=True)

        return {
            "status": "COMPLETED",
            "drive_file_id": str(final_path),
            "verified_sha1": computed_sha1,
            "uploaded_this_run": uploaded_this_run,
        }
