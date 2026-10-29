"""Automated test suite for Backblaze B2 -> Google Drive Migrator."""

from __future__ import annotations

import hashlib
import http.client
import io
import tempfile
import unittest
from pathlib import Path

from migrator.b2_client import extract_b2_sha1, iter_exact_chunks
from migrator.config import compute_relative_dest_path, parse_b2_key_file
from migrator.db import StateDB
from migrator.gdrive_cloud import GDriveCloudStreamer
from migrator.gdrive_mounted import GDriveMountedStreamer


class _FakeStreamResponse:
    def __init__(self, data: bytes) -> None:
        self._bio = io.BytesIO(data)
        self.closed = False

    def iter_content(self, chunk_size: int = 1024):
        # Intentionally yield irregular small slices to verify exact-chunk buffering
        while True:
            piece = self._bio.read(max(1, min(chunk_size, 7777)))
            if not piece:
                break
            yield piece

    def close(self) -> None:
        self.closed = True


class _FakeB2Client:
    def __init__(self, blobs: dict[str, bytes]) -> None:
        self.blobs = blobs

    def open_download_stream(self, b2_file_id: str, start_byte: int = 0, total_size: int | None = None):
        raw = self.blobs[b2_file_id]
        end = total_size if total_size is not None else len(raw)
        return _FakeStreamResponse(raw[start_byte:end])


class MigratorTestSuite(unittest.TestCase):
    def test_parse_b2_key_formats(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p1 = Path(td) / "labeled.txt"
            p1.write_text("keyID:\n005abc\nkeyName:\ntest\napplicationKey:\nK005xyz", encoding="utf-8")
            kid, akey, bkt = parse_b2_key_file(p1)
            self.assertEqual(kid, "005abc")
            self.assertEqual(akey, "K005xyz")
            self.assertIsNone(bkt)

            p2 = Path(td) / "raw3.txt"
            p2.write_text("005id2\nK005secret2\nfourvectors-staging\n", encoding="utf-8")
            kid2, akey2, bkt2 = parse_b2_key_file(p2)
            self.assertEqual(kid2, "005id2")
            self.assertEqual(akey2, "K005secret2")
            self.assertEqual(bkt2, "fourvectors-staging")

    def test_extract_b2_sha1(self) -> None:
        h1 = "a" * 40
        h2 = "b" * 40
        self.assertEqual(extract_b2_sha1({"contentSha1": h1}), h1)
        self.assertEqual(extract_b2_sha1({"contentSha1": f"unverified:{h1}"}), h1)
        self.assertEqual(
            extract_b2_sha1({"contentSha1": "none", "fileInfo": {"large_file_sha1": h2.upper()}}),
            h2,
        )
        self.assertIsNone(extract_b2_sha1({"contentSha1": "none", "fileInfo": {}}))

    def test_iter_exact_chunks_256k_alignment(self) -> None:
        payload = bytes(range(256)) * 4096 + b"tail123"  # 1,048,583 bytes
        chunk_sz = 256 * 1024  # 262,144 bytes
        resp = _FakeStreamResponse(payload)
        chunks = list(iter_exact_chunks(resp, chunk_size=chunk_sz))
        self.assertTrue(resp.closed)
        self.assertEqual(len(chunks), 5)
        for c in chunks[:-1]:
            self.assertEqual(len(c), chunk_sz)
        self.assertEqual(len(chunks[-1]), 7)
        self.assertEqual(b"".join(chunks), payload)

    def test_category_normalization(self) -> None:
        p_raw = compute_relative_dest_path(
            "fourvectors-archive",
            "metal-fabrication-welding/FV-C001.mp4",
            include_bucket_folder=True,
            normalize_categories=False,
        )
        self.assertEqual(p_raw, "fourvectors-archive/metal-fabrication-welding/FV-C001.mp4")

        p_norm = compute_relative_dest_path(
            "fourvectors-archive",
            "metal-fabrication-welding/FV-C001.mp4",
            include_bucket_folder=True,
            normalize_categories=True,
        )
        self.assertEqual(p_norm, "fourvectors-archive/metal-fab-welding/FV-C001.mp4")

    def test_db_dedup_and_checkpoint_preservation(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db = StateDB(Path(td) / "state.sqlite3")
            sha = "17eff470abd03d377c1e37782fb7db1510f9e201"
            db.upsert_bucket_catalog(
                "b_arc",
                "fourvectors-archive",
                "allPrivate",
                [
                    {
                        "b2_file_id": "f1",
                        "bucket_id": "b_arc",
                        "bucket_name": "fourvectors-archive",
                        "file_name": "carpentry-workshop/FV-B00112.mp4",
                        "basename": "FV-B00112.mp4",
                        "top_prefix": "carpentry-workshop",
                        "size_bytes": 1000,
                        "b2_sha1": sha,
                        "content_type": "video/mp4",
                        "upload_timestamp": 1,
                    }
                ],
            )
            db.mark_file_completed(
                "f1",
                "cloud",
                "fourvectors-archive/carpentry-workshop/FV-B00112.mp4",
                1000,
                sha,
                "drv_1",
                target_account="cloud:acct_a@example.com",
            )

            # Re-scanning must preserve COMPLETED status!
            db.upsert_bucket_catalog(
                "b_arc",
                "fourvectors-archive",
                "allPrivate",
                [
                    {
                        "b2_file_id": "f1",
                        "bucket_id": "b_arc",
                        "bucket_name": "fourvectors-archive",
                        "file_name": "carpentry-workshop/FV-B00112.mp4",
                        "basename": "FV-B00112.mp4",
                        "top_prefix": "carpentry-workshop",
                        "size_bytes": 1000,
                        "b2_sha1": sha,
                        "content_type": "video/mp4",
                        "upload_timestamp": 1,
                    }
                ],
            )
            summaries = db.get_bucket_summaries()
            self.assertEqual(summaries[0]["completed_files"], 1)

            # Cross-bucket duplicate detection matches SAME account, NEVER falsely skips on a DIFFERENT account!
            dup_same = db.find_completed_duplicate(
                "fourvectors-16-sep-27", sha, "FV-B00112.mp4", 1000, target_account="cloud:acct_a@example.com"
            )
            self.assertIsNotNone(dup_same)
            self.assertEqual(dup_same["drive_file_id"], "drv_1")

            dup_other = db.find_completed_duplicate(
                "fourvectors-16-sep-27", sha, "FV-B00112.mp4", 1000, target_account="cloud:acct_b@example.com"
            )
            self.assertIsNone(dup_other)

            # Reset bucket progress returns files cleanly to PENDING
            reset_cnt = db.reset_bucket_progress("fourvectors-archive")
            self.assertEqual(reset_cnt, 1)
            self.assertEqual(db.get_bucket_summaries()[0]["pending_files"], 1)

    def test_mounted_streamer_resume_and_sha1_verify(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            fake_g = Path(td) / "FakeGDrive"
            (fake_g / "My Drive").mkdir(parents=True)

            data = b"EgocentricVideoPayload-" * 50000  # ~1.15 MB
            sha1 = hashlib.sha1(data).hexdigest()
            fake_b2 = _FakeB2Client({"file_123": data})
            streamer = GDriveMountedStreamer(fake_b2)  # type: ignore[arg-type]

            rec = {
                "b2_file_id": "file_123",
                "size_bytes": len(data),
                "b2_sha1": sha1,
            }

            # Simulate partial .b2part left from an interrupted transfer (first 300 KB)
            dest_dir = fake_g / "My Drive" / "MigrationRoot" / "fourvectors-staging"
            dest_dir.mkdir(parents=True)
            part_file = dest_dir / "GL010001.LRV.b2part"
            meta_file = dest_dir / "GL010001.LRV.b2part.json"
            part_file.write_bytes(data[:300_000])
            meta_file.write_text(
                f'{{"b2_file_id": "file_123", "total_size": {len(data)}, "expected_sha1": "{sha1}"}}',
                encoding="utf-8",
            )

            res = streamer.stream_file(
                file_rec=rec,
                dest_rel_path="fourvectors-staging/GL010001.LRV",
                mounted_root=str(fake_g),
                drive_location="my_drive",
                shared_drive_name="",
                dest_root_folder="MigrationRoot",
                min_c_free_gib=1.0,
                chunk_size_mib=1,
                should_stop=lambda: False,
                on_chunk_written=lambda _d, _o: None,
            )
            self.assertEqual(res["status"], "COMPLETED")
            self.assertEqual(res["verified_sha1"], sha1)
            self.assertEqual(res["uploaded_this_run"], len(data) - 300_000)
            final_file = dest_dir / "GL010001.LRV"
            self.assertTrue(final_file.exists())
            self.assertFalse(part_file.exists())
            self.assertEqual(final_file.read_bytes(), data)

    def test_rate_limit_classification_and_incomplete_read(self) -> None:
        rclone_rpm_err = (
            '{"error": {"code": 403, "message": "Quota exceeded for quota metric \'Queries\' '
            'and limit \'Previous quota: Requests per minute\' of service \'drive.googleapis.com\'"}}'
        )
        self.assertTrue(GDriveCloudStreamer._is_transient_rate_limit(403, rclone_rpm_err))
        self.assertFalse(GDriveCloudStreamer._is_daily_or_storage_quota_error(rclone_rpm_err))

        daily_cap_err = '{"error": {"code": 403, "errors": [{"reason": "userRateLimitExceeded"}]}}'
        self.assertFalse(GDriveCloudStreamer._is_transient_rate_limit(403, daily_cap_err))
        self.assertTrue(GDriveCloudStreamer._is_daily_or_storage_quota_error(daily_cap_err))


if __name__ == "__main__":
    unittest.main()

