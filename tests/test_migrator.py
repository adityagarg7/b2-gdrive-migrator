"""Automated test suite for Backblaze B2 -> Google Drive Migrator."""

from __future__ import annotations

import hashlib
import http.client
import io
import json
import os
import tempfile
import threading
import time
import types
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest import mock

from migrator import config
from migrator.b2_client import extract_b2_sha1, iter_exact_chunks
from migrator.config import (
    CATEGORY_NORMALIZATION_MAP,
    MigrationSettings,
    compute_relative_dest_path,
    parse_b2_key_file,
)
from migrator.db import StateDB
from migrator.gdrive_auth import GDriveAuthManager
from migrator.gdrive_cloud import GDriveCloudStreamer
from migrator.gdrive_mounted import GDriveMountedStreamer
from migrator.web.server import allowed_hosts_for, create_handler

SHA_A = "a" * 40
SHA_B = "b" * 40
ACCT = "cloud:acct_a"


def _catalog_rec(bucket: str, name: str, size: int, sha: str | None, file_id: str) -> dict:
    return {
        "b2_file_id": file_id,
        "bucket_id": f"id_{bucket}",
        "bucket_name": bucket,
        "file_name": name,
        "basename": name.split("/")[-1],
        "top_prefix": name.split("/")[0],
        "size_bytes": size,
        "b2_sha1": sha,
        "content_type": "video/mp4",
        "upload_timestamp": 1,
    }


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
        # Hermetic: ignore any real B2 credentials in the environment or in data/b2_credentials.json
        with (
            tempfile.TemporaryDirectory() as td,
            mock.patch.dict(os.environ),
            mock.patch.object(config, "B2_CREDS_JSON_PATH", Path(td) / "no_saved_creds.json"),
        ):
            for var in ("B2_KEY_ID", "B2_APPLICATION_KEY_ID", "B2_APPLICATION_KEY"):
                os.environ.pop(var, None)
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


    def test_saved_b2_credentials_take_precedence_over_key_file(self) -> None:
        with (
            tempfile.TemporaryDirectory() as td,
            mock.patch.dict(os.environ),
            mock.patch.object(config, "B2_CREDS_JSON_PATH", Path(td) / "saved.json"),
        ):
            for var in ("B2_KEY_ID", "B2_APPLICATION_KEY_ID", "B2_APPLICATION_KEY"):
                os.environ.pop(var, None)
            (Path(td) / "saved.json").write_text(
                json.dumps({"key_id": "saved-id", "application_key": "saved-key"}), encoding="utf-8"
            )
            self.assertEqual(parse_b2_key_file(Path(td) / "missing.txt"), ("saved-id", "saved-key", None))

    def test_dedup_matches_on_sha1_and_never_on_name_when_sha1_known(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db = StateDB(Path(td) / "state.sqlite3")
            db.upsert_bucket_catalog(
                "id_bkt-a", "bkt-a", "allPrivate", [_catalog_rec("bkt-a", "cat/GH010001.MP4", 1000, SHA_A, "f1")]
            )
            db.mark_file_completed("f1", "cloud", "bkt-a/cat/GH010001.MP4", 1000, SHA_A, "drv", target_account=ACCT)

            # Same bytes under a different name: duplicate.
            self.assertIsNotNone(db.find_completed_duplicate("bkt-b", SHA_A, "renamed.MP4", 1000, ACCT))
            # Same name and size but a different SHA-1 is a different clip and must still be copied.
            self.assertIsNone(db.find_completed_duplicate("bkt-b", SHA_B, "GH010001.MP4", 1000, ACCT))
            # Only when B2 has no SHA-1 does basename + size act as the fallback.
            self.assertIsNotNone(db.find_completed_duplicate("bkt-b", None, "GH010001.MP4", 1000, ACCT))

    def test_opening_db_does_not_reset_in_progress_files(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "state.sqlite3"
            db = StateDB(path)
            db.upsert_bucket_catalog(
                "id_bkt-a", "bkt-a", "allPrivate", [_catalog_rec("bkt-a", "cat/FV-1.mp4", 1000, SHA_A, "f1")]
            )
            self.assertIsNotNone(db.claim_next_file("bkt-a"))

            StateDB(path)  # e.g. `migrate.py status` opened next to a live dashboard
            self.assertEqual(db.get_bucket_summaries()[0]["in_progress_files"], 1)

            self.assertEqual(db.reset_stale_in_progress(), 1)
            summary = db.get_bucket_summaries()[0]
            self.assertEqual((summary["in_progress_files"], summary["pending_files"]), (0, 1))

    def test_category_merging_defaults_on_and_maps_every_split_name(self) -> None:
        self.assertTrue(MigrationSettings().normalize_categories)
        self.assertFalse(set(CATEGORY_NORMALIZATION_MAP) & set(CATEGORY_NORMALIZATION_MAP.values()))
        for old, new in CATEGORY_NORMALIZATION_MAP.items():
            self.assertEqual(
                compute_relative_dest_path("bkt", f"{old}/no-batch/FV-1.mp4", True, True),
                f"bkt/{new}/no-batch/FV-1.mp4",
            )
        # Only the first path segment is a category.
        self.assertEqual(
            compute_relative_dest_path("bkt", "x/restaurant-commercial-kitchen/a.mp4", True, True),
            "bkt/x/restaurant-commercial-kitchen/a.mp4",
        )

    def test_start_migration_requeues_interrupted_files(self) -> None:
        from migrator.engine import MigrationEngine

        with tempfile.TemporaryDirectory() as td:
            db = StateDB(Path(td) / "state.sqlite3")
            db.upsert_bucket_catalog(
                "id_bkt-a", "bkt-a", "allPrivate", [_catalog_rec("bkt-a", "cat/FV-1.mp4", 1000, SHA_A, "f1")]
            )
            db.claim_next_file("bkt-a")  # left IN_PROGRESS by a crash

            settings = MigrationSettings(transfer_mode="cloud", active_bucket="bkt-a")
            with mock.patch.object(MigrationSettings, "save"), mock.patch.object(
                GDriveAuthManager, "is_connected", return_value=True
            ), mock.patch.object(MigrationEngine, "_worker_loop"), mock.patch.object(
                MigrationEngine, "_set_system_keep_awake"
            ):
                engine = MigrationEngine(settings=settings, db=db)
                ok, _ = engine.start_migration()
                for w in engine._workers:
                    w.join(timeout=5)
            self.assertTrue(ok)
            self.assertEqual(db.get_bucket_summaries()[0]["in_progress_files"], 0)


class _FakeOAuthProc:
    """Stands in for `rclone authorize drive` exiting with an error."""

    def __init__(self) -> None:
        self.stdout = iter(())
        self.returncode = None

    def poll(self):
        return self.returncode

    def wait(self):
        if self.returncode is None:
            self.returncode = 1
        return self.returncode

    def terminate(self) -> None:
        self.returncode = 1


class GDriveAuthLoginSafetyTests(unittest.TestCase):
    TOKEN_CONF = '[gdrive]\ntype = drive\ntoken = {"access_token": "abc", "refresh_token": "ref"}\n'

    def test_failed_signin_restores_previous_login(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            conf = Path(td) / "rclone.conf"
            conf.write_text(self.TOKEN_CONF, encoding="utf-8")
            mgr = GDriveAuthManager(rclone_conf=conf)

            with mock.patch("migrator.gdrive_auth.get_rclone_exe", return_value=Path("rclone")), mock.patch(
                "migrator.gdrive_auth.subprocess.Popen", return_value=_FakeOAuthProc()
            ):
                mgr.start_one_click_oauth(open_browser=False)
                deadline = time.time() + 5
                status = mgr.get_auth_status()
                while time.time() < deadline and not (status["connected"] and status["auth_error"]):
                    time.sleep(0.05)
                    status = mgr.get_auth_status()

            self.assertTrue(status["connected"])
            self.assertTrue(status["auth_error"])
            self.assertEqual(conf.read_text(encoding="utf-8"), self.TOKEN_CONF)
            self.assertFalse((Path(td) / "rclone.conf.prev").exists())

    def test_parked_login_is_restored_on_startup(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            (Path(td) / "rclone.conf.prev").write_text(self.TOKEN_CONF, encoding="utf-8")
            mgr = GDriveAuthManager(rclone_conf=Path(td) / "rclone.conf")
            self.assertTrue(mgr.is_connected())
            self.assertFalse((Path(td) / "rclone.conf.prev").exists())


class _FakeDashboardEngine:
    def __init__(self) -> None:
        self.settings = types.SimpleNamespace(active_bucket="fourvectors-staging")
        self.paused: list[str] = []

    def get_status_snapshot(self, include_cloud_quota: bool = False) -> dict:
        return {"state": "IDLE"}

    def pause_migration(self, reason: str = "Paused by user") -> None:
        self.paused.append(reason)


class DashboardGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = _FakeDashboardEngine()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), create_handler(self.engine))
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()

    def _request(self, method: str, path: str, headers: dict | None = None, body: str = "{}") -> int:
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        try:
            conn.request(method, path, body=body if method == "POST" else None, headers=headers or {})
            resp = conn.getresponse()
            resp.read()
            return resp.status
        finally:
            conn.close()

    def test_get_allowed_for_loopback_host_only(self) -> None:
        self.assertEqual(self._request("GET", "/api/status"), 200)
        self.assertEqual(self._request("GET", "/api/status", {"Host": f"evil.example:{self.port}"}), 403)

    def test_post_requires_json_and_same_origin(self) -> None:
        json_hdr = {"Content-Type": "application/json"}
        self.assertEqual(self._request("POST", "/api/pause", {"Content-Type": "text/plain"}), 415)
        self.assertEqual(self._request("POST", "/api/pause", {**json_hdr, "Origin": "http://evil.example"}), 403)
        self.assertEqual(self.engine.paused, [])

        self.assertEqual(self._request("POST", "/api/pause", json_hdr), 200)  # curl-style, no Origin
        same_origin = {**json_hdr, "Origin": f"http://127.0.0.1:{self.port}"}
        self.assertEqual(self._request("POST", "/api/pause", same_origin), 200)
        self.assertEqual(len(self.engine.paused), 2)

    def test_allowed_hosts_for_bind_address(self) -> None:
        self.assertIsNone(allowed_hosts_for("0.0.0.0"))
        self.assertEqual(allowed_hosts_for("127.0.0.1"), frozenset({"127.0.0.1", "localhost", "::1"}))
        self.assertIn("my-pc", allowed_hosts_for("my-pc"))


if __name__ == "__main__":
    unittest.main()

