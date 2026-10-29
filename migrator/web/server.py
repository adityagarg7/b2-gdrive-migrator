"""Threaded HTTP API & Dashboard server using Python stdlib (zero external web dependencies)."""

from __future__ import annotations

import json
import traceback
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse

from ..engine import MigrationEngine

INDEX_HTML_PATH = Path(__file__).resolve().parent / "index.html"


def create_handler(engine: MigrationEngine):
    class DashboardHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            # Silence noisy 1-second polling logs in terminal
            return

        def _send_json(self, payload: Dict[str, Any], status: int = 200) -> None:
            raw = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(raw)

        def _read_json_body(self) -> Dict[str, Any]:
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0:
                return {}
            body = self.rfile.read(length).decode("utf-8")
            return json.loads(body) if body.strip() else {}

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            path = parsed.path
            qs = parse_qs(parsed.query)

            try:
                if path in ("/", "/index.html"):
                    html = INDEX_HTML_PATH.read_bytes()
                    self.send_response(HTTPStatus.OK)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(html)))
                    self.send_header("Cache-Control", "no-store")
                    self.end_headers()
                    self.wfile.write(html)
                    return

                if path == "/api/status":
                    include_quota = qs.get("quota", ["0"])[0] == "1"
                    self._send_json(engine.get_status_snapshot(include_cloud_quota=include_quota))
                    return

                if path == "/api/plan":
                    bname = qs.get("bucket", [engine.settings.active_bucket])[0]
                    self._send_json(engine.preview_bucket_plan(bucket_name=bname))
                    return

                if path == "/api/files":
                    bname = qs.get("bucket", [engine.settings.active_bucket])[0]
                    status_filter = qs.get("status", [""])[0] or None
                    limit = min(500, max(10, int(qs.get("limit", ["120"])[0])))
                    files = engine.db.list_bucket_files(
                        bucket_name=bname,
                        status_filter=status_filter,
                        limit=limit,
                    )
                    self._send_json({"bucket": bname, "files": files})
                    return

                self._send_json({"error": "Not found"}, status=404)
            except Exception as exc:
                traceback.print_exc()
                self._send_json({"error": str(exc)}, status=500)

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            path = parsed.path

            try:
                body = self._read_json_body()

                if path == "/api/settings":
                    updated = engine.update_settings(body)
                    self._send_json({"ok": True, "settings": updated.__dict__})
                    return

                if path == "/api/b2-credentials":
                    key_id = str(body.get("key_id") or "").strip()
                    app_key = str(body.get("application_key") or "").strip()
                    if not key_id or not app_key:
                        self._send_json({"ok": False, "error": "Both keyID and applicationKey are required."}, status=400)
                        return
                    res = engine.configure_b2_credentials(key_id, app_key)
                    self._send_json(res)
                    return

                if path == "/api/scan":
                    bucket = body.get("bucket") or None
                    started = engine.start_background_scan(bucket_name=bucket)
                    self._send_json({"ok": started})
                    return

                if path == "/api/start":
                    bucket = body.get("bucket") or None
                    prefix = body.get("prefix") or None
                    ok, msg = engine.start_migration(bucket_name=bucket, prefix_filter=prefix)
                    self._send_json({"ok": ok, "message": msg}, status=200 if ok else 400)
                    return

                if path == "/api/pause":
                    engine.pause_migration("Paused from Web Dashboard")
                    self._send_json({"ok": True})
                    return

                if path == "/api/retry-failed":
                    bucket = body.get("bucket") or engine.settings.active_bucket
                    cnt = engine.db.reset_failed_files(bucket)
                    with engine._lock:
                        if engine._state == "ERROR":
                            engine._state = "IDLE"
                            engine._state_detail = f"Re-queued failed file(s) in '{bucket}' — click Start / Resume Bucket"
                    engine.db.log_event("INFO", f"Reset {cnt} failed file(s) to PENDING", bucket)
                    self._send_json({"ok": True, "reset_count": cnt})
                    return

                if path == "/api/reset-progress":
                    if engine._state in ("RUNNING", "SCANNING", "PAUSING", "CACHE_WAIT"):
                        self._send_json({"ok": False, "error": "Pause active migration before resetting progress."}, status=400)
                        return
                    reset_all = bool(body.get("all"))
                    bucket = None if reset_all else (body.get("bucket") or engine.settings.active_bucket)
                    cnt = engine.db.reset_bucket_progress(bucket_name=bucket)
                    engine.db.clear_cached_drive_folder()
                    with engine._lock:
                        if engine._state == "COMPLETED_BUCKET":
                            engine._state = "IDLE"
                            engine._state_detail = "Ready"
                    target_desc = "all buckets" if reset_all else f"bucket '{bucket}'"
                    engine.db.log_event("INFO", f"Reset progress on {target_desc} ({cnt} file(s) back to PENDING)", bucket)
                    self._send_json({"ok": True, "reset_count": cnt})
                    return

                if path == "/api/auth/start":
                    open_browser = body.get("open_browser", True)
                    switch_acct = bool(body.get("switch_account", False))
                    st = engine.auth.start_one_click_oauth(
                        open_browser=bool(open_browser),
                        switch_account=switch_acct,
                    )
                    self._send_json({"ok": True, "auth": st})
                    return

                if path == "/api/auth/cancel":
                    engine.auth.cancel_oauth()
                    self._send_json({"ok": True, "auth": engine.auth.get_auth_status()})
                    return

                if path == "/api/auth/disconnect":
                    engine.auth.disconnect()
                    engine.db.clear_cached_drive_folder()
                    if engine.settings.transfer_mode == "cloud":
                        engine.update_settings({"transfer_mode": "mounted"})
                    engine.db.log_event("INFO", "Disconnected Google Drive Cloud OAuth account")
                    self._send_json({"ok": True, "auth": engine.auth.get_auth_status()})
                    return

                self._send_json({"error": "Not found"}, status=404)
            except Exception as exc:
                traceback.print_exc()
                self._send_json({"error": str(exc)}, status=500)

    return DashboardHandler


def run_web_server(
    engine: MigrationEngine,
    host: str = "127.0.0.1",
    port: int = 8765,
    auto_scan_if_empty: bool = True,
) -> ThreadingHTTPServer:
    from ..config import check_b2_credentials_status

    b2_ok, _ = check_b2_credentials_status(engine.settings.b2_key_file)
    if auto_scan_if_empty and b2_ok and not engine.db.get_bucket_summaries():
        engine.start_background_scan()
    server = ThreadingHTTPServer((host, port), create_handler(engine))
    return server

