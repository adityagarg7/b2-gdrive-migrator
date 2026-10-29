"""Google Drive 1-Click OAuth manager (supports multi-account switching & custom browser/Incognito link)."""

from __future__ import annotations

import configparser
import datetime as dt
import hashlib
import json
import re
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import requests

from .config import RCLONE_CONF_PATH, get_rclone_exe

AUTH_URL_RE = re.compile(r"(http://127\.0\.0\.1:\d+/auth\S*)")
TOKEN_JSON_RE = re.compile(r'(\{"access_token":.*?\})', re.DOTALL)


class GDriveAuthManager:
    """Manages Google Drive OAuth tokens, account switching, browser/Incognito login URLs, and cloud quota."""

    REMOTE_NAME = "gdrive"

    def __init__(
        self,
        rclone_conf: Path = RCLONE_CONF_PATH,
        on_account_connected: Optional[Callable[[Optional[str]], None]] = None,
    ) -> None:
        self.rclone_conf = Path(rclone_conf)
        self.rclone_conf.parent.mkdir(parents=True, exist_ok=True)
        self.on_account_connected = on_account_connected
        self._lock = threading.RLock()
        self._auth_proc: Optional[subprocess.Popen] = None
        self._auth_state: str = "IDLE"  # IDLE | SIGNING_IN | ERROR
        self._auth_error: Optional[str] = None
        self._auth_url: Optional[str] = None
        self._auth_logs: list[str] = []
        self._reader_done: bool = False
        self._cached_quota: Optional[Dict[str, Any]] = None
        self._cached_quota_ts: float = 0.0
        self._notified_connected: bool = self.is_connected()

    def is_connected(self) -> bool:
        tok = self._read_token_dict()
        return bool(tok and (tok.get("access_token") or tok.get("refresh_token")))

    def get_account_fingerprint(self) -> str:
        """Return a stable, zero-network-call identifier for the currently signed-in OAuth token."""
        tok = self._read_token_dict()
        if not tok:
            return "disconnected"
        seed = str(tok.get("refresh_token") or tok.get("access_token") or "cloud")
        return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:12]

    def get_connected_email(self) -> Optional[str]:
        if not self.is_connected():
            return None
        if self._cached_quota and self._cached_quota.get("email"):
            return str(self._cached_quota["email"])
        try:
            info = self.get_cloud_drive_info(force=False)
            return info.get("email")
        except Exception:
            return None

    def get_auth_status(self) -> Dict[str, Any]:
        newly_connected = False
        with self._lock:
            if self._auth_proc is not None:
                ret = self._auth_proc.poll()
                if ret is None or not self._reader_done:
                    self._auth_state = "SIGNING_IN"
                else:
                    self._auth_proc = None
                    if ret == 0 and self.is_connected():
                        self._auth_state = "IDLE"
                        self._auth_error = None
                        self._auth_url = None
                        self._cached_quota = None
                        if not self._notified_connected:
                            self._notified_connected = True
                            newly_connected = True
                    else:
                        self._auth_state = "ERROR"
                        tail = "\n".join(self._auth_logs[-5:]).strip()
                        self._auth_error = tail[-300:] if tail else f"OAuth exited with code {ret}"
                        self._auth_url = None

            connected = self.is_connected()
            rclone_exe = get_rclone_exe(auto_download=False)
            status = {
                "connected": connected,
                "auth_state": "CONNECTED" if connected else self._auth_state,
                "auth_url": None if connected else self._auth_url,
                "auth_error": self._auth_error,
                "rclone_available": rclone_exe is not None,
                "rclone_path": str(rclone_exe) if rclone_exe else None,
            }

        if newly_connected and self.on_account_connected:
            try:
                email = self.get_connected_email()
                self.on_account_connected(email)
            except Exception:
                pass
        return status

    def start_one_click_oauth(self, open_browser: bool = True, switch_account: bool = False) -> Dict[str, Any]:
        """
        Launch non-blocking Google OAuth flow.
        - If switch_account=True, disconnects the previous Google account first.
        - If open_browser=False, passes --auth-no-open-browser and captures the local URL
          so the user can paste it into any Chrome profile / Incognito / Edge window!
        """
        with self._lock:
            rclone_exe = get_rclone_exe()
            if not rclone_exe:
                self._auth_state = "ERROR"
                self._auth_error = "rclone.exe not found in bin/ or PATH."
                return self.get_auth_status()

            if switch_account or self._auth_proc is not None:
                self.cancel_oauth()
            if switch_account:
                if self.rclone_conf.exists():
                    self.rclone_conf.unlink(missing_ok=True)
                self._cached_quota = None
                self._notified_connected = False

            self._auth_state = "SIGNING_IN"
            self._auth_error = None
            self._auth_url = None
            self._auth_logs = []
            self._reader_done = False
            self._notified_connected = False

            if self.rclone_conf.exists():
                self.rclone_conf.unlink(missing_ok=True)

            cmd = [
                str(rclone_exe),
                "authorize",
                "drive",
            ]
            if not open_browser:
                cmd.append("--auth-no-open-browser")

            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                creationflags=creationflags,
            )
            self._auth_proc = proc

            def _reader() -> None:
                assert proc.stdout is not None
                collected: list[str] = []
                for line in proc.stdout:
                    ln = line.strip()
                    if not ln:
                        continue
                    collected.append(ln)
                    with self._lock:
                        self._auth_logs.append(ln)
                        m = AUTH_URL_RE.search(ln)
                        if m:
                            self._auth_url = m.group(1)
                proc.wait()
                full_out = "\n".join(collected)
                tm = TOKEN_JSON_RE.search(full_out)
                with self._lock:
                    if proc.returncode == 0 and tm:
                        token_json = tm.group(1).strip()
                        conf_text = f"[{self.REMOTE_NAME}]\ntype = drive\nscope = drive\ntoken = {token_json}\n"
                        self.rclone_conf.write_text(conf_text, encoding="utf-8")
                    self._reader_done = True

            threading.Thread(target=_reader, daemon=True, name="rclone-oauth-reader").start()

        # Wait up to 1.2s so _auth_url is already populated on the very first response!
        deadline = time.time() + 1.2
        while time.time() < deadline:
            with self._lock:
                if self._auth_url or (self._auth_proc and self._auth_proc.poll() is not None):
                    break
            time.sleep(0.1)

        return self.get_auth_status()

    def cancel_oauth(self) -> None:
        with self._lock:
            if self._auth_proc is not None and self._auth_proc.poll() is None:
                self._auth_proc.terminate()
            self._auth_proc = None
            self._auth_url = None
            self._auth_state = "IDLE"

    def disconnect(self) -> None:
        with self._lock:
            self.cancel_oauth()
            if self.rclone_conf.exists():
                self.rclone_conf.unlink(missing_ok=True)
            self._cached_quota = None
            self._notified_connected = False

    def _read_token_dict(self) -> Optional[Dict[str, Any]]:
        if not self.rclone_conf.exists():
            return None
        cp = configparser.ConfigParser(interpolation=None)
        try:
            cp.read(self.rclone_conf, encoding="utf-8")
            if self.REMOTE_NAME not in cp:
                return None
            raw_tok = cp[self.REMOTE_NAME].get("token", "").strip()
            if not raw_tok:
                return None
            return json.loads(raw_tok)
        except Exception:
            return None

    def _token_needs_refresh(self, tok: Dict[str, Any]) -> bool:
        expiry_str = tok.get("expiry")
        if not expiry_str:
            return False
        try:
            clean = expiry_str.replace("Z", "+00:00")
            exp = dt.datetime.fromisoformat(clean)
            now = dt.datetime.now(dt.timezone.utc)
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=dt.timezone.utc)
            return (exp - now).total_seconds() < 600
        except Exception:
            return True

    def get_valid_access_token(self, force_refresh: bool = False) -> str:
        """Return a valid Google OAuth2 Bearer access_token, refreshing via rclone if needed."""
        with self._lock:
            tok = self._read_token_dict()
            if not tok:
                raise RuntimeError(
                    "Google Drive Cloud OAuth is not connected yet. Sign into your target Google account first."
                )

            if force_refresh or self._token_needs_refresh(tok):
                self._refresh_via_rclone()
                tok = self._read_token_dict()
                if not tok or not tok.get("access_token"):
                    raise RuntimeError("Failed to refresh Google Drive OAuth token.")

            return str(tok["access_token"])

    def _refresh_via_rclone(self) -> None:
        rclone_exe = get_rclone_exe()
        if not rclone_exe:
            raise RuntimeError("rclone.exe not found to refresh Google Drive OAuth token.")
        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        res = subprocess.run(
            [
                str(rclone_exe),
                "about",
                f"{self.REMOTE_NAME}:",
                "--json",
                "--config",
                str(self.rclone_conf),
            ],
            capture_output=True,
            text=True,
            timeout=30,
            creationflags=creationflags,
        )
        if res.returncode != 0:
            raise RuntimeError(f"rclone token refresh failed: {res.stderr.strip()[:300]}")

    def get_cloud_drive_info(self, force: bool = False) -> Dict[str, Any]:
        """Fetch signed-in Google account email, cloud storage quota, and Shared Drives."""
        if not self.is_connected():
            return {"connected": False}

        now = time.time()
        if not force and self._cached_quota and (now - self._cached_quota_ts) < 60:
            return self._cached_quota

        token = self.get_valid_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        about_resp = requests.get(
            "https://www.googleapis.com/drive/v3/about",
            params={"fields": "user(displayName,emailAddress),storageQuota"},
            headers=headers,
            timeout=15,
        )
        if about_resp.status_code == 401:
            token = self.get_valid_access_token(force_refresh=True)
            headers = {"Authorization": f"Bearer {token}"}
            about_resp = requests.get(
                "https://www.googleapis.com/drive/v3/about",
                params={"fields": "user(displayName,emailAddress),storageQuota"},
                headers=headers,
                timeout=15,
            )

        if about_resp.status_code != 200 and self._cached_quota and self._cached_quota.get("email"):
            return self._cached_quota

        info: Dict[str, Any] = {"connected": True}
        if about_resp.status_code == 200:
            data = about_resp.json()
            user = data.get("user") or {}
            sq = data.get("storageQuota") or {}
            info["email"] = user.get("emailAddress")
            info["display_name"] = user.get("displayName")
            info["limit_bytes"] = int(sq["limit"]) if "limit" in sq else None
            info["usage_bytes"] = int(sq.get("usage", 0))
            info["usage_in_drive_bytes"] = int(sq.get("usageInDrive", 0))
            info["usage_in_trash_bytes"] = int(sq.get("usageInDriveTrash", 0))
            if info["limit_bytes"] is not None:
                info["free_bytes"] = max(0, info["limit_bytes"] - info["usage_bytes"])

        drives_resp = requests.get(
            "https://www.googleapis.com/drive/v3/drives",
            params={"pageSize": 50},
            headers=headers,
            timeout=15,
        )
        if drives_resp.status_code == 200:
            info["shared_drives"] = drives_resp.json().get("drives", [])
        else:
            info["shared_drives"] = []

        self._cached_quota = info
        self._cached_quota_ts = now
        return info
