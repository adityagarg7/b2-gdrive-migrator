"""Configuration, credential discovery, portable rclone bootstrap, and path mapping helpers."""

from __future__ import annotations

import io
import json
import os
import platform
import shutil
import stat
import urllib.request
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
BIN_DIR = PROJECT_ROOT / "bin"
DB_PATH = DATA_DIR / "migration_state.sqlite3"
SETTINGS_PATH = DATA_DIR / "settings.json"
RCLONE_CONF_PATH = DATA_DIR / "rclone.conf"
B2_CREDS_JSON_PATH = DATA_DIR / "b2_credentials.json"
DESKTOP_DIR = Path.home() / "Desktop"

# Default recommended bucket migration order (starting with staging as requested)
RECOMMENDED_BUCKET_ORDER = [
    "fourvectors-staging",
    "fourvectors-archive",
    "fourvectors-16-sep-27",
    "fourvectors-rejected",
]

# Optional category prefix normalization map (if user enables category merging)
CATEGORY_NORMALIZATION_MAP: Dict[str, str] = {
    "carpentry-furniture-workshop": "carpentry-workshop",
    "metal-fabrication-welding": "metal-fab-welding",
    "restaurant-commercial-kitchen": "restaurant-kitchen",
    "plumbing-hvac-electrical": "trade-services-hvac-electrical",
}


@dataclass
class MigrationSettings:
    # Transfer mode: "cloud" (Direct B2 -> Drive v3 API RAM stream) or "mounted" (G:\ Drive with C:\ guard)
    transfer_mode: str = "cloud"
    # Selected bucket to migrate
    active_bucket: str = "fourvectors-staging"
    # Destination location type: "my_drive" or "shared_drive"
    drive_location: str = "my_drive"
    # Shared drive name/ID if drive_location == "shared_drive"
    shared_drive_name: str = "FV-status"
    shared_drive_id: str = ""
    # Root folder inside My Drive or Shared Drive
    dest_root_folder: str = "FourVectors-B2-Migration"
    # Mounted Google Drive letter/root on Windows
    mounted_drive_root: str = "G:\\"
    # Keep bucket name as a subfolder under dest_root_folder (recommended: True)
    include_bucket_folder: bool = True
    # Normalize split category folder names (default False = preserve exact B2 paths)
    normalize_categories: bool = False
    # Skip files already migrated from another bucket matching SHA-1 (or exact basename + byte size)
    skip_cross_bucket_duplicates: bool = True
    # Minimum free space on C:\ (in GiB) before Mounted G:\ mode pauses for DriveFS cache flush
    min_c_free_gib: float = 15.0
    # Rolling 24-hour upload soft limit in GiB (Google Drive hard cap is ~750 GB = ~698.5 GiB)
    daily_upload_limit_gib: float = 690.0
    # Stream chunk size in MiB (must be multiple of 256 KiB; 32 MiB minimizes Drive API query quota)
    chunk_size_mib: int = 32
    # Parallel file workers (1-3 recommended for multi-GB video streams)
    max_workers: int = 2
    # Path to Backblaze key file (optional when configured via Web UI / data/b2_credentials.json)
    b2_key_file: str = str(DESKTOP_DIR / "codex-read-only-key.txt")

    def save(self, path: Path = SETTINGS_PATH) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(asdict(self), indent=2), encoding="utf-8")
        tmp.replace(path)

    @classmethod
    def load(cls, path: Path = SETTINGS_PATH) -> "MigrationSettings":
        if not path.exists():
            settings = cls()
            if not RCLONE_CONF_PATH.exists() and Path("G:\\My Drive").exists():
                settings.transfer_mode = "mounted"
            return settings
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            valid_keys = {f.name for f in cls.__dataclass_fields__.values()}
            filtered = {k: v for k, v in raw.items() if k in valid_keys}
            return cls(**filtered)
        except Exception:
            return cls()


def save_b2_credentials(key_id: str, application_key: str) -> None:
    """Save Backblaze B2 credentials locally in gitignored data/b2_credentials.json."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload = {"key_id": key_id.strip(), "application_key": application_key.strip()}
    B2_CREDS_JSON_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def parse_b2_key_file(file_path: str | Path) -> Tuple[str, str, Optional[str]]:
    """
    Resolve Backblaze B2 credentials from:
      1. Environment variables (B2_KEY_ID + B2_APPLICATION_KEY)
      2. Local gitignored data/b2_credentials.json (set via Web UI or CLI)
      3. Key txt file in project folder or Desktop
    Returns (key_id, application_key, restricted_bucket_name_or_None).
    """
    env_id = os.environ.get("B2_KEY_ID") or os.environ.get("B2_APPLICATION_KEY_ID")
    env_key = os.environ.get("B2_APPLICATION_KEY")
    if env_id and env_key:
        return env_id.strip(), env_key.strip(), None

    if B2_CREDS_JSON_PATH.exists():
        try:
            saved = json.loads(B2_CREDS_JSON_PATH.read_text(encoding="utf-8"))
            if saved.get("key_id") and saved.get("application_key"):
                return str(saved["key_id"]).strip(), str(saved["application_key"]).strip(), None
        except Exception:
            pass

    p = Path(file_path) if file_path else Path()
    if not p.is_file():
        candidates = [
            PROJECT_ROOT / "codex-read-only-key.txt",
            PROJECT_ROOT / "b2-key.txt",
            DESKTOP_DIR / "codex-read-only-key.txt",
            DESKTOP_DIR / "b2-key-raw.txt",
            DESKTOP_DIR / "b2-key-rejected.txt",
        ]
        for cp in candidates:
            if cp.is_file():
                p = cp
                break

    if not p.is_file():
        raise FileNotFoundError(
            "Backblaze B2 key not configured yet. Enter your keyID & applicationKey in the Web Dashboard "
            "(or place codex-read-only-key.txt on Desktop)."
        )

    lines = [ln.strip() for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]

    lower_map: Dict[str, str] = {}
    for i, ln in enumerate(lines):
        clean = ln.rstrip(":").lower()
        if clean in ("keyid", "key_id", "applicationkeyid") and i + 1 < len(lines):
            lower_map["key_id"] = lines[i + 1]
        elif clean in ("applicationkey", "application_key", "appkey") and i + 1 < len(lines):
            lower_map["app_key"] = lines[i + 1]
        elif ":" in ln:
            k, v = ln.split(":", 1)
            k_clean = k.strip().lower()
            v_clean = v.strip()
            if v_clean:
                if k_clean in ("keyid", "key_id"):
                    lower_map["key_id"] = v_clean
                elif k_clean in ("applicationkey", "application_key"):
                    lower_map["app_key"] = v_clean

    if "key_id" in lower_map and "app_key" in lower_map:
        return lower_map["key_id"], lower_map["app_key"], None

    if len(lines) >= 2:
        bucket_hint = lines[2] if len(lines) >= 3 else None
        return lines[0], lines[1], bucket_hint

    raise ValueError(f"Could not parse Backblaze keyID and applicationKey from {p}")


def check_b2_credentials_status(file_path: str | Path) -> Tuple[bool, str]:
    """Return (is_configured, masked_key_id_or_hint) without throwing."""
    try:
        kid, _, _ = parse_b2_key_file(file_path)
        masked = f"{kid[:8]}…{kid[-4:]}" if len(kid) > 12 else kid
        return True, masked
    except Exception:
        return False, "Not configured"


def compute_relative_dest_path(
    bucket_name: str,
    b2_file_name: str,
    include_bucket_folder: bool = True,
    normalize_categories: bool = False,
) -> str:
    """Compute forward-slash relative path inside the destination root folder."""
    parts = [seg for seg in b2_file_name.split("/") if seg]
    if normalize_categories and len(parts) >= 2:
        parts[0] = CATEGORY_NORMALIZATION_MAP.get(parts[0], parts[0])
    rel = "/".join(parts)
    if include_bucket_folder:
        return f"{bucket_name}/{rel}"
    return rel


def get_rclone_exe(auto_download: bool = True) -> Optional[Path]:
    """Locate portable rclone binary in project bin/, system PATH, or auto-download on first use."""
    exe_name = "rclone.exe" if os.name == "nt" else "rclone"
    local_exe = BIN_DIR / exe_name
    if local_exe.exists():
        return local_exe
    found = shutil.which("rclone")
    if found:
        return Path(found)
    if auto_download:
        try:
            return ensure_rclone_installed()
        except Exception:
            return None
    return None


def ensure_rclone_installed() -> Path:
    """Download official portable rclone binary into bin/ if missing (keeps Git repo lightweight)."""
    exe_name = "rclone.exe" if os.name == "nt" else "rclone"
    target = BIN_DIR / exe_name
    if target.exists():
        return target

    sys_name = platform.system().lower()
    mach = platform.machine().lower()
    arch = "arm64" if mach in ("arm64", "aarch64") else "amd64"
    if sys_name == "windows":
        os_slug = "windows"
    elif sys_name == "darwin":
        os_slug = "osx"
    else:
        os_slug = "linux"

    url = f"https://downloads.rclone.org/rclone-current-{os_slug}-{arch}.zip"
    BIN_DIR.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(url, headers={"User-Agent": "b2-gdrive-migrator/1.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        raw_zip = resp.read()

    with zipfile.ZipFile(io.BytesIO(raw_zip)) as zf:
        for member in zf.namelist():
            if member.endswith(f"/{exe_name}") or member == exe_name:
                data = zf.read(member)
                target.write_bytes(data)
                if os.name != "nt":
                    target.chmod(target.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
                return target

    raise RuntimeError(f"Could not extract {exe_name} from {url}")
