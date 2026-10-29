# FourVectors Backblaze B2 → Google Drive Migrator

Purpose-built migration studio (Web Dashboard + CLI) for copying multi-terabyte egocentric video datasets (`fourvectors-staging`, `fourvectors-archive`, `fourvectors-16-sep-27`, `fourvectors-rejected`) from Backblaze B2 to Google Drive.

> **100% Read-Only on Backblaze B2:** Only reads/downloads from Backblaze B2—never deletes or modifies source files.

---

## Setup on Office PC (Always-On Machine)

1. **Clone the repository** (requires Python 3.10+ installed on Windows):
   ```powershell
   git clone <YOUR_GITHUB_REPO_URL>
   cd b2-gdrive-migrator
   ```
2. **Double-click `setup_and_run.bat`** (or run `setup_and_run.bat` in PowerShell):
   - Automatically installs `requests` + `tabulate`
   - Automatically downloads portable `bin\rclone.exe` on first sign-in
   - Prevents Windows from idle-sleeping whenever a transfer is actively running
   - Opens the Web Dashboard at http://127.0.0.1:8765
3. **In the Web Dashboard (http://127.0.0.1:8765):**
   - Paste your Backblaze read-only `keyID` & `applicationKey` and click **Save & Scan** (saved locally in gitignored `data/b2_credentials.json`).
   - Click **🔑 Sign In (Open Browser)** (or **📋 Link for Other Profile / Incognito**) to connect your target Google Drive account (e.g., `egodatasets@gmail.com`) in **Direct Cloud Stream** mode (`0 GB` local disk used).
   - Select your bucket and click **▶ Start / Resume Bucket**!

*(Tip: If you also want to carry over your existing checkpoint progress from another PC, copy `data\migration_state.sqlite3` into `b2-gdrive-migrator\data\`—optional, since Direct Cloud Stream also automatically detects & skips files that already exist on Google Drive with matching size and SHA-1!)*

---

## Key Capabilities

- **Direct Cloud Streaming Mode (`0 GB` Local Disk Used):** Streams Backblaze B2 HTTP Range chunks directly in RAM (`32 MiB` chunks) to Google Drive API v3 Resumable Uploads, persisting 7-day resumable session URIs in SQLite with automatic backoff across per-minute API rate limits and mid-stream network drops.
- **Mounted `G:\` Drive Mode:** Optional local Google Drive for Desktop mode with an automatic `C:\` free-space guard + `.b2part` byte-level resume.
- **End-to-End SHA-1 Verification:** Verifies Backblaze B2's `contentSha1` / `fileInfo.large_file_sha1` against Google Drive's server `sha1Checksum` on every file.
- **Google Drive 750 GB/Day Quota Auto-Pause & Auto-Resume:** Automatically pauses before/at Google's 750 GB rolling 24-hour upload cap and resumes automatically once the 24-hour window frees up.
- **Cross-Bucket Deduplication:** Automatically skips clips already migrated to the same Google account from another bucket (saving ~1.63 TiB / 356 duplicate `.mp4` files between `fourvectors-16-sep-27` and `fourvectors-archive`).

## CLI Reference

```powershell
python migrate.py dashboard                        # Launch Web UI at http://127.0.0.1:8765
python migrate.py scan                             # Scan all Backblaze B2 buckets
python migrate.py status                           # View per-bucket progress & 24h quota
python migrate.py plan --bucket fourvectors-archive
python migrate.py auth --switch                    # Sign into / switch Google Drive account
python migrate.py run --bucket fourvectors-archive --mode cloud
python migrate.py reset --bucket fourvectors-staging
```
