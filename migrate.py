#!/usr/bin/env python3
"""
FourVectors Backblaze B2 -> Google Drive Migration Tool (Web Dashboard + CLI).

Examples:
  python migrate.py                         # Launch Web UI Dashboard at http://127.0.0.1:8765
  python migrate.py scan                    # Scan all Backblaze B2 buckets into local SQLite catalog
  python migrate.py status                  # Show bucket progress, C: space, and quota status
  python migrate.py plan --bucket fourvectors-staging
  python migrate.py auth                    # 1-Click Google Drive OAuth login for Cloud Streaming mode
  python migrate.py run --bucket fourvectors-staging --mode mounted
"""

from __future__ import annotations

import argparse
import signal
import sys
import time
import webbrowser
from typing import Optional

from tabulate import tabulate

from migrator.engine import MigrationEngine
from migrator.web.server import run_web_server


def fmt_bytes(num: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(num) < 1024.0 or unit == "TiB":
            return f"{num:.2f} {unit}" if unit in ("GiB", "TiB") else f"{num:.1f} {unit}"
        num /= 1024.0
    return f"{num:.2f} TiB"


def cmd_scan(engine: MigrationEngine, args: argparse.Namespace) -> int:
    print(f"Scanning Backblaze B2 ({args.bucket or 'all buckets'})...")

    def _on_prog(bname: str, cnt: int, sz: int) -> None:
        print(f"  -> {bname}: {cnt:,} files ({fmt_bytes(sz)})   ", end="\r", flush=True)

    results = engine.scan_buckets_sync(bucket_name=args.bucket, on_progress=_on_prog)
    print(" " * 70, end="\r")
    rows = [[r["bucket_name"], f"{r['files']:,}", fmt_bytes(r["bytes"])] for r in results]
    print(tabulate(rows, headers=["Bucket", "Files", "Total Size"], tablefmt="github"))
    return 0


def cmd_status(engine: MigrationEngine, _args: argparse.Namespace) -> int:
    snap = engine.get_status_snapshot(include_cloud_quota=True)
    buckets = snap["buckets"]
    if not buckets:
        print("Catalog is empty. Running initial Backblaze B2 scan...")
        engine.scan_buckets_sync()
        snap = engine.get_status_snapshot(include_cloud_quota=True)
        buckets = snap["buckets"]

    rows = []
    for b in buckets:
        step = f"Step {b['step_number']}" if b.get("step_number") else "-"
        rows.append(
            [
                step,
                b["bucket_name"] + (" *" if b["bucket_name"] == snap["settings"]["active_bucket"] else ""),
                f"{b['done_files']:,} / {b['total_files']:,}",
                f"{fmt_bytes(b['done_bytes'])} / {fmt_bytes(b['total_bytes'])}",
                f"{b['progress_pct']:.1f}%",
                b["failed_files"],
            ]
        )

    print("\n=== Backblaze B2 -> Google Drive Migration Status ===")
    print(tabulate(rows, headers=["Order", "Bucket", "Files Done", "Size Done", "Progress", "Failed"], tablefmt="github"))
    print(f"\nActive Mode      : {snap['settings']['transfer_mode'].upper()}")
    print(f"Mounted G: Ready : {snap['telemetry']['mounted_available']} ({snap['telemetry']['mounted_dest_preview']})")
    print(f"Cloud API OAuth  : {'CONNECTED' if snap['auth']['connected'] else 'Not signed in (optional)'}")
    print(f"Local C: Free    : {snap['telemetry']['c_drive_free_gib']:.2f} GiB (Guard floor: {snap['settings']['min_c_free_gib']} GiB)")
    print(
        f"24h Upload Usage : {fmt_bytes(snap['telemetry']['rolling_24h_bytes'])} / "
        f"{snap['settings']['daily_upload_limit_gib']:.0f} GiB"
    )
    return 0


def cmd_plan(engine: MigrationEngine, args: argparse.Namespace) -> int:
    if not engine.db.get_bucket_summaries():
        engine.scan_buckets_sync()
    bname = args.bucket or engine.settings.active_bucket
    plan = engine.preview_bucket_plan(bname)
    if not plan.get("scanned"):
        print(f"Bucket '{bname}' not found in catalog. Run `python migrate.py scan` first.")
        return 1

    print(f"\n=== Dry-Run Plan: {bname} ===")
    print(f"Destination Mode     : {engine.settings.transfer_mode}")
    print(f"Destination Root     : {engine.settings.dest_root_folder}")
    print(f"Files to Transfer    : {plan['net_transfer_files']:,} ({fmt_bytes(plan['net_transfer_bytes'])})")
    if plan["would_dedup_now_files"]:
        print(f"Cross-Bucket Skipped : {plan['would_dedup_now_files']:,} ({fmt_bytes(plan['would_dedup_now_bytes'])})")
    if plan["archive_overlap_files"]:
        print(
            f"Archive Overlap Note : {plan['archive_overlap_files']:,} files ({fmt_bytes(plan['archive_overlap_bytes'])}) "
            f"match fourvectors-archive"
        )
    print(f"Est. Min Days @750GB : ~{plan['estimated_min_days_at_750gb_cap']} day(s)")
    print("\nSample Path Mappings:")
    mrows = [[m["source"], m["destination"], fmt_bytes(m["size_bytes"]), m["status"]] for m in plan["sample_mappings"]]
    print(tabulate(mrows, headers=["B2 Source", "Drive Destination", "Size", "Status"], tablefmt="github"))
    return 0


def cmd_auth(engine: MigrationEngine, args: argparse.Namespace) -> int:
    open_b = not getattr(args, "no_browser", False)
    sw = bool(getattr(args, "switch", False))
    print("Starting Google Drive OAuth flow...")
    st = engine.auth.start_one_click_oauth(open_browser=open_b, switch_account=sw)
    if st.get("auth_state") == "ERROR":
        print(f"Error: {st.get('auth_error')}")
        return 1
    if st.get("auth_url"):
        print(f"\nSign-in link (open in your target Google account's browser profile):\n  {st['auth_url']}\n")
    while True:
        cur = engine.auth.get_auth_status()
        if cur["connected"]:
            info = engine.auth.get_cloud_drive_info(force=True)
            email = info.get("email") or "Google Account"
            print(f"\nConnected successfully as {email}!")
            engine.update_settings({"transfer_mode": "cloud"})
            print("Default transfer mode switched to 'cloud' (Direct B2 -> Drive API streaming).")
            return 0
        if cur["auth_state"] == "ERROR":
            print(f"\nSign-in failed: {cur.get('auth_error')}")
            return 1
        time.sleep(1.0)


def cmd_reset(engine: MigrationEngine, args: argparse.Namespace) -> int:
    bucket = None if args.all else (args.bucket or engine.settings.active_bucket)
    cnt = engine.db.reset_bucket_progress(bucket_name=bucket)
    engine.db.clear_cached_drive_folder()
    target = "ALL buckets" if args.all else f"bucket '{bucket}'"
    print(f"Reset {cnt:,} file(s) in {target} back to PENDING.")
    return 0


def cmd_run(engine: MigrationEngine, args: argparse.Namespace) -> int:
    updates = {}
    if args.bucket:
        updates["active_bucket"] = args.bucket
    if args.mode:
        updates["transfer_mode"] = args.mode
    if args.dest:
        updates["dest_root_folder"] = args.dest
    if args.workers:
        updates["max_workers"] = args.workers
    if args.normalize_categories:
        updates["normalize_categories"] = True
    if updates:
        engine.update_settings(updates)

    if not engine.db.get_bucket_summaries():
        print("Scanning Backblaze B2 catalog first...")
        engine.scan_buckets_sync()

    ok, msg = engine.start_migration(bucket_name=engine.settings.active_bucket, prefix_filter=args.prefix)
    if not ok:
        print(f"Cannot start migration: {msg}")
        return 1

    print(f"{msg} (Press Ctrl+C anytime to safely checkpoint and pause)\n")

    def _sigint(_sig, _frame):
        print("\nCtrl+C received — saving checkpoints and pausing gracefully...")
        engine.pause_migration("Paused via CLI Ctrl+C")

    signal.signal(signal.SIGINT, _sigint)

    while True:
        snap = engine.get_status_snapshot(include_cloud_quota=False)
        st = snap["state"]
        act_b = next((b for b in snap["buckets"] if b["bucket_name"] == snap["settings"]["active_bucket"]), None)
        speed_mib = snap["telemetry"]["speed_bps"] / (1024 * 1024)
        pct = act_b["progress_pct"] if act_b else 0.0
        done_f = act_b["done_files"] if act_b else 0
        tot_f = act_b["total_files"] if act_b else 0
        c_free = snap["telemetry"]["c_drive_free_gib"]

        line = (
            f"[{st}] {snap['settings']['active_bucket']}: {pct:5.1f}% "
            f"({done_f}/{tot_f} files) | {speed_mib:6.2f} MiB/s | C: free {c_free:.1f} GiB   "
        )
        print(line[:115], end="\r", flush=True)

        if st not in ("RUNNING", "CACHE_WAIT", "PAUSING"):
            print("\n" + snap["state_detail"])
            return 0 if st in ("COMPLETED_BUCKET", "PAUSED", "IDLE") else 1
        time.sleep(1.0)


def cmd_dashboard(engine: MigrationEngine, args: argparse.Namespace) -> int:
    url = f"http://{args.host}:{args.port}"
    server = run_web_server(engine, host=args.host, port=args.port, auto_scan_if_empty=True)
    print("===============================================================")
    print(" FourVectors Backblaze B2 -> Google Drive Migration Dashboard")
    print(f" Local URL : {url}")
    print(" Press Ctrl+C to stop server (checkpoints are saved in SQLite)")
    if args.host not in ("127.0.0.1", "localhost", "::1"):
        print(" WARNING: the dashboard has no login; anyone who can reach this address controls the migration.")
    print("===============================================================")
    if not args.no_browser:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down dashboard...")
        engine.pause_migration("Dashboard server stopped")
        server.shutdown()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backblaze B2 to Google Drive Migration Tool")
    sub = parser.add_subparsers(dest="command")

    p_dash = sub.add_parser("dashboard", help="Launch the local Web UI Dashboard (default)")
    p_dash.add_argument("--host", default="127.0.0.1")
    p_dash.add_argument("--port", type=int, default=8765)
    p_dash.add_argument("--no-browser", action="store_true")

    p_scan = sub.add_parser("scan", help="Scan Backblaze B2 buckets into SQLite catalog")
    p_scan.add_argument("--bucket", help="Scan a specific bucket name only")

    sub.add_parser("status", help="Show current catalog & migration status")

    p_plan = sub.add_parser("plan", help="Preview migration plan and path mappings for a bucket")
    p_plan.add_argument("--bucket", help="Bucket name to preview")

    p_auth = sub.add_parser("auth", help="1-Click Google Drive browser OAuth for Direct Cloud Streaming")
    p_auth.add_argument("--no-browser", action="store_true", help="Print sign-in link instead of opening default browser")
    p_auth.add_argument("--switch", action="store_true", help="Disconnect current account and sign into a different Google account")

    p_reset = sub.add_parser("reset", help="Reset bucket progress back to PENDING (e.g., when switching target Google account)")
    p_reset.add_argument("--bucket", help="Bucket name to reset")
    p_reset.add_argument("--all", action="store_true", help="Reset all buckets")

    p_run = sub.add_parser("run", help="Run migration in terminal with live progress")
    p_run.add_argument("--bucket", help="Bucket to migrate (e.g., fourvectors-staging)")
    p_run.add_argument("--mode", choices=["mounted", "cloud"], help="Transfer backend")
    p_run.add_argument("--dest", help="Destination root folder name in Google Drive")
    p_run.add_argument("--workers", type=int, help="Number of parallel transfer workers")
    p_run.add_argument("--prefix", help="Optional prefix filter inside bucket")
    p_run.add_argument("--normalize-categories", action="store_true", help="Merge split category folder names")

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    engine = MigrationEngine()

    cmd = args.command or "dashboard"
    if cmd == "dashboard":
        if not hasattr(args, "host"):
            args.host = "127.0.0.1"
            args.port = 8765
            args.no_browser = False
        return cmd_dashboard(engine, args)
    if cmd == "scan":
        return cmd_scan(engine, args)
    if cmd == "status":
        return cmd_status(engine, args)
    if cmd == "plan":
        return cmd_plan(engine, args)
    if cmd == "auth":
        return cmd_auth(engine, args)
    if cmd == "reset":
        return cmd_reset(engine, args)
    if cmd == "run":
        return cmd_run(engine, args)
    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())

