"""
Step 11: Live loop runner (prop-ready)

Runs backtest.journal.live_signal_runner_auto (or any command) on a schedule,
with:
- per-run log files + latest.log
- crash-safe loop (exceptions don't kill the loop unless --fail_fast)
- heartbeat file

Usage (PowerShell):
  python -m backtest.journal.live_loop --every_seconds 1800 -- \
    python -m backtest.journal.live_signal_runner_auto ... (args)

The `--` separates live_loop args from the command you want to run.
"""

from __future__ import annotations
import argparse
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _ts() -> str:
    return _utc_now().strftime("%Y-%m-%d_%H-%M-%S")


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _write_text(p: Path, s: str) -> None:
    p.write_text(s, encoding="utf-8")


def run_cmd(cmd: List[str], log_path: Path, env: Optional[dict] = None) -> int:
    """Run cmd and stream stdout/stderr to console + log file."""
    _ensure_dir(log_path.parent)
    with log_path.open("w", encoding="utf-8") as f:
        f.write(f"[{_utc_now().isoformat()}] CMD: {' '.join(cmd)}\n\n")
        f.flush()

        p = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
        )
        assert p.stdout is not None
        for line in p.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
            f.write(line)
            f.flush()
        return int(p.wait())


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Run a command periodically with logs.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument("--every_seconds", type=int, default=1800, help="Loop interval (seconds).")
    ap.add_argument("--jitter_seconds", type=int, default=10, help="Random jitter added to sleep (0 disables).")
    ap.add_argument("--log_dir", default="logs/live_loop", help="Folder for logs.")
    ap.add_argument("--latest_log", default="latest.log", help="Filename for latest log copy.")
    ap.add_argument("--heartbeat_path", default="logs/live_loop/heartbeat.txt", help="Write UTC timestamp after each run.")
    ap.add_argument("--max_runs", type=int, default=0, help="0=run forever, else stop after N runs.")
    ap.add_argument("--fail_fast", action="store_true", help="Stop loop if command returns non-zero.")
    ap.add_argument("--sleep_after_fail", type=int, default=60, help="Sleep seconds after a failed run (if not fail_fast).")
    ap.add_argument("--dry_run", action="store_true", help="Print the command but do not execute.")
    ap.add_argument("cmd", nargs=argparse.REMAINDER, help="Command to run (prefix with --).")
    args = ap.parse_args()

    if not args.cmd or args.cmd == ["--"]:
        ap.error("Provide a command after --, e.g. -- python -m ...")

    cmd = args.cmd[1:] if args.cmd and args.cmd[0] == "--" else args.cmd

    log_dir = Path(args.log_dir)
    _ensure_dir(log_dir)
    hb = Path(args.heartbeat_path)
    _ensure_dir(hb.parent)

    runs = 0
    while True:
        runs += 1
        run_id = _ts()
        log_path = log_dir / f"run_{run_id}.log"
        latest_path = log_dir / args.latest_log

        print(f"\n[LOOP] run #{runs} @ {run_id} UTC")
        print(f"[LOOP] log: {log_path}")

        if args.dry_run:
            print("[LOOP] dry_run=1 -> would execute:")
            print(" ".join(cmd))
            rc = 0
        else:
            rc = run_cmd(cmd, log_path)

        # update latest log (copy)
        try:
            latest_path.write_text(log_path.read_text(encoding="utf-8"), encoding="utf-8")
        except Exception:
            pass

        # heartbeat
        try:
            _write_text(hb, _utc_now().isoformat())
        except Exception:
            pass

        if rc != 0:
            print(f"[LOOP] command exit code = {rc}")
            if args.fail_fast:
                return rc
            print(f"[LOOP] sleeping {args.sleep_after_fail}s after fail ...")
            time.sleep(max(1, int(args.sleep_after_fail)))

        if args.max_runs and runs >= args.max_runs:
            print("[LOOP] max_runs reached, exiting.")
            return 0

        sleep_s = max(1, int(args.every_seconds))
        if args.jitter_seconds and args.jitter_seconds > 0:
            try:
                import random
                sleep_s += random.randint(0, int(args.jitter_seconds))
            except Exception:
                pass

        print(f"[LOOP] sleeping {sleep_s}s ...")
        time.sleep(sleep_s)


if __name__ == "__main__":
    raise SystemExit(main())
