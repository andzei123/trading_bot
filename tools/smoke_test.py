#!/usr/bin/env python3
"""
Smoke test runner (Windows-safe):

- Uses current interpreter (sys.executable)
- Avoids Unicode/emoji in output (ASCII only) to survive cp1251/cp866 pipes
- Closes subprocess pipes via communicate() (avoids ResourceWarning under -W error)
"""
from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


REQUIRED_TAGS = [
    "[BOOT]",
    "[WATCHDOG]",
]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    if not path.exists():
        return "MISSING"
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def run_runner() -> tuple[int, list[str]]:
    print("Running live_signal_runner...\n")

    runner_cmd = [
        sys.executable,  # always use current venv interpreter
        "-m",
        "backtest.journal.live_signal_runner",
        "--once",
        "--debug_regime",
        "--emit_last_candles",
        "200",
        "--regime_min_trades",
        "0",
        "--debug_entry_filters",
    ]

    # Force UTF-8 in the child process (best-effort). Even if the parent console is cp1251,
    # we decode bytes as utf-8 with replacement, and print ASCII markers only.
    env = os.environ.copy()
    env.setdefault("PYTHONUTF8", "1")
    env.setdefault("PYTHONIOENCODING", "utf-8")

    # Use communicate() to ensure pipes are always closed (avoids ResourceWarning on Windows)
    proc = subprocess.Popen(
        runner_cmd,
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    out, _ = proc.communicate()
    out = out or ""
    lines = out.splitlines(keepends=True)

    # Replay output to our stdout
    for line in lines:
        print(line, end="")

    return proc.returncode, lines


def main() -> int:
    print("\n========== SMOKE TEST START ==========\n")

    portfolio_state = ROOT / "backtest" / "journal" / "exports_live" / "portfolio_state.json"
    h_before = sha256_file(portfolio_state)
    print(f"Portfolio hash BEFORE: {h_before}")

    returncode, log_lines = run_runner()

    combined = "".join(log_lines)

    missing = [tag for tag in REQUIRED_TAGS if tag not in combined]
    if missing:
        print(f"\nFAIL: missing required tags: {missing}")
        return 2

    print("\nMacro status: UNKNOWN")
    h_after = sha256_file(portfolio_state)
    print(f"Portfolio hash AFTER:  {h_after}\n")

    if returncode == 0:
        print("SMOKE TEST PASSED")
        print("\n========== SMOKE TEST END ==========\n")
        return 0

    print("SMOKE TEST FAILED")
    print("\n========== SMOKE TEST END ==========\n")
    return returncode if isinstance(returncode, int) else 1


if __name__ == "__main__":
    raise SystemExit(main())
