import subprocess
import hashlib
import sys
import os
import re
from pathlib import Path

# ===== CONFIG =====

RUNNER_CMD = [
    "python",
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

PORTFOLIO_PATH = Path("backtest/journal/exports_live/portfolio_state.json")

REQUIRED_TAGS = [
    "[WATCHDOG]",
    "[CORR_CAP]",
    "[BUDGET]",
    "[EQUITY_GOVERNOR]",
    "[KILL_SWITCH]",
]

REQUIRE_MACRO_OK = False  # set True for release freeze

# ===== HELPERS =====

def sha256_file(path):
    if not path.exists():
        return None
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()

def run_runner():
    print("Running live_signal_runner...\n")
    process = subprocess.Popen(
        RUNNER_CMD,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    output_lines = []
    for line in process.stdout:
        print(line, end="")
        output_lines.append(line)

    process.wait()
    return process.returncode, output_lines

def check_required_tags(log_lines):
    missing = []
    for tag in REQUIRED_TAGS:
        if not any(tag in line for line in log_lines):
            missing.append(tag)
    return missing

def check_macro_status(log_lines):
    for line in log_lines:
        if "[WATCHDOG]" in line:
            if "macro_meta=STALE" in line:
                return "STALE"
            if "macro_meta=OK" in line:
                return "OK"
    return "UNKNOWN"

# ===== MAIN =====

def main():

    print("\n========== SMOKE TEST START ==========\n")

    # Portfolio hash before
    before_hash = sha256_file(PORTFOLIO_PATH)
    print(f"Portfolio hash BEFORE: {before_hash}")

    # Run runner
    returncode, log_lines = run_runner()

    if returncode != 0:
        print("\n❌ FAIL: Runner crashed")
        sys.exit(1)

    # Check required tags
    missing = check_required_tags(log_lines)
    if missing:
        print(f"\n❌ FAIL: Missing required log tags: {missing}")
        sys.exit(1)

    # Macro check
    macro_status = check_macro_status(log_lines)
    print(f"\nMacro status: {macro_status}")

    if REQUIRE_MACRO_OK and macro_status != "OK":
        print("\n❌ FAIL: Macro meta not OK (release mode)")
        sys.exit(1)

    # Portfolio hash after
    after_hash = sha256_file(PORTFOLIO_PATH)
    print(f"Portfolio hash AFTER:  {after_hash}")

    if before_hash != after_hash:
        print("\n❌ FAIL: portfolio_state.json was modified!")
        sys.exit(1)

    print("\n✅ SMOKE TEST PASSED")
    print("\n========== SMOKE TEST END ==========\n")

if __name__ == "__main__":
    main()