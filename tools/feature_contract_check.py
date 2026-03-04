from __future__ import annotations

import re
import subprocess
import sys
from typing import List, Tuple

# Optional shared pattern source (if present in repo)
try:
    from tools.contract_patterns import REQUIRED_PATTERNS  # type: ignore
except Exception:
    REQUIRED_PATTERNS = {
        "core": [
            r"\[WATCHDOG\]",
            r"\[KILL_SWITCH\]",
            r"\[BUDGET\]",
            r"\[CORR_CAP\]",
            r"\[EQUITY_GOVERNOR\]",
        ],
        "spec": [
            r"\[PYRAMID\]",
            r"\[PORTFOLIO_CAP\]",
            r"\[SYMBOL_PERF\].*sharpe=",
            r"\[CROSS_ASSET\].*regime=.*strength=",
        ],
    }


def _run_runner_once() -> str:
    """
    Execute the canonical entrypoint and return combined stdout/stderr.
    """
    cmd = [sys.executable, "-m", "backtest.journal.live_signal_runner", "--once"]
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        env={**dict(**__import__("os").environ), "PYTHONUTF8": "1"},
    )
    out, _ = proc.communicate()
    return out or ""


def _check_patterns(output: str, patterns: List[str]) -> Tuple[List[str], List[str]]:
    """
    Returns (found, missing) for the provided regex patterns.
    """
    found: List[str] = []
    missing: List[str] = []

    for pat in patterns:
        if re.search(pat, output, flags=re.MULTILINE):
            found.append(pat)
        else:
            missing.append(pat)

    return found, missing


def main() -> int:
    out = _run_runner_once()

    core_patterns = list(REQUIRED_PATTERNS.get("core", []))
    spec_patterns = list(REQUIRED_PATTERNS.get("spec", []))

    _, missing_core = _check_patterns(out, core_patterns)
    _, missing_spec = _check_patterns(out, spec_patterns)

    missing_all = missing_core + missing_spec

    if missing_all:
        print("FEATURE CONTRACT CHECK: FAIL")
        print("MISSING_TAGS=" + ",".join(missing_all))
        return 2

    print("FEATURE CONTRACT CHECK: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())