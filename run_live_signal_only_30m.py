"""Locked, repeatable live signal-only runner.

This wrapper reads config/live.json and runs the existing
backtest.journal.live_signal_runner module with fixed arguments.

Why a wrapper?
- avoids editing the core runner during live tests
- keeps config in one place
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def main() -> int:
    repo_root = Path(__file__).resolve().parent
    cfg_path = repo_root / "config" / "live.json"
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))

    symbols = cfg.get("symbols", [])
    if not symbols:
        print("No symbols in config/live.json")
        return 2

    paths = cfg.get("paths", {})
    strat = cfg.get("strategy", {})

    cmd = [
        sys.executable,
        "-m",
        "backtest.journal.live_signal_runner",
        "--out",
        str(paths.get("out_csv", "backtest/journal/live_entries.csv")),
        "--state",
        str(paths.get("state_path", "backtest/journal/live_state.txt")),
        "--interval",
        str(int(cfg.get("loop_interval_seconds", 60))),
        "--mode",
        str(strat.get("mode", "combined")),
        "--rr",
        str(float(strat.get("rr", 2.0))),
        "--sl_atr_buffer",
        str(float(strat.get("sl_atr_buffer", 0.15))),
    ]

    if bool(strat.get("require_impulse_before_tdp", True)):
        cmd.append("--require_impulse_before_tdp")

    cmd += [
        "--impulse_lookback",
        str(int(strat.get("impulse_lookback", 10))),
        "--impulse_size_atr",
        str(float(strat.get("impulse_size_atr", 1.0))),
        "--tdp_dev_lookback",
        str(int(strat.get("tdp_dev_lookback", 8))),
        "--tts_retest_lookback",
        str(int(strat.get("tts_retest_lookback", 24))),
        "--source",
        str(cfg.get("source", "bybit")),
        "--bybit_category",
        str(cfg.get("bybit_category", "linear")),
        "--bybit_interval",
        str(cfg.get("bybit_interval", "30")),
        "--bybit_candles",
        str(int(cfg.get("bybit_candles", 1500))),
        "--symbols",
        ",".join(symbols),
        "--emit_last_candles",
        str(int(cfg.get("emit_last_candles", 6))),
    ]

    print("[WRAPPER] running:")
    print(" ".join(cmd))
    return subprocess.call(cmd, cwd=str(repo_root))


if __name__ == "__main__":
    raise SystemExit(main())
