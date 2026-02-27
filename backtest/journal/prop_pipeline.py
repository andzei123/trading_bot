"""
Step 10: One-command PROP pipeline.

Runs (in order):
  1) run_entry_model_multi  -> exports_trades/trades_simulated.csv
  2) analyze_trades_multi   -> exports_reports/*.csv
  3) engine equity curve    -> reports/equity_simulated/equity_curve.csv
  4) live_signal_runner_auto (dry_run optional) -> executes live_signal_runner per allowed symbol

Usage (PowerShell):
  python -m backtest.journal.prop_pipeline --symbols BTCUSDT,ETHUSDT,XRPUSDT

This script shells out to your existing modules, so logic stays centralized.
"""
from __future__ import annotations

import argparse
import os
import sys
import subprocess
from pathlib import Path
from typing import List


def _py() -> str:
    return sys.executable


def _run(cmd: List[str], *, cwd: str | None = None) -> None:
    print("\n" + "=" * 120)
    print("CMD:", " ".join(cmd))
    print("=" * 120 + "\n")
    p = subprocess.run(cmd, cwd=cwd)
    if p.returncode != 0:
        raise SystemExit(p.returncode)


def _ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Run full prop-ready pipeline: backtest -> reports -> equity -> auto live allowlist with guards."
    )

    ap.add_argument("--symbols", default="BTCUSDT,ETHUSDT,XRPUSDT", help="Comma-separated symbols.")
    ap.add_argument("--source", default="bybit", choices=["bybit", "csv"])
    ap.add_argument("--bybit_category", default="linear")
    ap.add_argument("--bybit_interval", default="30")
    ap.add_argument("--bybit_candles_backtest", type=int, default=3000, help="Candles used for run_entry_model_multi.")
    ap.add_argument("--bybit_candles_live", type=int, default=1500, help="Candles used for live_signal_runner.")
    ap.add_argument("--rr", type=float, default=2.0)
    ap.add_argument("--sl_atr_buffer", type=float, default=0.15)

    # Reports
    ap.add_argument("--top_n_reports", type=int, default=20)
    ap.add_argument("--reports_dir", default="backtest/journal/exports_reports")
    ap.add_argument("--trades_csv", default="backtest/journal/exports_trades/trades_simulated.csv")

    # Equity
    ap.add_argument("--equity_out_dir", default="reports/equity_simulated")
    ap.add_argument("--equity", type=float, default=10000.0)
    ap.add_argument("--risk", type=float, default=0.01, help="Risk per trade (fraction), used by engine.py.")
    ap.add_argument("--engine_path", default="backtest/engine.py", help="Path to engine script.")

    # Auto allowlist + guards
    ap.add_argument("--leaderboard_csv", default="backtest/journal/exports_reports/summary_by_symbol.csv")
    ap.add_argument("--metric", default="total_R", help="Leaderboard metric column (e.g. total_R, exp_R, expectancy_R).")
    ap.add_argument("--top_n_symbols", type=int, default=5)
    ap.add_argument("--monthly_risk_csv", default="backtest/journal/exports_reports/monthly_risk_by_symbol.csv")
    ap.add_argument("--bad_month_r", type=float, default=-10.0)
    ap.add_argument("--bad_month_min_trades", type=int, default=20)
    ap.add_argument("--monthly_action", default="neutral", choices=["off", "neutral"],
                    help="off=skip symbol, neutral=pass-through as 'none' to live runner.")
    ap.add_argument("--maxdd_threshold", type=float, default=-25.0)
    ap.add_argument("--killswitch_r", type=float, default=-10.0)
    ap.add_argument("--killswitch_window_days", type=int, default=7)

    ap.add_argument("--regime_window_months", type=int, default=12)
    ap.add_argument("--regime_min_trades", type=int, default=10)
    ap.add_argument("--emit_last_candles", type=int, default=1)
    ap.add_argument("--debug_regime", action="store_true")
    ap.add_argument("--dry_run", action="store_true", help="Do not execute the live runner step.")
    ap.add_argument("--skip_live", action="store_true", help="Skip live step entirely (useful for pure backtest runs).")

    return ap.parse_args()


def main() -> int:
    args = parse_args()
    symbols = args.symbols

    # 1) Backtest multi
    _run([
        _py(), "-m", "backtest.journal.run_entry_model_multi",
        "--symbols", symbols,
        "--source", args.source,
        "--bybit_category", args.bybit_category,
        "--bybit_interval", str(args.bybit_interval),
        "--bybit_candles", str(args.bybit_candles_backtest),
        "--rr", str(args.rr),
        "--sl_atr_buffer", str(args.sl_atr_buffer),
    ])

    # 2) Reports
    _ensure_dir(args.reports_dir)
    _run([
        _py(), "-m", "backtest.journal.analyze_trades_multi",
        "--trades", args.trades_csv,
        "--out_dir", args.reports_dir,
        "--top_n", str(args.top_n_reports),
    ])

    # 3) Equity curve
    _ensure_dir(args.equity_out_dir)
    engine_path = args.engine_path.replace("/", os.sep)
    _run([
        _py(), engine_path,
        args.trades_csv.replace("/", os.sep),
        args.equity_out_dir.replace("/", os.sep),
        "--equity", str(args.equity),
        "--risk", str(args.risk),
    ])

    # 4) Auto live step
    if args.skip_live:
        print("\n[PIPELINE] skip_live=True -> done.\n")
        return 0

    runner_auto_cmd = [
        _py(), "-m", "backtest.journal.live_signal_runner_auto",
        "--leaderboard_csv", args.leaderboard_csv,
        "--metric", args.metric,
        "--top_n", str(args.top_n_symbols),
        "--monthly_risk_csv", args.monthly_risk_csv,
        "--bad_month_r", str(args.bad_month_r),
        "--bad_month_min_trades", str(args.bad_month_min_trades),
        "--monthly_action", args.monthly_action,
        "--trades_csv", args.trades_csv,
        "--maxdd_threshold", str(args.maxdd_threshold),
        "--killswitch_r", str(args.killswitch_r),
        "--killswitch_window_days", str(args.killswitch_window_days),
    ]
    if args.dry_run:
        runner_auto_cmd.append("--dry_run")

    runner_auto_cmd += [
        "--",
        "--once",
        "--source", args.source,
        "--bybit_category", args.bybit_category,
        "--bybit_interval", str(args.bybit_interval),
        "--bybit_candles", str(args.bybit_candles_live),
        "--regime_perf_csv", args.trades_csv,
        "--regime_window_months", str(args.regime_window_months),
        "--regime_min_trades", str(args.regime_min_trades),
        "--regime_per_symbol",
        "--emit_last_candles", str(args.emit_last_candles),
    ]
    if args.debug_regime:
        runner_auto_cmd.append("--debug_regime")

    _run(runner_auto_cmd)
    print("\n[PIPELINE] done.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
