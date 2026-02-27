"""Step 12: one-command live wrapper for prop mode.

Uses live_loop to run live_signal_runner_auto repeatedly with safe defaults.

Example:
  python -m backtest.journal.prop_live --every_seconds 1800 --emit_last_candles 1 --debug_regime

To "see more history signals" use --emit_last_candles 50.
"""

from __future__ import annotations
import argparse
import subprocess
import sys

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--every_seconds", type=int, default=1800)
    ap.add_argument("--emit_last_candles", type=int, default=1)
    ap.add_argument("--dry_run", action="store_true")

    ap.add_argument("--leaderboard_csv", default="backtest/journal/exports_reports/summary_by_symbol.csv")
    ap.add_argument("--metric", default="total_R")
    ap.add_argument("--top_n", type=int, default=5)

    ap.add_argument("--monthly_risk_csv", default="backtest/journal/exports_reports/monthly_risk_by_symbol.csv")
    ap.add_argument("--bad_month_r", type=float, default=-10.0)
    ap.add_argument("--bad_month_min_trades", type=int, default=20)
    ap.add_argument("--monthly_action", default="neutral", choices=["off", "neutral"])

    ap.add_argument("--trades_csv", default="backtest/journal/exports_trades/trades_simulated.csv")
    ap.add_argument("--maxdd_threshold", type=float, default=-25.0)
    ap.add_argument("--killswitch_r", type=float, default=-10.0)
    ap.add_argument("--killswitch_window_days", type=int, default=7)

    ap.add_argument("--source", default="bybit", choices=["bybit", "csv"])
    ap.add_argument("--bybit_category", default="linear")
    ap.add_argument("--bybit_interval", default="30")
    ap.add_argument("--bybit_candles", type=int, default=1500)
    ap.add_argument("--regime_window_months", type=int, default=12)
    ap.add_argument("--regime_min_trades", type=int, default=10)
    ap.add_argument("--debug_regime", action="store_true")

    ap.add_argument("rest", nargs="*", help="Use `--` then extra args to pass to live_signal_runner (rare).")
    args = ap.parse_args()

    py = sys.executable

    cmd = [
        py, "-m", "backtest.journal.live_loop",
        "--every_seconds", str(args.every_seconds),
        "--",
        py, "-m", "backtest.journal.live_signal_runner_auto",
        "--leaderboard_csv", args.leaderboard_csv,
        "--metric", args.metric,
        "--top_n", str(args.top_n),
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
        cmd.append("--dry_run")

    cmd += [
        "--",
        "--once",
        "--source", args.source,
        "--bybit_category", args.bybit_category,
        "--bybit_interval", str(args.bybit_interval),
        "--bybit_candles", str(args.bybit_candles),
        "--regime_perf_csv", args.trades_csv,
        "--regime_window_months", str(args.regime_window_months),
        "--regime_min_trades", str(args.regime_min_trades),
        "--regime_per_symbol",
        "--emit_last_candles", str(args.emit_last_candles),
    ]
    if args.debug_regime:
        cmd.append("--debug_regime")

    if args.rest:
        cmd += args.rest

    print("[STEP12] exec:", " ".join(cmd))
    return subprocess.call(cmd)

if __name__ == "__main__":
    raise SystemExit(main())
