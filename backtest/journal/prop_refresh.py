"""Step 12: one-command refresh for prop pipeline.

Runs:
  1) run_entry_model_multi  -> exports_trades/trades_simulated.csv
  2) analyze_trades_multi   -> exports_reports/*
  3) optional monthly risk guard builder (if module exists)
  4) optional walkforward regime

This script is defensive: it still finishes even if optional modules are missing.
"""

from __future__ import annotations
import argparse
import subprocess
import sys
from pathlib import Path

def _run(cmd: list[str]) -> int:
    print("[STEP12] exec:", " ".join(cmd))
    return subprocess.call(cmd)

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", default="BTCUSDT,ETHUSDT,XRPUSDT")
    ap.add_argument("--source", default="bybit", choices=["bybit", "csv"])
    ap.add_argument("--bybit_category", default="linear")
    ap.add_argument("--bybit_interval", default="30")
    ap.add_argument("--bybit_candles", type=int, default=3000)

    ap.add_argument("--rr", type=float, default=2.0)
    ap.add_argument("--sl_atr_buffer", type=float, default=0.15)

    ap.add_argument("--top_n", type=int, default=20)
    ap.add_argument("--window_months", type=int, default=12)
    ap.add_argument("--min_trades", type=int, default=30)
    ap.add_argument("--bad_month_r", type=float, default=-10.0)
    ap.add_argument("--bad_month_min_trades", type=int, default=20)

    ap.add_argument("--trades_out", default="backtest/journal/exports_trades/trades_simulated.csv")
    ap.add_argument("--reports_dir", default="backtest/journal/exports_reports")
    ap.add_argument("--walkforward_dir", default="backtest/journal/exports_walkforward")
    ap.add_argument("--skip_walkforward", action="store_true")
    args = ap.parse_args()

    py = sys.executable

    rc = _run([
        py, "-m", "backtest.journal.run_entry_model_multi",
        "--symbols", args.symbols,
        "--source", args.source,
        "--bybit_category", args.bybit_category,
        "--bybit_interval", str(args.bybit_interval),
        "--bybit_candles", str(args.bybit_candles),
        "--rr", str(args.rr),
        "--sl_atr_buffer", str(args.sl_atr_buffer),
    ])
    if rc != 0:
        return rc

    rc = _run([
        py, "-m", "backtest.journal.analyze_trades_multi",
        "--trades", args.trades_out,
        "--out_dir", args.reports_dir,
        "--top_n", str(args.top_n),
    ])
    if rc != 0:
        return rc

    # optional monthly risk guard builder
    step3_mod = "backtest.journal.build_monthly_risk_guard"
    try:
        __import__(step3_mod)
        rc = _run([
            py, "-m", step3_mod,
            "--trades", args.trades_out,
            "--out", str(Path(args.reports_dir) / "monthly_risk_by_symbol.csv"),
            "--bad_month_r", str(args.bad_month_r),
            "--min_trades", str(args.bad_month_min_trades),
        ])
        if rc != 0:
            return rc
    except Exception:
        pass

    if not args.skip_walkforward:
        try:
            rc = _run([
                py, "-m", "backtest.journal.walkforward_regime_backtest",
                "--trades", args.trades_out,
                "--out_dir", args.walkforward_dir,
                "--window_months", str(args.window_months),
                "--min_trades", str(args.min_trades),
                "--guard_bad_month_r", str(args.bad_month_r),
                "--guard_bad_month_trades", str(args.bad_month_min_trades),
            ])
            # don't fail pipeline if optional step fails
        except Exception:
            pass

    print("[STEP12] OK. Updated:")
    print(" -", args.trades_out)
    print(" -", args.reports_dir)
    if not args.skip_walkforward:
        print(" -", args.walkforward_dir)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
