"""Prop preflight checks (Step 13)

One command that tells you:
  - required artifacts present + fresh
  - CSVs have required columns
  - which symbols are selected by leaderboard
  - the exact live command template to run

Read-only (safe)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List, Optional

import pandas as pd

REQUIRED_TRADE_COLS = [
    "timestamp","side","entry","sl","tp","rr","model","ctx_sub_label","regime","phase","R"
]

def _mtime_str(p: Path) -> str:
    if not p.exists():
        return "MISSING"
    import datetime
    ts = datetime.datetime.fromtimestamp(p.stat().st_mtime)
    return ts.strftime("%Y-%m-%d %H:%M:%S")

def _age_hours(p: Path) -> Optional[float]:
    if not p.exists():
        return None
    now = pd.Timestamp.utcnow().timestamp()
    return max(0.0, (now - p.stat().st_mtime) / 3600.0)

def _read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except Exception as e:
        raise RuntimeError(f"Failed to read CSV: {path} ({e})") from e

def _missing_cols(df: pd.DataFrame, req: List[str]) -> List[str]:
    return [c for c in req if c not in df.columns]

def _kv(k: str, v: str) -> None:
    print(f"{k:<24}{v}")

def main() -> int:
    ap = argparse.ArgumentParser(description="Step 13: Prop preflight checks")
    ap.add_argument("--trades_csv", default="backtest/journal/exports_trades/trades_simulated.csv")
    ap.add_argument("--leaderboard_csv", default="backtest/journal/exports_reports/summary_by_symbol.csv")
    ap.add_argument("--monthly_risk_csv", default="backtest/journal/exports_reports/monthly_risk_by_symbol.csv")
    ap.add_argument("--top_n", type=int, default=5)
    ap.add_argument("--metric", default="total_R")
    ap.add_argument("--max_age_hours", type=float, default=24.0)
    ap.add_argument("--emit_last_candles", type=int, default=1)
    ap.add_argument("--dry_run", action="store_true")
    args = ap.parse_args()

    trades_p = Path(args.trades_csv)
    lb_p = Path(args.leaderboard_csv)
    mr_p = Path(args.monthly_risk_csv)

    print("\n[STEP13] PRE-FLIGHT\n")

    for name, p in [("trades_csv", trades_p), ("leaderboard_csv", lb_p), ("monthly_risk_csv", mr_p)]:
        _kv(name, str(p))
        _kv("  exists", "YES" if p.exists() else "NO")
        _kv("  mtime", _mtime_str(p))
        age = _age_hours(p)
        if age is not None:
            _kv("  age_hours", f"{age:.2f}")
            if age > float(args.max_age_hours):
                print(f"  [WARN] {name} older than {args.max_age_hours}h")

    if not trades_p.exists():
        print("\n[FAIL] trades_csv missing. Run Step12 pipeline first.")
        return 2

    df = _read_csv(trades_p)
    _kv("rows", str(len(df)))

    if "R" in df.columns and len(df) > 0:
        _kv("total_R", f"{df['R'].sum():.2f}")
        _kv("expectancy_R", f"{df['R'].mean():.4f}")

    # symbol may be missing in older single-symbol files
    if "symbol" in df.columns:
        syms = sorted(df["symbol"].dropna().astype(str).unique().tolist())
        _kv("symbols_in_trades", ",".join(syms))
    else:
        print("[WARN] trades_csv has no 'symbol' column (multi-symbol guards/leaderboard expect it).")

    missing = _missing_cols(df, REQUIRED_TRADE_COLS)
    if missing:
        print("[WARN] trades_csv missing cols:", missing)

    if lb_p.exists():
        lb = _read_csv(lb_p)
        if "symbol" in lb.columns and args.metric in lb.columns:
            lb2 = lb.sort_values(args.metric, ascending=False).head(int(args.top_n))
            print("\n[LEADERBOARD TOP]")
            print(lb2[["symbol", args.metric]].to_string(index=False))
            selected = lb2["symbol"].astype(str).tolist()
            print("\nSelected symbols:", ", ".join(selected))
        else:
            print("\n[WARN] leaderboard missing 'symbol' or metric column.")
    else:
        print("\n[WARN] leaderboard_csv missing. Run analyze_trades_multi first.")

    print("\n[LIVE COMMAND TEMPLATE]")
    cmd = [
        sys.executable, "-m", "backtest.journal.live_signal_runner_auto",
        "--leaderboard_csv", str(lb_p),
        "--metric", str(args.metric),
        "--top_n", str(args.top_n),
        "--monthly_risk_csv", str(mr_p),
        "--bad_month_r", "-10",
        "--bad_month_min_trades", "20",
        "--monthly_action", "neutral",
        "--trades_csv", str(trades_p),
        "--maxdd_threshold", "-25",
        "--killswitch_r", "-10",
        "--killswitch_window_days", "7",
    ]
    if args.dry_run:
        cmd.append("--dry_run")
    cmd += ["--",
            "--once", "--source", "bybit", "--bybit_category", "linear",
            "--bybit_interval", "30", "--bybit_candles", "1500",
            "--regime_perf_csv", str(trades_p),
            "--regime_window_months", "12", "--regime_min_trades", "10",
            "--regime_per_symbol",
            "--emit_last_candles", str(args.emit_last_candles),
            "--debug_regime"]
    print(" ".join(cmd))

    print("\n[OK] Preflight complete\n")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
