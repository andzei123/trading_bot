# backtest/journal/compute_monthly_risk_by_symbol.py
"""Monthly risk guard for prop-firm safety.

Reads trades CSV (simulate_trades output) that contains at minimum:
  - symbol (optional; if missing -> 'ALL')
  - timestamp (entry time) OR exit_timestamp (preferred for month attribution)
  - R (float, per-trade R multiple)

Produces monthly_risk_by_symbol.csv with:
  symbol, month, trades, total_R, exp_R, maxDD_R, worst_trade_R, best_trade_R, profile

Profile rule (requested):
  - bad_month_threshold = -10R
  - min_trades = 20

Profile:
  DEFENSIVE if (trades >= min_trades) and (total_R <= bad_month_threshold)
  else NORMAL

Usage:
  python -m backtest.journal.compute_monthly_risk_by_symbol \
    --trades backtest/journal/exports_trades/trades_simulated.csv \
    --out backtest/journal/exports_reports/monthly_risk_by_symbol.csv \
    --bad_month_threshold -10 \
    --min_trades 20

Notes:
  - Month attribution uses exit_timestamp if present; otherwise uses timestamp.
  - maxDD_R is computed on cumulative R within each (symbol, month) ordered by time.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def _pick_time_col(df: pd.DataFrame) -> str:
    for c in ["exit_timestamp", "exit_time", "timestamp", "entry_timestamp", "time"]:
        if c in df.columns:
            return c
    raise SystemExit(f"No time column found. Have: {list(df.columns)}")


def _ensure_symbol(df: pd.DataFrame) -> pd.DataFrame:
    if "symbol" not in df.columns:
        df = df.copy()
        df["symbol"] = "ALL"
    else:
        df = df.copy()
        df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
        df.loc[df["symbol"].eq("") | df["symbol"].isna(), "symbol"] = "ALL"
    return df


def _max_drawdown(cum: pd.Series) -> float:
    running_max = cum.cummax()
    dd = cum - running_max
    return float(dd.min()) if len(dd) else 0.0


def compute_monthly_risk_by_symbol(
    trades_csv: str | Path,
    out_csv: str | Path,
    bad_month_threshold: float = -10.0,
    min_trades: int = 20,
) -> pd.DataFrame:
    trades_csv = Path(trades_csv)
    out_csv = Path(out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(trades_csv)
    if df.empty:
        raise SystemExit("Trades CSV is empty.")

    if "R" not in df.columns:
        raise SystemExit("Trades CSV missing required column: R")

    df = _ensure_symbol(df)

    tcol = _pick_time_col(df)
    df[tcol] = pd.to_datetime(df[tcol], utc=True, errors="coerce")
    df = df.dropna(subset=[tcol]).copy()

    df["R"] = pd.to_numeric(df["R"], errors="coerce")
    df = df.dropna(subset=["R"]).copy()

    df["month"] = df[tcol].dt.to_period("M").astype(str)

    rows = []
    for (sym, month), g in df.sort_values(tcol).groupby(["symbol", "month"], sort=True):
        trades = int(len(g))
        total_R = float(g["R"].sum())
        exp_R = float(g["R"].mean()) if trades else 0.0
        cum = g["R"].cumsum()
        maxDD_R = _max_drawdown(cum)
        worst_trade_R = float(g["R"].min()) if trades else 0.0
        best_trade_R = float(g["R"].max()) if trades else 0.0

        profile = "NORMAL"
        if trades >= int(min_trades) and total_R <= float(bad_month_threshold):
            profile = "DEFENSIVE"

        rows.append(
            dict(
                symbol=sym,
                month=month,
                trades=trades,
                total_R=round(total_R, 6),
                exp_R=round(exp_R, 6),
                maxDD_R=round(maxDD_R, 6),
                worst_trade_R=round(worst_trade_R, 6),
                best_trade_R=round(best_trade_R, 6),
                profile=profile,
            )
        )

    out = pd.DataFrame(rows).sort_values(["symbol", "month"]).reset_index(drop=True)
    out.to_csv(out_csv, index=False)
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--trades", type=str, required=True, help="Path to trades_simulated.csv (must include column R).")
    p.add_argument("--out", type=str, required=True, help="Output CSV path.")
    p.add_argument("--bad_month_threshold", type=float, default=-10.0)
    p.add_argument("--min_trades", type=int, default=20)
    args = p.parse_args()

    out = compute_monthly_risk_by_symbol(
        trades_csv=args.trades,
        out_csv=args.out,
        bad_month_threshold=args.bad_month_threshold,
        min_trades=args.min_trades,
    )

    print(f"Wrote: {args.out}")
    if not out.empty:
        for sym in sorted(out["symbol"].unique()):
            tail = out[out["symbol"] == sym].tail(6)
            print(f"\n[{sym}] last months:")
            print(tail.to_string(index=False))


if __name__ == "__main__":
    main()
