
"""
Leaderboard + risk-aware symbol scoring for multi-symbol backtests.

Score formula (prop-safe):
    score = total_R - dd_penalty * abs(maxDD_R)

Where:
- total_R is sum of R for a symbol
- maxDD_R is maximum drawdown in R units for that symbol (negative number)
- dd_penalty controls how much you punish drawdown (default 1.0)

Inputs:
- trades CSV produced by simulate_trades / run_entry_model_multi
  expected columns: ['timestamp','R','symbol' ...]
  'timestamp' should be parseable datetime; timezone is OK.

Outputs (written into out_dir):
- leaderboard_by_symbol.csv
- monthly_by_symbol.csv
- monthly_risk_by_symbol.csv (compatible with live risk_guard_csv)
"""

from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def _max_drawdown_r(r_series: pd.Series) -> float:
    # r_series: sequence of R per trade in chronological order
    equity = r_series.fillna(0.0).cumsum()
    peak = equity.cummax()
    dd = equity - peak
    # dd is <= 0 ; we return min (most negative)
    return float(dd.min()) if len(dd) else 0.0


def _wl_summary(df: pd.DataFrame) -> dict:
    # uses standard outcome: win if R>0, loss if R<0, be if R==0
    r = pd.to_numeric(df["R"], errors="coerce").fillna(0.0)
    win = int((r > 0).sum())
    loss = int((r < 0).sum())
    be = int((r == 0).sum())
    total = int(len(r))
    wr = (win / (win + loss)) if (win + loss) > 0 else 0.0
    exp = float(r.mean()) if total > 0 else 0.0
    total_r = float(r.sum()) if total > 0 else 0.0
    maxdd = _max_drawdown_r(r)
    return dict(total=total, win=win, loss=loss, be=be, winrate=wr, exp_R=exp, total_R=total_r, maxDD_R=maxdd)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--trades", type=str, required=True, help="Path to trades_simulated.csv (must include symbol column).")
    p.add_argument("--out_dir", type=str, required=True, help="Output directory for reports.")
    p.add_argument("--period", type=str, default="M", choices=["M", "W"], help="Monthly (M) or weekly (W) grouping.")
    p.add_argument("--bad_month_r", type=float, default=-10.0, help="Threshold for 'bad month' total_R.")
    p.add_argument("--min_trades", type=int, default=20, help="Min trades in period to count as 'bad month'.")
    p.add_argument("--dd_penalty", type=float, default=1.0, help="Penalty multiplier for abs(maxDD_R).")
    args = p.parse_args()

    trades_path = Path(args.trades)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(trades_path)
    if "symbol" not in df.columns:
        raise SystemExit("Trades CSV must contain 'symbol' column (multi-symbol run).")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    df["R"] = pd.to_numeric(df["R"], errors="coerce").fillna(0.0)

    # === Leaderboard ===
    rows = []
    for sym, d in df.groupby("symbol", sort=True):
        s = _wl_summary(d)
        score = s["total_R"] - float(args.dd_penalty) * abs(s["maxDD_R"])
        rows.append({
            "symbol": sym,
            **s,
            "score": float(score),
            "dd_penalty": float(args.dd_penalty),
        })

    leaderboard = pd.DataFrame(rows).sort_values(["score", "total_R"], ascending=False).reset_index(drop=True)
    leaderboard.to_csv(out_dir / "leaderboard_by_symbol.csv", index=False)

    # === Monthly/Weekly aggregates per symbol ===
    period = "M" if args.period.upper() == "M" else "W"
    # Period conversion drops tz info (pandas warning). That's OK; we store as string label.
    df["period"] = df["timestamp"].dt.to_period(period).astype(str)

    monthly = (
        df.groupby(["symbol", "period"], as_index=False)
        .agg(trades=("R", "size"), total_R=("R", "sum"), exp_R=("R", "mean"))
    )
    monthly.to_csv(out_dir / "monthly_by_symbol.csv", index=False)

    # === Risk-guard table (compatible with live runner) ===
    monthly_rg = monthly.copy()
    monthly_rg["is_bad_month"] = (monthly_rg["total_R"] <= float(args.bad_month_r)) & (monthly_rg["trades"] >= int(args.min_trades))
    monthly_rg["bad_month_r"] = float(args.bad_month_r)
    monthly_rg["min_trades"] = int(args.min_trades)
    monthly_rg.to_csv(out_dir / "monthly_risk_by_symbol.csv", index=False)

    print(f"Wrote to: {out_dir}")
    print(" - leaderboard_by_symbol.csv")
    print(" - monthly_by_symbol.csv")
    print(" - monthly_risk_by_symbol.csv")


if __name__ == "__main__":
    main()
