from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Any, List

import pandas as pd

from backtest.live.regime_controller_multi import decide_profiles_by_symbol, RegimeDecision


def _read_trades(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"]).copy()
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df["R"] = pd.to_numeric(df.get("R", 0.0), errors="coerce").fillna(0.0)
    return df


def _month_key(ts: pd.Series) -> pd.Series:
    # Use UTC month labels
    return ts.dt.to_period("M").astype(str)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--trades", required=True, type=str, help="CSV with simulated trades containing timestamp, R, symbol.")
    p.add_argument("--out_dir", required=True, type=str, help="Output directory.")
    p.add_argument("--window_months", type=int, default=12)
    p.add_argument("--min_trades", type=int, default=30)
    p.add_argument("--guard_bad_month_r", type=float, default=-10.0)
    p.add_argument("--guard_bad_month_trades", type=int, default=20)
    args = p.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = _read_trades(args.trades)
    if df.empty:
        raise SystemExit("No trades found")

    if "symbol" not in df.columns:
        df["symbol"] = "ALL"

    df["month"] = _month_key(df["timestamp"])
    months = sorted(df["month"].unique())

    rows: List[Dict[str, Any]] = []

    # Walk-forward: for each month i, train on previous window_months (by calendar months), apply to month i
    for sym, g in df.groupby("symbol"):
        g = g.sort_values("timestamp").copy()
        sym_months = sorted(g["month"].unique())

        forced_defensive_until: str | None = None

        for m in sym_months:
            # build training set: months < m and within last window_months
            idx = sym_months.index(m)
            train_months = sym_months[max(0, idx - args.window_months):idx]
            train = g[g["month"].isin(train_months)].copy()

            if forced_defensive_until is not None and m <= forced_defensive_until:
                dec = RegimeDecision(profile="DEFENSIVE", reason=f"guarded after bad month <= {forced_defensive_until}")
            else:
                # decide on rolling history
                dec_map = decide_profiles_by_symbol(
                    trades_csv=args.trades,
                    window_months=args.window_months,
                    min_trades=args.min_trades,
                )
                dec = dec_map.get(sym, dec_map.get("ALL", RegimeDecision()))

            test = g[g["month"] == m]
            test_trades = int(len(test))
            test_total_r = float(test["R"].sum())

            # guard: if this month is very bad, force defensive for next month only
            if (test_trades >= int(args.guard_bad_month_trades)) and (test_total_r <= float(args.guard_bad_month_r)):
                # next month
                mi = sym_months.index(m)
                if mi + 1 < len(sym_months):
                    forced_defensive_until = sym_months[mi + 1]

            rows.append(
                {
                    "symbol": sym,
                    "month": m,
                    "train_months": ",".join(train_months),
                    "train_trades": int(len(train)),
                    "train_total_R": float(train["R"].sum()) if not train.empty else 0.0,
                    "decision_profile": dec.profile,
                    "decision_reason": dec.reason,
                    "test_trades": test_trades,
                    "test_total_R": test_total_r,
                }
            )

    out = pd.DataFrame(rows).sort_values(["symbol", "month"]).reset_index(drop=True)
    out_path = out_dir / "walkforward_regime_by_symbol.csv"
    out.to_csv(out_path, index=False)
    print(f"Wrote: {out_path}")


if __name__ == "__main__":
    main()
