from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def expectancy_from_R(df: pd.DataFrame) -> float:
    if df.empty or "R" not in df.columns:
        return float("nan")
    return float(pd.to_numeric(df["R"], errors="coerce").dropna().mean())


def wl_winrate(df: pd.DataFrame) -> float:
    if df.empty or "outcome" not in df.columns:
        return float("nan")
    o = df["outcome"].astype(str).str.upper()
    w = int((o == "WIN").sum())
    l = int((o == "LOSS").sum())
    wl = w + l
    return (w / wl * 100.0) if wl else float("nan")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--trades",
        type=str,
        default="backtest/journal/exports_trades/best_trades_all.csv",
        help="path to trades csv (e.g. best_trades_all.csv or best_trades_gated.csv)",
    )
    ap.add_argument(
        "--out",
        type=str,
        default="backtest/journal/exports_trades/best_on_tertiles.csv",
        help="output csv path",
    )
    args = ap.parse_args()

    tpath = Path(args.trades)
    if not tpath.exists():
        raise SystemExit(f"Missing trades csv: {tpath}")

    df = pd.read_csv(tpath, engine="python", on_bad_lines="skip")

    # timestamp column
    if "timestamp" not in df.columns:
        raise SystemExit("Trades CSV missing 'timestamp' column")

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    n = len(df)
    if n == 0:
        raise SystemExit("No trades after timestamp parse")

    a = n // 3
    b = 2 * n // 3

    parts = {
        "early_33%": df.iloc[:a].copy(),
        "mid_33%": df.iloc[a:b].copy(),
        "late_33%": df.iloc[b:].copy(),
    }

    rows = []
    for name, part in parts.items():
        rows.append(
            {
                "segment": name,
                "trades": int(len(part)),
                "expectancy_R": round(expectancy_from_R(part), 6),
                "winrate_WL_%": round(wl_winrate(part), 6),
            }
        )

    out_df = pd.DataFrame(rows)
    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(outp, index=False)

    print(out_df.to_string(index=False))
    print("Saved:", outp.resolve())


if __name__ == "__main__":
    main()
