from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def load_trades(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"File not found: {path}")

    df = pd.read_csv(p)

    if "timestamp" not in df.columns:
        raise SystemExit("Missing column: timestamp")

    # R stulpelis būtinas RR suvestinei
    if "R" not in df.columns:
        raise SystemExit("Missing column: R (R-units). Use trades_simulated.csv or add R to paper trades when you have exits.")

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df = df.dropna(subset=["timestamp"])

    # month key
    df["year_month"] = df["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None).dt.to_period("M").astype(str)

    # ensure columns
    if "symbol" not in df.columns:
        df["symbol"] = "ALL"

    if "win" not in df.columns:
        # jei neturi win/loss stulpelių, bandome išvesti iš R:
        # win: R>0, loss: R<0, be: R==0
        df["win"] = (pd.to_numeric(df["R"], errors="coerce") > 0).astype(int)
        df["loss"] = (pd.to_numeric(df["R"], errors="coerce") < 0).astype(int)

    return df


def monthly_table(df: pd.DataFrame) -> pd.DataFrame:
    r = pd.to_numeric(df["R"], errors="coerce").fillna(0.0)

    g = df.assign(R=r).groupby("year_month", as_index=False).agg(
        trades=("R", "count"),
        wins=("win", "sum"),
        losses=("loss", "sum"),
        expectancy_R=("R", "mean"),
        total_R=("R", "sum"),
    )

    return g.sort_values("year_month")


def monthly_table_by_symbol(df: pd.DataFrame) -> pd.DataFrame:
    r = pd.to_numeric(df["R"], errors="coerce").fillna(0.0)

    g = df.assign(R=r).groupby(["symbol", "year_month"], as_index=False).agg(
        trades=("R", "count"),
        wins=("win", "sum"),
        losses=("loss", "sum"),
        expectancy_R=("R", "mean"),
        total_R=("R", "sum"),
    )

    return g.sort_values(["symbol", "year_month"])


def inactive_months(df: pd.DataFrame) -> pd.DataFrame:
    # surandam visą mėnesių diapazoną
    months = pd.period_range(
        df["timestamp"].min().to_period("M"),
        df["timestamp"].max().to_period("M"),
        freq="M",
    ).astype(str)

    # bendras inactivity
    mcount = df.groupby("year_month").size().reindex(months, fill_value=0)
    inactive_all = pd.DataFrame({"year_month": mcount.index, "trades": mcount.values})
    inactive_all = inactive_all[inactive_all["trades"] == 0].copy()
    inactive_all["symbol"] = "ALL"

    # per symbol inactivity
    out = [inactive_all]
    for sym, d in df.groupby("symbol"):
        mcount_s = d.groupby("year_month").size().reindex(months, fill_value=0)
        tmp = pd.DataFrame({"year_month": mcount_s.index, "trades": mcount_s.values})
        tmp = tmp[tmp["trades"] == 0].copy()
        tmp["symbol"] = sym
        out.append(tmp)

    return pd.concat(out, ignore_index=True)[["symbol", "year_month", "trades"]].sort_values(["symbol", "year_month"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="CSV with trades containing timestamp and R columns")
    ap.add_argument("--outdir", default="backtest/journal/exports_reports", help="output folder")
    args = ap.parse_args()

    df = load_trades(args.inp)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    m_all = monthly_table(df)
    m_sym = monthly_table_by_symbol(df)
    inact = inactive_months(df)

    m_all.to_csv(outdir / "monthly_all.csv", index=False)
    m_sym.to_csv(outdir / "monthly_by_symbol.csv", index=False)
    inact.to_csv(outdir / "inactive_months.csv", index=False)

    print("Saved:")
    print(" -", (outdir / "monthly_all.csv"))
    print(" -", (outdir / "monthly_by_symbol.csv"))
    print(" -", (outdir / "inactive_months.csv"))

    print("\n=== MONTHLY PERFORMANCE (ALL, R UNITS) ===")
    print(m_all.to_string(index=False))


if __name__ == "__main__":
    main()
