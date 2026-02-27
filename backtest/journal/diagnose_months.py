from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


KEY_COLS = ["timestamp", "R", "model", "side", "phase", "ctx_sub_label"]
DIMS = ["model", "side", "phase", "ctx_sub_label"]


def load_trades(path: str, from_ts: str | None, to_ts: str | None) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"File not found: {path}")

    df = pd.read_csv(p)

    missing = [c for c in ["timestamp", "R"] if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing required columns: {missing}")

    # Normalize columns (ensure they exist)
    for c in ["model", "side", "phase", "ctx_sub_label", "symbol"]:
        if c not in df.columns:
            df[c] = ""

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df = df.dropna(subset=["timestamp"])

    # time filter
    if from_ts:
        t0 = pd.to_datetime(from_ts, utc=True)
        df = df[df["timestamp"] >= t0]
    if to_ts:
        t1 = pd.to_datetime(to_ts, utc=True)
        df = df[df["timestamp"] <= t1]

    # month key (tz-safe, no warning)
    ts = df["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)
    df["year_month"] = ts.dt.to_period("M").astype(str)

    df["R"] = pd.to_numeric(df["R"], errors="coerce").fillna(0.0)
    df["model"] = df["model"].astype(str)
    df["side"] = df["side"].astype(str)
    df["phase"] = df["phase"].astype(str)
    df["ctx_sub_label"] = df["ctx_sub_label"].astype(str)
    df["symbol"] = df["symbol"].astype(str)

    return df.reset_index(drop=True)


def monthly_summary(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby("year_month", as_index=False).agg(
        trades=("R", "count"),
        expectancy_R=("R", "mean"),
        total_R=("R", "sum"),
        wins=("R", lambda x: int((x > 0).sum())),
        losses=("R", lambda x: int((x < 0).sum())),
        be=("R", lambda x: int((x == 0).sum())),
    )
    g["winrate_WL"] = g.apply(lambda r: (r["wins"] / max(1, (r["wins"] + r["losses"]))), axis=1)
    return g.sort_values("year_month")


def breakdown(df: pd.DataFrame, dim: str) -> pd.DataFrame:
    g = df.groupby(["year_month", dim], as_index=False).agg(
        trades=("R", "count"),
        expectancy_R=("R", "mean"),
        total_R=("R", "sum"),
        wins=("R", lambda x: int((x > 0).sum())),
        losses=("R", lambda x: int((x < 0).sum())),
    )
    g["winrate_WL"] = g.apply(lambda r: (r["wins"] / max(1, (r["wins"] + r["losses"]))), axis=1)
    return g.sort_values(["year_month", "total_R"])


def worst_contributors(df: pd.DataFrame, n: int = 3) -> pd.DataFrame:
    """
    For each negative month, show top N worst buckets across multiple dims:
    model / side / phase / ctx_sub_label
    """
    ms = monthly_summary(df)
    bad_months = ms[ms["total_R"] < 0].copy()
    if bad_months.empty:
        return pd.DataFrame(columns=["year_month", "dim", "bucket", "trades", "total_R", "expectancy_R", "winrate_WL"])

    rows = []
    for ym in bad_months["year_month"].tolist():
        dym = df[df["year_month"] == ym].copy()
        for dim in DIMS:
            b = dym.groupby(dim, as_index=False).agg(
                trades=("R", "count"),
                expectancy_R=("R", "mean"),
                total_R=("R", "sum"),
                wins=("R", lambda x: int((x > 0).sum())),
                losses=("R", lambda x: int((x < 0).sum())),
            )
            b["winrate_WL"] = b.apply(lambda r: (r["wins"] / max(1, (r["wins"] + r["losses"]))), axis=1)
            b = b.sort_values("total_R").head(n)
            for _, r in b.iterrows():
                rows.append({
                    "year_month": ym,
                    "dim": dim,
                    "bucket": str(r[dim]),
                    "trades": int(r["trades"]),
                    "total_R": float(r["total_R"]),
                    "expectancy_R": float(r["expectancy_R"]),
                    "winrate_WL": float(r["winrate_WL"]),
                })

    return pd.DataFrame(rows).sort_values(["year_month", "total_R"])


def regime_hints(df: pd.DataFrame) -> pd.DataFrame:
    """
    Very simple hints:
    - if a model is negative in a month => suggest disabling that model next month
    - same for side and phase
    You will later convert these hints into real controller rules.
    """
    ms = monthly_summary(df)
    out = []
    for ym in ms["year_month"].tolist():
        dym = df[df["year_month"] == ym]
        # model totals
        m = dym.groupby("model", as_index=False)["R"].sum().sort_values("R")
        s = dym.groupby("side", as_index=False)["R"].sum().sort_values("R")
        p = dym.groupby("phase", as_index=False)["R"].sum().sort_values("R")

        # pick negative buckets only
        bad_models = m[m["R"] < 0]["model"].tolist()
        bad_sides = s[s["R"] < 0]["side"].tolist()
        bad_phases = p[p["R"] < 0]["phase"].tolist()

        out.append({
            "year_month": ym,
            "month_total_R": float(dym["R"].sum()),
            "disable_models_suggest": ",".join(bad_models),
            "disable_sides_suggest": ",".join(bad_sides),
            "disable_phases_suggest": ",".join(bad_phases),
        })

    return pd.DataFrame(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="CSV with trades (must include timestamp and R)")
    ap.add_argument("--from", dest="from_ts", default=None, help="YYYY-MM-DD (optional)")
    ap.add_argument("--to", dest="to_ts", default=None, help="YYYY-MM-DD (optional)")
    ap.add_argument("--outdir", default="backtest/journal/exports_reports", help="output folder")
    ap.add_argument("--worst_n", type=int, default=3, help="top N worst buckets per dim for bad months")
    args = ap.parse_args()

    df = load_trades(args.inp, args.from_ts, args.to_ts)

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    ms = monthly_summary(df)
    ms.to_csv(outdir / "monthly_summary.csv", index=False)

    for dim in DIMS:
        bd = breakdown(df, dim)
        bd.to_csv(outdir / f"monthly_by_{dim}.csv", index=False)

    wc = worst_contributors(df, n=args.worst_n)
    wc.to_csv(outdir / "bad_months_worst_contributors.csv", index=False)

    hints = regime_hints(df)
    hints.to_csv(outdir / "regime_hints.csv", index=False)

    print("Saved:")
    print(" -", outdir / "monthly_summary.csv")
    print(" -", outdir / "monthly_by_model.csv")
    print(" -", outdir / "monthly_by_side.csv")
    print(" -", outdir / "monthly_by_phase.csv")
    print(" -", outdir / "monthly_by_ctx_sub_label.csv")
    print(" -", outdir / "bad_months_worst_contributors.csv")
    print(" -", outdir / "regime_hints.csv")

    print("\n=== MONTHLY SUMMARY (R UNITS) ===")
    print(ms.to_string(index=False))

    bad = ms[ms["total_R"] < 0]
    if len(bad):
        print("\n=== BAD MONTHS (total_R < 0) ===")
        print(bad[["year_month", "trades", "expectancy_R", "total_R"]].to_string(index=False))
    else:
        print("\nNo negative months in selected period.")


if __name__ == "__main__":
    main()
