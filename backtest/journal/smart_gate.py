# backtest/journal/smart_gate.py
from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def equity_summary(df: pd.DataFrame, title: str) -> None:
    if df.empty:
        print(f"\n=== {title} ===\nEMPTY")
        return
    r = pd.to_numeric(df.get("R", pd.Series(index=df.index)), errors="coerce").fillna(0.0)
    eq = r.cumsum()
    dd = eq - eq.cummax()
    wl = df.get("outcome", "").astype(str).str.upper()
    wins = int((wl == "WIN").sum())
    loss = int((wl == "LOSS").sum())
    winrate = (wins / (wins + loss) * 100.0) if (wins + loss) else 0.0
    print(f"\n=== {title} ===")
    print(
        f"trades={len(df)} sum_R={r.sum():.4f} exp_R={r.mean():.6f} "
        f"median_R={r.median():.6f} maxDD_R={dd.min():.4f} winrate_WL%={winrate:.2f}"
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trades", type=str, default="backtest/journal/exports_trades/best_trades_all.csv")
    ap.add_argument("--regime", type=str, default="backtest/journal/exports_trades/market_regime.csv")
    ap.add_argument("--out", type=str, default="backtest/journal/exports_trades/best_trades_gated.csv")

    # pressure knobs (kaip prašei)
    ap.add_argument("--trend-min", type=float, default=0.0,
                    help="jei >0: gate veiks tik kai trend_strength >= trend-min")
    ap.add_argument("--atr-min", type=float, default=0.0015,
                    help="min atr_pct kai --need-atr įjungtas")
    ap.add_argument("--need-atr", action="store_true",
                    help="jei įjungta: papildomai reikalauja atr_pct >= atr-min")

    # merge safety (FutureWarning fix: mažoji 'h')
    ap.add_argument("--tolerance", type=str, default="8h",
                    help="merge_asof tolerance, pvz '4h','8h','12h'")
    args = ap.parse_args()

    tpath = Path(args.trades)
    rpath = Path(args.regime)
    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)

    if not tpath.exists():
        raise SystemExit(f"Missing trades csv: {tpath}")
    if not rpath.exists():
        raise SystemExit(f"Missing market_regime csv: {rpath}")

    tr = pd.read_csv(tpath, engine="python", on_bad_lines="skip")
    mr = pd.read_csv(rpath, engine="python", on_bad_lines="skip")

    # time
    tr["timestamp"] = pd.to_datetime(tr.get("timestamp"), errors="coerce")
    mr["timestamp"] = pd.to_datetime(mr.get("timestamp"), errors="coerce")
    tr = tr.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    mr = mr.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    # ensure columns exist in market_regime
    if "regime" not in mr.columns:
        mr["regime"] = pd.NA
    if "trend_dir" not in mr.columns:
        mr["trend_dir"] = pd.NA
    if "trend_strength" not in mr.columns:
        mr["trend_strength"] = 0.0
    if "atr_pct" not in mr.columns:
        mr["atr_pct"] = 0.0

    # normalize market_regime columns
    mr["regime"] = mr["regime"].astype(str).str.upper()
    mr["trend_dir"] = mr["trend_dir"].astype(str).str.upper()
    mr["trend_strength"] = pd.to_numeric(mr["trend_strength"], errors="coerce").fillna(0.0)
    mr["atr_pct"] = pd.to_numeric(mr["atr_pct"], errors="coerce").fillna(0.0)

    # merge regime into trades (su tolerance)
    m = pd.merge_asof(
        tr,
        mr[["timestamp", "regime", "trend_dir", "trend_strength", "atr_pct"]],
        on="timestamp",
        direction="backward",
        tolerance=pd.Timedelta(args.tolerance),
    )

    # normalize trade columns safely (avoid "nan" strings)
    m["side"] = m.get("side", pd.Series("", index=m.index)).astype(str).str.upper()
    m["ctx_sub_label"] = m.get("ctx_sub_label", pd.Series("", index=m.index)).astype(str).str.upper()

    # normalize merged cols (NaN -> empty, not "NAN")
    m["trend_dir"] = m.get("trend_dir", pd.Series("", index=m.index)).fillna("").astype(str).str.upper()
    m["regime"] = m.get("regime", pd.Series("", index=m.index)).fillna("").astype(str).str.upper()

    # numerics for knobs
    m["trend_strength"] = pd.to_numeric(m.get("trend_strength", 0.0), errors="coerce").fillna(0.0)
    m["atr_pct"] = pd.to_numeric(m.get("atr_pct", 0.0), errors="coerce").fillna(0.0)

    equity_summary(m, "BASE (with regime merged)")

    # SANITY (kaip prašei)
    print("\nRegime counts:")
    print(m["regime"].value_counts(dropna=False).head(10))
    print("\nTrend_dir counts:")
    print(m["trend_dir"].value_counts(dropna=False).head(10))

    # === SMART GATE (minimaliai: tik TDP_BOT LONG DOWN) ===
    bad_bot = (
        (m["ctx_sub_label"] == "TDP_BOT")
        & (m["side"] == "LONG")
        & (m["trend_dir"] == "DOWN")
    )

    # optional pressure (pagal tavo specifikaciją)
    if float(args.trend_min) > 0:
        bad_bot &= (m["trend_strength"] >= float(args.trend_min))

    if args.need_atr:
        bad_bot &= (m["atr_pct"] >= float(args.atr_min))

    gated = m[~bad_bot].copy()
    equity_summary(gated, "GATED (smart: drop only TDP_BOT LONG DOWN)")

    gated.to_csv(outp, index=False)
    print("\nSaved:", outp.resolve())

    print("\nDropped counts:")
    print("drop TDP_BOT LONG (DOWN + optional pressure):", int(bad_bot.sum()))


if __name__ == "__main__":
    main()
