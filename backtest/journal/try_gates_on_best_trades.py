from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

THIS_DIR = Path(__file__).resolve().parent
EXPORT_DIR = THIS_DIR / "exports_trades"
TRADES_CSV = EXPORT_DIR / "best_trades_all.csv"


def wl_winrate(df: pd.DataFrame) -> float:
    if df.empty or "outcome" not in df.columns:
        return float("nan")
    o = df["outcome"].astype(str).str.upper()
    w = int((o == "WIN").sum())
    l = int((o == "LOSS").sum())
    wl = w + l
    return (w / wl * 100.0) if wl else float("nan")


def expectancy_from_R(df: pd.DataFrame) -> float:
    if df.empty or "R" not in df.columns:
        return float("nan")
    return float(pd.to_numeric(df["R"], errors="coerce").dropna().mean())


def max_drawdown_R(df: pd.DataFrame) -> float:
    if df.empty or "R" not in df.columns:
        return float("nan")
    r = pd.to_numeric(df["R"], errors="coerce").fillna(0.0).to_numpy()
    eq = np.cumsum(r)
    peak = np.maximum.accumulate(eq)
    dd = eq - peak
    return float(dd.min()) if len(dd) else float("nan")


def equity_summary(name: str, df: pd.DataFrame) -> None:
    exp_r = expectancy_from_R(df)
    med_r = float(pd.to_numeric(df.get("R", pd.Series([], dtype=float)), errors="coerce").dropna().median()) if len(df) else float("nan")
    s = float(pd.to_numeric(df.get("R", pd.Series([], dtype=float)), errors="coerce").dropna().sum()) if len(df) else 0.0
    dd = max_drawdown_R(df)
    wr = wl_winrate(df)

    print(f"\n=== {name} ===")
    print(f"trades={len(df)} sum_R={s:.4f} exp_R={exp_r:.6f} median_R={med_r:.6f} maxDD_R={dd:.4f} winrate_WL%={wr:.2f}")


def segment_slice(df: pd.DataFrame, segment: str) -> pd.DataFrame:
    d = df.copy()
    if "timestamp" in d.columns:
        d["timestamp"] = pd.to_datetime(d["timestamp"], errors="coerce")
        d = d.sort_values("timestamp")
    d = d.reset_index(drop=True)

    n = len(d)
    if n == 0 or segment.lower() == "all":
        return d

    a = int(round(n * 1 / 3))
    b = int(round(n * 2 / 3))

    seg = segment.lower()
    if seg == "early":
        return d.iloc[:a].copy()
    if seg == "mid":
        return d.iloc[a:b].copy()
    if seg == "late":
        return d.iloc[b:].copy()

    raise SystemExit("segment must be one of: all/early/mid/late")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", type=str, default=str(TRADES_CSV))
    ap.add_argument("--segment", type=str, default="late", choices=["all", "early", "mid", "late"])
    ap.add_argument("--also-short", action="store_true", help="add SHORT/TDP_TOP gates too")
    args = ap.parse_args()

    p = Path(args.csv)
    if not p.exists():
        raise SystemExit(f"Missing trades csv: {p}")

    df = pd.read_csv(p)

    # normalize
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    df["side"] = df.get("side", "").astype(str).str.upper()
    if "ctx_sub_label" in df.columns:
        df["ctx_sub_label"] = df["ctx_sub_label"].astype(str).str.upper()

    equity_summary("BASE (all trades)", df)

    seg_df = segment_slice(df, args.segment)
    equity_summary(f"SEGMENT={args.segment.upper()} (trade-time tertile)", seg_df)

    # ---- GATES for MID/LATE/EARLY ----
    # LONG pain point
    gA = seg_df[~(seg_df["side"] == "LONG")].copy()
    equity_summary(f"GATE A: {args.segment.upper()} drop ALL LONG", gA)

    gB = seg_df.copy()
    if "ctx_sub_label" in gB.columns:
        gB = gB[~(gB["ctx_sub_label"] == "TDP_BOT")].copy()
    equity_summary(f"GATE B: {args.segment.upper()} drop ctx_sub_label==TDP_BOT", gB)

    gC = seg_df.copy()
    if "ctx_sub_label" in gC.columns:
        gC = gC[~((gC["side"] == "LONG") & (gC["ctx_sub_label"] == "TDP_BOT"))].copy()
    equity_summary(f"GATE C: {args.segment.upper()} drop LONG & TDP_BOT", gC)

    if args.also_short:
        gD = seg_df[~(seg_df["side"] == "SHORT")].copy()
        equity_summary(f"GATE D: {args.segment.upper()} drop ALL SHORT", gD)

        gE = seg_df.copy()
        if "ctx_sub_label" in gE.columns:
            gE = gE[~(gE["ctx_sub_label"] == "TDP_TOP")].copy()
        equity_summary(f"GATE E: {args.segment.upper()} drop ctx_sub_label==TDP_TOP", gE)

        gF = seg_df.copy()
        if "ctx_sub_label" in gF.columns:
            gF = gF[~((gF["side"] == "SHORT") & (gF["ctx_sub_label"] == "TDP_TOP"))].copy()
        equity_summary(f"GATE F: {args.segment.upper()} drop SHORT & TDP_TOP", gF)


if __name__ == "__main__":
    main()
