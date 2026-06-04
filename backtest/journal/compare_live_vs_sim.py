#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import pandas as pd


def read_csv_safe(path: Optional[str]) -> pd.DataFrame:
    if not path:
        return pd.DataFrame()
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(p)


def normalize_setup_id(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "setup_id" not in out.columns:
        out["setup_id"] = pd.NA
    out["setup_id"] = out["setup_id"].astype(str)
    return out


def ensure_symbol_model_side(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ["symbol", "model", "side"]:
        if col not in out.columns:
            out[col] = pd.NA
    return out


def parse_ts_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = pd.to_datetime(out[c], utc=True, errors="coerce")
    return out


def summarize_setups(name: str, df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame([{
            "dataset": name,
            "rows": 0,
            "unique_setup_id": 0,
            "symbols": "",
            "models": ""
        }])

    syms = sorted(set(df["symbol"].dropna().astype(str))) if "symbol" in df.columns else []
    mods = sorted(set(df["model"].dropna().astype(str))) if "model" in df.columns else []

    return pd.DataFrame([{
        "dataset": name,
        "rows": int(len(df)),
        "unique_setup_id": int(df["setup_id"].nunique(dropna=True)) if "setup_id" in df.columns else 0,
        "symbols": ",".join(syms[:10]),
        "models": ",".join(mods[:10]),
    }])


def compare_overlap(left_name: str, left: pd.DataFrame, right_name: str, right: pd.DataFrame) -> pd.DataFrame:
    if left.empty or right.empty:
        return pd.DataFrame([{
            "left": left_name,
            "right": right_name,
            "left_unique": 0,
            "right_unique": 0,
            "intersection": 0,
            "left_only": 0,
            "right_only": 0,
            "jaccard": 0.0,
        }])

    lset = set(left["setup_id"].dropna().astype(str))
    rset = set(right["setup_id"].dropna().astype(str))

    inter = lset & rset
    union = lset | rset

    return pd.DataFrame([{
        "left": left_name,
        "right": right_name,
        "left_unique": len(lset),
        "right_unique": len(rset),
        "intersection": len(inter),
        "left_only": len(lset - rset),
        "right_only": len(rset - lset),
        "jaccard": round(len(inter) / len(union), 6) if union else 0.0,
    }])


def build_setup_diff(left_name: str, left: pd.DataFrame, right_name: str, right: pd.DataFrame, limit: int = 200) -> pd.DataFrame:
    l = left[["setup_id", "symbol", "model", "side"]].drop_duplicates() if not left.empty else pd.DataFrame(columns=["setup_id", "symbol", "model", "side"])
    r = right[["setup_id", "symbol", "model", "side"]].drop_duplicates() if not right.empty else pd.DataFrame(columns=["setup_id", "symbol", "model", "side"])

    l["present_in_" + left_name] = True
    r["present_in_" + right_name] = True

    merged = l.merge(r, on=["setup_id", "symbol", "model", "side"], how="outer")

    merged["present_in_" + left_name] = merged["present_in_" + left_name].fillna(False)
    merged["present_in_" + right_name] = merged["present_in_" + right_name].fillna(False)

    return merged.head(limit)


def flow_stage_summary(flow: pd.DataFrame) -> pd.DataFrame:
    if flow.empty:
        return pd.DataFrame(columns=["metric", "value"])

    out = []

    if "death_stage" in flow.columns:
        counts = flow["death_stage"].fillna("NA").astype(str).value_counts(dropna=False)
        for k, v in counts.items():
            out.append({"metric": f"death_stage::{k}", "value": int(v)})

    count_cols = [
        "raw_entries_count",
        "after_wait_count",
        "after_freshness_count",
        "after_idempotency_count",
        "after_stale_count",
        "after_per_cycle_guard_count",
        "emitted_count",
    ]

    for c in count_cols:
        if c in flow.columns:
            s = pd.to_numeric(flow[c], errors="coerce")
            out.append({"metric": f"{c}::sum", "value": float(s.fillna(0).sum())})
            out.append({"metric": f"{c}::avg", "value": float(s.mean()) if len(s) else 0.0})

    return pd.DataFrame(out)


def lifecycle_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["metric", "value"])

    out = []

    ts_cols = [
        "timestamp", "visible_ts", "pipeline_visible_ts",
        "signal_ts", "confirm_ts", "execution_ts",
        "trade_open_ts", "trade_close_ts", "emit_ts"
    ]

    d = parse_ts_cols(df, ts_cols)

    if {"timestamp", "signal_ts"}.issubset(d.columns):
        delta = (d["signal_ts"] - d["timestamp"]).dt.total_seconds() / 60.0
        if delta.notna().any():
            out.append({"metric": "minutes_signal_minus_timestamp_avg", "value": float(delta.mean())})
            out.append({"metric": "minutes_signal_minus_timestamp_max", "value": float(delta.max())})

    if {"timestamp", "trade_open_ts"}.issubset(d.columns):
        delta = (d["trade_open_ts"] - d["timestamp"]).dt.total_seconds() / 60.0
        if delta.notna().any():
            out.append({"metric": "minutes_open_minus_timestamp_avg", "value": float(delta.mean())})

    if {"signal_ts", "trade_open_ts"}.issubset(d.columns):
        delta = (d["trade_open_ts"] - d["signal_ts"]).dt.total_seconds() / 60.0
        if delta.notna().any():
            out.append({"metric": "minutes_open_minus_signal_avg", "value": float(delta.mean())})

    if "outcome" in d.columns:
        vc = d["outcome"].fillna("NA").astype(str).value_counts(dropna=False)
        for k, v in vc.items():
            out.append({"metric": f"outcome::{k}", "value": int(v)})

    return pd.DataFrame(out)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--live-fired")
    parser.add_argument("--live-flow")
    parser.add_argument("--parity-fired")
    parser.add_argument("--parity-flow")
    parser.add_argument("--parity-lifecycle")
    parser.add_argument("--oldsim-fired")
    parser.add_argument("--oldsim-trades")
    parser.add_argument("--classic-trades")
    parser.add_argument("--outdir", required=True)

    args = parser.parse_args()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    live = ensure_symbol_model_side(normalize_setup_id(read_csv_safe(args.live_fired)))
    parity = ensure_symbol_model_side(normalize_setup_id(read_csv_safe(args.parity_fired)))
    oldsim = ensure_symbol_model_side(normalize_setup_id(read_csv_safe(args.oldsim_fired)))
    classic = ensure_symbol_model_side(normalize_setup_id(read_csv_safe(args.classic_trades)))

    pd.concat([
        summarize_setups("live", live),
        summarize_setups("parity", parity),
        summarize_setups("oldsim", oldsim),
        summarize_setups("classic", classic),
    ]).to_csv(outdir / "summary.csv", index=False)

    pd.concat([
        compare_overlap("live", live, "parity", parity),
        compare_overlap("live", live, "oldsim", oldsim),
        compare_overlap("parity", parity, "oldsim", oldsim),
    ]).to_csv(outdir / "overlap.csv", index=False)

    build_setup_diff("live", live, "parity", parity).to_csv(outdir / "diff_live_vs_parity.csv", index=False)
    build_setup_diff("live", live, "oldsim", oldsim).to_csv(outdir / "diff_live_vs_oldsim.csv", index=False)

    print(f"[DONE] Results saved to {outdir}")


if __name__ == "__main__":
    main()