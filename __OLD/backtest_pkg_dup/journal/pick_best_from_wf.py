from __future__ import annotations

import argparse
from pathlib import Path
import json
import sys

import pandas as pd

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[2]
JOURNAL_DIR = THIS_FILE.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(JOURNAL_DIR))

import filter_trades as ft


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-folds", type=int, default=2)
    ap.add_argument("--min-trades", type=int, default=500)
    ap.add_argument("--min-median", type=float, default=0.0)
    ap.add_argument(
        "--rank-by",
        type=str,
        default="test_expectancy_R_sqrtN_mean",
        choices=["test_expectancy_R_sqrtN_mean", "test_expectancy_R_mean"],
    )
    args = ap.parse_args()

    p = ft.EXPORT_DIR / "wf_summary.csv"
    if not p.exists():
        raise SystemExit(f"Missing: {p}. Run walk_forward_expectancy.py first.")

    df = pd.read_csv(p)

    # param columns = tie patys kaip grid keys
    keys = [
        "RR",
        "SL_ATR_BUFFER",
        "TDP_DEV_LOOKBACK",
        "REQUIRE_IMPULSE_BEFORE_TDP",
        "IMPULSE_LOOKBACK",
        "IMPULSE_SIZE_ATR",
        "TTS_RETEST_LOOKBACK",
        "MAX_HOLD_BARS",
        "BE_AFTER_R",
        "PARTIAL_AT_R",
        "PARTIAL_FRAC",
    ]
    missing = [k for k in keys if k not in df.columns]
    if missing:
        raise SystemExit(f"wf_summary.csv missing columns: {missing}")

    f = df.copy()
    f = f[
        (f["folds_seen"] >= args.min_folds)
        & (f["test_trades_total"] >= args.min_trades)
        & (f["test_expectancy_R_median"] >= args.min_median)
    ].copy()

    if f.empty:
        print("No rows after filters. Showing top 15 unfiltered:")
        print(df.head(15).to_string(index=False))
        raise SystemExit(1)

    f = f.sort_values(
        [args.rank_by, "test_expectancy_R_median", "folds_seen", "test_trades_total"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)

    best = f.iloc[0]
    best_params = {k: best[k] for k in keys}

    # types (csv -> python)
    best_params["REQUIRE_IMPULSE_BEFORE_TDP"] = bool(best_params["REQUIRE_IMPULSE_BEFORE_TDP"])
    best_params["TDP_DEV_LOOKBACK"] = int(best_params["TDP_DEV_LOOKBACK"])
    best_params["IMPULSE_LOOKBACK"] = int(best_params["IMPULSE_LOOKBACK"])
    best_params["TTS_RETEST_LOOKBACK"] = int(best_params["TTS_RETEST_LOOKBACK"])
    best_params["MAX_HOLD_BARS"] = int(best_params["MAX_HOLD_BARS"])

    out_json = ft.EXPORT_DIR / "best_params.json"
    out_json.write_text(json.dumps(best_params, indent=2), encoding="utf-8")

    print("Saved:", out_json)
    print("\n=== BEST PARAMSET (by filters + rank) ===")
    print(best_params)

    show_cols = keys + [
        "folds_seen",
        "test_trades_total",
        "test_expectancy_R_mean",
        "test_expectancy_R_median",
        "test_expectancy_R_sqrtN_mean",
        "test_winrate_mean",
    ]
    show_cols = [c for c in show_cols if c in f.columns]
    print("\nTop 10 candidates:")
    print(f[show_cols].head(10).to_string(index=False))


if __name__ == "__main__":
    main()
