from __future__ import annotations

import argparse
import itertools
import math
from pathlib import Path
import sys

# --- paths (robust, drop-in) ---
THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[2]  # .../PythonProject
BACKTEST_DIR = PROJECT_ROOT / "backtest"
ENGINE_DIR = BACKTEST_DIR / "engine"
JOURNAL_DIR = THIS_FILE.parent

# Ensure "backtest" is importable as a package
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
# Ensure journal-local imports work (filter_trades.py)
if str(JOURNAL_DIR) not in sys.path:
    sys.path.insert(0, str(JOURNAL_DIR))

import pandas as pd
import numpy as np

import filter_trades as ft
from backtest.engine import entry_model as em  # ✅ always points to backtest/engine/entry_model.py


def wl_winrate(df: pd.DataFrame) -> float:
    if df.empty:
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


def score_row(params: dict, ctx: pd.DataFrame, candle_cache: em.CandleCache) -> dict:
    entries = em.generate_entries_from_ctx(
        ctx,
        rr=params["RR"],
        sl_atr_buffer=params["SL_ATR_BUFFER"],
        tdp_dev_lookback=params["TDP_DEV_LOOKBACK"],
        require_impulse_before_tdp=params["REQUIRE_IMPULSE_BEFORE_TDP"],
        impulse_lookback=params["IMPULSE_LOOKBACK"],
        impulse_size_atr=params["IMPULSE_SIZE_ATR"],
        tts_retest_lookback=params["TTS_RETEST_LOOKBACK"],
    )

    sim = em.simulate_trades(
        candles=pd.DataFrame(),  # unused when candle_cache is passed
        entries=entries,
        max_hold_bars=params["MAX_HOLD_BARS"],
        be_after_r=params["BE_AFTER_R"],
        partial_at_r=params["PARTIAL_AT_R"],
        partial_frac=params["PARTIAL_FRAC"],
        candle_cache=candle_cache,  # ✅ cache mode
    )

    if "ctx_sub_label" not in sim.columns:
        sim["ctx_sub_label"] = pd.NA
    sim["side"] = sim["side"].astype(str).str.upper()

    exp_r = expectancy_from_R(sim)
    total = int(len(sim))
    wr = wl_winrate(sim)

    out = dict(params)
    out.update(
        {
            "trades_total": total,
            "winrate(W/L)%": round(float(wr), 6) if np.isfinite(wr) else np.nan,
            "expectancy_R": round(float(exp_r), 6) if np.isfinite(exp_r) else np.nan,
            "expectancy_R_sqrtN": round(float(exp_r) * math.sqrt(max(1, total)), 6)
            if np.isfinite(exp_r)
            else np.nan,
        }
    )
    return out


def build_grid() -> dict:
    # same as grid_search_expectancy.py
    return {
        "RR": [2.0, 3.0],
        "SL_ATR_BUFFER": [0.15, 0.25],
        "TDP_DEV_LOOKBACK": [4, 6, 8],
        "REQUIRE_IMPULSE_BEFORE_TDP": [True],
        "IMPULSE_LOOKBACK": [10, 20, 30],
        "IMPULSE_SIZE_ATR": [1.0, 1.2, 1.5],
        "TTS_RETEST_LOOKBACK": [24],
        "MAX_HOLD_BARS": [200],
        "BE_AFTER_R": [1.0],
        "PARTIAL_AT_R": [1.0],
        "PARTIAL_FRAC": [0.7],
    }


def time_split(candles: pd.DataFrame, train_frac: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    c = candles.sort_values("timestamp").reset_index(drop=True)
    n = len(c)
    cut = int(max(1, min(n - 1, round(n * train_frac))))
    train = c.iloc[:cut].copy()
    test = c.iloc[cut:].copy()
    return train, test


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--train-frac", type=float, default=0.70, help="Time-based train fraction (0..1)")
    ap.add_argument("--top-k", type=int, default=10, help="How many best paramsets to evaluate on test")
    ap.add_argument(
        "--metric",
        type=str,
        default="expectancy_R_sqrtN",
        choices=["expectancy_R", "expectancy_R_sqrtN"],
        help="Ranking metric on TRAIN",
    )
    args = ap.parse_args()

    # --- load candles ---
    candles, _ = ft.load_inputs()
    candles = candles.copy()
    candles["timestamp"] = pd.to_datetime(candles["timestamp"])

    train_c, test_c = time_split(candles, args.train_frac)

    print(f"All:   {len(candles)} | {candles['timestamp'].min()} -> {candles['timestamp'].max()}")
    print(f"Train: {len(train_c)} | {train_c['timestamp'].min()} -> {train_c['timestamp'].max()}")
    print(f"Test:  {len(test_c)} | {test_c['timestamp'].min()} -> {test_c['timestamp'].max()}")

    # --- caches per split ---
    ctx_train = ft.label_tts_tdp(train_c)
    cache_train = em.build_candle_cache(train_c)
    print(f"TRAIN CTX: {len(ctx_train)} | TRAIN cache ts: {len(cache_train.ts)}")

    ctx_test = ft.label_tts_tdp(test_c)
    cache_test = em.build_candle_cache(test_c)
    print(f"TEST  CTX: {len(ctx_test)} | TEST  cache ts: {len(cache_test.ts)}")

    # --- grid ---
    grid = build_grid()
    keys = list(grid.keys())
    combos = list(itertools.product(*(grid[k] for k in keys)))
    print("Combos:", len(combos))

    # --- run TRAIN grid ---
    rows_train = []
    for i, vals in enumerate(combos, 1):
        params = dict(zip(keys, vals))
        r = score_row(params, ctx_train, cache_train)
        rows_train.append(r)
        if i % 25 == 0:
            print(f"{i}/{len(combos)} train {args.metric}={r[args.metric]:.4f} trades={r['trades_total']}")

    df_train = pd.DataFrame(rows_train)
    df_train = df_train.sort_values([args.metric, "trades_total"], ascending=[False, False]).reset_index(drop=True)

    out_train_grid = ft.EXPORT_DIR / "train_grid_results.csv"
    df_train.to_csv(out_train_grid, index=False)
    print("Saved:", out_train_grid)

    # --- pick TOP-K (by TRAIN metric) ---
    topk = df_train.head(int(args.top_k)).copy()
    out_topk_train = ft.EXPORT_DIR / "topK_train.csv"
    topk.to_csv(out_topk_train, index=False)
    print("Saved:", out_topk_train)

    # --- evaluate TOP-K on TEST ---
    rows_test = []
    for rank, row in enumerate(topk.to_dict("records"), 1):
        params = {k: row[k] for k in keys}
        rtest = score_row(params, ctx_test, cache_test)
        rtest["rank_train"] = rank
        rtest["train_metric"] = row[args.metric]
        rows_test.append(rtest)

    df_test = pd.DataFrame(rows_test).sort_values(["rank_train"]).reset_index(drop=True)

    out_topk_test = ft.EXPORT_DIR / "topK_test_eval.csv"
    df_test.to_csv(out_topk_test, index=False)
    print("Saved:", out_topk_test)

    # --- show summary ---
    print("\n=== TOP-K summary (evaluated on TEST) ===")
    show_cols = (
        ["rank_train"]
        + keys
        + ["trades_total", "winrate(W/L)%", "expectancy_R", "expectancy_R_sqrtN", "train_metric"]
    )
    show_cols = [c for c in show_cols if c in df_test.columns]
    print(df_test[show_cols].to_string(index=False))


if __name__ == "__main__":
    main()
