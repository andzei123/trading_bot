from __future__ import annotations

import argparse
import itertools
import math
from dataclasses import dataclass
from pathlib import Path
import sys

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[2]
JOURNAL_DIR = THIS_FILE.parent
ENGINE_DIR = PROJECT_ROOT / "backtest" / "engine"

# make imports stable when running as a script
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(JOURNAL_DIR))
sys.path.insert(0, str(ENGINE_DIR))

import pandas as pd
import numpy as np

import filter_trades as ft
from backtest.engine import entry_model as em


# -----------------------------
# metrics
# -----------------------------
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
        candle_cache=candle_cache,
    )

    if "ctx_sub_label" not in sim.columns:
        sim["ctx_sub_label"] = pd.NA
    if "side" in sim.columns:
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


# -----------------------------
# grid
# -----------------------------
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


# -----------------------------
# walk-forward splitting
# -----------------------------
@dataclass
class WFSegment:
    i: int
    train: pd.DataFrame
    test: pd.DataFrame


def build_walk_forward_segments(
    candles: pd.DataFrame,
    folds: int = 4,
    train_frac: float = 1.0,
) -> list[WFSegment]:
    """
    Split the full series into `folds` contiguous segments (time-based).
    For fold i (i=1..folds-1):
      - test = segment i
      - train = everything before test (expanding window)
      - if train_frac < 1.0: keep only the most recent `train_frac` of that history (TAIL),
        so train is closer to the test regime.
    """
    c = candles.sort_values("timestamp").reset_index(drop=True)
    n = len(c)

    if folds < 2:
        folds = 2
    if n < folds * 50:
        # still works, but avoids absurd tiny splits
        folds = max(2, min(folds, max(2, n // 50)))

    # equal-ish segment sizes
    seg_edges = [int(round(k * n / folds)) for k in range(folds + 1)]
    seg_edges[0] = 0
    seg_edges[-1] = n

    segments: list[WFSegment] = []
    for i in range(1, folds):  # folds-1 tests
        test_start = seg_edges[i]
        test_end = seg_edges[i + 1]
        test = c.iloc[test_start:test_end].copy()

        history = c.iloc[:test_start].copy()
        if history.empty or test.empty:
            continue

        train = history
        if 0.0 < train_frac < 1.0:
            h = len(history)
            cut = int(max(1, min(h, round(h * train_frac))))
            # IMPORTANT: take the most recent part of history (tail), not the oldest (head)
            train = history.tail(cut).copy()

        segments.append(WFSegment(i=i, train=train, test=test))

    return segments


# -----------------------------
# main
# -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--folds", type=int, default=4, help="Walk-forward folds (default 4)")
    ap.add_argument(
        "--train-frac",
        type=float,
        default=1.0,
        help="Train fraction of available history (default 1.0 = full expanding window). "
             "If <1.0, uses the most recent tail of history.",
    )
    ap.add_argument("--top-k", type=int, default=10, help="How many best paramsets (from TRAIN) to evaluate on TEST")
    ap.add_argument(
        "--metric",
        type=str,
        default="expectancy_R_sqrtN",
        choices=["expectancy_R", "expectancy_R_sqrtN"],
        help="Ranking metric on TRAIN",
    )
    args = ap.parse_args()

    candles, _ = ft.load_inputs()
    candles = candles.copy()
    candles["timestamp"] = pd.to_datetime(candles["timestamp"])

    all_min = candles["timestamp"].min()
    all_max = candles["timestamp"].max()
    print(f"All: {len(candles)} | {all_min} -> {all_max}")

    segments = build_walk_forward_segments(candles, folds=int(args.folds), train_frac=float(args.train_frac))
    if not segments:
        raise SystemExit("No walk-forward segments created (too little data?).")

    grid = build_grid()
    keys = list(grid.keys())
    combos = list(itertools.product(*(grid[k] for k in keys)))

    print(f"Folds requested: {args.folds} | WF tests: {len(segments)} | Combos: {len(combos)}")
    print(f"Metric (train ranking): {args.metric} | top-k: {args.top_k}")

    # outputs
    out_dir = ft.EXPORT_DIR
    wf_train_rows = []
    wf_test_rows = []

    for seg in segments:
        train_c = seg.train
        test_c = seg.test

        print("\n" + "=" * 80)
        print(
            f"WF Fold {seg.i} | "
            f"Train {len(train_c)} ({train_c['timestamp'].min()} -> {train_c['timestamp'].max()}) | "
            f"Test {len(test_c)} ({test_c['timestamp'].min()} -> {test_c['timestamp'].max()})"
        )

        # caches per fold
        ctx_train = ft.label_tts_tdp(train_c)
        cache_train = em.build_candle_cache(train_c)

        ctx_test = ft.label_tts_tdp(test_c)
        cache_test = em.build_candle_cache(test_c)

        # run TRAIN grid
        rows_train = []
        for i, vals in enumerate(combos, 1):
            params = dict(zip(keys, vals))
            r = score_row(params, ctx_train, cache_train)
            rows_train.append(r)
            if i % 25 == 0:
                v = r.get(args.metric, float("nan"))
                print(f"{i}/{len(combos)} train {args.metric}={v:.4f} trades={r['trades_total']}")

        df_train = pd.DataFrame(rows_train)
        df_train = df_train.sort_values([args.metric, "trades_total"], ascending=[False, False]).reset_index(drop=True)

        # store full train grid for this fold
        df_train_out = df_train.copy()
        df_train_out.insert(0, "wf_fold", seg.i)
        wf_train_rows.append(df_train_out)

        # pick top-k from TRAIN
        topk = df_train.head(int(args.top_k)).copy()

        # evaluate top-k on TEST
        rows_test = []
        for rank, row in enumerate(topk.to_dict("records"), 1):
            params = {k: row[k] for k in keys}
            rtest = score_row(params, ctx_test, cache_test)
            rtest["wf_fold"] = seg.i
            rtest["rank_train"] = rank
            rtest["train_metric"] = row[args.metric]
            rtest["train_expectancy_R"] = row["expectancy_R"]
            rtest["train_expectancy_R_sqrtN"] = row["expectancy_R_sqrtN"]
            rtest["train_trades_total"] = row["trades_total"]
            rows_test.append(rtest)

        df_test = pd.DataFrame(rows_test).sort_values(["rank_train"]).reset_index(drop=True)
        wf_test_rows.append(df_test)

        show_cols = ["wf_fold", "rank_train"] + keys + [
            "trades_total",
            "winrate(W/L)%",
            "expectancy_R",
            "expectancy_R_sqrtN",
            "train_metric",
        ]
        show_cols = [c for c in show_cols if c in df_test.columns]
        print("\nTOP-K (evaluated on TEST) for this fold:")
        print(df_test[show_cols].to_string(index=False))

    # save combined outputs
    df_train_all = pd.concat(wf_train_rows, ignore_index=True) if wf_train_rows else pd.DataFrame()
    df_test_all = pd.concat(wf_test_rows, ignore_index=True) if wf_test_rows else pd.DataFrame()

    out_train = out_dir / "wf_train_grids.csv"
    out_test = out_dir / "wf_topK_test_eval.csv"
    out_summary = out_dir / "wf_summary.csv"

    if not df_train_all.empty:
        df_train_all.to_csv(out_train, index=False)
        print("\nSaved:", out_train)
    if not df_test_all.empty:
        df_test_all.to_csv(out_test, index=False)
        print("Saved:", out_test)

    # aggregate summary: average test expectancy + counts, by paramset across folds (only those that appeared in topK)
    if not df_test_all.empty:
        group_cols = keys
        agg = (
            df_test_all.groupby(group_cols, dropna=False)
            .agg(
                folds_seen=("wf_fold", "nunique"),
                test_trades_total=("trades_total", "sum"),
                test_expectancy_R_mean=("expectancy_R", "mean"),
                test_expectancy_R_median=("expectancy_R", "median"),
                test_expectancy_R_sqrtN_mean=("expectancy_R_sqrtN", "mean"),
                test_winrate_mean=("winrate(W/L)%", "mean"),
            )
            .reset_index()
        )

        agg = agg.sort_values(
            ["test_expectancy_R_sqrtN_mean", "test_expectancy_R_mean", "folds_seen", "test_trades_total"],
            ascending=[False, False, False, False],
        ).reset_index(drop=True)

        agg.to_csv(out_summary, index=False)
        print("Saved:", out_summary)

        print("\n" + "=" * 80)
        print("WF SUMMARY (top 15 paramsets across folds, ranked on TEST mean sqrtN):")
        print(agg.head(15).to_string(index=False))


if __name__ == "__main__":
    main()
