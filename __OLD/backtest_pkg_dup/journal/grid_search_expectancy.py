from __future__ import annotations

import itertools
import math
from pathlib import Path
import sys

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[2]
JOURNAL_DIR = THIS_FILE.parent
ENGINE_DIR = PROJECT_ROOT / "backtest" / "engine"

sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(JOURNAL_DIR))
sys.path.insert(0, str(ENGINE_DIR))

import pandas as pd
import numpy as np

import filter_trades as ft
from backtest.engine import entry_model as em



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
        candle_cache=candle_cache,  # <<< Level 2.5
    )

    if "ctx_sub_label" not in sim.columns:
        sim["ctx_sub_label"] = pd.NA
    sim["side"] = sim["side"].astype(str).str.upper()

    exp_r = expectancy_from_R(sim)
    total = int(len(sim))
    wr = wl_winrate(sim)

    out = dict(params)
    out.update({
        "trades_total": total,
        "winrate(W/L)%": round(float(wr), 4) if np.isfinite(wr) else np.nan,
        "expectancy_R": round(float(exp_r), 6) if np.isfinite(exp_r) else np.nan,
        "expectancy_R_sqrtN": round(float(exp_r) * math.sqrt(max(1, total)), 6) if np.isfinite(exp_r) else np.nan,
    })
    return out


def _top10(df: pd.DataFrame, metric: str, title: str) -> None:
    if df.empty or metric not in df.columns:
        print(f"\n{title}: no data")
        return
    x = df.copy()
    x = x.sort_values([metric, "trades_total"], ascending=[False, False]).head(10)
    print(f"\n{title}")
    print(x.to_string(index=False))


def _bucket_eval(ctx: pd.DataFrame, candle_cache: em.CandleCache, params_grid_rows: list[dict], bucket_col: str, bucket_val: str) -> pd.DataFrame:
    rows = []
    for params in params_grid_rows:
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
            candle_cache=candle_cache,  # <<< Level 2.5
        )

        if bucket_col not in sim.columns:
            continue

        part = sim[sim[bucket_col].astype(str).str.upper() == bucket_val.upper()].copy()
        total = int(len(part))
        exp_r = expectancy_from_R(part)
        wr = wl_winrate(part)

        r = dict(params)
        r.update({
            "bucket": bucket_val,
            "trades_total": total,
            "winrate(W/L)%": round(float(wr), 6) if np.isfinite(wr) else np.nan,
            "expectancy_R": round(float(exp_r), 6) if np.isfinite(exp_r) else np.nan,
            "expectancy_R_sqrtN": round(float(exp_r) * math.sqrt(max(1, total)), 6) if np.isfinite(exp_r) else np.nan,
        })
        rows.append(r)

    return pd.DataFrame(rows)


def main():
    candles, _ = ft.load_inputs()
    print(f"Candles: {len(candles)}  Period: {candles['timestamp'].min()} -> {candles['timestamp'].max()}")

    # Level 2.5 caches (1x)
    ctx = ft.label_tts_tdp(candles)
    candle_cache = em.build_candle_cache(candles)
    print(f"CTX cached: {len(ctx)} rows | Candle cache: {len(candle_cache.ts)} rows")

    grid = {
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

    keys = list(grid.keys())
    combos = list(itertools.product(*(grid[k] for k in keys)))
    print("Combos:", len(combos))

    rows = []
    for i, vals in enumerate(combos, 1):
        params = dict(zip(keys, vals))
        r = score_row(params, ctx, candle_cache)
        rows.append(r)
        if i % 25 == 0:
            print(f"{i}/{len(combos)} expectancy_R={r['expectancy_R']:.4f} trades_total={r['trades_total']}")

    df = pd.DataFrame(rows)

    out_path = ft.EXPORT_DIR / "grid_results_expectancy.csv"
    df.sort_values(["expectancy_R", "trades_total"], ascending=[False, False]).to_csv(out_path, index=False)
    print("\nSaved:", out_path)

    _top10(df, "expectancy_R", "[TOP-10] by expectancy_R")
    _top10(df, "expectancy_R_sqrtN", "[TOP-10] by expectancy_R * sqrt(trades_total)")

    params_rows = df[keys].to_dict("records")

    for bucket in ["TDP_BOT", "TDP_TOP"]:
        bdf = _bucket_eval(ctx, candle_cache, params_rows, "ctx_sub_label", bucket)
        if not bdf.empty:
            _top10(bdf, "expectancy_R", f"[TOP-10 {bucket}] by expectancy_R")
            _top10(bdf, "expectancy_R_sqrtN", f"[TOP-10 {bucket}] by expectancy_R * sqrt(trades_total)")

    for bucket in ["LONG", "SHORT"]:
        bdf = _bucket_eval(ctx, candle_cache, params_rows, "side", bucket)
        if not bdf.empty:
            _top10(bdf, "expectancy_R", f"[TOP-10 SIDE={bucket}] by expectancy_R")
            _top10(bdf, "expectancy_R_sqrtN", f"[TOP-10 SIDE={bucket}] by expectancy_R * sqrt(trades_total)")


if __name__ == "__main__":
    main()
