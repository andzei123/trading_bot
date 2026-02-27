from __future__ import annotations

import itertools
from pathlib import Path
import math

import pandas as pd
import numpy as np

import filter_trades as ft

# allow running as script if needed
import sys
from pathlib import Path as _P
THIS_FILE = _P(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[2]
ENGINE_DIR = PROJECT_ROOT / "backtest" / "engine"
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(ENGINE_DIR))
import entry_model as em


def _winrate_wl(sim: pd.DataFrame) -> float:
    if sim.empty:
        return float("nan")
    o = sim["outcome"].astype(str).str.upper()
    w = int((o == "WIN").sum())
    l = int((o == "LOSS").sum())
    wl = w + l
    return (w / wl * 100.0) if wl else float("nan")


def _expectancy(sim: pd.DataFrame) -> float:
    if sim.empty or "r_multiple" not in sim.columns:
        return float("nan")
    return float(sim["r_multiple"].mean())


def _score_sqrtN(exp_r: float, n: int) -> float:
    if (not np.isfinite(exp_r)) or n <= 0:
        return float("nan")
    return exp_r * math.sqrt(n)


def _top10(df: pd.DataFrame, score_col: str, title: str, cols_show: list[str]) -> None:
    if df.empty or score_col not in df.columns:
        print(f"\n{title}: (no data)")
        return
    x = df.copy()
    x = x[np.isfinite(x[score_col].astype(float))]
    x = x.sort_values([score_col, "trades_total"], ascending=[False, False]).head(10)
    print(f"\n{title}")
    print(x[cols_show].to_string(index=False))


def _bucket_eval(sim: pd.DataFrame, bucket: str) -> pd.DataFrame:
    if sim.empty:
        return pd.DataFrame(columns=["bucket", "trades_total", "winrate(W/L)%", "expectancy_R", "expectancy_R_sqrtN"])
    s = sim.copy()
    if bucket == "TDP_TOP":
        s = s[s["ctx_sub_label"] == "TDP_TOP"]
    elif bucket == "TDP_BOT":
        s = s[s["ctx_sub_label"] == "TDP_BOT"]
    elif bucket == "LONG":
        s = s[s["side"] == "LONG"]
    elif bucket == "SHORT":
        s = s[s["side"] == "SHORT"]
    else:
        return pd.DataFrame()

    n = len(s)
    wr = _winrate_wl(s)
    exp_r = _expectancy(s)
    return pd.DataFrame([{
        "bucket": bucket,
        "trades_total": n,
        "winrate(W/L)%": wr,
        "expectancy_R": exp_r,
        "expectancy_R_sqrtN": _score_sqrtN(exp_r, n)
    }])


def run_once(params: dict, candles: pd.DataFrame) -> dict:
    ctx = ft.label_tts_tdp(candles)

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

    # entries df for ctx_sub_label inference
    entries_df = pd.DataFrame([e.__dict__ for e in entries])
    if entries_df.empty:
        out = dict(params)
        out.update({"trades_total": 0, "winrate(W/L)%": float("nan"), "expectancy_R": float("nan"), "expectancy_R_sqrtN": float("nan")})
        return out

    entries_df["timestamp"] = pd.to_datetime(entries_df["timestamp"], errors="coerce")
    entries_df["side"] = entries_df["side"].astype(str).str.upper()
    entries_df["model"] = entries_df["model"].astype(str)
    entries_df["ctx_sub_label"] = pd.NA
    entries_df.loc[(entries_df["model"] == "TDP_REENTRY") & (entries_df["side"] == "SHORT"), "ctx_sub_label"] = "TDP_TOP"
    entries_df.loc[(entries_df["model"] == "TDP_REENTRY") & (entries_df["side"] == "LONG"), "ctx_sub_label"] = "TDP_BOT"

    sim = em.simulate_trades(
        candles,
        entries,
        max_hold_bars=params["MAX_HOLD_BARS"],
        be_after_r=params["BE_AFTER_R"],
        partial_at_r=params["PARTIAL_AT_R"],
        partial_frac=params["PARTIAL_FRAC"],
    )

    if sim.empty:
        out = dict(params)
        out.update({"trades_total": 0, "winrate(W/L)%": float("nan"), "expectancy_R": float("nan"), "expectancy_R_sqrtN": float("nan")})
        return out

    sim["timestamp"] = pd.to_datetime(sim["timestamp"], errors="coerce")
    sim["side"] = sim["side"].astype(str).str.upper()

    sim = sim.merge(
        entries_df[["timestamp", "side", "model", "ctx_sub_label"]],
        on=["timestamp", "side"],
        how="left",
    )

    n = len(sim)
    wr = _winrate_wl(sim)
    exp_r = _expectancy(sim)
    exp_sqrt = _score_sqrtN(exp_r, n)

    out = dict(params)
    out.update({
        "trades_total": n,
        "winrate(W/L)%": wr,
        "expectancy_R": exp_r,
        "expectancy_R_sqrtN": exp_sqrt,
    })

    # store bucket metrics for later top10 per bucket
    for b in ["TDP_TOP", "TDP_BOT", "LONG", "SHORT"]:
        bdf = _bucket_eval(sim, b)
        if len(bdf):
            out[f"{b}_trades_total"] = int(bdf.loc[0, "trades_total"])
            out[f"{b}_winrate(W/L)%"] = float(bdf.loc[0, "winrate(W/L)%"]) if np.isfinite(bdf.loc[0, "winrate(W/L)%"]) else float("nan")
            out[f"{b}_expectancy_R"] = float(bdf.loc[0, "expectancy_R"]) if np.isfinite(bdf.loc[0, "expectancy_R"]) else float("nan")
            out[f"{b}_expectancy_R_sqrtN"] = float(bdf.loc[0, "expectancy_R_sqrtN"]) if np.isfinite(bdf.loc[0, "expectancy_R_sqrtN"]) else float("nan")

    return out


def main():
    candles, _ = ft.load_inputs()
    print(f"Candles: {len(candles)}  Period: {candles['timestamp'].min()} -> {candles['timestamp'].max()}")

    grid = {
        "RR": [2.0, 3.0],
        "SL_ATR_BUFFER": [0.15, 0.25],
        "TDP_DEV_LOOKBACK": [4, 8],
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
        r = run_once(params, candles)
        rows.append(r)
        if i % 25 == 0:
            print(f"{i}/{len(combos)} expectancy_R={r['expectancy_R']:.4f} trades_total={r['trades_total']}")

    df = pd.DataFrame(rows)

    out_path = ft.EXPORT_DIR / "grid_results_expectancy.csv"
    df.to_csv(out_path, index=False)
    print(f"\nSaved: {out_path}")

    # common columns to show
    base_cols = keys + ["trades_total", "winrate(W/L)%", "expectancy_R", "expectancy_R_sqrtN"]

    _top10(df, "expectancy_R", "[TOP-10] by expectancy_R", base_cols)
    _top10(df, "expectancy_R_sqrtN", "[TOP-10] by expectancy_R * sqrt(trades_total)", base_cols)

    # bucket top10s
    for bucket in ["TDP_BOT", "TDP_TOP", "LONG", "SHORT"]:
        cols = keys + ["trades_total", "winrate(W/L)%", "expectancy_R", "expectancy_R_sqrtN"]
        b_exp = f"{bucket}_expectancy_R"
        b_sqrt = f"{bucket}_expectancy_R_sqrtN"
        b_n = f"{bucket}_trades_total"
        b_wr = f"{bucket}_winrate(W/L)%"

        if b_exp in df.columns:
            tmp = df.copy()
            tmp = tmp.rename(columns={
                b_n: "trades_total",
                b_wr: "winrate(W/L)%",
                b_exp: "expectancy_R",
                b_sqrt: "expectancy_R_sqrtN",
            })
            _top10(tmp, "expectancy_R", f"[TOP-10 {bucket}] by expectancy_R", keys + ["trades_total", "winrate(W/L)%", "expectancy_R", "expectancy_R_sqrtN"])
            _top10(tmp, "expectancy_R_sqrtN", f"[TOP-10 {bucket}] by expectancy_R * sqrt(trades_total)", keys + ["trades_total", "winrate(W/L)%", "expectancy_R", "expectancy_R_sqrtN"])


if __name__ == "__main__":
    main()
