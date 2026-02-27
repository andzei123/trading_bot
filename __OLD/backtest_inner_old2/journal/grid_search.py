from __future__ import annotations

import itertools
from pathlib import Path
import pandas as pd

import backtest.journal.filter_trades as ft


def bucket_stats(df: pd.DataFrame):
    wl = df[df["outcome"].isin(["WIN", "LOSS"])]
    w = int((wl["outcome"] == "WIN").sum())
    l = int((wl["outcome"] == "LOSS").sum())
    t = w + l
    return t, (w / t if t else 0.0)


def run_once(candles: pd.DataFrame, trades: pd.DataFrame, params: dict) -> dict:
    # --- set globals for this run (TDP-only) ---
    ft.DEV_MIN_TDP = int(params["DEV_MIN_TDP"])
    ft.EXTREME_Q = float(params["EXTREME_Q"])
    ft.TDP_RANGE_ATR_MAX = float(params["TDP_RANGE_ATR_MAX"])

    ft.REQUIRE_IMPULSE_BEFORE_TDP = bool(params["REQUIRE_IMPULSE_BEFORE_TDP"])
    ft.IMPULSE_LOOKBACK = int(params["IMPULSE_LOOKBACK"])
    ft.IMPULSE_SIZE_ATR = float(params["IMPULSE_SIZE_ATR"])

    # compute ctx (depends on params!)
    ctx = ft.label_tts_tdp(candles)

    ctx_m = ctx.rename(columns={
        "label": "ctx_label",
        "sub_label": "ctx_sub_label",
        "tts_dir": "ctx_tts_dir",
        "tdp_dir": "ctx_tdp_dir",
    })

    merged = pd.merge_asof(
        trades,
        ctx_m[[
            "timestamp",
            "ctx_label", "ctx_sub_label",
            "htf_trend", "ctx_tts_dir", "ctx_tdp_dir",
            "impulse_atr", "range_width_atr", "dev_count", "pos_in_range",
            "impulse_recent", "impulse_dir", "impulse_move_atr",
        ]],
        on="timestamp",
        direction="backward",
    )

    merged["side"] = merged["side"].astype(str).str.upper()
    merged["outcome"] = merged["outcome"].astype(str).str.upper()

    # --- TDP-only filtras: imam tik TDP trade’us su teisinga kryptimi ---
    ok_dir = (
        (merged["ctx_label"] == "TDP") &
        merged["ctx_tdp_dir"].notna() &
        (merged["side"] == merged["ctx_tdp_dir"])
    )
    f = merged[ok_dir].copy()

    # optional trend filter (jei kada nors įjungsi ft.REQUIRE_TREND_FOR_TDP)
    if ft.REQUIRE_TREND_FOR_TDP:
        f = f[~((f["ctx_sub_label"] == "TDP_TOP") & (f["htf_trend"] != "DOWN"))].copy()
        f = f[~((f["ctx_sub_label"] == "TDP_BOT") & (f["htf_trend"] != "UP"))].copy()

    if ft.REQUIRE_EXTREME_FOR_TDP:
        f = f[~((f["ctx_label"] == "TDP") & (f["ctx_tdp_dir"].isna()))].copy()

    # stats
    t_all, wr_all = bucket_stats(f)

    tdp = f  # jau tdp-only
    t_tdp, wr_tdp = bucket_stats(tdp)
    t_tdp_top, wr_tdp_top = bucket_stats(tdp[tdp["ctx_sub_label"] == "TDP_TOP"])
    t_tdp_bot, wr_tdp_bot = bucket_stats(tdp[tdp["ctx_sub_label"] == "TDP_BOT"])

    out = dict(params)
    out.update({
        "trades_total": t_all,
        "winrate": wr_all,

        "tdp_trades": t_tdp,
        "tdp_wr": wr_tdp,

        "tdp_top_trades": t_tdp_top,
        "tdp_top_wr": wr_tdp_top,
        "tdp_bot_trades": t_tdp_bot,
        "tdp_bot_wr": wr_tdp_bot,
    })
    return out


def main():
    # load once (greičiau)
    if not ft.CANDLES_PATH.exists():
        raise FileNotFoundError(f"Missing {ft.CANDLES_PATH}")
    if not ft.TRADES_PATH.exists():
        raise FileNotFoundError(f"Missing {ft.TRADES_PATH}")

    candles = pd.read_csv(ft.CANDLES_PATH, engine="python", on_bad_lines="skip")
    trades  = pd.read_csv(ft.TRADES_PATH, engine="python", on_bad_lines="skip")

    candles = ft._to_dt(candles, "timestamp").sort_values("timestamp").reset_index(drop=True)
    trades  = ft._to_dt(trades, "timestamp").sort_values("timestamp").reset_index(drop=True)

    # --- TDP-only grid ---
    grid = {
        "DEV_MIN_TDP": [4, 5, 6],
        "EXTREME_Q": [0.10, 0.15, 0.20, 0.25],
        "TDP_RANGE_ATR_MAX": [6.0, 10.0, 14.0, 18.0],

        "REQUIRE_IMPULSE_BEFORE_TDP": [True],   # rekomenduoju palikti True
        "IMPULSE_LOOKBACK": [10, 20, 30, 40],
        "IMPULSE_SIZE_ATR": [1.0, 1.2, 1.5, 2.0],
    }

    keys = list(grid.keys())
    combos = list(itertools.product(*(grid[k] for k in keys)))
    print("Combos:", len(combos))

    rows = []
    for i, vals in enumerate(combos, 1):
        params = dict(zip(keys, vals))
        r = run_once(candles, trades, params)
        rows.append(r)
        if i % 50 == 0:
            print(f"{i}/{len(combos)} winrate={r['winrate']:.4f} trades={r['trades_total']}")

    df = pd.DataFrame(rows).sort_values(["winrate", "trades_total"], ascending=[False, False])

    out_dir = Path("backtest/journal/exports_trades")
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "grid_results_tdp_only.csv"
    df.to_csv(out_path, index=False)

    print("Saved:", out_path)
    print(df.head(25).to_string(index=False))


if __name__ == "__main__":
    main()
