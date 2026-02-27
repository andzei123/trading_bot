from __future__ import annotations

import itertools
import pandas as pd

import filter_trades as ft


def bucket_stats(df: pd.DataFrame):
    if df.empty:
        return 0, 0.0
    wl = df[df["outcome"].isin(["WIN", "LOSS"])].copy()
    w = int((wl["outcome"] == "WIN").sum())
    l = int((wl["outcome"] == "LOSS").sum())
    t = w + l
    return t, (w / t if t else 0.0)


def run_once(params: dict, candles: pd.DataFrame, trades: pd.DataFrame) -> dict:
    # set globals (TDP-only)
    ft.DEV_MIN_TDP = params["DEV_MIN_TDP"]
    ft.EXTREME_Q = params["EXTREME_Q"]
    ft.TDP_RANGE_ATR_MAX = params["TDP_RANGE_ATR_MAX"]

    ctx_m = ft.build_ctx(candles)
    merged = ft.merge_trades(trades, ctx_m)

    # tik TDP
    merged = merged[merged["ctx_label"] == "TDP"].copy()
    f = ft.apply_filters(merged)

    t_all, wr_all = bucket_stats(f)

    tdp = f[f["ctx_label"] == "TDP"]
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
    candles, trades = ft.load_inputs()

    grid = {
        "DEV_MIN_TDP": [2, 3, 4, 5, 6],
        "EXTREME_Q": [0.10, 0.15, 0.20, 0.25],
        "TDP_RANGE_ATR_MAX": [6.0, 10.0, 14.0, 18.0],
    }

    keys = list(grid.keys())
    combos = list(itertools.product(*(grid[k] for k in keys)))
    print("Combos:", len(combos))

    rows = []
    for i, vals in enumerate(combos, 1):
        params = dict(zip(keys, vals))
        r = run_once(params, candles, trades)
        rows.append(r)
        if i % 50 == 0:
            print(f"{i}/{len(combos)} winrate={r['winrate']:.4f} trades={r['trades_total']}")

    df = pd.DataFrame(rows).sort_values(["winrate", "trades_total"], ascending=[False, False])

    ft.EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = ft.EXPORT_DIR / "grid_results_tdp_only.csv"
    df.to_csv(out_path, index=False)

    print("Saved:", out_path)
    print(df.head(25).to_string(index=False))


if __name__ == "__main__":
    main()
