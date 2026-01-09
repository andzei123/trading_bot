from __future__ import annotations

from pathlib import Path
import sys
import pandas as pd
import matplotlib.pyplot as plt

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import backtest.journal.filter_trades as ft


def main(N=1000):
    candles = pd.read_csv(ft.CANDLES_PATH, engine="python", on_bad_lines="skip")
    trades = pd.read_csv(ft.TRADES_PATH, engine="python", on_bad_lines="skip")

    candles = ft._to_dt(candles, "timestamp").sort_values("timestamp").reset_index(drop=True)
    trades = ft._to_dt(trades, "timestamp").sort_values("timestamp").reset_index(drop=True)

    params = ft.Params()  # naudos tavo default (su impulse-before-TDP jei įjungta)
    ctx_m = ft.build_ctx(candles, params=params)

    merged = ft.merge_trades(trades, ctx_m)
    filtered = ft.apply_filters(merged, params=params)

    # paskutinės N žvakių
    c = candles.tail(N).copy()
    t0, t1 = c["timestamp"].min(), c["timestamp"].max()

    # ctx to last N window (kad lengviau žymėti labels)
    ctx = ft.label_tts_tdp(candles, params=params).tail(N).copy()

    # trades kurie patenka į tą patį laiką
    tr = filtered[(filtered["timestamp"] >= t0) & (filtered["timestamp"] <= t1)].copy()
    tr["side"] = tr["side"].astype(str).str.upper()
    tr["outcome"] = tr["outcome"].astype(str).str.upper()

    # kainos taškas trade'ui: jei yra entry -> naudok entry, kitaip close
    price_col = "entry" if "entry" in tr.columns else None
    if price_col is None:
        # pabandyk rasti pagal tavo CSV struktūrą
        for cand in ["entry_price", "price", "open_price"]:
            if cand in tr.columns:
                price_col = cand
                break
    if price_col is None:
        # fallback
        price_col = "close"
        # bet close nėra trades df – tada imsim merge su candles
        tr = pd.merge_asof(
            tr.sort_values("timestamp"),
            candles[["timestamp", "close"]].sort_values("timestamp"),
            on="timestamp",
            direction="backward"
        )

    # label taškai
    top = ctx[ctx["sub_label"] == "TDP_TOP"]
    bot = ctx[ctx["sub_label"] == "TDP_BOT"]

    # trades
    tr_long = tr[tr["side"] == "LONG"]
    tr_short = tr[tr["side"] == "SHORT"]

    plt.figure(figsize=(16, 6))
    plt.plot(c["timestamp"], c["close"])

    # labels
    plt.scatter(top["timestamp"], top["close"], marker="v", s=70, label="TDP_TOP")
    plt.scatter(bot["timestamp"], bot["close"], marker="^", s=70, label="TDP_BOT")

    # trades markers
    plt.scatter(tr_long["timestamp"], tr_long[price_col], marker="P", s=80, label="TRADES_LONG")
    plt.scatter(tr_short["timestamp"], tr_short[price_col], marker="X", s=80, label="TRADES_SHORT")

    # annotate outcome
    for _, row in tr.iterrows():
        txt = row.get("outcome", "")
        plt.text(row["timestamp"], float(row[price_col]), str(txt), fontsize=8)

    plt.title(f"TDP/TTS labels + trades (last {N} candles) | HTF={ft.HTF} | EXTREME_Q={params.extreme_q}")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main(N=1000)
