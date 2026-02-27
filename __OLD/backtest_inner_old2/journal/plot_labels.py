from __future__ import annotations

import sys
from pathlib import Path

THIS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(THIS_DIR))

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import filter_trades as ft

N_CANDLES = 1200
SHOW_TTS = True
SHOW_TDP = True
SHOW_TRADES = True

def _stats(df: pd.DataFrame) -> tuple[int, int, int, float]:
    if df.empty or "outcome" not in df.columns:
        return (len(df), 0, 0, float("nan"))
    o = df["outcome"].astype(str).str.upper()
    w = int((o == "WIN").sum())
    l = int((o == "LOSS").sum())
    wl = w + l
    wr = (w / wl * 100.0) if wl else float("nan")
    return (len(df), w, l, wr)

def main():
    candles = pd.read_csv(ft.CANDLES_PATH, engine="python", on_bad_lines="skip")
    candles = ft._to_dt(candles, "timestamp").sort_values("timestamp").reset_index(drop=True)

    ctx = ft.label_tts_tdp(candles)
    tail = ctx.tail(N_CANDLES).copy()
    t0, t1 = tail["timestamp"].iloc[0], tail["timestamp"].iloc[-1]

    trades = pd.DataFrame()
    if SHOW_TRADES and ft.TRADES_PATH.exists():
        trades = pd.read_csv(ft.TRADES_PATH, engine="python", on_bad_lines="skip")
        trades = ft._to_dt(trades, "timestamp").sort_values("timestamp").reset_index(drop=True)
        trades["side"] = trades["side"].astype(str).str.upper()
        if "outcome" in trades.columns:
            trades["outcome"] = trades["outcome"].astype(str).str.upper()
        trades = trades[(trades["timestamp"] >= t0) & (trades["timestamp"] <= t1)].copy()

    fig, ax = plt.subplots(figsize=(16, 7))
    ax.plot(tail["timestamp"], tail["close"], linewidth=1)

    # signals
    if SHOW_TDP:
        top = tail[tail["sub_label"] == "TDP_TOP"]
        bot = tail[tail["sub_label"] == "TDP_BOT"]
        ax.scatter(top["timestamp"], top["close"], marker="^", s=60, label="TDP_TOP")
        ax.scatter(bot["timestamp"], bot["close"], marker="v", s=60, label="TDP_BOT")

    if SHOW_TTS:
        tts_up = tail[tail["sub_label"] == "TTS_UP"]
        tts_dn = tail[tail["sub_label"] == "TTS_DN"]
        ax.scatter(tts_up["timestamp"], tts_up["close"], marker="o", s=25, label="TTS_UP")
        ax.scatter(tts_dn["timestamp"], tts_dn["close"], marker="o", s=25, label="TTS_DN")

    # trades
    if not trades.empty:
        price_col = "entry" if "entry" in trades.columns else None
        if price_col:
            long_t = trades[trades["side"] == "LONG"]
            short_t = trades[trades["side"] == "SHORT"]
            ax.scatter(long_t["timestamp"], long_t[price_col], marker="P", s=70, label="TRADES_LONG")
            ax.scatter(short_t["timestamp"], short_t[price_col], marker="X", s=70, label="TRADES_SHORT")

            # outcome labels
            if "outcome" in trades.columns:
                for _, r in trades.iterrows():
                    ax.text(r["timestamp"], r[price_col], str(r["outcome"]), fontsize=8)

    # stats box
    total, w, l, wr = _stats(trades)
    wrs = f"{wr:.2f}%" if np.isfinite(wr) else "n/a"

    lines = [
        f"Period: {t0} -> {t1}",
        f"Candles shown: {len(tail)}",
        f"Trades: total={total} win={w} loss={l} winrate={wrs}",
        f"Signals: TDP_TOP={int((tail['sub_label']=='TDP_TOP').sum())}  TDP_BOT={int((tail['sub_label']=='TDP_BOT').sum())}",
        f"         TTS_UP={int((tail['sub_label']=='TTS_UP').sum())}   TTS_DN={int((tail['sub_label']=='TTS_DN').sum())}",
    ]
    ax.text(
        0.01, 0.99, "\n".join(lines),
        transform=ax.transAxes,
        va="top", ha="left",
        bbox=dict(boxstyle="round", alpha=0.85),
        fontsize=10
    )

    ax.set_title("TDP/TTS labels + trades (last N candles)")
    ax.set_xlabel("Time")
    ax.set_ylabel("Price")
    ax.legend(loc="upper right")
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    main()
