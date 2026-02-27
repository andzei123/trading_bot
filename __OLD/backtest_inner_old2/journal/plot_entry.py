from __future__ import annotations

import sys
from pathlib import Path

THIS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(THIS_DIR))

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import filter_trades as ft


# ====== PLOT SETTINGS ======
N_CANDLES = 1200

SHOW_TDP_MARKERS = True
SHOW_TTS_MARKERS = True
SHOW_ENTRIES = True
SHOW_TRADES_WL = True

ENTRIES_PATH = Path("backtest/journal/exports_trades/entries_from_ctx.csv")
TRADES_FROM_ENTRIES_PATH = Path("backtest/journal/exports_trades/trades_from_entries.csv")


def _to_dt(df: pd.DataFrame, col="timestamp") -> pd.DataFrame:
    df = df.copy()
    df[col] = pd.to_datetime(df[col], errors="coerce")
    return df.dropna(subset=[col])


def stats_from_trades(df: pd.DataFrame) -> dict:
    out = {"total": len(df), "wins": 0, "losses": 0, "winrate": np.nan, "avg_rr": np.nan}
    if df.empty:
        return out

    o = df["outcome"].astype(str).str.upper() if "outcome" in df.columns else pd.Series([], dtype=str)
    wins = int((o == "WIN").sum()) if len(o) else 0
    losses = int((o == "LOSS").sum()) if len(o) else 0
    wl = wins + losses
    winrate = (wins / wl * 100.0) if wl else np.nan

    avg_rr = np.nan
    if "rr" in df.columns and len(df):
        rr = pd.to_numeric(df["rr"], errors="coerce")
        avg_rr = float(rr.mean()) if rr.notna().any() else np.nan

    out.update({"wins": wins, "losses": losses, "winrate": winrate, "avg_rr": avg_rr})
    return out


def main():
    # candles + ctx labels
    candles = pd.read_csv(ft.CANDLES_PATH, engine="python", on_bad_lines="skip")
    candles = ft._to_dt(candles, "timestamp").sort_values("timestamp").reset_index(drop=True)
    ctx = ft.label_tts_tdp(candles)

    tail = ctx.tail(N_CANDLES).copy()
    t0, t1 = tail["timestamp"].iloc[0], tail["timestamp"].iloc[-1]

    # load entries
    entries = pd.DataFrame()
    if SHOW_ENTRIES and ENTRIES_PATH.exists():
        entries = pd.read_csv(ENTRIES_PATH, engine="python", on_bad_lines="skip")
        entries = _to_dt(entries, "timestamp").sort_values("timestamp").reset_index(drop=True)
        if "side" in entries.columns:
            entries["side"] = entries["side"].astype(str).str.upper()
        entries = entries[(entries["timestamp"] >= t0) & (entries["timestamp"] <= t1)].copy()

    # load simulated trades (with outcome)
    trades = pd.DataFrame()
    if SHOW_TRADES_WL and TRADES_FROM_ENTRIES_PATH.exists():
        trades = pd.read_csv(TRADES_FROM_ENTRIES_PATH, engine="python", on_bad_lines="skip")
        trades = _to_dt(trades, "timestamp").sort_values("timestamp").reset_index(drop=True)
        trades["side"] = trades["side"].astype(str).str.upper() if "side" in trades.columns else "NA"
        trades["outcome"] = trades["outcome"].astype(str).str.upper() if "outcome" in trades.columns else "NO_HIT"
        trades = trades[(trades["timestamp"] >= t0) & (trades["timestamp"] <= t1)].copy()

    fig, ax = plt.subplots(figsize=(16, 7))
    ax.plot(tail["timestamp"], tail["close"], linewidth=1)

    # --- markers: TDP/TTS labels ---
    if SHOW_TDP_MARKERS:
        top = tail[tail["sub_label"] == "TDP_TOP"]
        bot = tail[tail["sub_label"] == "TDP_BOT"]
        ax.scatter(top["timestamp"], top["close"], marker="^", s=60)
        ax.scatter(bot["timestamp"], bot["close"], marker="v", s=60)

    if SHOW_TTS_MARKERS:
        tts_up = tail[tail["sub_label"] == "TTS_UP"]
        tts_dn = tail[tail["sub_label"] == "TTS_DN"]
        ax.scatter(tts_up["timestamp"], tts_up["close"], marker="o", s=25)
        ax.scatter(tts_dn["timestamp"], tts_dn["close"], marker="o", s=25)

    # --- entry markers ---
    if SHOW_ENTRIES and not entries.empty:
        long_e = entries[entries["side"] == "LONG"] if "side" in entries.columns else pd.DataFrame()
        short_e = entries[entries["side"] == "SHORT"] if "side" in entries.columns else pd.DataFrame()
        if not long_e.empty and "entry" in long_e.columns:
            ax.scatter(long_e["timestamp"], long_e["entry"], marker="x", s=60)
        if not short_e.empty and "entry" in short_e.columns:
            ax.scatter(short_e["timestamp"], short_e["entry"], marker="x", s=60)

    # --- trades W/L markers ---
    if SHOW_TRADES_WL and not trades.empty:
        price_col = "entry" if "entry" in trades.columns else None
        if price_col:
            win = trades[trades["outcome"] == "WIN"]
            loss = trades[trades["outcome"] == "LOSS"]
            if not win.empty:
                ax.scatter(win["timestamp"], win[price_col], marker="P", s=70)
            if not loss.empty:
                ax.scatter(loss["timestamp"], loss[price_col], marker="P", s=70)

    ax.set_title("Close + Signals + Entries + Outcomes")
    ax.set_xlabel("Time")
    ax.set_ylabel("Price")

    # ===== stats box =====
    s = stats_from_trades(trades) if not trades.empty else {"total": 0, "wins": 0, "losses": 0, "winrate": np.nan, "avg_rr": np.nan}
    wr = f"{s['winrate']:.2f}%" if np.isfinite(s["winrate"]) else "n/a"
    avg_rr = f"{s['avg_rr']:.2f}" if np.isfinite(s["avg_rr"]) else "n/a"

    lines = [
        f"Period: {t0} -> {t1}",
        f"Candles shown: {len(tail)}",
        f"TRADES (sim): total={s['total']}  win={s['wins']}  loss={s['losses']}  winrate={wr}  avg_rr={avg_rr}",
    ]

    ax.text(
        0.01, 0.99, "\n".join(lines),
        transform=ax.transAxes,
        va="top", ha="left",
        bbox=dict(boxstyle="round", alpha=0.8),
        fontsize=10
    )

    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
