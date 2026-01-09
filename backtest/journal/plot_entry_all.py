from __future__ import annotations

import sys
from pathlib import Path
import argparse

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[2]
JOURNAL_DIR = THIS_FILE.parent
ENGINE_DIR = PROJECT_ROOT / "backtest" / "engine"

sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(JOURNAL_DIR))
sys.path.insert(0, str(ENGINE_DIR))

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import filter_trades as ft


def _to_dt(df: pd.DataFrame, col: str = "timestamp") -> pd.DataFrame:
    df = df.copy()
    df[col] = pd.to_datetime(df[col], errors="coerce")
    return df.dropna(subset=[col])


def _apply_filters(df: pd.DataFrame, args) -> pd.DataFrame:
    if df.empty:
        return df
    x = df.copy()

    if "model" in x.columns and args.model:
        x = x[x["model"].astype(str).str.upper() == args.model.upper()]
    if "side" in x.columns and args.side:
        x = x[x["side"].astype(str).str.upper() == args.side.upper()]
    if "outcome" in x.columns and args.outcome:
        x = x[x["outcome"].astype(str).str.upper() == args.outcome.upper()]
    if "ctx_sub_label" in x.columns and args.ctx:
        x = x[x["ctx_sub_label"].astype(str).str.upper() == args.ctx.upper()]

    return x


def _wl_stats(df: pd.DataFrame) -> tuple[int, int, int, int, float, float]:
    if df.empty:
        return 0, 0, 0, 0, float("nan"), float("nan")
    o = df["outcome"].astype(str).str.upper() if "outcome" in df.columns else pd.Series([], dtype=str)
    w = int((o == "WIN").sum()) if len(o) else 0
    l = int((o == "LOSS").sum()) if len(o) else 0
    be = int((o == "BE").sum()) if len(o) else 0
    nh = int((o == "NO_HIT").sum()) if len(o) else 0
    wl = w + l
    wr = (w / wl * 100.0) if wl else float("nan")
    exp_r = float(pd.to_numeric(df["R"], errors="coerce").dropna().mean()) if "R" in df.columns else float("nan")
    return len(df), w, l, nh, wr, exp_r


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=1400, help="How many last candles to plot")
    ap.add_argument("--model", type=str, default=None, help="Filter: TDP_REENTRY / TTS_RETEST")
    ap.add_argument("--side", type=str, default=None, help="Filter: LONG / SHORT")
    ap.add_argument("--outcome", type=str, default=None, help="Filter: WIN / LOSS / BE / NO_HIT")
    ap.add_argument("--ctx", type=str, default=None, help="Filter: TDP_TOP / TDP_BOT / TTS_UP / TTS_DN")
    ap.add_argument("--from_ts", type=str, default=None, help="Override start timestamp (e.g. 2025-06-01 00:00:00)")
    ap.add_argument("--to_ts", type=str, default=None, help="Override end timestamp")
    args = ap.parse_args()

    candles = pd.read_csv(ft.CANDLES_PATH, engine="python", on_bad_lines="skip")
    candles = ft._to_dt(candles, "timestamp").sort_values("timestamp").reset_index(drop=True)

    ctx = ft.label_tts_tdp(candles)

    if args.from_ts or args.to_ts:
        t0 = pd.to_datetime(args.from_ts) if args.from_ts else ctx["timestamp"].min()
        t1 = pd.to_datetime(args.to_ts) if args.to_ts else ctx["timestamp"].max()
        tail = ctx[(ctx["timestamp"] >= t0) & (ctx["timestamp"] <= t1)].copy()
    else:
        tail = ctx.tail(args.n).copy()

    if tail.empty:
        print("Nothing to plot (empty slice).")
        return

    t0, t1 = tail["timestamp"].iloc[0], tail["timestamp"].iloc[-1]

    entries_path = ft.EXPORT_DIR / "entries_generated.csv"
    sim_path = ft.EXPORT_DIR / "trades_simulated.csv"

    entries = pd.DataFrame()
    if entries_path.exists():
        entries = pd.read_csv(entries_path, engine="python", on_bad_lines="skip")
        entries = _to_dt(entries, "timestamp").sort_values("timestamp").reset_index(drop=True)
        if "side" in entries.columns:
            entries["side"] = entries["side"].astype(str).str.upper()
        if "model" in entries.columns:
            entries["model"] = entries["model"].astype(str).str.upper()
        if "ctx_sub_label" in entries.columns:
            entries["ctx_sub_label"] = entries["ctx_sub_label"].astype(str).str.upper()
        entries = entries[(entries["timestamp"] >= t0) & (entries["timestamp"] <= t1)].copy()
        entries = _apply_filters(entries, args)

    sim = pd.DataFrame()
    if sim_path.exists():
        sim = pd.read_csv(sim_path, engine="python", on_bad_lines="skip")
        sim = _to_dt(sim, "timestamp").sort_values("timestamp").reset_index(drop=True)
        for col in ["side", "model", "ctx_sub_label", "outcome"]:
            if col in sim.columns:
                sim[col] = sim[col].astype(str).str.upper()
        sim = sim[(sim["timestamp"] >= t0) & (sim["timestamp"] <= t1)].copy()
        sim = _apply_filters(sim, args)

    fig, ax = plt.subplots(figsize=(17, 7))
    ax.plot(tail["timestamp"], tail["close"], linewidth=1)

    # signals
    top = tail[tail["sub_label"] == "TDP_TOP"]
    bot = tail[tail["sub_label"] == "TDP_BOT"]
    ax.scatter(top["timestamp"], top["close"], marker="^", s=55, label="TDP_TOP")
    ax.scatter(bot["timestamp"], bot["close"], marker="v", s=55, label="TDP_BOT")

    tts_up = tail[tail["sub_label"] == "TTS_UP"]
    tts_dn = tail[tail["sub_label"] == "TTS_DN"]
    ax.scatter(tts_up["timestamp"], tts_up["close"], marker="o", s=20, label="TTS_UP")
    ax.scatter(tts_dn["timestamp"], tts_dn["close"], marker="o", s=20, label="TTS_DN")

    # entries
    if not entries.empty and "entry" in entries.columns:
        e_long = entries[entries["side"] == "LONG"]
        e_short = entries[entries["side"] == "SHORT"]
        ax.scatter(e_long["timestamp"], e_long["entry"], marker="x", s=60, label="ENTRY_LONG")
        ax.scatter(e_short["timestamp"], e_short["entry"], marker="x", s=60, label="ENTRY_SHORT")

    # outcomes
    if not sim.empty and "entry" in sim.columns:
        win = sim[sim["outcome"] == "WIN"]
        loss = sim[sim["outcome"] == "LOSS"]
        be = sim[sim["outcome"] == "BE"]
        ax.scatter(win["timestamp"], win["entry"], marker="P", s=75, label="WIN")
        ax.scatter(loss["timestamp"], loss["entry"], marker="X", s=75, label="LOSS")
        ax.scatter(be["timestamp"], be["entry"], marker="s", s=60, label="BE")

    # stats box
    sig_top = int((tail["sub_label"] == "TDP_TOP").sum())
    sig_bot = int((tail["sub_label"] == "TDP_BOT").sum())
    sig_tu = int((tail["sub_label"] == "TTS_UP").sum())
    sig_td = int((tail["sub_label"] == "TTS_DN").sum())

    en_total = int(len(entries))
    sim_total, sim_w, sim_l, sim_nh, sim_wr, sim_exp = _wl_stats(sim)
    sim_wr_s = f"{sim_wr:.2f}%" if np.isfinite(sim_wr) else "n/a"

    lines = [
        f"Period: {t0} -> {t1}",
        f"Candles shown: {len(tail)}",
        "",
        "Signals:",
        f"  TDP_TOP={sig_top}  TDP_BOT={sig_bot}",
        f"  TTS_UP={sig_tu}   TTS_DN={sig_td}",
        "",
        f"Entries (filtered): total={en_total}",
        f"Sim Trades (filtered): total={sim_total} win={sim_w} loss={sim_l} no_hit={sim_nh} winrate(W/L)={sim_wr_s}",
        f"Expectancy_R (mean R): {sim_exp:.6f}" if np.isfinite(sim_exp) else "Expectancy_R: n/a",
        "",
        f"Filters: model={args.model} side={args.side} outcome={args.outcome} ctx={args.ctx}",
    ]

    ax.text(
        0.01, 0.99, "\n".join(lines),
        transform=ax.transAxes,
        va="top", ha="left",
        bbox=dict(boxstyle="round", alpha=0.85),
        fontsize=10
    )

    ax.set_title("Signals + Entries + Simulated Trades (with CLI filters)")
    ax.set_xlabel("Time")
    ax.set_ylabel("Price")
    ax.legend(loc="lower left", ncol=4)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
