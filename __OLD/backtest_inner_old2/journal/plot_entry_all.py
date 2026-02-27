from __future__ import annotations

import sys
from pathlib import Path
import argparse

THIS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = THIS_DIR.parents[2]
ENGINE_DIR = PROJECT_ROOT / "backtest" / "engine"

sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(THIS_DIR))
sys.path.insert(0, str(ENGINE_DIR))

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import filter_trades as ft


def _to_dt(df: pd.DataFrame, col: str = "timestamp") -> pd.DataFrame:
    df = df.copy()
    df[col] = pd.to_datetime(df[col], errors="coerce")
    return df.dropna(subset=[col])


def _apply_filters(df: pd.DataFrame, model: str | None, side: str | None, outcome: str | None, ctx: str | None) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()

    if "model" in out.columns and model:
        out["model"] = out["model"].astype(str)
        out = out[out["model"] == model]

    if "side" in out.columns and side:
        out["side"] = out["side"].astype(str).str.upper()
        out = out[out["side"] == side.upper()]

    if "outcome" in out.columns and outcome:
        out["outcome"] = out["outcome"].astype(str).str.upper()
        out = out[out["outcome"] == outcome.upper()]

    if "ctx_sub_label" in out.columns and ctx:
        out["ctx_sub_label"] = out["ctx_sub_label"].astype(str)
        out = out[out["ctx_sub_label"] == ctx]

    return out


def _wl_stats(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"total": 0, "win": 0, "loss": 0, "be": 0, "no_hit": 0, "winrate": float("nan"), "expectancy": float("nan")}
    o = df["outcome"].astype(str).str.upper() if "outcome" in df.columns else pd.Series(["NA"] * len(df))
    w = int((o == "WIN").sum())
    l = int((o == "LOSS").sum())
    be = int((o == "BE").sum())
    nh = int((o == "NO_HIT").sum())
    wl = w + l
    wr = (w / wl * 100.0) if wl else float("nan")
    exp = df["r_multiple"].mean() if "r_multiple" in df.columns else float("nan")
    return {"total": len(df), "win": w, "loss": l, "be": be, "no_hit": nh, "winrate": wr, "expectancy": exp}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=1400, help="kiek žvakių rodyti (tail)")
    ap.add_argument("--model", type=str, default=None, help="pvz TDP_REENTRY arba TTS_RETEST")
    ap.add_argument("--side", type=str, default=None, help="LONG arba SHORT")
    ap.add_argument("--outcome", type=str, default=None, help="WIN / LOSS / BE / NO_HIT")
    ap.add_argument("--ctx", type=str, default=None, help="TDP_TOP / TDP_BOT (iš ctx_sub_label)")
    ap.add_argument("--no-lines", action="store_true", help="neišvedinėti entry->exit linijų")
    args = ap.parse_args()

    # candles + ctx (signals)
    candles = pd.read_csv(ft.CANDLES_PATH, engine="python", on_bad_lines="skip")
    candles = ft._to_dt(candles, "timestamp").sort_values("timestamp").reset_index(drop=True)

    ctx = ft.label_tts_tdp(candles)
    tail = ctx.tail(args.n).copy()
    t0, t1 = tail["timestamp"].iloc[0], tail["timestamp"].iloc[-1]

    # load entries + simulated trades
    entries_path = ft.EXPORT_DIR / "entries_generated.csv"
    sim_path = ft.EXPORT_DIR / "trades_simulated.csv"

    entries = pd.DataFrame()
    if entries_path.exists():
        entries = pd.read_csv(entries_path, engine="python", on_bad_lines="skip")
        entries = _to_dt(entries, "timestamp").sort_values("timestamp").reset_index(drop=True)
        if "side" in entries.columns:
            entries["side"] = entries["side"].astype(str).str.upper()
        entries = entries[(entries["timestamp"] >= t0) & (entries["timestamp"] <= t1)].copy()

    sim = pd.DataFrame()
    if sim_path.exists():
        sim = pd.read_csv(sim_path, engine="python", on_bad_lines="skip")
        sim = _to_dt(sim, "timestamp").sort_values("timestamp").reset_index(drop=True)
        if "side" in sim.columns:
            sim["side"] = sim["side"].astype(str).str.upper()
        if "outcome" in sim.columns:
            sim["outcome"] = sim["outcome"].astype(str).str.upper()
        sim = sim[(sim["timestamp"] >= t0) & (sim["timestamp"] <= t1)].copy()

    # apply CLI filters
    entries_f = _apply_filters(entries, args.model, args.side, None, args.ctx)
    sim_f = _apply_filters(sim, args.model, args.side, args.outcome, args.ctx)

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

    # entries (filtered)
    if not entries_f.empty and "entry" in entries_f.columns:
        e_long = entries_f[entries_f["side"] == "LONG"]
        e_short = entries_f[entries_f["side"] == "SHORT"]
        ax.scatter(e_long["timestamp"], e_long["entry"], marker="x", s=60, label="ENTRY_LONG (filtered)")
        ax.scatter(e_short["timestamp"], e_short["entry"], marker="x", s=60, label="ENTRY_SHORT (filtered)")

    # simulated outcomes (filtered)
    if not sim_f.empty and "entry" in sim_f.columns:
        win = sim_f[sim_f["outcome"] == "WIN"]
        loss = sim_f[sim_f["outcome"] == "LOSS"]
        be = sim_f[sim_f["outcome"] == "BE"]

        ax.scatter(win["timestamp"], win["entry"], marker="P", s=80, label="WIN (filtered)")
        ax.scatter(loss["timestamp"], loss["entry"], marker="X", s=80, label="LOSS (filtered)")
        ax.scatter(be["timestamp"], be["entry"], marker="s", s=60, label="BE (filtered)")

        if (not args.no_lines) and ("exit_timestamp" in sim_f.columns) and ("exit_price" in sim_f.columns):
            for _, r in sim_f.iterrows():
                if pd.isna(r.get("exit_timestamp")) or pd.isna(r.get("exit_price")):
                    continue
                ax.plot([r["timestamp"], r["exit_timestamp"]], [r["entry"], r["exit_price"]], linewidth=0.8, alpha=0.35)

    # stats box
    sig_top = int((tail["sub_label"] == "TDP_TOP").sum())
    sig_bot = int((tail["sub_label"] == "TDP_BOT").sum())
    sig_tu = int((tail["sub_label"] == "TTS_UP").sum())
    sig_td = int((tail["sub_label"] == "TTS_DN").sum())

    s_sim = _wl_stats(sim_f)
    wr = f"{s_sim['winrate']:.2f}%" if np.isfinite(s_sim["winrate"]) else "n/a"
    exp = f"{s_sim['expectancy']:.4f}" if np.isfinite(s_sim["expectancy"]) else "n/a"

    filters_line = f"Filters: model={args.model or '*'} side={args.side or '*'} outcome={args.outcome or '*'} ctx={args.ctx or '*'}"

    lines = [
        f"Period: {t0} -> {t1}",
        f"Candles shown: {len(tail)}",
        "",
        "Signals (shown window):",
        f"  TDP_TOP={sig_top}  TDP_BOT={sig_bot}",
        f"  TTS_UP={sig_tu}   TTS_DN={sig_td}",
        "",
        filters_line,
        f"Filtered SIM trades: total={s_sim['total']} win={s_sim['win']} loss={s_sim['loss']} be={s_sim['be']} no_hit={s_sim['no_hit']}",
        f"Winrate(W/L)={wr}   Expectancy_R={exp}",
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
