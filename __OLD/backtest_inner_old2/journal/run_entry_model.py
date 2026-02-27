from __future__ import annotations

from pathlib import Path
import sys

# Veikia ir taip:
# 1) python backtest/journal/run_entry_model.py
# 2) python -m backtest.journal.run_entry_model
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
import entry_model as em


# ===== Entry generation params =====
RR = 3.0
SL_ATR_BUFFER = 0.25
TDP_DEV_LOOKBACK = 6
REQUIRE_IMPULSE_BEFORE_TDP = True
IMPULSE_LOOKBACK = 10
IMPULSE_SIZE_ATR = 1.2
TTS_RETEST_LOOKBACK = 24

# ===== Simulation params =====
MAX_HOLD_BARS = 200
BE_AFTER_R = 1.0          # pvz 1.0 -> perkeliam SL į BE kai pasiekia +1R
PARTIAL_AT_R = 1.0        # pvz 1.0 -> partial at +1R
PARTIAL_FRAC = 0.7        # pvz 0.7 -> uždarom 70% ant partial


def _wl_summary(df: pd.DataFrame) -> tuple[int, int, int, int, float]:
    if df.empty:
        return 0, 0, 0, 0, float("nan")
    o = df["outcome"].astype(str).str.upper()
    w = int((o == "WIN").sum())
    l = int((o == "LOSS").sum())
    nh = int((o == "NO_HIT").sum())
    be = int((o == "BE").sum())
    wl = w + l
    wr = (w / wl * 100.0) if wl else float("nan")
    return len(df), w, l, (nh + be), wr  # "no_hit" bucket = NO_HIT + BE


def _print_result(df: pd.DataFrame) -> None:
    if df.empty:
        print("RESULT: no trades")
        return
    o = df["outcome"].astype(str).str.upper()
    win = int((o == "WIN").sum())
    loss = int((o == "LOSS").sum())
    be = int((o == "BE").sum())
    no_hit = int((o == "NO_HIT").sum())
    wl = win + loss
    wr = (win / wl * 100.0) if wl else float("nan")
    wr_s = f"{wr:.2f}%" if np.isfinite(wr) else "n/a"
    exp = df["r_multiple"].mean() if "r_multiple" in df.columns and len(df) else float("nan")
    exp_s = f"{exp:.4f}" if np.isfinite(exp) else "n/a"
    print(f"RESULT: total={len(df)} win={win} loss={loss} be={be} no_hit={no_hit} winrate(W/L)={wr_s} expectancy_R={exp_s}")


def _group_table(df: pd.DataFrame, by: str) -> pd.DataFrame:
    if df.empty or by not in df.columns:
        return pd.DataFrame(columns=[by, "total", "win", "loss", "no_hit", "winrate(W/L)", "expectancy_R"])
    rows = []
    for k, g in df.groupby(by, dropna=False):
        total, w, l, nh, wr = _wl_summary(g)
        wr_s = (f"{wr:.2f}%" if np.isfinite(wr) else "n/a")
        exp = g["r_multiple"].mean() if "r_multiple" in g.columns and len(g) else float("nan")
        exp_s = f"{exp:.4f}" if np.isfinite(exp) else "n/a"
        rows.append({
            by: str(k),
            "total": total,
            "win": w,
            "loss": l,
            "no_hit": nh,
            "winrate(W/L)": wr_s,
            "expectancy_R": exp_s
        })
    out = pd.DataFrame(rows)
    # sort by expectancy_R desc, then total desc
    def _to_num(x):
        try:
            return float(str(x))
        except:
            return -1e9
    out["_exp"] = out["expectancy_R"].apply(_to_num)
    out = out.sort_values(["_exp", "total"], ascending=[False, False]).drop(columns=["_exp"])
    return out.reset_index(drop=True)


def main():
    candles, _ = ft.load_inputs()
    print(f"\nCandles: {len(candles)}  Period: {candles['timestamp'].min()} -> {candles['timestamp'].max()}")

    ctx = ft.label_tts_tdp(candles)

    # 1) generate entries
    entries = em.generate_entries_from_ctx(
        ctx,
        rr=RR,
        sl_atr_buffer=SL_ATR_BUFFER,
        tdp_dev_lookback=TDP_DEV_LOOKBACK,
        require_impulse_before_tdp=REQUIRE_IMPULSE_BEFORE_TDP,
        impulse_lookback=IMPULSE_LOOKBACK,
        impulse_size_atr=IMPULSE_SIZE_ATR,
        tts_retest_lookback=TTS_RETEST_LOOKBACK,
    )

    entries_df = pd.DataFrame([e.__dict__ for e in entries])
    if entries_df.empty:
        print("Entries generated: 0")
        return

    entries_df["timestamp"] = pd.to_datetime(entries_df["timestamp"], errors="coerce")
    entries_df["side"] = entries_df["side"].astype(str).str.upper()
    entries_df["model"] = entries_df["model"].astype(str)

    # ctx_sub_label: inferinam (TDP_REENTRY SHORT=TOP, LONG=BOT), TTS paliekam NA
    entries_df["ctx_sub_label"] = pd.NA
    m_top = (entries_df["model"] == "TDP_REENTRY") & (entries_df["side"] == "SHORT")
    m_bot = (entries_df["model"] == "TDP_REENTRY") & (entries_df["side"] == "LONG")
    entries_df.loc[m_top, "ctx_sub_label"] = "TDP_TOP"
    entries_df.loc[m_bot, "ctx_sub_label"] = "TDP_BOT"

    entries_path = ft.EXPORT_DIR / "entries_generated.csv"
    entries_df.to_csv(entries_path, index=False)

    # 2) simulate with BE/partials
    sim_trades = em.simulate_trades(
        candles,
        entries,
        max_hold_bars=MAX_HOLD_BARS,
        be_after_r=BE_AFTER_R,
        partial_at_r=PARTIAL_AT_R,
        partial_frac=PARTIAL_FRAC,
    )

    # attach model + ctx_sub_label by timestamp+side
    sim_trades["timestamp"] = pd.to_datetime(sim_trades["timestamp"], errors="coerce")
    sim_trades["side"] = sim_trades["side"].astype(str).str.upper()
    sim_trades = sim_trades.merge(
        entries_df[["timestamp", "side", "model", "ctx_sub_label"]],
        on=["timestamp", "side"],
        how="left",
    )

    sim_path = ft.EXPORT_DIR / "trades_simulated.csv"
    sim_trades.to_csv(sim_path, index=False)

    print(f"Entries generated: {len(entries_df)}  -> {entries_path}")
    print(f"Trades simulated : {len(sim_trades)}  -> {sim_path}")
    _print_result(sim_trades)

    # D: breakdowns
    print("\n[D1] Winrate/Expectancy by ctx_sub_label (TDP_TOP vs TDP_BOT)")
    print(_group_table(sim_trades[sim_trades["ctx_sub_label"].notna()], "ctx_sub_label").to_string(index=False))

    print("\n[D2] Winrate/Expectancy by side (LONG vs SHORT)")
    print(_group_table(sim_trades, "side").to_string(index=False))

    print("\n[D3] Winrate/Expectancy by model (TDP_REENTRY vs TTS_RETEST)")
    print(_group_table(sim_trades, "model").to_string(index=False))


if __name__ == "__main__":
    main()
