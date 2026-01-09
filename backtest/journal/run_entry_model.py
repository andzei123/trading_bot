from __future__ import annotations

from pathlib import Path
import sys
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

import filter_trades as ft
import entry_model as em


# ===== defaults =====
RR = 2.0
RR_LONG = 2.0
SL_ATR_BUFFER = 0.15
TDP_DEV_LOOKBACK = 8

REQUIRE_IMPULSE_BEFORE_TDP = True
IMPULSE_LOOKBACK = 10
IMPULSE_SIZE_ATR = 1.0

# reclaim mechanics
RECLAIM_BUF_ATR = 0.02
RECLAIM_LOOKAHEAD = 6
HAD_IMP_DOWN_WINDOW = 24

TTS_RETEST_LOOKBACK = 24
MAX_HOLD_BARS = 200

# BE/partials
BE_AFTER_R = 1.0
PARTIAL_AT_R = 1.0
PARTIAL_FRAC = 0.7

# =========================
# ✅ STEP 6.3 — Profile flags (prop-safe)
# =========================
ENABLE_LONG_TREND_UP = True
ENABLE_SHORT_TREND_DOWN = False
SHORT_TREND_STRENGTH_MIN = 0.5


def _wl_summary(df: pd.DataFrame) -> tuple[int, int, int, int, int, float, float]:
    if df.empty:
        return 0, 0, 0, 0, 0, float("nan"), float("nan")
    o = df["outcome"].astype(str).str.upper()
    w = int((o == "WIN").sum())
    l = int((o == "LOSS").sum())
    be = int((o == "BE").sum())
    nh = int((o == "NO_HIT").sum())
    wl = w + l
    wr = (w / wl * 100.0) if wl else float("nan")
    exp_r = float(pd.to_numeric(df["R"], errors="coerce").dropna().mean()) if "R" in df.columns else float("nan")
    return len(df), w, l, be, nh, wr, exp_r


def _group_table(df: pd.DataFrame, by: str) -> pd.DataFrame:
    if df.empty or by not in df.columns:
        return pd.DataFrame(columns=[by, "total", "win", "loss", "be", "no_hit", "winrate(W/L)", "expectancy_R"])
    rows = []
    for k, g in df.groupby(by):
        total, w, l, be, nh, wr, exp_r = _wl_summary(g)
        rows.append({
            by: k,
            "total": total,
            "win": w,
            "loss": l,
            "be": be,
            "no_hit": nh,
            "winrate(W/L)": (f"{wr:.2f}%" if np.isfinite(wr) else "n/a"),
            "expectancy_R": (round(exp_r, 6) if np.isfinite(exp_r) else np.nan),
        })
    out = pd.DataFrame(rows)

    def _wr_num(x):
        try:
            return float(str(x).replace("%", ""))
        except Exception:
            return -1.0

    out["_wr"] = out["winrate(W/L)"].apply(_wr_num)
    out = out.sort_values(["_wr", "total"], ascending=[False, False]).drop(columns=["_wr"])
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        type=str,
        default="combined",
        choices=["combined", "long_only", "short_only", "range_only"],
        help="combined=all entries, long_only=only LONG, short_only=only SHORT, range_only=only PHASE_RANGE entries (MVP).",
    )

    # split-test params
    parser.add_argument("--from", dest="from_ts", default=None, help="YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--to", dest="to_ts", default=None, help="YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")

    # --- Trend params ---
    parser.add_argument("--trend_rr", type=float, default=None)
    parser.add_argument("--trend_rr_long", type=float, default=None)
    parser.add_argument("--trend_sl_atr", type=float, default=None)

    # --- Range params (v2) ---
    parser.add_argument("--range_sl_atr", type=float, default=1.5)
    parser.add_argument("--range_dev_buf_atr", type=float, default=0.08)
    parser.add_argument("--range_retest_tol_atr", type=float, default=0.15)
    parser.add_argument("--range_reclaim_lookahead", type=int, default=24)
    parser.add_argument("--range_retest_lookahead", type=int, default=36)
    parser.add_argument("--range_min_width_atr", type=float, default=4.0)
    parser.add_argument("--range_cooldown", type=int, default=6)

    args = parser.parse_args()

    candles, _ = ft.load_inputs()
    candles["timestamp"] = pd.to_datetime(candles["timestamp"], errors="coerce")
    candles = candles.dropna(subset=["timestamp"])

    if args.from_ts:
        t0 = pd.to_datetime(args.from_ts)
        candles = candles[candles["timestamp"] >= t0]
    if args.to_ts:
        t1 = pd.to_datetime(args.to_ts)
        candles = candles[candles["timestamp"] <= t1]

    candles = candles.sort_values("timestamp").reset_index(drop=True)

    print(f"\nCandles: {len(candles)}  Period: {candles['timestamp'].min()} -> {candles['timestamp'].max()}")
    print(f"Mode: {args.mode}")

    ctx = ft.build_ctx(candles)

    entries = em.generate_entries_from_ctx(
        ctx,
        rr=RR,
        rr_long=RR_LONG,
        sl_atr_buffer=SL_ATR_BUFFER,

        trend_rr=args.trend_rr,
        trend_rr_long=args.trend_rr_long,
        trend_sl_atr_buffer=args.trend_sl_atr,

        range_sl_atr_buffer=float(args.range_sl_atr),
        range_dev_buf_atr=float(args.range_dev_buf_atr),
        range_retest_tol_atr=float(args.range_retest_tol_atr),
        range_reclaim_lookahead=int(args.range_reclaim_lookahead),
        range_retest_lookahead=int(args.range_retest_lookahead),
        range_min_width_atr=float(args.range_min_width_atr),
        range_cooldown_candles=int(args.range_cooldown),

        tdp_dev_lookback=TDP_DEV_LOOKBACK,
        require_impulse_before_tdp=REQUIRE_IMPULSE_BEFORE_TDP,
        impulse_lookback=IMPULSE_LOOKBACK,
        impulse_size_atr=IMPULSE_SIZE_ATR,
        reclaim_buf_atr=RECLAIM_BUF_ATR,
        reclaim_lookahead=RECLAIM_LOOKAHEAD,
        had_imp_down_window=HAD_IMP_DOWN_WINDOW,
        tts_retest_lookback=TTS_RETEST_LOOKBACK,
        debug_long_funnel=True,
    )

    # ============================================================
    # ✅ STEP 6.3 Profile routing (optional, outside entry_model)
    # ============================================================
    if entries:
        df_e = pd.DataFrame([e.__dict__ for e in entries])

        # normalize
        df_e["side"] = df_e["side"].astype(str).str.upper()
        if "phase" in df_e.columns:
            df_e["phase"] = df_e["phase"].astype(str).str.upper()
        else:
            df_e["phase"] = ""

        # Profile filters (optional)
        if args.mode != "combined":
            if ENABLE_LONG_TREND_UP and (not ENABLE_SHORT_TREND_DOWN):
                df_e = df_e[(df_e["side"] == "LONG") & (df_e["phase"] == "PHASE_TREND_UP")].copy()
            else:
                keep_long = ENABLE_LONG_TREND_UP
                keep_short = ENABLE_SHORT_TREND_DOWN

                mask = pd.Series(False, index=df_e.index)
                if keep_long:
                    mask |= (df_e["side"] == "LONG") & (df_e["phase"] == "PHASE_TREND_UP")
                if keep_short:
                    mask |= (df_e["side"] == "SHORT") & (df_e["phase"] == "PHASE_TREND_DOWN")

                df_e = df_e[mask].copy()

                if keep_short and ("trend_strength" in df_e.columns):
                    ts = pd.to_numeric(df_e["trend_strength"], errors="coerce").fillna(0.0)
                    df_e = df_e[~((df_e["side"] == "SHORT") & (ts < float(SHORT_TREND_STRENGTH_MIN)))].copy()

        entries = [em.Entry(**row) for row in df_e.to_dict("records")]

    # mode filter (MVP)
    if args.mode == "long_only":
        entries = [e for e in entries if str(getattr(e, "side", "")).upper() == "LONG"]
    elif args.mode == "short_only":
        entries = [e for e in entries if str(getattr(e, "side", "")).upper() == "SHORT"]
    elif args.mode == "range_only":
        entries = [e for e in entries if str(getattr(e, "phase", "")).upper() == "PHASE_RANGE"]

    entries_df = pd.DataFrame([e.__dict__ for e in entries])
    entries_path = ft.EXPORT_DIR / "entries_generated.csv"
    entries_df.to_csv(entries_path, index=False)

    sim = em.simulate_trades(
        candles,
        entries,
        max_hold_bars=MAX_HOLD_BARS,
        be_after_r=BE_AFTER_R,
        partial_at_r=PARTIAL_AT_R,
        partial_frac=PARTIAL_FRAC,
    )

    sim_path = ft.EXPORT_DIR / "trades_simulated.csv"
    sim.to_csv(sim_path, index=False)

    total, w, l, be, nh, wr, exp_r = _wl_summary(sim)
    wr_s = f"{wr:.2f}%" if np.isfinite(wr) else "n/a"
    print(f"RESULT: total={total} win={w} loss={l} be={be} no_hit={nh} winrate(W/L)={wr_s} expectancy_R={exp_r:.6f}")

    print(f"Entries generated: {len(entries_df)}  -> {entries_path}")
    print(f"Trades simulated : {len(sim)}  -> {sim_path}")
    print(f"RESULT: total={total} win={w} loss={l} no_hit={nh} winrate(W/L)={wr_s} expectancy_R={exp_r:.6f}")

    print("\n[D1] ctx_sub_label (TDP_TOP/BOT, TTS_UP/DN)")
    print(_group_table(sim, "ctx_sub_label").to_string(index=False))

    print("\n[D2] side (LONG vs SHORT)")
    print(_group_table(sim, "side").to_string(index=False))

    print("\n[D3] model (TDP_REENTRY vs TTS_RETEST)")
    print(_group_table(sim, "model").to_string(index=False))


if __name__ == "__main__":
    main()
