from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Dict, Any

import numpy as np
import pandas as pd

# Your existing modules (repo paths):
# - backtest/journal/filter_trades.py has build_ctx()
# - backtest/engine/entry_model.py has Entry + simulate_trades()
from backtest.journal.filter_trades import build_ctx
from backtest.engine.entry_model import Entry, simulate_trades, build_candle_cache

from backtest.journal.lt_entry import find_retest_entry


# ============================================================
# Contracts
# ============================================================
SETUP_LABELS = {"TDP_TOP", "TDP_BOT", "TTS_UP", "TTS_DN", "WYCKOFF_SPRING", "WYCKOFF_UPTHRUST"}


@dataclass(frozen=True)
class SetupEvent:
    timestamp_htf: pd.Timestamp     # 4h bar start
    model: str                      # "TDP"/"TTS"/"WYCKOFF"
    side: str                       # "LONG"/"SHORT"
    ctx_sub_label: str              # e.g. "TDP_BOT"
    regime: str
    trend_dir: str
    trend_strength: float
    atr_pct: float
    range_hi: Optional[float] = None
    range_lo: Optional[float] = None
    meta: str = ""


# ============================================================
# Gate (minimal smart gate MVP)
# ============================================================
def pass_smart_gate(setup: SetupEvent, trend_min: float = 0.0, need_atr: bool = False, atr_min: float = 0.0015) -> bool:
    """
    MVP smart gate:
      drop only: TDP_BOT + LONG while trend_dir == DOWN
      (optional pressure knobs: trend_strength >= trend_min, atr_pct >= atr_min when need_atr)
    """
    bad = (
        setup.ctx_sub_label == "TDP_BOT"
        and setup.side == "LONG"
        and str(setup.trend_dir).upper() == "DOWN"
    )
    if not bad:
        return True

    if trend_min and float(trend_min) > 0:
        bad = bad and (float(setup.trend_strength) >= float(trend_min))
    if need_atr:
        bad = bad and (float(setup.atr_pct) >= float(atr_min))
    return not bad


# ============================================================
# HTF context table (4h)
# ============================================================
def build_ctx_htf(ctx_m_15m: pd.DataFrame, htf: str = "4h") -> pd.DataFrame:
    """
    Input: ctx_m on LTF grid (15m) from build_ctx()
    Output: one row per HTF bar (4h), taking the LAST LTF row inside the bar.
    Adds: timestamp_htf (bar start).
    """
    c = ctx_m_15m.copy()
    c["timestamp"] = pd.to_datetime(c["timestamp"], errors="coerce")
    c = c.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    bar_start = c["timestamp"].dt.floor(htf)
    c["timestamp_htf"] = bar_start

    # last row in each HTF bin is the "context snapshot" for that bar
    last = c.groupby("timestamp_htf", as_index=False).tail(1).copy()
    last = last.sort_values("timestamp_htf").reset_index(drop=True)

    # keep and rename to nice schema
    # (We keep original names too so it's easy to debug.)
    if "ctx_label" not in last.columns and "label" in last.columns:
        last["ctx_label"] = last["label"]

    return last


def extract_setup_events(ctx_htf: pd.DataFrame) -> List[SetupEvent]:
    c = ctx_htf.copy()
    c["ctx_sub_label"] = c.get("ctx_sub_label", c.get("sub_label", "")).fillna("").astype(str).str.upper()

    events: List[SetupEvent] = []
    for _, r in c.iterrows():
        sub = str(r.get("ctx_sub_label", "")).upper()
        if sub not in SETUP_LABELS:
            continue

        # model inference
        if sub.startswith("TDP"):
            model = "TDP"
            side = str(r.get("ctx_tdp_dir", "")).upper()
        elif sub.startswith("TTS"):
            model = "TTS"
            side = str(r.get("ctx_tts_dir", "")).upper()
        else:
            model = "WYCKOFF"
            side = str(r.get("ctx_tdp_dir", r.get("ctx_tts_dir", ""))).upper()

        if side not in ("LONG", "SHORT"):
            continue

        events.append(SetupEvent(
            timestamp_htf=pd.Timestamp(r["timestamp_htf"]),
            model=model,
            side=side,
            ctx_sub_label=sub,
            regime=str(r.get("regime", "")).upper(),
            trend_dir=str(r.get("trend_dir", "")).upper(),
            trend_strength=float(pd.to_numeric(r.get("trend_strength", 0.0), errors="coerce") or 0.0),
            atr_pct=float(pd.to_numeric(r.get("atr_pct", 0.0), errors="coerce") or 0.0),
            range_hi=float(r["range_hi"]) if pd.notna(r.get("range_hi")) else None,
            range_lo=float(r["range_lo"]) if pd.notna(r.get("range_lo")) else None,
            meta="",
        ))
    return events


# ============================================================
# Runner
# ============================================================
def main():
    ap = argparse.ArgumentParser(description="HTF->LTF pipeline MVP: HTF setup -> gate -> LTF retest -> simulate")

    ap.add_argument("--candles", type=str, default="backtest/journal/candles_ohlc.csv")
    ap.add_argument("--out", type=str, default="backtest/journal/exports_trades/htf_ltf_trades.csv")

    ap.add_argument("--htf", type=str, default="4h")
    ap.add_argument("--ltf", type=str, default="15min")  # kept for CLI symmetry (data is already 15m)

    # LTF search window
    ap.add_argument("--entry-window-hours", type=float, default=48.0)
    ap.add_argument("--tol-atr-mult", type=float, default=0.25)

    # risk/sim knobs (same semantics as entry_model)
    ap.add_argument("--rr", type=float, default=3.0)
    ap.add_argument("--sl-atr-buffer", type=float, default=0.25)

    ap.add_argument("--max-hold-bars", type=int, default=200)
    ap.add_argument("--be-after-r", type=float, default=1.0)
    ap.add_argument("--partial-at-r", type=float, default=1.0)
    ap.add_argument("--partial-frac", type=float, default=0.7)

    # gate knobs (optional pressure)
    ap.add_argument("--trend-min", type=float, default=0.0)
    ap.add_argument("--need-atr", action="store_true")
    ap.add_argument("--atr-min", type=float, default=0.0015)

    args = ap.parse_args()

    candles_path = Path(args.candles)
    if not candles_path.exists():
        raise SystemExit(f"Missing candles CSV: {candles_path}")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    candles = pd.read_csv(candles_path, engine="python", on_bad_lines="skip")
    candles["timestamp"] = pd.to_datetime(candles["timestamp"], errors="coerce")
    candles = candles.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    # 1) LTF ctx + merge market_regime (build_ctx already does merge if csv exists)
    ctx_m = build_ctx(candles)

    # 2) HTF context table (one row per 4h)
    ctx_htf = build_ctx_htf(ctx_m, htf=args.htf)
    ctx_htf_out = out_path.parent / "ctx_htf.csv"
    ctx_htf.to_csv(ctx_htf_out, index=False)

    # 3) setup events from HTF ctx
    setups = extract_setup_events(ctx_htf)
    setups_df = pd.DataFrame([s.__dict__ for s in setups])
    setups_out = out_path.parent / "setup_events.csv"
    if not setups_df.empty:
        setups_df.to_csv(setups_out, index=False)
    else:
        # still write headers
        pd.DataFrame(columns=[f.name for f in SetupEvent.__dataclass_fields__.values()]).to_csv(setups_out, index=False)

    # 4) gate – FINAL
    # === C: SHORT core gate ===
    MIN_TS_SHORT_TREND_DOWN = 0.01  # labai žemas, tik kad išmesti triukšmą

    def ts(s):
        return float(getattr(s, "trend_strength", 0.0) or 0.0)

    gated = [
        s for s in setups
        if pass_smart_gate(
            s,
            trend_min=args.trend_min,
            need_atr=args.need_atr,
            atr_min=args.atr_min
        )
           # A core (paliekam)
           and not (
                s.ctx_sub_label == "TDP_TOP"
                and s.side == "SHORT"
                and s.regime == "TREND_UP"
        )
           # drop SHORT RANGE completely (C cleanup)
           and not (
                s.ctx_sub_label == "TDP_TOP"
                and s.side == "SHORT"
                and s.regime == "RANGE"
        )

    ]

    gated_df = pd.DataFrame([s.__dict__ for s in gated])
    gated_out = out_path.parent / "setup_events_gated.csv"
    if not gated_df.empty:
        gated_df.to_csv(gated_out, index=False)
    else:
        pd.DataFrame(columns=setups_df.columns if not setups_df.empty else []).to_csv(gated_out, index=False)

    # 5) LTF entry search (retest MVP) -> Entry objects
    entries: List[Entry] = []
    for s in gated:
        e = find_retest_entry(
            candles=candles,
            setup=s,
            window_hours=float(args.entry_window_hours),
            tol_atr_mult=float(args.tol_atr_mult),
            rr=float(args.rr),
            sl_atr_buffer=float(args.sl_atr_buffer),
        )
        if e is not None:
            entries.append(e)

    entries_df = pd.DataFrame([e.__dict__ for e in entries]) if entries else pd.DataFrame()
    entries_out = out_path.parent / "entries.csv"
    entries_df.to_csv(entries_out, index=False)

    # 6) Simulate (single sim engine)
    cache = build_candle_cache(candles)
    trades = simulate_trades(
        candles=candles,
        entries=entries,
        max_hold_bars=int(args.max_hold_bars),
        be_after_r=float(args.be_after_r) if args.be_after_r is not None else None,
        partial_at_r=float(args.partial_at_r) if args.partial_at_r is not None else None,
        partial_frac=float(args.partial_frac),
        candle_cache=cache,
    )

    trades.to_csv(out_path, index=False)

    print("✅ Saved:")
    print("-", out_path.resolve())
    print("-", ctx_htf_out.resolve())
    print("-", setups_out.resolve())
    print("-", gated_out.resolve())
    print("-", entries_out.resolve())
    print(f"Setups: {len(setups)} | gated: {len(gated)} | entries: {len(entries)} | trades: {len(trades)}")


if __name__ == "__main__":
    main()
