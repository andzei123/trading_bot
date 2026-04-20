from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List, Dict
import logging

import numpy as np
import pandas as pd

from backtest.journal.gates import allow_entry, GateConfig  # palikta dėl suderinamumo (čia nenaudojama)

# --- feature toggles (MVP) ---
ENABLE_TTS_RETEST = False  # keep OFF until TTS logic is validated

# ============================================================
# ENTRY_DIAG (DEV-1)
# Explainable drop diagnostics for KPI_VALIDATION.
# Active only when debug_entry_filters=True; does not change strategy logic.
# Reasons: IMPULSE_TOO_SMALL, TREND_MISMATCH, RETEST_FAIL, RR_TOO_LOW, SL_INVALID
# ============================================================
def _entry_diag_bump(ctx: pd.DataFrame, reason: str, n: int = 1) -> None:
    """Increment per-symbol entry drop diagnostics counter.

    Stored in ctx.attrs["_entry_diag"] as dict[str, int].
    """
    try:
        if ctx is None:
            return
        if not hasattr(ctx, "attrs") or not isinstance(ctx.attrs, dict):
            return
        d = ctx.attrs.get("_entry_diag")
        if not isinstance(d, dict):
            d = {}
            ctx.attrs["_entry_diag"] = d
        d[reason] = int(d.get(reason, 0)) + int(n)
    except Exception:
        return


# ============================================================
# DEV1 — Regime Drift weighting (SPRINT-3.3)
# Applies trend_weight/range_weight to entry scores.
# trend_weight/range_weight are expected via ctx.attrs or ctx columns.
# ============================================================
def _apply_regime_drift_weights(entries: list[Entry], ctx: pd.DataFrame) -> list[Entry]:
    tw = 1.0
    rw = 1.0
    try:
        if hasattr(ctx, "attrs") and isinstance(ctx.attrs, dict):
            tw = float(ctx.attrs.get("trend_weight", tw) or tw)
            rw = float(ctx.attrs.get("range_weight", rw) or rw)
    except Exception:
        pass
    # optional columns (last row)
    try:
        if "trend_weight" in ctx.columns and len(ctx) > 0:
            tw = float(ctx["trend_weight"].iloc[-1])
        if "range_weight" in ctx.columns and len(ctx) > 0:
            rw = float(ctx["range_weight"].iloc[-1])
    except Exception:
        pass

    for e in entries:
        # base score: RR from levels (safe)
        try:
            risk = abs(float(e.entry) - float(e.sl))
            rr0 = abs(float(e.tp) - float(e.entry)) / max(1e-9, risk)
        except Exception:
            rr0 = 0.0

        model_u = str(getattr(e, "model", "") or "").upper()
        if model_u == "TDP_REENTRY":  # trend
            e.score = float(rr0) * float(tw)
        elif model_u.startswith("RANGE"):  # range
            e.score = float(rr0) * float(rw)
        else:
            e.score = float(rr0)

        # Signal Scoring V1 base: RR-only pre-runner component.
        # Other components are attached later in live_signal_runner once
        # execution / macro / liquidation / TTS context is available.
        e.score_rr = max(0.0, min(4.0, float(rr0))) * 0.5
        e.signal_score = float(e.score_rr)
    return entries


# ============================================================
# Data structures
# ============================================================

@dataclass
class Entry:
    timestamp: pd.Timestamp
    model: str               # "TDP_REENTRY" / "TTS_RETEST" / "RANGE_RECLAIM"
    side: str                # "LONG" / "SHORT"
    entry: float
    sl: float
    tp: float
    symbol: str = ""
    meta: str = ""
    ctx_sub_label: Optional[str] = None   # "TDP_TOP"/"TDP_BOT"/"TTS_UP"/"TTS_DN"

    # --- regime fields (ateina iš ctx, kuris turi market_regime merge) ---
    regime: Optional[str] = None
    trend_dir: Optional[str] = None
    trend_strength: Optional[float] = None
    atr_pct: Optional[float] = None
    phase: Optional[str] = None
    # --- scoring / ranking (used by runner for drift weighting + clustering) ---
    score: float = 0.0

    # --- Signal Scoring V1 (telemetry-safe; does not change strategy rules) ---
    score_rr: float = 0.0
    score_exec: float = 0.0
    score_phase_align: float = 0.0
    score_macro_align: float = 0.0
    score_liq_align: float = 0.0
    score_tts: float = 0.0
    signal_score: float = 0.0


# -----------------------------
# Level 2.5: candle cache
# -----------------------------
@dataclass(frozen=True)
class CandleCache:
    ts: np.ndarray                 # datetime64[ns]
    high: np.ndarray               # float64
    low: np.ndarray                # float64
    ts_to_idx: Dict[pd.Timestamp, int]


def build_candle_cache(candles: pd.DataFrame) -> CandleCache:
    """
    One-time preprocessing:
      - sort by timestamp
      - ensure timestamp is datetime
      - build numpy arrays + ts->index mapping
    """
    if candles is None or candles.empty:
        return CandleCache(
            ts=np.array([], dtype="datetime64[ns]"),
            high=np.array([], dtype=float),
            low=np.array([], dtype=float),
            ts_to_idx={},
        )

    c = candles.copy()
    ts = pd.to_datetime(c["timestamp"], errors="coerce", utc=True)
    c["timestamp"] = ts.dt.tz_convert(None)

    c = c.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    ts = c["timestamp"].to_numpy(dtype="datetime64[ns]")
    high = pd.to_numeric(c["high"], errors="coerce").to_numpy(dtype=float)
    low = pd.to_numeric(c["low"], errors="coerce").to_numpy(dtype=float)

    ts_to_idx = {pd.Timestamp(t): i for i, t in enumerate(ts)}
    return CandleCache(ts=ts, high=high, low=low, ts_to_idx=ts_to_idx)


# ============================================================
# Simulator helpers
# ============================================================

def _touches(side: str, h: float, l: float, px: float, kind: str) -> bool:
    # kind: "TP"/"SL"/"PX"
    if kind in ("TP", "PX"):
        return (h >= px) if side == "LONG" else (l <= px)
    if kind == "SL":
        return (l <= px) if side == "LONG" else (h >= px)
    raise ValueError(kind)


def _conservative_order(side: str, h: float, l: float, sl_px: float, tp_px: float) -> Optional[str]:
    """
    Conservative: if both SL and TP touched in same candle -> assume SL first.
    """
    sl_hit = _touches(side, h, l, sl_px, "SL")
    tp_hit = _touches(side, h, l, tp_px, "TP")
    if sl_hit and tp_hit:
        return "LOSS"
    if sl_hit:
        return "LOSS"
    if tp_hit:
        return "WIN"
    return None


# ============================================================
# Simulator (with PROP RISK LAYER)
# ============================================================

def simulate_trades(
    candles: pd.DataFrame,
    entries: List[Entry],
    max_hold_bars: int = 200,
    be_after_r: Optional[float] = None,       # e.g. 1.0 => after +1R move, SL -> BE
    partial_at_r: Optional[float] = None,     # e.g. 1.0 => take partial at +1R
    partial_frac: float = 0.7,                # fraction closed at partial
    candle_cache: Optional[CandleCache] = None,

    # =========================
    # ✅ STEP 7 — PROP RISK LAYER
    # =========================
    max_trades_per_day: Optional[int] = None,      # e.g. 1
    daily_max_loss_r: Optional[float] = None,      # e.g. -1.0
    cooldown_bars_after_loss: int = 0,             # e.g. 1–2
    atr_pct_min: Optional[float] = None,           # e.g. 0.0015
    atr_pct_max: Optional[float] = None,           # e.g. 0.01

    # schema support (DEV3/DEV-C): keep keyword-only at the end to avoid breaking positional callers
    symbol: str = "",
) -> pd.DataFrame:
    """
    Simulates outcomes and returns per-trade R-multiple with optional BE/partials.

    ✅ STEP 7 adds risk layer (entry logikos neliečia):
      - Max trades per day
      - Daily loss stop (R)
      - Cooldown after LOSS (bars)
      - ATR% kill switch (Entry.atr_pct within [min, max])
    """
    if not entries:
        return pd.DataFrame()

    cache = candle_cache if candle_cache is not None else build_candle_cache(candles)
    if cache.ts.size == 0:
        return pd.DataFrame()

    ts_arr = cache.ts
    high_arr = cache.high
    low_arr = cache.low
    ts_to_idx = cache.ts_to_idx

    # ---- STEP 7 state ----
    day_trade_count: Dict[pd.Timestamp, int] = {}
    day_r_sum: Dict[pd.Timestamp, float] = {}
    cooldown_until_idx: int = -1  # inclusive

    def _day_key(ts: pd.Timestamp) -> pd.Timestamp:
        return pd.Timestamp(ts).normalize()

    rows = []
    for i, e in enumerate(entries, 1):
        ts0 = pd.Timestamp(e.timestamp)

        # normalize entry timestamp to naive UTC to match candle cache keys
        if ts0.tzinfo is not None:
            ts0 = ts0.tz_convert("UTC").tz_localize(None)

        idx0 = ts_to_idx.get(ts0)
        if idx0 is None:
            continue

        # 7.4 ATR% kill switch
        if (atr_pct_min is not None) or (atr_pct_max is not None):
            try:
                atrp = float(e.atr_pct) if e.atr_pct is not None else float("nan")
            except Exception:
                atrp = float("nan")

            if not np.isfinite(atrp):
                continue
            if (atr_pct_min is not None) and (atrp < float(atr_pct_min)):
                continue
            if (atr_pct_max is not None) and (atrp > float(atr_pct_max)):
                continue

        # 7.3 cooldown after loss
        if cooldown_bars_after_loss and idx0 <= cooldown_until_idx:
            continue

        dkey = _day_key(ts0)
        if dkey not in day_trade_count:
            day_trade_count[dkey] = 0
            day_r_sum[dkey] = 0.0

        # 7.1 max trades/day
        if (max_trades_per_day is not None) and (day_trade_count[dkey] >= int(max_trades_per_day)):
            continue

        # 7.2 daily loss stop
        if (daily_max_loss_r is not None) and (day_r_sum[dkey] <= float(daily_max_loss_r)):
            continue

        side = str(e.side).upper()
        entry_px = float(e.entry)
        sl0 = float(e.sl)
        tp = float(e.tp)

        risk = abs(entry_px - sl0)
        if risk <= 0:
            continue

        rr = abs(tp - entry_px) / max(1e-9, risk)

        start_idx = idx0
        end_idx = min(len(ts_arr) - 1, start_idx + int(max_hold_bars))

        remaining = 1.0
        realized_r = 0.0
        partial_done = False
        be_active = False
        sl_active = sl0

        def price_at_r(r_mult: float) -> float:
            return (entry_px + r_mult * risk) if side == "LONG" else (entry_px - r_mult * risk)

        partial_px = price_at_r(float(partial_at_r)) if partial_at_r is not None else None
        be_px = price_at_r(float(be_after_r)) if be_after_r is not None else None

        outcome = "NO_HIT"
        exit_price = np.nan
        exit_ts = pd.NaT
        exit_reason = ""
        exit_idx = start_idx

        for j in range(start_idx, end_idx + 1):
            h = float(high_arr[j])
            l = float(low_arr[j])

            # 0) partial check
            if (not partial_done) and (partial_px is not None) and remaining > 0:
                if _touches(side, h, l, partial_px, "PX"):
                    if _touches(side, h, l, sl_active, "SL"):
                        outcome = "LOSS"
                        exit_idx = j
                        exit_ts = pd.Timestamp(ts_arr[j])
                        exit_price = sl_active
                        exit_reason = "SL_before_partial"
                        realized_r += -1.0 * remaining
                        remaining = 0.0
                        break

                    realized_r += float(partial_at_r) * float(partial_frac)
                    remaining = max(0.0, 1.0 - float(partial_frac))
                    partial_done = True

            # 1) BE activation
            if (not be_active) and (be_px is not None) and remaining > 0:
                if _touches(side, h, l, be_px, "PX"):
                    if _touches(side, h, l, sl_active, "SL"):
                        outcome = "LOSS"
                        exit_idx = j
                        exit_ts = pd.Timestamp(ts_arr[j])
                        exit_price = sl_active
                        exit_reason = "SL_before_BE"
                        realized_r += -1.0 * remaining
                        remaining = 0.0
                        break

                    be_active = True
                    sl_active = entry_px

            # 2) TP/SL check
            res = _conservative_order(side, h, l, sl_active, tp)
            if res is None:
                continue

            exit_idx = j
            exit_ts = pd.Timestamp(ts_arr[j])

            if res == "LOSS":
                if be_active and abs(sl_active - entry_px) < 1e-9:
                    outcome = "BE"
                    exit_price = entry_px
                    exit_reason = "BE_stop"
                else:
                    outcome = "LOSS"
                    exit_price = sl_active
                    exit_reason = "SL"
                    realized_r += -1.0 * remaining
                remaining = 0.0
                break

            if res == "WIN":
                outcome = "WIN"
                exit_price = tp
                exit_reason = "TP"
                realized_r += rr * remaining
                remaining = 0.0
                break

        # STEP 7 bookkeeping
        day_trade_count[dkey] += 1
        day_r_sum[dkey] += float(realized_r)

        if (outcome == "LOSS") and cooldown_bars_after_loss:
            cooldown_until_idx = max(cooldown_until_idx, int(exit_idx) + int(cooldown_bars_after_loss))

        rows.append({
            "id": i,
            "timestamp": pd.Timestamp(e.timestamp),
            "symbol": str(symbol).strip(),
            "status": "OK",
            "side": side,
            "entry": entry_px,
            "sl": sl0,
            "tp": tp,
            "rr": round(rr, 6),

            "model": e.model,
            "ctx_sub_label": e.ctx_sub_label,

            "regime": e.regime,
            "trend_dir": e.trend_dir,
            "trend_strength": e.trend_strength,
            "atr_pct": e.atr_pct,
            "phase": e.phase,

            "meta": f"model={e.model} {e.meta}".strip(),
            "outcome": outcome,
            "exit_price": exit_price,
            "exit_timestamp": exit_ts,
            "exit_reason": exit_reason,

            "partial_taken": bool(partial_done),
            "be_moved": bool(be_active),
            "R": round(float(realized_r), 6),
        })

    return pd.DataFrame(rows)


# ============================================================
# Router helpers (B1)
# ============================================================

def _phase_upper(ctx: pd.DataFrame) -> pd.Series:
    if "phase" not in ctx.columns:
        return pd.Series(["PHASE_UNKNOWN"] * len(ctx), index=ctx.index)
    return ctx["phase"].astype(str).str.upper().fillna("PHASE_UNKNOWN")


# ============================================================
# Trend generator (B1)
# ============================================================

def generate_trend_entries(
    ctx: pd.DataFrame,
    rr: float,
    rr_long: float,
    sl_atr_buffer: float,
    tdp_dev_lookback: int,
    require_impulse_before_tdp: bool,
    reclaim_buf_atr: float,
    reclaim_lookahead: int,
    had_imp_down_window: int,
    debug_entry_filters: bool = False,
    symbol: str = "",
) -> list[Entry]:

    c = ctx.copy().sort_values("timestamp").reset_index(drop=True)
    phase_u = _phase_upper(c)
    c = c[phase_u.isin(["PHASE_TREND_UP", "PHASE_TREND_DOWN"])].copy().reset_index(drop=True)
    if c.empty:
        return []

    entries: list[Entry] = []

    # ---------------------------
    # debug: entry candidate filters
    # ---------------------------
    def _dbg(reason: str, extra: str = "") -> None:
        if not debug_entry_filters:
            return
        tag = f"[ENTRY_FILTER][{symbol}]" if symbol else "[ENTRY_FILTER]"
        if extra:
            print(f"{tag} {reason} ({extra})")
        else:
            print(f"{tag} {reason}")

    recent_high = c["high"].rolling(tdp_dev_lookback).max()
    recent_low = c["low"].rolling(tdp_dev_lookback).min()

    # =================================================
    # TDP SHORT (trend continuation)
    # =================================================
    dev_up_recent = c["dev_up"].fillna(False).astype(bool).rolling(tdp_dev_lookback).max().fillna(False).astype(bool)
    # ENTRY_DIAG: trend/phase mismatch (TDP_TOP exists but phase is not TREND_DOWN)
    if debug_entry_filters:
        try:
            base_lbl = ((c["sub_label"] == "TDP_TOP") & dev_up_recent).fillna(False)
            mismatch = (base_lbl & (c["phase"] != "PHASE_TREND_DOWN")).fillna(False)
            n_mismatch = int(mismatch.sum())
            if n_mismatch > 0:
                _entry_diag_bump(ctx, "TREND_MISMATCH", n_mismatch)
        except Exception:
            pass
    reentry_short = (
            (c["sub_label"] == "TDP_TOP")
            & dev_up_recent
            & (c["phase"] == "PHASE_TREND_DOWN")
    )

    base_short = reentry_short.copy()
    if require_impulse_before_tdp:
        reentry_short &= c["impulse_recent"] & (c["impulse_dir"] == "UP")

    # ENTRY_DIAG: impulse gate removed candidates
    if debug_entry_filters:
        try:
            removed = (base_short & (~reentry_short)).fillna(False)
            n_removed = int(removed.sum())
            if n_removed > 0:
                _entry_diag_bump(ctx, "IMPULSE_TOO_SMALL", n_removed)
        except Exception:
            pass

    if debug_entry_filters:
        b = int(base_short.sum())
        a = int(reentry_short.sum())
        if b > 0 and a == 0:
            _dbg("TDP_SHORT_REMOVED_BY_IMPULSE", f"base={b} after=0 (need impulse_recent & dir==UP)")
    for i in np.where(reentry_short.values)[0]:
        entry_px = float(c.loc[i, "close"])
        atr = float(c.loc[i, "atr"])
        sl = float(recent_high.loc[i] + sl_atr_buffer * atr)
        risk0 = float(sl - entry_px)
        if (not np.isfinite(risk0)) or (risk0 <= 0):
            if debug_entry_filters:
                _entry_diag_bump(ctx, "SL_INVALID", 1)
            continue
        risk = max(1e-9, risk0)
        tp = entry_px - rr * risk

        entries.append(Entry(
            timestamp=pd.Timestamp(c.loc[i, "timestamp"]),
            model="TDP_REENTRY",
            side="SHORT",
            entry=entry_px,
            sl=sl,
            tp=tp,
            symbol=str(symbol).strip(),
            meta="TDP_TOP trend continuation",
            ctx_sub_label="TDP_TOP",
            regime=str(c.loc[i, "regime"]),
            trend_dir=str(c.loc[i, "trend_dir"]),
            trend_strength=float(c.loc[i, "trend_strength"]),
            atr_pct=float(c.loc[i, "atr_pct"]),
            phase=str(c.loc[i, "phase"]),
        ))

    # =================================================
    # TDP LONG (trend continuation via sweep->reclaim)
    # =================================================
    dev_dn_recent = c["dev_dn"].fillna(False).astype(bool).rolling(tdp_dev_lookback).max().fillna(False).astype(bool)


    buf = reclaim_buf_atr * c["atr"]
    sweep = c["low"] < (c["range_lo"] - buf)
    reclaim = c["close"] > (c["range_lo"] + buf)

    future_reclaim = reclaim.rolling(reclaim_lookahead).max().shift(-(reclaim_lookahead - 1))
    future_reclaim = future_reclaim.fillna(False)
    # ENTRY_DIAG: trend/phase mismatch (TDP_BOT exists but phase is not TREND_UP)
    if debug_entry_filters:
        try:
            base_lbl = ((c["sub_label"] == "TDP_BOT") & dev_dn_recent).fillna(False)
            mismatch = (base_lbl & (c["phase"] != "PHASE_TREND_UP")).fillna(False)
            n_mismatch = int(mismatch.sum())
            if n_mismatch > 0:
                _entry_diag_bump(ctx, "TREND_MISMATCH", n_mismatch)
        except Exception:
            pass
    # --- base (no impulse required) ---
    reentry_long = (
            (c["sub_label"] == "TDP_BOT")
            & dev_dn_recent
            & sweep
            & future_reclaim
            & (c["phase"] == "PHASE_TREND_UP")
    )

    base_long = reentry_long.copy()

    # --- impulse gate (ONLY when enabled) ---
    if require_impulse_before_tdp:
        imp_down = (c["impulse_recent"] & (c["impulse_dir"] == "DOWN")).fillna(False)
        had_imp_down = imp_down.rolling(had_imp_down_window).max().fillna(False)
        reentry_long &= had_imp_down

    # ENTRY_DIAG: impulse gate removed candidates
    if debug_entry_filters:
        try:
            removed = (base_long & (~reentry_long)).fillna(False)
            n_removed = int(removed.sum())
            if n_removed > 0:
                _entry_diag_bump(ctx, "IMPULSE_TOO_SMALL", n_removed)
        except Exception:
            pass

    setup_long = reentry_long

    if debug_entry_filters:
        base = ((c["sub_label"] == "TDP_BOT") & dev_dn_recent & (c["phase"] == "PHASE_TREND_UP")).fillna(False)
        b = int(base.sum())
        a = int(setup_long.sum())
        if b > 0 and a == 0:
            # Figure which gate killed it (rough breakdown)
            had = int((base & had_imp_down).sum())
            sw = int((base & had_imp_down & sweep).sum())
            fr = int((base & had_imp_down & sweep & future_reclaim).sum())
            _dbg("TDP_LONG_FILTER_BREAKDOWN", f"base={b} had_imp_down={had} sweep={sw} future_reclaim={fr} final={a}")
    for i in np.where(setup_long.values)[0]:
        j_end = min(len(c) - 1, i + reclaim_lookahead - 1)
        reclaim_window = reclaim.iloc[i:j_end + 1]
        if not reclaim_window.any():
            continue

        j = int(reclaim_window.idxmax())

        entry_px = float(c.loc[j, "close"])
        atr = float(c.loc[j, "atr"])
        base_low = float(c.loc[i:j_end, "low"].min())
        sl = float(base_low - sl_atr_buffer * atr)
        risk0 = float(entry_px - sl)
        if (not np.isfinite(risk0)) or (risk0 <= 0):
            if debug_entry_filters:
                _entry_diag_bump(ctx, "SL_INVALID", 1)
            continue
        risk = max(1e-9, risk0)
        tp = entry_px + rr_long * risk

        entries.append(Entry(
            timestamp=pd.Timestamp(c.loc[j, "timestamp"]),
            model="TDP_REENTRY",
            side="LONG",
            entry=entry_px,
            sl=sl,
            tp=tp,
            symbol=str(symbol).strip(),
            meta="TDP_BOT sweep->reclaim",
            ctx_sub_label="TDP_BOT",
            regime=str(c.loc[j, "regime"]),
            trend_dir=str(c.loc[j, "trend_dir"]),
            trend_strength=float(c.loc[j, "trend_strength"]),
            atr_pct=float(c.loc[j, "atr_pct"]),
            phase=str(c.loc[j, "phase"]),
        ))


    # ============================================================
    # C1) Side policy fallback (phase-based)
    # Enforce phase/side consistency even if side-selector/TTS gate is off.
    #   - PHASE_TREND_UP   -> only LONG trend setups
    #   - PHASE_TREND_DOWN -> only SHORT trend setups
    #   - PHASE_RANGE      -> range_short (+ range_long if enabled)
    # ============================================================
    phase_now = ""
    try:
        if isinstance(ctx, pd.DataFrame):
            if ("phase" in ctx.columns) and len(ctx) > 0:
                phase_now = str(ctx["phase"].iloc[-1] or "").upper()
        elif hasattr(ctx, "get"):
            phase_now = str(ctx.get("phase", "") or "").upper()
    except Exception:
        phase_now = ""

    if not phase_now:
        try:
            if "phase" in c.columns and len(c) > 0:
                phase_now = str(c["phase"].iloc[-1] or "").upper()
        except Exception:
            phase_now = ""

    if debug_entry_filters:
        tag = f"[ENTRY_FILTER][{symbol}]" if symbol else "[ENTRY_FILTER]"
        print(f"{tag} phase_fallback_resolved phase_now={phase_now or 'UNKNOWN'}")

    # "enable_range_long" may not be available in every generator scope; default to False safely.
    enable_range_long_flag = bool(locals().get("enable_range_long", False))

    if phase_now == "PHASE_TREND_UP":
        entries = [
            e for e in entries
            if str(getattr(e, "side", "")).upper() == "LONG"
            and str(getattr(e, "model", "")).upper() == "TDP_REENTRY"
        ]
    elif phase_now == "PHASE_TREND_DOWN":
        entries = [
            e for e in entries
            if str(getattr(e, "side", "")).upper() == "SHORT"
            and str(getattr(e, "model", "")).upper() == "TDP_REENTRY"
        ]
    elif phase_now == "PHASE_RANGE":
        if enable_range_long_flag:
            entries = [e for e in entries if str(getattr(e, "model", "")).upper().startswith("RANGE_")]
        else:
            entries = [
                e for e in entries
                if str(getattr(e, "model", "")).upper().startswith("RANGE_")
                and str(getattr(e, "side", "")).upper() != "LONG"
            ]

    if debug_entry_filters:
        tag = f"[ENTRY_FILTER][{symbol}]" if symbol else "[ENTRY_FILTER]"
        print(f"{tag} generate_entries_from_ctx returned {len(entries)} entries")

    return entries



def _enable_range_bot_long_mvp(c: pd.DataFrame, idx: int) -> bool:
    """
    MVP gate for RANGE_BOT_LONG.

    Default: OFF.
    Turns ON only in "bullish regime" conditions:
      - phase == PHASE_RANGE
      - trend_dir in {UP, NEUTRAL}
      - regime in {BULL, ACCUMULATION}

    Robust to missing columns -> returns False.
    """
    if c is None or len(c) == 0:
        return False
    if idx < 0 or idx >= len(c):
        return False

    if "phase" not in c.columns or "trend_dir" not in c.columns or "regime" not in c.columns:
        return False

    phase = str(c.loc[idx, "phase"]).upper()
    trend_dir = str(c.loc[idx, "trend_dir"]).upper()
    regime = str(c.loc[idx, "regime"]).upper()

    allow_range_long = (
        (phase == "PHASE_RANGE")
        and (trend_dir in ("UP", "NEUTRAL"))
        and (regime in ("BULL", "ACCUMULATION"))
    )
    return bool(allow_range_long)


# ============================================================
# Range generator
# ============================================================



def _generate_range_top_short_v2(
    c: pd.DataFrame,
    *,
    sl_atr_buffer: float,
    dev_buf_atr: float,
    reclaim_buf_atr: float,
    retest_tol_atr: float,
    reclaim_lookahead: int,
    retest_lookahead: int,
    min_range_width_atr: float,
    cooldown_candles: int,
    debug_entry_filters: bool = False,
    symbol: str = "",
    diag_ctx: Optional[pd.DataFrame] = None,
) -> list[Entry]:
    """RANGE_TOP_SHORT_V2 (BTC-first, stable)

    Logic: sweep_up -> reclaim -> retest -> SHORT to mid.

    sweep   : high > range_hi + dev_buf_atr * atr
    reclaim : close < range_hi - reclaim_buf_atr * atr   (within reclaim_lookahead)
    retest  : high >= range_hi - retest_tol_atr * atr AND close < range_hi - reclaim_buf_atr * atr
              (within retest_lookahead after reclaim)

    Blocks:
      - range width must be >= min_range_width_atr (ATR-normalized)
      - anti-late: skip if close <= mid (already at/under mid)
      - cooldown after an entry
    """
    if c is None or c.empty:
        return []

    tag = f"[ENTRY_FILTER][{symbol}]" if symbol else "[ENTRY_FILTER]"

    # One-time diag target (avoid pandas truthiness issues like: diag_ctx or c)
    _diag_df: Optional[pd.DataFrame] = None
    if debug_entry_filters:
        _diag_df = diag_ctx if isinstance(diag_ctx, pd.DataFrame) else c

    # width gate (per candle)
    range_width_atr = (c["range_hi"] - c["range_lo"]).abs() / c["atr"]
    width_ok = (range_width_atr >= float(min_range_width_atr)).fillna(False)

    entries: list[Entry] = []
    n = len(c)
    i = 0

    while i < n:
        if not bool(width_ok.iloc[i]):
            if debug_entry_filters:
                print(f"{tag} RANGE_WIDTH_TOO_SMALL idx={i}")
            i += 1
            continue

        atr_i = float(c.loc[i, "atr"])
        hi_i = float(c.loc[i, "range_hi"])

        # sweep
        dev_up_level = hi_i + float(dev_buf_atr) * atr_i
        if float(c.loc[i, "high"]) <= dev_up_level:
            i += 1
            continue

        # reclaim search
        j_reclaim = None
        sweep_high = float(c.loc[i, "high"])
        j_end = min(n - 1, i + int(reclaim_lookahead))
        for j in range(i, j_end + 1):
            sweep_high = max(sweep_high, float(c.loc[j, "high"]))

            atr_j = float(c.loc[j, "atr"])
            hi_j = float(c.loc[j, "range_hi"])
            close_j = float(c.loc[j, "close"])

            if close_j < (hi_j - float(reclaim_buf_atr) * atr_j):
                j_reclaim = j
                break

        if j_reclaim is None:
            if debug_entry_filters:
                _entry_diag_bump(_diag_df, "RETEST_FAIL", 1)  # (optional rename to RECLAIM_FAIL if you want)
                print(f"{tag} RANGE_TOP_NO_RECLAIM idx={i} lookahead={reclaim_lookahead}")
            i += 1
            continue

        # retest search
        k_entry = None
        k_end = min(n - 1, j_reclaim + int(retest_lookahead))
        for k in range(j_reclaim, k_end + 1):
            atr_k = float(c.loc[k, "atr"])
            hi_k = float(c.loc[k, "range_hi"])
            high_k = float(c.loc[k, "high"])
            close_k = float(c.loc[k, "close"])

            if (high_k >= (hi_k - float(retest_tol_atr) * atr_k)) and (
                close_k < (hi_k - float(reclaim_buf_atr) * atr_k)
            ):
                mid_k = float((float(c.loc[k, "range_hi"]) + float(c.loc[k, "range_lo"])) / 2.0)
                if close_k <= mid_k:
                    # too late (already at/under mid)
                    if debug_entry_filters:
                        _entry_diag_bump(_diag_df, "RR_TOO_LOW", 1)
                    continue
                k_entry = k
                break

        if k_entry is None:
            if debug_entry_filters:
                _entry_diag_bump(_diag_df, "RETEST_FAIL", 1)
                print(f"{tag} RANGE_TOP_NO_RETEST idx={i} reclaim_idx={j_reclaim} lookahead={retest_lookahead}")
            i += 1
            continue

        entry_px = float(c.loc[k_entry, "close"])
        atr_e = float(c.loc[k_entry, "atr"])
        sl = float(sweep_high + float(sl_atr_buffer) * atr_e)
        tp = float((float(c.loc[k_entry, "range_hi"]) + float(c.loc[k_entry, "range_lo"])) / 2.0)

        # sanity: TP < entry < SL
        if not (tp < entry_px < sl):
            if debug_entry_filters:
                _entry_diag_bump(_diag_df, "SL_INVALID", 1)
            i = k_entry + 1
            continue

        risk = sl - entry_px
        if risk <= 0:
            i = k_entry + 1
            continue

        entries.append(
            Entry(
                timestamp=pd.Timestamp(c.loc[k_entry, "timestamp"]),
                model="RANGE_TOP_SHORT_V2",
                side="SHORT",
                entry=entry_px,
                sl=sl,
                tp=tp,
                symbol=str(symbol).strip(),
                meta="RANGE_TOP_SHORT v2: sweep_up -> reclaim -> retest -> mid",
                ctx_sub_label="RANGE_TOP_SHORT",
                regime=str(c.loc[k_entry, "regime"]) if "regime" in c.columns else None,
                trend_dir=str(c.loc[k_entry, "trend_dir"]) if "trend_dir" in c.columns else None,
                trend_strength=float(c.loc[k_entry, "trend_strength"]) if "trend_strength" in c.columns else None,
                atr_pct=float(c.loc[k_entry, "atr_pct"]) if "atr_pct" in c.columns else None,
                phase=str(c.loc[k_entry, "phase"]) if "phase" in c.columns else "PHASE_RANGE",
            )
        )

        i = k_entry + int(max(1, cooldown_candles))

    return entries


def generate_range_entries(
    ctx: pd.DataFrame,
    rr_long: float,                 # kept for backwards compatibility (TP=mid)
    sl_atr_buffer: float,

    # router / regime controller flags
    enable_range_short: bool = True,
    enable_range_long: bool = False,  # ignored (no longs in v2)

    # v2 params
    dev_buf_atr: float = 0.08,
    reclaim_buf_atr: float = 0.00,
    retest_tol_atr: float = 0.15,
    reclaim_lookahead: int = 24,
    retest_lookahead: int = 36,
    min_range_width_atr: float = 4.0,
    cooldown_candles: int = 6,
    debug_entry_filters: bool = False,
    symbol: str = "",
) -> list[Entry]:
    """RANGE router (BTC-first)

    ✅ Only: RANGE_TOP_SHORT_V2
    ✅ Only: PHASE_RANGE (filtered inside)
    ❌ No LONGs (enable_range_long ignored)
    """

    if not enable_range_short:
        return []

    c = ctx.copy().sort_values("timestamp").reset_index(drop=True)
    phase_u = _phase_upper(c)
    c = c[phase_u == "PHASE_RANGE"].copy().reset_index(drop=True)
    if c.empty:
        return []

    needed = ["timestamp", "high", "low", "close", "atr", "range_hi", "range_lo"]
    for col in needed:
        if col not in c.columns:
            if debug_entry_filters:
                tag = f"[ENTRY_FILTER][{symbol}]" if symbol else "[ENTRY_FILTER]"
                print(f"{tag} RANGE_MISSING_COLUMN col={col}")
            return []

    # clean numeric
    for col in ["atr", "range_hi", "range_lo", "high", "low", "close"]:
        c[col] = pd.to_numeric(c[col], errors="coerce")
    c = c.dropna(subset=["atr", "range_hi", "range_lo", "high", "low", "close"]).reset_index(drop=True)
    if c.empty:
        return []

    # ensure regime fields exist for logging/export
    for col, default in [
        ("regime", ""),
        ("trend_dir", ""),
        ("trend_strength", 0.0),
        ("atr_pct", 0.0),
        ("phase", "PHASE_RANGE"),
    ]:
        if col not in c.columns:
            c[col] = default

    # delegate to stable v2 module
    out = _generate_range_top_short_v2(
        c,
        sl_atr_buffer=float(sl_atr_buffer),
        dev_buf_atr=float(dev_buf_atr),
        reclaim_buf_atr=float(reclaim_buf_atr),
        retest_tol_atr=float(retest_tol_atr),
        reclaim_lookahead=int(reclaim_lookahead),
        retest_lookahead=int(retest_lookahead),
        min_range_width_atr=float(min_range_width_atr),
        cooldown_candles=int(cooldown_candles),
        debug_entry_filters=debug_entry_filters,
        symbol=symbol,
        diag_ctx=ctx,
    )

    # lock: ensure router guarantees are never violated
    for e in out:
        if str(getattr(e, "model", "")) != "RANGE_TOP_SHORT_V2":
            raise AssertionError(f"RANGE router lock violated: model={getattr(e, 'model', None)}")
        if str(getattr(e, "ctx_sub_label", "")) != "RANGE_TOP_SHORT":
            raise AssertionError(f"RANGE router lock violated: ctx_sub_label={getattr(e, 'ctx_sub_label', None)}")
        if str(getattr(e, "side", "")).upper() != "SHORT":
            raise AssertionError(f"RANGE router lock violated: side={getattr(e, 'side', None)}")
        if str(getattr(e, "phase", "")).upper() != "PHASE_RANGE":
            raise AssertionError(f"RANGE router lock violated: phase={getattr(e, 'phase', None)}")

    return out


# ============================================================
# Entry generator (Router) — B1
# ============================================================

def generate_entries_from_ctx(
    ctx: pd.DataFrame,
    # router / regime controller flags
    enable_trend: bool = True,
    enable_range_short: bool = True,
    enable_range_long: bool = False,

    rr: float = 2.0,
    rr_long: float = 2.2,
    sl_atr_buffer: float = 0.25,

    # ===== B2: per-mode params (backwards compatible) =====
    trend_rr: Optional[float] = None,
    trend_rr_long: Optional[float] = None,
    trend_sl_atr_buffer: Optional[float] = None,

    range_rr_long: float = 1.6,
    range_sl_atr_buffer: float = 0.25,

    # RANGE v2 params (C3)
    range_dev_buf_atr: float = 0.08,
    range_reclaim_buf_atr: float = 0.00,
    range_retest_tol_atr: float = 0.15,
    range_reclaim_lookahead: int = 24,
    range_retest_lookahead: int = 36,
    range_min_width_atr: float = 4.0,
    range_cooldown_candles: int = 6,

    # ======================================================
    tdp_dev_lookback: int = 6,
    require_impulse_before_tdp: bool = True,
    impulse_lookback: int = 10,
    impulse_size_atr: float = 1.2,
    reclaim_buf_atr: float = 0.02,
    reclaim_lookahead: int = 6,
    had_imp_down_window: int = 24,
    tts_retest_lookback: int = 24,
    enable_tts_gate: bool = False,
    debug_entry_filters: bool = False,
    symbol: str = "",
    debug_long_funnel: bool = True,
) -> list["Entry"]:

    c = ctx.copy().sort_values("timestamp").reset_index(drop=True)

    # ENTRY_DIAG: init drop counters (only in debug mode)
    # IMPORTANT: _entry_diag_bump() writes into *ctx.attrs*
    if debug_entry_filters:
        try:
            if hasattr(ctx, "attrs") and isinstance(ctx.attrs, dict):
                ctx.attrs["_entry_diag"] = {}
        except Exception:
            pass

        # optional mirror (ok to keep)
        try:
            if hasattr(c, "attrs") and isinstance(c.attrs, dict):
                c.attrs["_entry_diag"] = ctx.attrs.get("_entry_diag", {})
        except Exception:
            pass


    # ============================================================

    # DEV1 — Adaptive RR Engine (SPRINT-2 Profit Acceleration)

    # Dynamic RR selection based on phase + macro_strength.

    # ============================================================

    phase_last = None

    try:

        if isinstance(ctx, pd.DataFrame) and ("phase" in ctx.columns) and len(ctx) > 0:

            phase_last = str(ctx["phase"].iloc[-1]).upper()

    except Exception:

        phase_last = None


    macro_strength = None

    try:

        if hasattr(ctx, "attrs") and isinstance(ctx.attrs, dict):

            macro_strength = ctx.attrs.get("macro_strength")

    except Exception:

        macro_strength = None

    if macro_strength is None:

        try:

            if isinstance(ctx, pd.DataFrame) and ("macro_strength" in ctx.columns) and len(ctx) > 0:

                macro_strength = ctx["macro_strength"].iloc[-1]

        except Exception:

            macro_strength = None

    macro_strength_u = str(macro_strength).upper() if macro_strength is not None else ""


    is_trend = phase_last in ("PHASE_TREND_UP", "PHASE_TREND_DOWN")

    is_range = phase_last == "PHASE_RANGE"


    if is_trend and macro_strength_u == "HIGH":

        rr = 2.5

        rr_long = 2.5

    elif is_trend:

        rr = 2.0

        rr_long = 2.0

    elif is_range:

        rr = 1.5

        rr_long = 1.5

        range_rr_long = 1.5  # kept for compatibility (range TP logic may ignore)

    # resolve per-mode params
    trend_rr = rr if trend_rr is None else float(trend_rr)
    trend_rr_long = rr_long if trend_rr_long is None else float(trend_rr_long)
    trend_sl_atr_buffer = sl_atr_buffer if trend_sl_atr_buffer is None else float(trend_sl_atr_buffer)

    # SAFETY: ensure impulse columns exist
    if require_impulse_before_tdp:
        if ("impulse_dir" not in c.columns) or ("impulse_recent" not in c.columns):
            imp = (c["close"] - c["close"].shift(impulse_lookback)) / c["atr"]
            c["impulse_dir"] = np.where(imp > 0, "UP", np.where(imp < 0, "DOWN", "FLAT"))
            c["impulse_recent"] = (imp.abs() >= impulse_size_atr).fillna(False).astype(bool)
    else:
        if "impulse_dir" not in c.columns:
            c["impulse_dir"] = "FLAT"
        if "impulse_recent" not in c.columns:
            c["impulse_recent"] = False

    entries: list[Entry] = []
    phase_u = _phase_upper(c)

    # TREND route
    if enable_trend and phase_u.isin(["PHASE_TREND_UP", "PHASE_TREND_DOWN"]).any():
        entries += (generate_trend_entries(
            c,
            debug_entry_filters=debug_entry_filters,
            symbol=symbol,
            rr=trend_rr,
            rr_long=trend_rr_long,
            sl_atr_buffer=trend_sl_atr_buffer,
            tdp_dev_lookback=tdp_dev_lookback,
            require_impulse_before_tdp=require_impulse_before_tdp,
            reclaim_buf_atr=reclaim_buf_atr,
            reclaim_lookahead=reclaim_lookahead,
            had_imp_down_window=had_imp_down_window,
        ) or [])

    # RANGE route
    if (phase_u == "PHASE_RANGE").any():
        entries += (generate_range_entries(
            c,
            debug_entry_filters=debug_entry_filters,
            symbol=symbol,
            rr_long=range_rr_long,
            sl_atr_buffer=range_sl_atr_buffer,
            enable_range_short=enable_range_short,
            enable_range_long=enable_range_long,

            dev_buf_atr=float(range_dev_buf_atr),
            reclaim_buf_atr=float(range_reclaim_buf_atr),
            retest_tol_atr=float(range_retest_tol_atr),
            reclaim_lookahead=int(range_reclaim_lookahead),
            retest_lookahead=int(range_retest_lookahead),
            min_range_width_atr=float(range_min_width_atr),
            cooldown_candles=int(range_cooldown_candles),
        ) or [])

    # Telemetry: log chosen RR per produced entry

    try:

        log = logging.getLogger(__name__)

        for e in entries:

            model = str(getattr(e, "model", "") or "")

            if model == "TDP_REENTRY":

                rr_used = 2.5 if (is_trend and macro_strength_u == "HIGH") else (2.0 if is_trend else float(rr))

            elif model.upper().startswith("RANGE"):

                rr_used = 1.5 if is_range else float(rr)

            else:

                rr_used = float(rr)

            log.debug("[RR_DYNAMIC] model=%s rr=%s", model, rr_used)

    except Exception:

        pass
    # ENTRY_DIAG: explainable drop breakdown (only when entry_model returned 0)
    # --------------------------------------------------------
    # ENTRY_DIAG: explainable drop breakdown
    diag_always = False
    try:
        if hasattr(ctx, "attrs") and isinstance(ctx.attrs, dict):
            diag_always = bool(ctx.attrs.get("diag_always", False))
    except Exception:
        diag_always = False

    if debug_entry_filters and ((not entries) or diag_always):
        try:
            diag = {}
            if hasattr(ctx, "attrs") and isinstance(ctx.attrs, dict):
                diag = ctx.attrs.get("_entry_diag") or {}

            keys = [
                "RR_TOO_LOW",
                "IMPULSE_TOO_SMALL",
                "RETEST_FAIL",
                "TREND_MISMATCH",
                "SL_INVALID",
            ]
            pairs = [(k, int(diag.get(k, 0) or 0)) for k in keys]
            pairs = [(k, v) for (k, v) in pairs if v > 0]
            pairs = sorted(pairs, key=lambda kv: kv[1], reverse=True)[:3]

            tag = f"[ENTRY_DIAG][{symbol}]" if symbol else "[ENTRY_DIAG]"
            print(tag)

            if not pairs:
                print("NO_BUMPS (no candidates hit filter counters; check phase routing / early exits)")

            for k, v in pairs:
                print(f"{k}={v}")
        except Exception as _e:
            tag = f"[ENTRY_DIAG][{symbol}]" if symbol else "[ENTRY_DIAG]"
            print(f"{tag} diag_failed err={repr(_e)}")

    # --- DEBUG: force at least one synthetic entry so pipeline stages (cluster/invalidation/budget) can be exercised ---
    # This ONLY activates when caller passes debug_entry_filters=True and no setups matched.
    debug_force_entries = False
    try:
        if hasattr(ctx, "attrs") and isinstance(ctx.attrs, dict):
            debug_force_entries = bool(ctx.attrs.get("debug_force_entries", False))
    except Exception:
        debug_force_entries = False

    if debug_entry_filters and debug_force_entries and (len(entries) == 0):
        try:
            last = c.iloc[-1]
            ts = pd.Timestamp(last["timestamp"])
            close_px = float(last.get("close", last.get("c", np.nan)))
            if not np.isfinite(close_px):
                # fallback to mid of candle
                close_px = 0.5 * (float(last["high"]) + float(last["low"]))
            atr = float(last.get("atr", np.nan))
            if not np.isfinite(atr) or atr <= 0:
                atr = max(1e-9, close_px * 0.002)  # ~0.2% fallback
            # create one SHORT and one LONG around last close; use conservative buffers
            buf = 0.25
            rr_dbg = float(rr) if rr is not None else 2.0

            # SHORT synthetic
            sl_s = float(last["high"]) + buf * atr
            risk_s = max(1e-9, sl_s - close_px)
            tp_s = close_px - rr_dbg * risk_s
            entries.append(Entry(
                timestamp=ts,
                model="DEBUG_FORCED_SHORT",
                side="SHORT",
                entry=close_px,
                sl=sl_s,
                tp=tp_s,
                symbol=str(symbol).strip(),
                meta="DEBUG: forced entry (no setups matched)",
                ctx_sub_label="DEBUG",
                regime=str(last.get("market_regime", last.get("regime", ""))) or None,
                trend_dir=str(last.get("trend_dir", "")) or None,
                trend_strength=float(last.get("trend_strength", np.nan)) if np.isfinite(float(last.get("trend_strength", np.nan))) else None,
                atr_pct=float(last.get("atr_pct", np.nan)) if np.isfinite(float(last.get("atr_pct", np.nan))) else None,
                phase=str(last.get("phase", phase_last)) if (("phase" in last) or (phase_last is not None)) else None,
                score=0.5,
            ))

            # LONG synthetic
            sl_l = float(last["low"]) - buf * atr
            risk_l = max(1e-9, close_px - sl_l)
            tp_l = close_px + rr_dbg * risk_l
            entries.append(Entry(
                timestamp=ts,
                model="DEBUG_FORCED_LONG",
                side="LONG",
                entry=close_px,
                sl=sl_l,
                tp=tp_l,
                symbol=str(symbol).strip(),
                meta="DEBUG: forced entry (no setups matched)",
                ctx_sub_label="DEBUG",
                regime=str(last.get("market_regime", last.get("regime", ""))) or None,
                trend_dir=str(last.get("trend_dir", "")) or None,
                trend_strength=float(last.get("trend_strength", np.nan)) if np.isfinite(float(last.get("trend_strength", np.nan))) else None,
                atr_pct=float(last.get("atr_pct", np.nan)) if np.isfinite(float(last.get("atr_pct", np.nan))) else None,
                phase=str(last.get("phase", phase_last)) if (("phase" in last) or (phase_last is not None)) else None,
                score=0.5,
            ))

            tag = f"[DEBUG_ENTRY_FORCE][{symbol}]" if symbol else "[DEBUG_ENTRY_FORCE]"
            print(f"{tag} forced_entries={len(entries)} ts={ts} close={close_px:.6f} atr={atr:.6f} phase={phase_last}")
        except Exception as _e:
            tag = f"[DEBUG_ENTRY_FORCE][{symbol}]" if symbol else "[DEBUG_ENTRY_FORCE]"
            print(f"{tag} failed err={repr(_e)}")

    entries = _apply_regime_drift_weights(entries, ctx)

    return entries
# ============================================================
# Minimal sanity test (no pytest needed)
# ============================================================

def sanity_test_range_top_short_v2() -> None:
    """Run 2 minimal checks:
      1) no reclaim => 0 entries
      2) sweep+reclaim+retest => 1 entry (SHORT, model tag, labels, numeric SL/TP/RR sanity)
    """
    import pandas as pd
    import numpy as np

    base_cols = ["timestamp","open","high","low","close","atr","range_hi","range_lo","phase","regime","trend_dir","trend_strength","atr_pct"]
    t0 = pd.Timestamp("2024-01-01 00:00:00")

    # --- case 1: sweep but no reclaim ---
    c1 = pd.DataFrame([
        [t0 + pd.Timedelta(minutes=0), 100, 101.0, 99.0, 100.5, 1.0, 100.0, 80.0, "PHASE_RANGE", "", "", 0.0, 0.0],
        [t0 + pd.Timedelta(minutes=1), 100, 100.2, 99.5, 100.1, 1.0, 100.0, 80.0, "PHASE_RANGE", "", "", 0.0, 0.0],
        [t0 + pd.Timedelta(minutes=2), 100, 100.1, 99.7, 100.0, 1.0, 100.0, 80.0, "PHASE_RANGE", "", "", 0.0, 0.0],
    ], columns=base_cols)

    e1 = generate_range_entries(
        c1,
        rr_long=1.6,
        sl_atr_buffer=0.25,
        enable_range_short=True,
        enable_range_long=True,   # must be ignored
        dev_buf_atr=0.08,
        reclaim_buf_atr=0.00,
        retest_tol_atr=0.15,
        reclaim_lookahead=24,
        retest_lookahead=36,
        min_range_width_atr=4.0,
        cooldown_candles=1,
    )
    assert len(e1) == 0, f"expected 0 entries (no reclaim), got {len(e1)}"

    # --- case 2: sweep + reclaim + retest => 1 entry ---
    c2 = pd.DataFrame([
        [t0 + pd.Timedelta(minutes=0), 100, 101.0, 99.0, 100.5, 1.0, 100.0, 80.0, "PHASE_RANGE", "", "", 0.0, 0.0],  # sweep
        [t0 + pd.Timedelta(minutes=1), 100, 100.4, 99.2,  99.6, 1.0, 100.0, 80.0, "PHASE_RANGE", "", "", 0.0, 0.0],  # reclaim (close < 100)
        [t0 + pd.Timedelta(minutes=2),  99.6, 100.0, 99.0, 99.5, 1.0, 100.0, 80.0, "PHASE_RANGE", "", "", 0.0, 0.0],  # retest touch + close < 100 + close > mid
    ], columns=base_cols)

    e2 = generate_range_entries(
        c2,
        rr_long=1.6,
        sl_atr_buffer=0.25,
        enable_range_short=True,
        enable_range_long=True,   # must be ignored
        dev_buf_atr=0.08,
        reclaim_buf_atr=0.00,
        retest_tol_atr=0.15,
        reclaim_lookahead=24,
        retest_lookahead=36,
        min_range_width_atr=4.0,
        cooldown_candles=1,
    )
    assert len(e2) == 1, f"expected 1 entry, got {len(e2)}"

    e = e2[0]
    assert e.model == "RANGE_TOP_SHORT_V2", f"model mismatch: {e.model}"
    assert e.ctx_sub_label == "RANGE_TOP_SHORT", f"ctx_sub_label mismatch: {e.ctx_sub_label}"
    assert str(e.side).upper() == "SHORT", f"side mismatch: {e.side}"
    assert np.isfinite(float(e.entry)) and np.isfinite(float(e.sl)) and np.isfinite(float(e.tp))
    assert e.tp < e.entry < e.sl, f"price ordering invalid: tp={e.tp} entry={e.entry} sl={e.sl}"

    # RR sanity
    rr = abs(e.tp - e.entry) / max(1e-9, abs(e.entry - e.sl))
    assert rr > 0.0, "RR must be positive"

    print("[OK] sanity_test_range_top_short_v2 passed")


if __name__ == "__main__":
    # Allow quick local run: python backtest/engine/entry_model.py
    sanity_test_range_top_short_v2()