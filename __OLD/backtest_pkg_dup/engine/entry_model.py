from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List, Dict

import numpy as np
import pandas as pd

from backtest.journal.gates import allow_entry, GateConfig  # palikta dėl suderinamumo (čia nenaudojama)

# --- feature toggles (MVP) ---
ENABLE_TTS_RETEST = False  # keep OFF until TTS logic is validated


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
    meta: str = ""
    ctx_sub_label: Optional[str] = None   # "TDP_TOP"/"TDP_BOT"/"TTS_UP"/"TTS_DN"

    # --- regime fields (ateina iš ctx, kuris turi market_regime merge) ---
    regime: Optional[str] = None
    trend_dir: Optional[str] = None
    trend_strength: Optional[float] = None
    atr_pct: Optional[float] = None
    phase: Optional[str] = None


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
) -> list[Entry]:

    c = ctx.copy().sort_values("timestamp").reset_index(drop=True)
    phase_u = _phase_upper(c)
    c = c[phase_u.isin(["PHASE_TREND_UP", "PHASE_TREND_DOWN"])].copy().reset_index(drop=True)
    if c.empty:
        return []

    entries: list[Entry] = []

    recent_high = c["high"].rolling(tdp_dev_lookback).max()
    recent_low = c["low"].rolling(tdp_dev_lookback).min()

    # =================================================
    # TDP SHORT (trend continuation)
    # =================================================
    reentry_short = (
        (c["sub_label"] == "TDP_TOP")
        & c["dev_up"].rolling(tdp_dev_lookback).max().fillna(False)
        & (c["phase"] == "PHASE_TREND_DOWN")
    )

    if require_impulse_before_tdp:
        reentry_short &= c["impulse_recent"] & (c["impulse_dir"] == "UP")

    for i in np.where(reentry_short.values)[0]:
        entry_px = float(c.loc[i, "close"])
        atr = float(c.loc[i, "atr"])
        sl = float(recent_high.loc[i] + sl_atr_buffer * atr)
        risk = max(1e-9, sl - entry_px)
        tp = entry_px - rr * risk

        entries.append(Entry(
            timestamp=pd.Timestamp(c.loc[i, "timestamp"]),
            model="TDP_REENTRY",
            side="SHORT",
            entry=entry_px,
            sl=sl,
            tp=tp,
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
    dev_dn_recent = c["dev_dn"].rolling(tdp_dev_lookback).max().fillna(False)
    imp_down = (c["impulse_recent"] & (c["impulse_dir"] == "DOWN")).fillna(False)
    had_imp_down = imp_down.rolling(had_imp_down_window).max().fillna(False)

    buf = reclaim_buf_atr * c["atr"]
    sweep = c["low"] < (c["range_lo"] - buf)
    reclaim = c["close"] > (c["range_lo"] + buf)

    future_reclaim = reclaim.rolling(reclaim_lookahead).max().shift(-(reclaim_lookahead - 1))
    future_reclaim = future_reclaim.fillna(False)

    setup_long = (
        (c["sub_label"] == "TDP_BOT")
        & dev_dn_recent
        & had_imp_down
        & sweep
        & future_reclaim
        & (c["phase"] == "PHASE_TREND_UP")
    )

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
        risk = max(1e-9, entry_px - sl)
        tp = entry_px + rr_long * risk

        entries.append(Entry(
            timestamp=pd.Timestamp(c.loc[j, "timestamp"]),
            model="TDP_REENTRY",
            side="LONG",
            entry=entry_px,
            sl=sl,
            tp=tp,
            meta="TDP_BOT sweep->reclaim",
            ctx_sub_label="TDP_BOT",
            regime=str(c.loc[j, "regime"]),
            trend_dir=str(c.loc[j, "trend_dir"]),
            trend_strength=float(c.loc[j, "trend_strength"]),
            atr_pct=float(c.loc[j, "atr_pct"]),
            phase=str(c.loc[j, "phase"]),
        ))

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

def generate_range_entries(
    ctx: pd.DataFrame,
    rr_long: float,                 # palikta suderinamumui (TP = mid)
    sl_atr_buffer: float,

    # router / regime controller flags
    enable_range_short: bool = True,
    enable_range_long: bool = False,


    # v2 params
    dev_buf_atr: float = 0.08,
    reclaim_buf_atr: float = 0.00,
    retest_tol_atr: float = 0.15,
    reclaim_lookahead: int = 24,
    retest_lookahead: int = 36,
    min_range_width_atr: float = 4.0,
    cooldown_candles: int = 6,
) -> list[Entry]:
    """
    RANGE_FADE:
      - Core: RANGE_TOP_SHORT (enabled)
      - Optional: RANGE_BOT_LONG (gated, default OFF)
        Gate: phase==PHASE_RANGE AND trend_dir in {UP,NEUTRAL} AND regime in {BULL,ACCUMULATION}

    Team note:
      Range bottom longs were tested with multiple filters; baseline had no positive expectancy on BTC,
      so longs are OFF by default and only enabled under bullish regime gate.
    """

    c = ctx.copy().sort_values("timestamp").reset_index(drop=True)
    phase_u = _phase_upper(c)
    c = c[phase_u == "PHASE_RANGE"].copy().reset_index(drop=True)
    if c.empty:
        return []

    needed = ["timestamp", "high", "low", "close", "atr", "range_hi", "range_lo"]
    for col in needed:
        if col not in c.columns:
            return []

    # clean numeric
    for col in ["atr", "range_hi", "range_lo", "high", "low", "close"]:
        c[col] = pd.to_numeric(c[col], errors="coerce")
    c = c.dropna(subset=["atr", "range_hi", "range_lo", "high", "low", "close"]).reset_index(drop=True)
    if c.empty:
        return []

    # ensure regime fields exist (for gate + logging)
    for col, default in [
        ("regime", ""),
        ("trend_dir", ""),
        ("trend_strength", 0.0),
        ("atr_pct", 0.0),
        ("phase", "PHASE_RANGE"),
    ]:
        if col not in c.columns:
            c[col] = default

    # bullish helper: use open if exists, else close > prev_close
    has_open = "open" in c.columns
    if has_open:
        c["open"] = pd.to_numeric(c["open"], errors="coerce")

    def is_bullish(idx: int) -> bool:
        cl = float(c.loc[idx, "close"])
        if has_open and pd.notna(c.loc[idx, "open"]):
            op = float(c.loc[idx, "open"])
            return cl > op
        if idx <= 0:
            return True
        prev = float(c.loc[idx - 1, "close"])
        return cl > prev

    # width gate
    range_width_atr = (c["range_hi"] - c["range_lo"]).abs() / c["atr"]
    width_ok = range_width_atr >= float(min_range_width_atr)

    entries: list[Entry] = []
    n = len(c)
    i = 0

    while i < n:
        if not bool(width_ok.iloc[i]):
            i += 1
            continue

        atr_i = float(c.loc[i, "atr"])
        lo_i = float(c.loc[i, "range_lo"])
        hi_i = float(c.loc[i, "range_hi"])

        dev_dn_level = lo_i - dev_buf_atr * atr_i
        dev_up_level = hi_i + dev_buf_atr * atr_i

        low_i = float(c.loc[i, "low"])
        high_i = float(c.loc[i, "high"])

        swept_down = low_i < dev_dn_level
        swept_up = high_i > dev_up_level

        if not (swept_down or swept_up):
            i += 1
            continue

        # =========================================================
        # RANGE_BOT_LONG (gated)
        # =========================================================
        if swept_down:
            enable_range_long = bool(enable_range_long) and _enable_range_bot_long_mvp(c, i)
            if not enable_range_long:
                # cooldown so we don't rescan every candle near bottom
                i += int(max(1, cooldown_candles))
                continue

            # --- BOT LONG logic (kept minimal but robust) ---
            j_reclaim = None
            sweep_low = low_i

            j_end = min(n - 1, i + reclaim_lookahead)
            for j in range(i, j_end + 1):
                sweep_low = min(sweep_low, float(c.loc[j, "low"]))

                atr_j = float(c.loc[j, "atr"])
                lo_j = float(c.loc[j, "range_lo"])
                close_j = float(c.loc[j, "close"])

                if close_j > (lo_j + reclaim_buf_atr * atr_j):
                    j_reclaim = j
                    break

            if j_reclaim is None:
                i += 1
                continue

            # reclaim candle bullish filter
            if not is_bullish(j_reclaim):
                i += 1
                continue

            # find retest
            k_entry = None
            k_end = min(n - 1, j_reclaim + retest_lookahead)
            for k in range(j_reclaim, k_end + 1):
                atr_k = float(c.loc[k, "atr"])
                lo_k = float(c.loc[k, "range_lo"])
                low_k = float(c.loc[k, "low"])
                close_k = float(c.loc[k, "close"])

                if (low_k <= (lo_k + retest_tol_atr * atr_k)) and (close_k > (lo_k + reclaim_buf_atr * atr_k)):
                    mid_k = float((c.loc[k, "range_hi"] + c.loc[k, "range_lo"]) / 2.0)
                    if mid_k <= close_k:
                        continue
                    k_entry = k
                    break

            if k_entry is None:
                i += 1
                continue

            # VARIANT 1: impulse confirmation (anti falling-knife)
            confirm_lookahead = 3
            body_min_atr = 0.20

            k_confirm = None
            k2_end = min(n - 1, k_entry + confirm_lookahead)
            for u in range(k_entry, k2_end + 1):
                atr_u = float(c.loc[u, "atr"])
                close_u = float(c.loc[u, "close"])

                if "open" in c.columns:
                    open_u = float(c.loc[u, "open"])
                else:
                    open_u = float(c.loc[u - 1, "close"]) if u > 0 else close_u

                prev_high = float(c.loc[u - 1, "high"]) if u > 0 else -1e18

                strong_green = (close_u > open_u) and ((close_u - open_u) >= body_min_atr * atr_u)
                bos = close_u > prev_high

                if strong_green or bos:
                    k_confirm = u
                    break

            if k_confirm is None:
                i += 1
                continue

            k_entry = k_confirm

            # VARIANT 2: higher-low filter
            if k_entry > 0:
                prev_low = float(c.loc[k_entry - 1, "low"])
                cur_low = float(c.loc[k_entry, "low"])
                if cur_low <= prev_low:
                    i += 1
                    continue

            # entry bullish
            if not is_bullish(k_entry):
                i = k_entry + int(max(1, cooldown_candles))
                continue

            entry_px = float(c.loc[k_entry, "close"])
            atr_e = float(c.loc[k_entry, "atr"])
            sl = float(sweep_low - sl_atr_buffer * atr_e)
            tp = float((c.loc[k_entry, "range_hi"] + c.loc[k_entry, "range_lo"]) / 2.0)

            if not (sl < entry_px < tp):
                i = k_entry + 1
                continue

            # reward/risk sanity (avoid flat trades)
            risk = entry_px - sl
            reward = tp - entry_px
            if risk <= 0:
                i = k_entry + 1
                continue
            if (reward / max(risk, 1e-9)) < 0.8:
                i = k_entry + int(max(1, cooldown_candles))
                continue

            entries.append(Entry(
                timestamp=pd.Timestamp(c.loc[k_entry, "timestamp"]),
                model="RANGE_FADE_LONG",
                side="LONG",
                entry=entry_px,
                sl=sl,
                tp=tp,
                meta="range bot long (GATED): sweep_dn -> reclaim -> retest + confirm + higher-low -> mid",
                ctx_sub_label="RANGE_BOT_LONG",
                regime=str(c.loc[k_entry, "regime"]),
                trend_dir=str(c.loc[k_entry, "trend_dir"]),
                trend_strength=float(c.loc[k_entry, "trend_strength"]),
                atr_pct=float(c.loc[k_entry, "atr_pct"]),
                phase=str(c.loc[k_entry, "phase"]),
            ))

            i = k_entry + int(max(1, cooldown_candles))
            continue

        # =========================================================
        # RANGE_TOP_SHORT (core)
        if not enable_range_short:
            i += 1
            continue
        # =========================================================
        if swept_up:
            j_reclaim = None
            sweep_high = high_i

            j_end = min(n - 1, i + reclaim_lookahead)
            for j in range(i, j_end + 1):
                sweep_high = max(sweep_high, float(c.loc[j, "high"]))

                atr_j = float(c.loc[j, "atr"])
                hi_j = float(c.loc[j, "range_hi"])
                close_j = float(c.loc[j, "close"])

                if close_j < (hi_j - reclaim_buf_atr * atr_j):
                    j_reclaim = j
                    break

            if j_reclaim is None:
                i += 1
                continue

            k_entry = None
            k_end = min(n - 1, j_reclaim + retest_lookahead)
            for k in range(j_reclaim, k_end + 1):
                atr_k = float(c.loc[k, "atr"])
                hi_k = float(c.loc[k, "range_hi"])
                high_k = float(c.loc[k, "high"])
                close_k = float(c.loc[k, "close"])

                if (high_k >= (hi_k - retest_tol_atr * atr_k)) and (close_k < (hi_k - reclaim_buf_atr * atr_k)):
                    mid_k = float((c.loc[k, "range_hi"] + c.loc[k, "range_lo"]) / 2.0)

                    # anti-late-entry filter: don't short if already at/below mid
                    if mid_k >= close_k:
                        continue

                    k_entry = k
                    break

            if k_entry is None:
                i += 1
                continue

            entry_px = float(c.loc[k_entry, "close"])
            atr_e = float(c.loc[k_entry, "atr"])
            sl = float(sweep_high + sl_atr_buffer * atr_e)
            tp = float((c.loc[k_entry, "range_hi"] + c.loc[k_entry, "range_lo"]) / 2.0)

            if not (tp < entry_px < sl):
                i = k_entry + 1
                continue

            # risk sanity
            risk = sl - entry_px
            if risk <= 0:
                i = k_entry + 1
                continue

            entries.append(Entry(
                timestamp=pd.Timestamp(c.loc[k_entry, "timestamp"]),
                model="RANGE_FADE",
                side="SHORT",
                entry=entry_px,
                sl=sl,
                tp=tp,
                meta="range fade: top sweep -> reclaim -> retest -> mid",
                ctx_sub_label="RANGE_TOP_SHORT",
                regime=str(c.loc[k_entry, "regime"]),
                trend_dir=str(c.loc[k_entry, "trend_dir"]),
                trend_strength=float(c.loc[k_entry, "trend_strength"]),
                atr_pct=float(c.loc[k_entry, "atr_pct"]),
                phase=str(c.loc[k_entry, "phase"]),
            ))

            i = k_entry + int(max(1, cooldown_candles))
            continue

        i += 1

    return entries





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
    debug_long_funnel: bool = True,
) -> list["Entry"]:

    c = ctx.copy().sort_values("timestamp").reset_index(drop=True)

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
        entries += generate_trend_entries(
            c,
            rr=trend_rr,
            rr_long=trend_rr_long,
            sl_atr_buffer=trend_sl_atr_buffer,
            tdp_dev_lookback=tdp_dev_lookback,
            require_impulse_before_tdp=require_impulse_before_tdp,
            reclaim_buf_atr=reclaim_buf_atr,
            reclaim_lookahead=reclaim_lookahead,
            had_imp_down_window=had_imp_down_window,
        )

    # RANGE route
    if (phase_u == "PHASE_RANGE").any():
        entries += generate_range_entries(
            c,
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
        )

    return entries
