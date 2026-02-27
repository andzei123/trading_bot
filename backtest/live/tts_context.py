"""backtest/live/tts_context.py

DEV #2 — TTS

LOCKED ROLE (per lead):
  - TTS is NOT an entry generator.
  - TTS is a **context gate + lifecycle controller** in trend environment.
  - No fixed RR targets. No range TTS. No standalone TTS.

This module implements the minimal, deterministic contract:

    TTSContext(
        allow_long: bool,
        allow_short: bool,
        htf_bias: "UP" | "DOWN" | "NONE",
        veto_reason: str,
    )

HTF hierarchy (hard rule):
    1M > 1W > 1D > 4H > 15m

Minimal allow condition:
    1D == 4H (same direction)

Hard veto:
    if 1W or 1M conflicts -> NO TRADE

Phase rule:
    phase must be TREND (PHASE_TREND_UP / PHASE_TREND_DOWN)

Notes
-----
This module is intentionally **side-effect free**:
  - does not create entries
  - does not set SL/TP
  - does not place trades

It only labels context in a way that other modules can consume.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

import numpy as np
import pandas as pd


Trend = Literal["UP", "DOWN", "FLAT"]
Bias = Literal["UP", "DOWN", "NONE"]


@dataclass(frozen=True)
class TTSContext:
    allow_long: bool
    allow_short: bool
    htf_bias: Bias
    veto_reason: str = ""


def _ema_trend(ohlc: pd.DataFrame, ema_fast: int = 20, ema_slow: int = 50) -> pd.Series:
    """Return trend direction by EMA crossover on `close`.

    Output values: UP / DOWN / FLAT
    Index: timestamp
    """
    h = ohlc.copy()
    if "timestamp" in h.columns:
        h["timestamp"] = pd.to_datetime(h["timestamp"], utc=True, errors="coerce")
        h = h.dropna(subset=["timestamp"]).sort_values("timestamp")
        h = h.set_index("timestamp")

    close = pd.to_numeric(h["close"], errors="coerce")
    ema_f = close.ewm(span=int(ema_fast), adjust=False).mean()
    ema_s = close.ewm(span=int(ema_slow), adjust=False).mean()

    out = np.where(
        ema_f > ema_s,
        "UP",
        np.where(ema_f < ema_s, "DOWN", "FLAT"),
    )
    return pd.Series(out, index=h.index, name="trend")


def _resample_ohlc(df_15m: pd.DataFrame, rule: str) -> pd.DataFrame:
    c = df_15m.copy()
    c["timestamp"] = pd.to_datetime(c["timestamp"], utc=True, errors="coerce")
    c = c.dropna(subset=["timestamp"]).sort_values("timestamp")
    h = (
        c.set_index("timestamp")[["open", "high", "low", "close"]]
        .resample(rule)
        .agg({"open": "first", "high": "max", "low": "min", "close": "last"})
        .dropna()
        .reset_index()
    )
    return h


def _merge_asof(base: pd.DataFrame, htf: pd.DataFrame, col: str) -> pd.DataFrame:
    b = base.copy()
    h = htf[["timestamp", col]].copy()
    b["timestamp"] = pd.to_datetime(b["timestamp"], utc=True, errors="coerce")
    h["timestamp"] = pd.to_datetime(h["timestamp"], utc=True, errors="coerce")
    b = b.dropna(subset=["timestamp"]).sort_values("timestamp")
    h = h.dropna(subset=["timestamp"]).sort_values("timestamp")
    out = pd.merge_asof(b, h, on="timestamp", direction="backward")
    return out


def annotate_tts_context(
    ctx_15m: pd.DataFrame,
    *,
    ema_fast: int = 20,
    ema_slow: int = 50,
    require_phase_trend: bool = True,
) -> pd.DataFrame:
    """Vectorized TTS context flags for a 15m ctx frame.

    Adds columns:
      - tts_trend_4h, tts_trend_1d, tts_trend_1w, tts_trend_1m
      - tts_veto_reason
      - tts_htf_bias
      - tts_allow_long
      - tts_allow_short

    Requirements (on ctx_15m):
      - timestamp, open, high, low, close
      - phase (optional but recommended)
    """
    if ctx_15m is None or ctx_15m.empty:
        return ctx_15m.copy() if ctx_15m is not None else pd.DataFrame()

    c = ctx_15m.copy()

    # --- Build HTF OHLC from 15m ---
    o4h = _resample_ohlc(c, "4H")
    o1d = _resample_ohlc(c, "1D")
    o1w = _resample_ohlc(c, "1W")
    o1m = _resample_ohlc(c, "1M")

    # --- Compute EMA trends ---
    o4h["tts_trend_4h"] = _ema_trend(o4h, ema_fast, ema_slow).values
    o1d["tts_trend_1d"] = _ema_trend(o1d, ema_fast, ema_slow).values
    o1w["tts_trend_1w"] = _ema_trend(o1w, ema_fast, ema_slow).values
    o1m["tts_trend_1m"] = _ema_trend(o1m, ema_fast, ema_slow).values

    # --- Merge back into 15m ---
    c = _merge_asof(c, o4h, "tts_trend_4h")
    c = _merge_asof(c, o1d, "tts_trend_1d")
    c = _merge_asof(c, o1w, "tts_trend_1w")
    c = _merge_asof(c, o1m, "tts_trend_1m")

    # Normalize
    for col in ["tts_trend_4h", "tts_trend_1d", "tts_trend_1w", "tts_trend_1m"]:
        if col in c.columns:
            c[col] = c[col].astype(str).str.upper().replace({"NAN": "FLAT"})

    phase_u = c.get("phase", pd.Series(["" for _ in range(len(c))])).astype(str).str.upper()

    # --- Determine veto + allow ---
    t4 = c["tts_trend_4h"]
    t1d = c["tts_trend_1d"]
    t1w = c["tts_trend_1w"]
    t1m = c["tts_trend_1m"]

    # minimal allow: 1D == 4H and not FLAT
    align_up = (t1d == "UP") & (t4 == "UP")
    align_dn = (t1d == "DOWN") & (t4 == "DOWN")

    # hard veto if 1W or 1M conflicts with the aligned direction
    veto_up = align_up & ((t1w == "DOWN") | (t1m == "DOWN"))
    veto_dn = align_dn & ((t1w == "UP") | (t1m == "UP"))

    # phase must be TREND (hard block in chop)
    if require_phase_trend and "phase" in c.columns:
        phase_ok_up = phase_u.eq("PHASE_TREND_UP")
        phase_ok_dn = phase_u.eq("PHASE_TREND_DOWN")
    else:
        phase_ok_up = pd.Series([True] * len(c), index=c.index)
        phase_ok_dn = pd.Series([True] * len(c), index=c.index)

    allow_long = align_up & phase_ok_up & (~veto_up)
    allow_short = align_dn & phase_ok_dn & (~veto_dn)

    # Bias is only defined when 1D==4H
    bias = np.where(align_up, "UP", np.where(align_dn, "DOWN", "NONE"))

    # veto reason (best-effort, stable categories)
    reason = np.full(len(c), "", dtype=object)
    reason[(bias == "NONE")] = "NO_ALIGN_1D_4H"
    if require_phase_trend and "phase" in c.columns:
        reason[(align_up & (~phase_ok_up)) | (align_dn & (~phase_ok_dn))] = "PHASE_NOT_TREND"
    reason[veto_up] = "VETO_1W_OR_1M_DOWN"
    reason[veto_dn] = "VETO_1W_OR_1M_UP"
    reason[allow_long | allow_short] = ""

    c["tts_htf_bias"] = bias
    c["tts_veto_reason"] = reason
    c["tts_allow_long"] = allow_long.fillna(False)
    c["tts_allow_short"] = allow_short.fillna(False)

    return c


def get_tts_context_at(
    ctx_row: pd.Series,
) -> TTSContext:
    """Single-row helper (useful in live loops). Expects annotate_tts_context columns."""
    allow_long = bool(ctx_row.get("tts_allow_long", False))
    allow_short = bool(ctx_row.get("tts_allow_short", False))
    bias = str(ctx_row.get("tts_htf_bias", "NONE")).upper()
    if bias not in ("UP", "DOWN", "NONE"):
        bias = "NONE"
    reason = str(ctx_row.get("tts_veto_reason", ""))
    return TTSContext(allow_long=allow_long, allow_short=allow_short, htf_bias=bias, veto_reason=reason)
