from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd


# =========================
# Context object
# =========================
@dataclass
class Context:
    # --- Hard rules ---
    stop_hunt_wick: bool = False
    stop_hunt_wick: bool = False
    rejection_wick: bool = False

    wyckoff_range: bool = False
    wyckoff_event: Optional[str] = None  # "SPRING", "UPTHRUST", None

    htf_bias: Optional[str] = None        # vėliau (HTF)
    deviation: bool = False

    liquidity_sweep: bool = False
    ob_before_sweep: bool = False

    rr: float = 0.0
    high_impact_news: bool = False

    # --- Debug / extras ---
    pattern: Optional[str] = None
    notes: str = ""


# =========================
# Wyckoff RANGE
# =========================
def detect_wyckoff_range(
    df: pd.DataFrame,
    idx: int,
    lookback: int = 96,
    breakout_k: float = 0.15,
    slope_k: float = 0.20,
) -> bool:
    if idx < lookback:
        return False

    window = df.iloc[idx - lookback : idx]

    hi = float(window["high"].max())
    lo = float(window["low"].min())
    last_close = float(df.iloc[idx]["close"])

    box = hi - lo
    if box <= 0:
        return False

    upper = hi + box * breakout_k
    lower = lo - box * breakout_k
    if last_close > upper or last_close < lower:
        return False

    closes = window["close"].astype(float).to_numpy()
    x = np.arange(len(closes), dtype=float)

    x_mean = x.mean()
    y_mean = closes.mean()
    cov = ((x - x_mean) * (closes - y_mean)).sum()
    var = ((x - x_mean) ** 2).sum()
    slope = cov / var if var != 0 else 0.0

    slope_norm = abs(slope) / box
    return slope_norm <= slope_k


# =========================
# Deviation (premium / discount)
# =========================
def detect_deviation(
    df: pd.DataFrame,
    idx: int,
    lookback: int = 96,
    k: float = 0.25,
) -> bool:
    if idx < lookback:
        return False

    window = df.iloc[idx - lookback : idx]
    hi = float(window["high"].max())
    lo = float(window["low"].min())
    if hi <= lo:
        return False

    last_close = float(df.iloc[idx]["close"])
    mid = (hi + lo) / 2.0
    box = hi - lo

    premium_start = mid + box * k
    discount_end = mid - box * k

    return (last_close >= premium_start) or (last_close <= discount_end)


# =========================
# Liquidity sweep
# =========================
def detect_sweep_dir(
    df: pd.DataFrame,
    idx: int,
    lookback: int = 20,
) -> Optional[str]:
    if idx < lookback:
        return None

    prev = df.iloc[idx - lookback : idx]
    cur = df.iloc[idx]

    prev_high = float(prev["high"].max())
    prev_low = float(prev["low"].min())

    cur_high = float(cur["high"])
    cur_low = float(cur["low"])
    cur_close = float(cur["close"])

    sweep_up = (cur_high > prev_high) and (cur_close < prev_high)
    sweep_down = (cur_low < prev_low) and (cur_close > prev_low)

    if sweep_up:
        return "UP"
    if sweep_down:
        return "DOWN"
    return None


# =========================
# Order Block (MVP)
# =========================
def detect_ob_before_sweep(
    df: pd.DataFrame,
    idx: int,
    sweep_dir: str,
    lookback: int = 30,
) -> bool:
    if idx < lookback:
        return False

    window = df.iloc[idx - lookback : idx]

    for j in range(len(window) - 1, -1, -1):
        c = window.iloc[j]
        o = float(c["open"])
        cl = float(c["close"])

        bullish = cl > o
        bearish = cl < o

        if sweep_dir == "DOWN" and bullish:
            return True
        if sweep_dir == "UP" and bearish:
            return True

    return False


# =========================
# RR from sweep
# =========================
def compute_rr_from_sweep(
    df: pd.DataFrame,
    idx: int,
    sdir: str,
    rr_target: float = 3.0,
) -> float:
    cur = df.iloc[idx]
    entry = float(cur["close"])

    if sdir == "DOWN":  # LONG
        sl = float(cur["low"])
        risk = entry - sl
        if risk <= 0:
            return 0.0
        tp = entry + rr_target * risk
        return (tp - entry) / risk

    if sdir == "UP":  # SHORT
        sl = float(cur["high"])
        risk = sl - entry
        if risk <= 0:
            return 0.0
        tp = entry - rr_target * risk
        return (entry - tp) / risk

    return 0.0


# =========================
# Context Builder
# =========================
class ContextBuilder:
    def __init__(
        self,
        df_15m: pd.DataFrame,
        df_1h: Optional[pd.DataFrame] = None,
        df_4h: Optional[pd.DataFrame] = None,
        df_1d: Optional[pd.DataFrame] = None,
    ):
        self.df_15m = df_15m
        self.df_1h = df_1h
        self.df_4h = df_4h
        self.df_1d = df_1d

    def build(self, idx: int) -> Dict[str, Any]:
        ctx = Context()

        # --- Market state ---
        ctx.wyckoff_range = detect_wyckoff_range(self.df_15m, idx)
        ctx.deviation = detect_deviation(self.df_15m, idx)

        # --- Sweep ---
        sdir = detect_sweep_dir(self.df_15m, idx)
        ctx.liquidity_sweep = sdir is not None

        # --- OB ---
        ctx.ob_before_sweep = False
        if ctx.liquidity_sweep and sdir:
            ctx.ob_before_sweep = detect_ob_before_sweep(
                self.df_15m, idx, sdir
            )

        # --- RR ---
        ctx.rr = 0.0
        if sdir:
            ctx.rr = compute_rr_from_sweep(self.df_15m, idx, sdir)

        # --- Wyckoff event (MVP) ---
        if ctx.liquidity_sweep:
            ctx.wyckoff_event = "SPRING" if sdir == "DOWN" else "UPTHRUST"
        else:
            ctx.wyckoff_event = None

        # --- News (stub) ---
        ctx.high_impact_news = False

        # --- Debug ---
        ctx.notes = (
            f"sdir={sdir} "
            f"range={ctx.wyckoff_range} "
            f"dev={ctx.deviation} "
            f"sweep={ctx.liquidity_sweep} "
            f"ob={ctx.ob_before_sweep} "
            f"rr={ctx.rr:.2f}"
        )
        ctx.stop_hunt_wick = False

        return ctx.__dict__

