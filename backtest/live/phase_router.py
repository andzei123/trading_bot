from __future__ import annotations

from dataclasses import dataclass
from typing import Set, Tuple, List
import pandas as pd


from typing import Optional

@dataclass
class PhaseDecision:
    phase: str
    allowed_models: Set[str]
    reason: str
    atr_pct: Optional[float] = None

def _sma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n, min_periods=max(5, n // 5)).mean()


def _atr_pct(df: pd.DataFrame, n: int = 14) -> float:
    if df is None or df.empty:
        return 0.0
    d = df.tail(max(n * 3, 50)).copy()
    for col in ["high", "low", "close"]:
        if col not in d.columns:
            return 0.0

    prev_close = d["close"].shift(1)
    tr = pd.concat(
        [
            (d["high"] - d["low"]).abs(),
            (d["high"] - prev_close).abs(),
            (d["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    atr = tr.rolling(n, min_periods=max(5, n // 2)).mean().iloc[-1]
    px = float(d["close"].iloc[-1])
    if px <= 0 or pd.isna(atr):
        return 0.0
    return float(atr) / px


def decide_phase(
    candles: pd.DataFrame,
    *,
    macro_bias: str = "NEUTRAL",
    trend_long_tag: str = "TDP_REENTRY",
    trend_short_tag: str = "TDP_REENTRY",
    range_short_tag: str = "RANGE_TOP_SHORT_V2",
    allow_range_long: bool = False,  # your current edge status: OFF
) -> PhaseDecision:
    """3-phase router:
    - LONG: trade trend-long model family
    - RANGE: trade range model family (short-only by default)
    - SHORT: trade short model family (and optionally range-short)

    Deterministic heuristic:
    - trend_up: close > SMA200 AND SMA50 slope > 0
    - trend_down: close < SMA200 AND SMA50 slope < 0
    - else: RANGE (ATR% used as tie-breaker), with macro tilt when ambiguous

    Risk is NOT handled here (news does risk throttle).
    """
    if candles is None or candles.empty or "close" not in candles.columns:
        return PhaseDecision("RANGE", {range_short_tag}, "PHASE: fallback (no candles)")

    c = candles.tail(400).copy()
    close = c["close"].astype(float)

    sma50 = _sma(close, 50)
    sma200 = _sma(close, 200)

    last_close = float(close.iloc[-1])
    last_sma200 = float(sma200.iloc[-1]) if not pd.isna(sma200.iloc[-1]) else last_close

    slope = 0.0
    if len(sma50.dropna()) >= 15:
        slope = float(sma50.iloc[-1] - sma50.iloc[-11])

    atrp = _atr_pct(c, 14)
    low_vol = (atrp > 0) and (atrp < 0.02)

    trend_up = (last_close > last_sma200) and (slope > 0)
    trend_down = (last_close < last_sma200) and (slope < 0)

    mb = (macro_bias or "NEUTRAL").upper()

    if trend_up:
        return PhaseDecision("LONG", {trend_long_tag}, f"PHASE: LONG trend_up macro_bias={mb} atr%={atrp:.4f}", atr_pct=atrp, )
    if trend_down:
        return PhaseDecision("SHORT", {trend_short_tag, range_short_tag}, f"PHASE: SHORT trend_down macro_bias={mb} atr%={atrp:.4f}", atr_pct=atrp,)

    # ambiguous -> macro tilt
    if mb == "ALT_LONG" and not low_vol:
        return PhaseDecision("LONG", {trend_long_tag}, f"PHASE: LONG macro_tilt macro_bias={mb} atr%={atrp:.4f}",atr_pct=atrp,)
    if mb == "ALT_SHORT" and not low_vol:
        return PhaseDecision("SHORT", {trend_short_tag, range_short_tag}, f"PHASE: SHORT macro_tilt macro_bias={mb} atr%={atrp:.4f}", atr_pct=atrp,)

    allowed = {range_short_tag}
    if allow_range_long:
        allowed.add("RANGE_LONG")
    return PhaseDecision("RANGE", allowed, f"PHASE: RANGE macro_bias={mb} atr%={atrp:.4f}", atr_pct=atrp,)
