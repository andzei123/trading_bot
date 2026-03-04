"""
Cross-Asset Macro Sync
DEV4

Goal:
- Derive a single cross-asset regime signal using multiple macro trends:
  BTC, ETH (optional), TOTAL3, BTC.D (and optional DXY)

Output:
- cross_asset_regime: "RISK_ON" | "RISK_OFF" | "NEUTRAL"

Design notes (production freeze friendly):
- Deterministic, low-risk heuristics.
- Accepts precomputed trend labels (preferred) OR raw close series (optional helper).
- Runner can log telemetry via `print` (or inject a logger).

Trend convention (recommended):
- "UP" / "DOWN" / "FLAT" (DEV4/DoD: never emit None in output)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence


Regime = str  # "RISK_ON" | "RISK_OFF" | "NEUTRAL"
Trend = str  # "UP" | "DOWN" | "FLAT"


@dataclass(frozen=True)
class CrossAssetRegimeResult:
    cross_asset_regime: Regime
    strength: float
    reason: str


def _norm_trend(t: Optional[str]) -> Trend:
    """Normalize trend values.

    DEV4/DoD: clamp anything unknown/None to FLAT so logs never show eth=None.
    """
    if t is None:
        return "FLAT"
    t2 = str(t).strip().upper()
    if t2 in {"UP", "DOWN", "FLAT"}:
        return t2
    # allow common synonyms
    if t2 in {"BULL", "BULLISH", "LONG"}:
        return "UP"
    if t2 in {"BEAR", "BEARISH", "SHORT"}:
        return "DOWN"
    return "FLAT"


def compute_cross_asset_regime(
    btc_trend: Optional[str],
    eth_trend: Optional[str],
    total3_trend: Optional[str],
    btcd_trend: Optional[str],
    dxy_trend: Optional[str] = None,
    *,
    emit_telemetry: bool = True,
) -> CrossAssetRegimeResult:
    """
    Core classifier. Uses simple voting + guardrails.

    Heuristics:
    - RISK_ON:
        BTC UP, ETH UP, TOTAL2 UP, BTC.D DOWN  (classic rotation into alts)
        Optional: DXY DOWN strengthens RISK_ON, DXY UP weakens to NEUTRAL.
    - RISK_OFF:
        TOTAL2 DOWN and BTC.D UP (dominance rising while alts falling)
        OR BTC DOWN and ETH DOWN (broad crypto weakness)
        Optional: DXY UP strengthens RISK_OFF, DXY DOWN weakens to NEUTRAL.
    - Otherwise NEUTRAL.
    """
    # Keep track of missing ETH for tagging only.
    eth_missing_optional = (eth_trend is None)

    btc = _norm_trend(btc_trend)
    eth = _norm_trend(eth_trend)
    t3 = _norm_trend(total3_trend)
    btcd = _norm_trend(btcd_trend)
    dxy = _norm_trend(dxy_trend)

    # Primary patterns
    tags: list[str] = []

    if eth_missing_optional:
        tags.append("ETH_MISSING_OPTIONAL")

    # RISK_ON: classic rotation + optional ETH
    # If ETH is missing, still allow BTC+TOTAL3+BTC.D to determine.
    risk_on = (btc == "UP" and t3 == "UP" and btcd == "DOWN" and (eth == "UP" or eth_missing_optional))

    # RISK_OFF: dominance up while alts down is sufficient. ETH DOWN strengthens when available.
    risk_off = ((t3 == "DOWN" and btcd == "UP") or (btc == "DOWN" and eth == "DOWN"))

    regime: Regime

    if risk_on and not risk_off:
        regime = "RISK_ON"
        tags.append("PATTERN_RISK_ON")
        if dxy == "UP":
            # dollar strength tends to dampen risk-on
            regime = "NEUTRAL"
            tags.append("DXY_UP_DAMPEN")
        elif dxy == "DOWN":
            tags.append("DXY_DOWN_CONFIRM")

    elif risk_off and not risk_on:
        regime = "RISK_OFF"
        tags.append("PATTERN_RISK_OFF")
        if dxy == "DOWN":
            # dollar weakness tends to soften risk-off
            regime = "NEUTRAL"
            tags.append("DXY_DOWN_DAMPEN")
        elif dxy == "UP":
            tags.append("DXY_UP_CONFIRM")

    else:
        # Mixed signals -> neutral
        regime = "NEUTRAL"
        tags.append("MIXED")

    # --- strength calculation (deterministic) ---
    score = 0

    if btc == "UP":
        score += 1
    elif btc == "DOWN":
        score -= 1

    if t3 == "UP":
        score += 1
    elif t3 == "DOWN":
        score -= 1

    if btcd == "DOWN":
        score += 1
    elif btcd == "UP":
        score -= 1

    if eth == "UP":
        score += 1
    elif eth == "DOWN":
        score -= 1

    strength = min(abs(score) / 4.0, 1.0)

    reason = f"btc={btc} eth={eth} total3={t3} btcd={btcd} dxy={dxy} tags={','.join(tags)}"

    return CrossAssetRegimeResult(
        cross_asset_regime=regime,
        strength=float(strength),
        reason=reason,
    )


def trend_from_closes(
    closes: Sequence[float],
    *,
    lookback: int = 20,
    flat_threshold: float = 0.01,
) -> Trend:
    """
    Optional helper: compares last close vs SMA(lookback).
    flat_threshold is relative (e.g., 0.01 -> 1%).
    """
    if closes is None:
        return "FLAT"
    closes = list(closes)
    if len(closes) < max(2, lookback):
        return "FLAT"

    window = closes[-lookback:]
    sma = sum(window) / float(len(window))
    last = float(closes[-1])
    if sma == 0:
        return "FLAT"

    rel = (last - sma) / sma
    if abs(rel) <= flat_threshold:
        return "FLAT"
    return "UP" if rel > 0 else "DOWN"
