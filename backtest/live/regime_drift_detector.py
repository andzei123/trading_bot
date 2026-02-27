"""
Regime Drift Detector
DEV4

Naudoja paskutinių N trades (default=40) R rezultatą
ir dinamiškai reguliuoja modelių svorius.
"""

from typing import Dict, List

LOOKBACK = 40


def compute_regime_drift(trade_results: List[float], lookback: int = LOOKBACK) -> Dict[str, float]:
    """
    trade_results: list of trade R values (pvz. +1.5, -0.7 ...)
    """

    if not trade_results:
        return {"trend_weight": 1.0, "range_weight": 1.0, "r_sum": 0.0}

    window = trade_results[-lookback:]
    r_sum = float(sum(window))

    trend_weight = 1.0
    range_weight = 1.0

    # --- Drift logic ---
    if r_sum < 0:
        trend_weight = 0.7
        range_weight = 1.3

    print(f"[REGIME_DRIFT] trend_weight={trend_weight:.2f} range_weight={range_weight:.2f} r_sum={r_sum:.2f}")

    return {
        "trend_weight": trend_weight,
        "range_weight": range_weight,
        "r_sum": r_sum,
    }
