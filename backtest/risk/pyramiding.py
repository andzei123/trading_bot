# risk/pyramiding.py
from __future__ import annotations

from typing import Dict, Any


def should_pyramid(position_state: Dict[str, Any], rr_progress: float) -> bool:
    """
    Decide whether position qualifies for pyramid add.
    rr_progress = how many R already achieved (float).
    """
    if not position_state:
        return False

    try:
        if rr_progress >= 1.0 and not position_state.get("pyramid_1R_done", False):
            return True
        if rr_progress >= 0.5 and not position_state.get("pyramid_05R_done", False):
            return True
    except Exception:
        return False

    return False


def compute_pyramid_add(risk_remaining: float, base_risk: float = 0.002) -> float:
    """
    Compute how much additional risk we can allocate.
    Never exceed remaining risk.
    """
    try:
        add = min(base_risk * 0.5, float(risk_remaining))
        return max(add, 0.0)
    except Exception:
        return 0.0