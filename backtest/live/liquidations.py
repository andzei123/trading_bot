from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class LiquidationState:
    updated_utc: str
    risk_multiplier: float
    block_new_entries: bool
    reason: str


def load_liq_state(path: str | Path) -> LiquidationState:
    """MVP: load liquidation/heatmap 'veto' state from JSON file.

    This is intentionally simple and robust: missing file => safe defaults.
    You can update the JSON manually (or via a Telegram bot) without restarting.
    """
    p = Path(path)
    if not p.exists():
        return LiquidationState(updated_utc="", risk_multiplier=1.0, block_new_entries=False, reason="")
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return LiquidationState(
            updated_utc=str(data.get("updated_utc", "")),
            risk_multiplier=float(data.get("risk_multiplier", 1.0)),
            block_new_entries=bool(data.get("block_new_entries", False)),
            reason=str(data.get("reason", "")),
        )
    except Exception:
        # On parse errors, fail safe (do NOT block trades unexpectedly)
        return LiquidationState(updated_utc="", risk_multiplier=1.0, block_new_entries=False, reason="")
