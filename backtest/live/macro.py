from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd


@dataclass(frozen=True)
class MacroState:
    alt_mode: bool
    risk_multiplier: float
    reason: str


def _read_last_trend(csv_path: Path) -> Optional[str]:
    """Read a simple 'trend' column from CSV (UP/DOWN), last row.

    MVP feeder: you can keep these CSVs updated manually.
    Expected columns: timestamp, trend (optional). If 'trend' missing, returns None.
    """
    if not csv_path.exists():
        return None
    try:
        df = pd.read_csv(csv_path)
        if df.empty:
            return None
        if "trend" not in df.columns:
            return None
        v = str(df.iloc[-1]["trend"]).upper().strip()
        if v in {"UP", "DOWN"}:
            return v
        return None
    except Exception:
        return None


def get_macro_state(
    macro_dir: str | Path,
    alt_mode_risk_multiplier: float = 1.0,
    base_risk_multiplier: float = 1.0,
) -> MacroState:
    """Compute a very simple alt-season switch:

    alt_mode=True when BTC.D trend is DOWN and OTHERS.D trend is UP.
    This module is intentionally conservative: if data is missing, alt_mode=False.
    """
    d = Path(macro_dir)
    btc_d = _read_last_trend(d / "BTC.D_4h.csv")
    others_d = _read_last_trend(d / "OTHERS.D_4h.csv")

    alt_mode = (btc_d == "DOWN") and (others_d == "UP")
    if alt_mode:
        return MacroState(
            alt_mode=True,
            risk_multiplier=float(alt_mode_risk_multiplier),
            reason="MACRO: BTC.D DOWN & OTHERS.D UP",
        )

    # default
    return MacroState(
        alt_mode=False,
        risk_multiplier=float(base_risk_multiplier),
        reason="MACRO: base",
    )
