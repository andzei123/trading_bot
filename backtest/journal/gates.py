# backtest/journal/gates.py
from __future__ import annotations

from dataclasses import dataclass
import pandas as pd


@dataclass(frozen=True)
class GateConfig:
    # jei trend_min == 0 -> pressure OFF (kaip tavo smart_gate minimalus variantas)
    trend_min: float = 0.0
    need_atr: bool = False
    atr_min: float = 0.0015


def allow_entry(row: pd.Series, cfg: GateConfig = GateConfig()) -> bool:
    """
    True = entry allowed, False = blocked.

    Reikia, kad row turėtų:
      - side (LONG/SHORT)
      - ctx_sub_label (TDP_BOT / TDP_TOP / TTS_UP / TTS_DN)  (arba sub_label)
      - trend_dir (UP/DOWN/FLAT)  [iš market_regime -> ctx]
    Optional:
      - trend_strength
      - atr_pct
    """
    side = str(row.get("side", "")).upper()
    sub = str(row.get("ctx_sub_label", row.get("sub_label", ""))).upper()
    trend_dir = str(row.get("trend_dir", "")).upper()

    # --- core gate: blokuojam TIK šitą bucket'ą ---
    bad_bot = (sub == "TDP_BOT") and (side == "LONG") and (trend_dir == "DOWN")
    if not bad_bot:
        return True

    # --- optional pressure (kaip smart_gate) ---
    if float(cfg.trend_min) > 0.0:
        ts = pd.to_numeric(pd.Series([row.get("trend_strength", 0.0)]), errors="coerce").fillna(0.0).iloc[0]
        if float(ts) < float(cfg.trend_min):
            return True  # nėra pressure -> neblokuojam

    if bool(cfg.need_atr):
        ap = pd.to_numeric(pd.Series([row.get("atr_pct", 0.0)]), errors="coerce").fillna(0.0).iloc[0]
        if float(ap) < float(cfg.atr_min):
            return True  # nėra atr pressure -> neblokuojam

    return False
