"""SPRINT-5 DEV4 — Volatility Regime Early Warning (fail-open).

Computes ATR% and ATR% z-score to classify volatility regimes:
  LOW / NORMAL / HIGH / SHOCK

Fail-open: on any error or insufficient candles -> NORMAL.
"""

from __future__ import annotations

from dataclasses import dataclass
import os

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class VolatilityRegime:
    regime: str
    atr_pct: float
    z: float


def _true_range(df: pd.DataFrame) -> pd.Series:
    high = pd.to_numeric(df.get("high"), errors="coerce")
    low = pd.to_numeric(df.get("low"), errors="coerce")
    close = pd.to_numeric(df.get("close"), errors="coerce")
    prev_close = close.shift(1)
    tr1 = (high - low).abs()
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    return pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)


def detect_volatility_regime(
    candles: pd.DataFrame,
    *,
    atr_window: int = 14,
    z_window: int = 100,
    low_z: float = -0.75,
    high_z: float = 1.0,
    shock_z: float = 2.0,
) -> VolatilityRegime:
    try:
        if candles is None or len(candles) == 0:
            # fail-open
            return VolatilityRegime("NORMAL", 0.0, 0.0)

        force = (os.getenv("VOL_REGIME_FORCE") or "").strip().upper()
        if force in {"LOW", "NORMAL", "HIGH", "SHOCK"}:
            # Forced mode for testing / ops. Still computes atr_pct/z when possible.
            forced_regime = force
        else:
            forced_regime = ""

        df = candles.copy()
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
            df = df.sort_values("timestamp")

        if not {"high", "low", "close"}.issubset(df.columns):
            return VolatilityRegime(forced_regime or "NORMAL", 0.0, 0.0)

        atr_window = int(max(2, atr_window))
        z_window = int(max(20, z_window))

        tr = _true_range(df)
        atr = tr.rolling(atr_window, min_periods=atr_window).mean()
        close = pd.to_numeric(df["close"], errors="coerce")
        atr_pct = atr / close.replace(0, np.nan)

        atr_pct_last = float(atr_pct.iloc[-1]) if len(atr_pct) else np.nan
        if not np.isfinite(atr_pct_last):
            return VolatilityRegime("NORMAL", 0.0, 0.0)

        w = atr_pct.dropna().iloc[-z_window:]
        if len(w) < max(20, z_window // 2):
            return VolatilityRegime("NORMAL", float(atr_pct_last), 0.0)

        mu = float(w.mean())
        sd = float(w.std(ddof=0))
        z = 0.0 if (not np.isfinite(sd) or sd <= 1e-12) else float((atr_pct_last - mu) / sd)

        if z <= float(low_z):
            reg = "LOW"
        elif z < float(high_z):
            reg = "NORMAL"
        elif z < float(shock_z):
            reg = "HIGH"
        else:
            reg = "SHOCK"

        if float(atr_pct_last) >= 0.08:
            reg = "SHOCK"
        if forced_regime:
            reg = forced_regime

        return VolatilityRegime(reg, float(atr_pct_last), float(z))

    except Exception:
        return VolatilityRegime("NORMAL", 0.0, 0.0)
