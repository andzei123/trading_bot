from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

from backtest.engine.entry_model import Entry
def atr_series(df: pd.DataFrame, n: int = 14) -> pd.Series:
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    close = pd.to_numeric(df["close"], errors="coerce")
    prev_close = close.shift(1)

    tr = pd.concat(
        [(high - low).abs(),
         (high - prev_close).abs(),
         (low - prev_close).abs()],
        axis=1
    ).max(axis=1)

    return tr.rolling(n).mean()


def _infer_retest_level(setup) -> Optional[float]:
    """
    MVP retest levels:
      - TDP_BOT LONG  -> retest range_lo
      - TDP_TOP SHORT -> retest range_hi
      - TTS_UP  LONG  -> retest range_hi (breakout level)
      - TTS_DN  SHORT -> retest range_lo (breakout level)
    """
    if setup.ctx_sub_label == "TDP_BOT" and setup.side == "LONG":
        return setup.range_lo
    if setup.ctx_sub_label == "TDP_TOP" and setup.side == "SHORT":
        return setup.range_hi
    if setup.ctx_sub_label == "TTS_UP" and setup.side == "LONG":
        return setup.range_hi
    if setup.ctx_sub_label == "TTS_DN" and setup.side == "SHORT":
        return setup.range_lo
    return None


def find_retest_entry(
    candles: pd.DataFrame,
    setup,
    window_hours: float = 48.0,
    tol_atr_mult: float = 0.25,
    rr: float = 3.0,
    sl_atr_buffer: float = 0.25,
    atr_win: int = 14,
) -> Optional[Entry]:
    """
    LTF retest trigger MVP (15m):
      LONG:  low <= level + tol  AND close > level
      SHORT: high >= level - tol AND close < level

    tol = tol_atr_mult * ATR_ltf (computed on LTF candles up to current bar).
    SL: for LONG -> min(low of last 3 bars incl current) - sl_atr_buffer*ATR
        for SHORT -> max(high of last 3 bars incl current) + sl_atr_buffer*ATR
    TP: RR * risk
    """
    if candles is None or candles.empty:
        return None

    level = _infer_retest_level(setup)
    if level is None or not np.isfinite(level):
        return None

    c = candles.copy()
    c["timestamp"] = pd.to_datetime(c["timestamp"], errors="coerce")
    c = c.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    t0 = pd.Timestamp(setup.timestamp_htf)
    t1 = t0 + pd.Timedelta(hours=float(window_hours))

    w = c[(c["timestamp"] >= t0) & (c["timestamp"] <= t1)].copy()
    if w.empty or len(w) < atr_win + 3:
        return None

    w["atr"] = atr_series(w, atr_win)
    w = w.dropna(subset=["atr"]).reset_index(drop=True)
    if w.empty:
        return None

    side = setup.side.upper()

    for i in range(2, len(w)):
        ts = pd.Timestamp(w.loc[i, "timestamp"])
        h = float(w.loc[i, "high"])
        l = float(w.loc[i, "low"])
        close = float(w.loc[i, "close"])
        atr = float(w.loc[i, "atr"])
        tol = float(tol_atr_mult) * atr

        if side == "LONG":
            touched = l <= (level + tol)
            confirmed = close > level
            if touched and confirmed:
                swing_low = float(w.loc[i-2:i, "low"].min())
                sl = swing_low - float(sl_atr_buffer) * atr
                risk = max(1e-9, close - sl)
                tp = close + float(rr) * risk
                return Entry(
                    timestamp=ts,
                    model="HTF_LTF_RETEST",
                    side="LONG",
                    entry=close,
                    sl=sl,
                    tp=tp,
                    meta=f"retest@{level:.6f} tol={tol_atr_mult}*ATR",
                    ctx_sub_label=setup.ctx_sub_label,
                    regime=setup.regime,
                    trend_dir=setup.trend_dir,
                    trend_strength=float(setup.trend_strength),
                    atr_pct=float(setup.atr_pct),
                )

        elif side == "SHORT":
            touched = h >= (level - tol)
            confirmed = close < level
            if touched and confirmed:
                swing_high = float(w.loc[i-2:i, "high"].max())
                sl = swing_high + float(sl_atr_buffer) * atr
                risk = max(1e-9, sl - close)
                tp = close - float(rr) * risk
                return Entry(
                    timestamp=ts,
                    model="HTF_LTF_RETEST",
                    side="SHORT",
                    entry=close,
                    sl=sl,
                    tp=tp,
                    meta=f"retest@{level:.6f} tol={tol_atr_mult}*ATR",
                    ctx_sub_label=setup.ctx_sub_label,
                    regime=setup.regime,
                    trend_dir=setup.trend_dir,
                    trend_strength=float(setup.trend_strength),
                    atr_pct=float(setup.atr_pct),
                )

    return None
