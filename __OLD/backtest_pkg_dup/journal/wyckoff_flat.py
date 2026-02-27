from __future__ import annotations
import pandas as pd
import numpy as np

def compute_range_bounds(htf_df: pd.DataFrame, lookback: int = 40) -> pd.DataFrame:
    h = htf_df.copy()
    h["range_hi"] = h["high"].rolling(lookback).max().shift(1)
    h["range_lo"] = h["low"].rolling(lookback).min().shift(1)
    return h

def _ensure_range_quality_cols(d: pd.DataFrame) -> pd.DataFrame:
    """
    Ensures:
      range_width, range_width_atr, pos_in_range
    Expects: range_hi, range_lo, atr, close
    """
    out = d.copy()
    out["range_width"] = out["range_hi"] - out["range_lo"]
    out["range_width_atr"] = out["range_width"] / out["atr"]

    rw = out["range_width"].replace(0, np.nan)
    out["pos_in_range"] = (out["close"] - out["range_lo"]) / rw
    return out

def mark_spring_upthrust(
    c: pd.DataFrame,
    spring_atr: float = 0.3,
    upthrust_atr: float = 0.3
) -> pd.DataFrame:
    # expects columns: close, low, high, atr, range_hi, range_lo
    d = c.copy()
    d["spring"] = (d["low"] < d["range_lo"] - spring_atr * d["atr"]) & (d["close"] > d["range_lo"])
    d["upthrust"] = (d["high"] > d["range_hi"] + upthrust_atr * d["atr"]) & (d["close"] < d["range_hi"])
    return d

def _find_confirmation(h: pd.DataFrame, i: int, side: str, n: int):
    """
    Returns (entry_ts, entry_price) or None.
    LONG: within next n bars close > spring_bar_high
    SHORT: within next n bars close < upthrust_bar_low
    """
    if n <= 0:
        return None

    base = h.iloc[i]
    future = h.iloc[i + 1 : i + 1 + n]
    if future.empty:
        return None

    if side == "LONG":
        hit = future[future["close"] > base["high"]]
    else:
        hit = future[future["close"] < base["low"]]

    if hit.empty:
        return None

    first = hit.iloc[0]
    return first["timestamp"], float(first["close"])

def wyckoff_entries(
    ctx: pd.DataFrame,
    confirm_n: int = 5,
    max_range_width_atr: float = 8.0,
    edge_frac: float = 0.25,
    spring_atr: float = 0.3,
    upthrust_atr: float = 0.3,
) -> pd.DataFrame:
    """
    Entry signals with confirmation + filters.

    Returns:
      columns: timestamp, side, entry_price, model='WYCKOFF'
    Expects ctx columns at least:
      timestamp, open/high/low/close, atr, range_hi, range_lo, regime
    """
    c = ctx.copy()
    if "regime" not in c.columns:
        return pd.DataFrame()

    c = c[c["regime"] == "RANGE"].copy()
    if c.empty:
        return pd.DataFrame()

    c = _ensure_range_quality_cols(c)
    c = mark_spring_upthrust(c, spring_atr=spring_atr, upthrust_atr=upthrust_atr)

    ent = []
    for i in range(len(c)):
        r = c.iloc[i]

        atrv = float(r.get("atr", np.nan))
        if not np.isfinite(atrv) or atrv <= 0:
            continue

        # range must exist
        rw = float(r.get("range_width", np.nan))
        if not np.isfinite(rw) or rw <= 0:
            continue

        # (1) range quality
        rwa = float(r.get("range_width_atr", np.nan))
        if not np.isfinite(rwa) or rwa > max_range_width_atr:
            continue

        # (2) near-edge
        pos = float(r.get("pos_in_range", np.nan))
        if not np.isfinite(pos):
            continue

        side = None
        if bool(r.get("spring", False)):
            if pos >= edge_frac:
                continue
            side = "LONG"
        elif bool(r.get("upthrust", False)):
            if pos <= (1.0 - edge_frac):
                continue
            side = "SHORT"
        else:
            continue

        # (A) confirmation -> entry on confirm bar close
        conf = _find_confirmation(c, i, side, int(confirm_n))
        if conf is None:
            continue
        entry_ts, entry_price = conf

        ent.append({
            "timestamp": entry_ts,
            "side": side,
            "entry_price": float(entry_price),
            "model": "WYCKOFF",
        })

    return pd.DataFrame(ent)

def wyckoff_entries_with_risk(
    ctx: pd.DataFrame,
    rr: float = 1.5,
    sl_buf: float = 0.25,
    confirm_n: int = 5,
    max_range_width_atr: float = 8.0,
    edge_frac: float = 0.25,
    spring_atr: float = 0.3,
    upthrust_atr: float = 0.3,
) -> pd.DataFrame:
    """
    Entries + SL/TP with confirmation + filters.

    LONG:
      entry = confirm close
      sl = spring_low - sl_buf*atr
      tp = entry + rr*(entry-sl)

    SHORT:
      entry = confirm close
      sl = upthrust_high + sl_buf*atr
      tp = entry - rr*(sl-entry)
    """
    c = ctx.copy()
    if "regime" in c.columns:
        c = c[c["regime"] == "RANGE"].copy()
    if c.empty:
        return pd.DataFrame()

    c = _ensure_range_quality_cols(c)
    c = mark_spring_upthrust(c, spring_atr=spring_atr, upthrust_atr=upthrust_atr)

    out = []
    for i in range(len(c)):
        r = c.iloc[i]

        atrv = float(r.get("atr", np.nan))
        if not np.isfinite(atrv) or atrv <= 0:
            continue

        rw = float(r.get("range_width", np.nan))
        if not np.isfinite(rw) or rw <= 0:
            continue

        # range quality
        rwa = float(r.get("range_width_atr", np.nan))
        if not np.isfinite(rwa) or rwa > max_range_width_atr:
            continue

        # near-edge
        pos = float(r.get("pos_in_range", np.nan))
        if not np.isfinite(pos):
            continue

        side = None
        spring_low = None
        upthrust_high = None

        if bool(r.get("spring", False)):
            if pos >= edge_frac:
                continue
            side = "LONG"
            spring_low = float(r["low"])
        elif bool(r.get("upthrust", False)):
            if pos <= (1.0 - edge_frac):
                continue
            side = "SHORT"
            upthrust_high = float(r["high"])
        else:
            continue

        conf = _find_confirmation(c, i, side, int(confirm_n))
        if conf is None:
            continue
        entry_ts, entry = conf

        if side == "LONG":
            sl = float(spring_low - sl_buf * atrv)
            risk = entry - sl
            if risk <= 0:
                continue
            tp = float(entry + rr * risk)
        else:
            sl = float(upthrust_high + sl_buf * atrv)
            risk = sl - entry
            if risk <= 0:
                continue
            tp = float(entry - rr * risk)

        out.append({
            "timestamp": entry_ts,
            "side": side,
            "entry": float(entry),
            "sl": float(sl),
            "tp": float(tp),
            "rr": float(rr),
            "model": "WYCKOFF",
        })

    return pd.DataFrame(out)
