from dataclasses import replace, is_dataclass
import pandas as pd
import numpy as np
import copy

def _get(e, k, d=None):
    return e.get(k, d) if isinstance(e, dict) else getattr(e, k, d)

def _set(e, **kw):
    if isinstance(e, dict):
        out = dict(e); out.update(kw); return out
    if is_dataclass(e):
        return replace(e, **kw)
    out = copy.copy(e)
    for k, v in kw.items():
        setattr(out, k, v)
    return out

def apply_wait_confirmation(entries, candles):
    """
    REAL EXECUTION VERSION (no cheating)

    - confirm on next candle close
    - execute on next candle open
    - keep same risk distance + RR
    """

    if candles is None or candles.empty:
        return []

    c = candles.copy()
    c["timestamp"] = pd.to_datetime(c["timestamp"], utc=True)
    c = c.sort_values("timestamp").reset_index(drop=True)

    ts_map = {pd.Timestamp(t): i for i, t in enumerate(c["timestamp"])}

    out = []

    for e in entries:
        ts = pd.to_datetime(_get(e, "timestamp"), utc=True)
        i = ts_map.get(ts)

        if i is None or i + 1 >= len(c):
            continue

        side = str(_get(e, "side")).upper()
        entry = float(_get(e, "entry"))
        sl = float(_get(e, "sl"))
        tp = float(_get(e, "tp"))

        close_now = float(c.loc[i, "close"])
        close_next = float(c.loc[i+1, "close"])

        # 🔥 confirmation
        if side == "LONG" and close_next <= close_now:
            continue
        if side == "SHORT" and close_next >= close_now:
            continue

        # 🔥 execution
        exec_price = float(c.loc[i+1, "open"])
        exec_ts = pd.Timestamp(c.loc[i+1, "timestamp"])

        # 🔥 preserve risk + RR
        risk = abs(entry - sl)
        if risk <= 0:
            continue

        rr = abs(tp - entry) / risk

        if side == "LONG":
            new_sl = exec_price - risk
            new_tp = exec_price + rr * risk
        else:
            new_sl = exec_price + risk
            new_tp = exec_price - rr * risk

        out.append(_set(
            e,
            timestamp=exec_ts,
            entry=exec_price,
            sl=new_sl,
            tp=new_tp,
            rr=rr,
        ))

    return out