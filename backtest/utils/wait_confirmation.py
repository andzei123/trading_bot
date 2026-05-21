from dataclasses import replace, is_dataclass
import pandas as pd
import numpy as np
import copy

from backtest.journal.identity import _ensure_canonical_setup_key

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

    - confirm on the candle immediately after setup_created_ts
    - execute on that confirmation candle open
    - keep same risk distance + RR

    IMPORTANT: wait confirmation is an EARLY STRUCTURAL VALIDATION.
    If setup_created_ts is present, it is the only timestamp used to choose
    the wait-confirmation candle. Live visibility/execution clocks must not
    move the wait-validation context forward.
    """

    if candles is None or candles.empty:
        return []

    c = candles.copy()
    c["timestamp"] = (
        pd.to_datetime(c["timestamp"], utc=True, errors="coerce")
        .dt.floor("15min")
    )
    c = c.dropna(subset=["timestamp"])
    c = c.sort_values("timestamp").reset_index(drop=True)

    ts_map = {pd.Timestamp(t): i for i, t in enumerate(c["timestamp"])}

    out = []

    for e in entries:
        e = _ensure_canonical_setup_key(e)

        setup_created_ts = pd.to_datetime(
            _get(e, "setup_created_ts", _get(e, "timestamp")),
            utc=True,
            errors="coerce",
        )
        if pd.isna(setup_created_ts):
            continue

        setup_created_ts_lookup = pd.Timestamp(setup_created_ts).floor("15min")
        i = ts_map.get(setup_created_ts_lookup)

        if i is None or i + 1 >= len(c):
            print(
                "[WAIT_CONTEXT] "
                f"canonical={_get(e, 'canonical_setup_key', '')} "
                f"setup_created_ts={setup_created_ts} "
                f"wait_confirm_ts=NaT "
                f"visible_ts={_get(e, 'visible_ts', '')} "
                f"pipeline_visible_ts={_get(e, 'pipeline_visible_ts', '')} "
                f"signal_ts={_get(e, 'signal_ts', '')} "
                f"cycle_ts={_get(e, 'cycle_ts', '')} "
                f"wait_context_source=setup_created_ts_plus_1_candle "
                f"decision=reject reason=missing_setup_or_confirm_candle"
            )
            continue

        wait_confirm_ts = pd.Timestamp(c.loc[i + 1, "timestamp"])

        side = str(_get(e, "side")).upper()
        entry = float(_get(e, "entry"))
        sl = float(_get(e, "sl"))
        tp = float(_get(e, "tp"))

        close_now = float(c.loc[i, "close"])
        close_next = float(c.loc[i+1, "close"])

        decision = "accept"
        reject_reason = ""

        # 🔥 confirmation
        if side == "LONG" and close_next <= close_now:
            decision = "reject"
            reject_reason = "long_next_close_not_higher"
        if side == "SHORT" and close_next >= close_now:
            decision = "reject"
            reject_reason = "short_next_close_not_lower"

        print(
            "[WAIT_CONTEXT] "
            f"canonical={_get(e, 'canonical_setup_key', '')} "
            f"setup_created_ts={setup_created_ts} "
            f"wait_confirm_ts={wait_confirm_ts} "
            f"visible_ts={_get(e, 'visible_ts', '')} "
            f"pipeline_visible_ts={_get(e, 'pipeline_visible_ts', '')} "
            f"signal_ts={_get(e, 'signal_ts', '')} "
            f"cycle_ts={_get(e, 'cycle_ts', '')} "
            f"wait_context_source=setup_created_ts_plus_1_candle "
            f"decision={decision} reason={reject_reason}"
        )

        if decision != "accept":
            continue

        # 🔥 execution
        exec_price = float(c.loc[i+1, "open"])
        exec_ts = wait_confirm_ts

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
            wait_confirm_ts=wait_confirm_ts,
            wait_context_source="setup_created_ts_plus_1_candle",
            entry=exec_price,
            sl=new_sl,
            tp=new_tp,
            rr=rr,
        ))

    return out