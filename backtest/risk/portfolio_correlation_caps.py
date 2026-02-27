# backtest/risk/portfolio_correlation_caps.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Tuple

import pandas as pd

# Default caps (can be overridden by runner)
BTC_CAP = 0.02
ALT_CAP = 0.02
MEME_CAP = 0.01


def _bucket(sym: str) -> str:
    s = str(sym or "").upper()
    if s.startswith("BTC"):
        return "BTC"
    # common meme tickers
    if any(x in s for x in ["DOGE", "PEPE", "SHIB", "FLOKI", "BONK", "WIF"]):
        return "MEME"
    return "ALT"


def _finite(x: Any, default: float) -> float:
    try:
        v = float(x)
    except Exception:
        return float(default)
    if v != v or v == float("inf") or v == float("-inf"):
        return float(default)
    return v


@dataclass
class CorrCapState:
    btc_used: float = 0.0
    alt_used: float = 0.0
    meme_used: float = 0.0


def apply_portfolio_correlation_caps_soft(
    df: pd.DataFrame,
    *,
    base_risk: float,
    state: CorrCapState,
    cap_btc: float = BTC_CAP,
    cap_alt: float = ALT_CAP,
    cap_meme: float = MEME_CAP,
    soft_mult: float = 0.25,
    hard_ratio: float = 1.2,
    debug: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame, CorrCapState]:
    """Apply correlation caps with SOFT throttle and HARD drop.

    Rules:
    - would = used + plan_risk
    - if would > cap * hard_ratio -> DROP
    - elif would > cap -> SOFT throttle: risk_multiplier *= soft_mult
    - always NaN/inf clamp so plan_risk is never NaN

    Returns: (kept_df, dropped_df, updated_state)
    """
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame(), pd.DataFrame(), state

    cap_btc = _finite(cap_btc, BTC_CAP)
    cap_alt = _finite(cap_alt, ALT_CAP)
    cap_meme = _finite(cap_meme, MEME_CAP)

    state = CorrCapState(
        btc_used=_finite(state.btc_used, 0.0),
        alt_used=_finite(state.alt_used, 0.0),
        meme_used=_finite(state.meme_used, 0.0),
    )

    kept_rows = []
    dropped_rows = []

    for _, r in df.iterrows():
        sym = str(r.get("symbol", "") or "")
        bucket = _bucket(sym)

        rm = _finite(r.get("risk_multiplier", 1.0), 1.0)
        plan_risk = _finite(base_risk, 0.0) * rm
        plan_risk = _finite(plan_risk, 0.0)

        if bucket == "BTC":
            used = state.btc_used
            cap = cap_btc
        elif bucket == "MEME":
            used = state.meme_used
            cap = cap_meme
        else:
            used = state.alt_used
            cap = cap_alt

        used = _finite(used, 0.0)
        cap = _finite(cap, 0.0)
        would = used + plan_risk
        would = _finite(would, used)  # if plan_risk was weird, fall back

        if debug:
            print(
                f"[CORR_CAP_DEBUG] cap_btc={cap_btc:.4f} cap_alt={cap_alt:.4f} cap_meme={cap_meme:.4f} "
                f"used_btc={state.btc_used:.4f} used_alt={state.alt_used:.4f} used_meme={state.meme_used:.4f} "
                f"plan_risk={plan_risk:.4f} bucket={bucket} would={would:.4f}"
            )

        # HARD drop only if significantly above cap
        if cap > 0 and would > cap * hard_ratio:
            dropped_rows.append(r)
            continue

        # SOFT throttle if slightly above cap
        if cap > 0 and would > cap:
            try:
                r = r.copy()
            except Exception:
                pass
            r["risk_multiplier"] = _finite(rm * soft_mult, rm)
            rm2 = _finite(r.get("risk_multiplier", rm), rm)
            plan_risk = _finite(_finite(base_risk, 0.0) * rm2, plan_risk)

        # update state
        if bucket == "BTC":
            state.btc_used = _finite(state.btc_used + plan_risk, state.btc_used)
        elif bucket == "MEME":
            state.meme_used = _finite(state.meme_used + plan_risk, state.meme_used)
        else:
            state.alt_used = _finite(state.alt_used + plan_risk, state.alt_used)

        kept_rows.append(r)

    return pd.DataFrame(kept_rows), pd.DataFrame(dropped_rows), state


# Backwards-compatible hard cap helper (older callers)
def apply_portfolio_correlation_caps(
    df: pd.DataFrame,
    *,
    plan_risk: float,
    btc_used: float,
    alt_used: float,
    meme_used: float,
):
    st = CorrCapState(btc_used=btc_used, alt_used=alt_used, meme_used=meme_used)
    kept, dropped, _ = apply_portfolio_correlation_caps_soft(
        df,
        base_risk=_finite(plan_risk, 0.0),
        state=st,
        cap_btc=BTC_CAP,
        cap_alt=ALT_CAP,
        cap_meme=MEME_CAP,
        soft_mult=0.0,  # emulate hard cap
        hard_ratio=1.0,
        debug=False,
    )
    return kept, dropped
