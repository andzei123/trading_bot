from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

import pandas as pd


def monthly_bad_month_guard(
    monthly_csv: str,
    symbol: str,
    bad_month_r: float = -10.0,
    min_trades: int = 20,
) -> Tuple[bool, str]:
    """
    Monthly risk guard.

    Expects monthly_csv (monthly_risk_by_symbol.csv) with columns at least:
      - symbol
      - period (YYYY-MM) OR month
      - total_R (float)
      - trades (int) OR total (int)
      - worst_R (optional)
      - maxDD_R (optional)

    Rule:
      - If there exists ANY month for this symbol with trades >= min_trades and total_R <= bad_month_r:
          -> FAIL (bad month)
      - Else -> OK

    Returns (ok, reason)
    """
    try:
        d = pd.read_csv(monthly_csv)
    except Exception as e:
        return False, f"monthly_guard: cannot read csv: {e}"

    if "symbol" not in d.columns:
        return False, "monthly_guard: missing column 'symbol'"

    sub = d[d["symbol"].astype(str) == str(symbol)]
    if sub.empty:
        return True, "monthly_guard: no rows for symbol -> skip"

    # trades column name variations
    trades_col = None
    for c in ["trades", "total", "n_trades", "count"]:
        if c in sub.columns:
            trades_col = c
            break
    if trades_col is None:
        # if no trades col, assume all rows are eligible
        eligible = sub.copy()
    else:
        eligible = sub[sub[trades_col].fillna(0).astype(float) >= float(min_trades)]

    if eligible.empty:
        return True, f"monthly_guard: no month with >= {min_trades} trades -> skip"

    if "total_R" not in eligible.columns:
        return False, "monthly_guard: missing column 'total_R'"

    bad = eligible[eligible["total_R"].astype(float) <= float(bad_month_r)]
    if bad.empty:
        return True, "monthly_guard: ok"

    # pick the worst month for messaging
    worst = bad.sort_values("total_R", ascending=True).iloc[0]
    month_label = None
    for c in ["period", "month"]:
        if c in bad.columns:
            month_label = str(worst[c])
            break
    month_label = month_label or "?"
    return False, f"monthly_guard: bad month {month_label} total_R={float(worst['total_R']):.2f} <= {bad_month_r:.2f}"


def maxdd_guard(
    trades_csv: str,
    symbol: str,
    maxdd_threshold: float = -25.0,
) -> Tuple[bool, str]:
    """
    Max drawdown guard in R units, computed from trade-level R series.

    Rule:
      - Compute running equity curve in R (cumsum of R).
      - Compute drawdown (equity - running_max).
      - maxDD_R = min(drawdown)
      - If maxDD_R < maxdd_threshold -> FAIL (too deep)
      - Else OK

    Returns (ok, reason)
    """
    try:
        d = pd.read_csv(trades_csv)
    except Exception as e:
        return False, f"maxdd_guard: cannot read csv: {e}"

    if "symbol" not in d.columns:
        return True, "maxdd_guard: no symbol column -> skip"

    sub = d[d["symbol"].astype(str) == str(symbol)]
    if sub.empty:
        return True, "maxdd_guard: no trades -> skip"

    if "R" not in sub.columns:
        return True, "maxdd_guard: no R column -> skip"

    r = pd.to_numeric(sub["R"], errors="coerce").fillna(0.0).astype(float)
    eq = r.cumsum()
    dd = eq - eq.cummax()
    maxdd = float(dd.min()) if len(dd) else 0.0

    if maxdd < float(maxdd_threshold):
        return False, f"maxdd_guard: maxDD={maxdd:.2f} < {float(maxdd_threshold):.2f}"
    return True, f"maxdd_guard: maxDD={maxdd:.2f} ok"


def killswitch_guard(
    trades_csv: str,
    symbol: str,
    threshold_r: float = -10.0,
    window_days: int = 7,
) -> Tuple[bool, str]:
    """
    Rolling window killswitch: sum of R over last window_days.

    Requires:
      - trades_csv has columns: symbol, timestamp, R
      - timestamp parseable by pandas

    Rule:
      - rolling_R = sum(R) for trades with timestamp >= (last_ts - window_days)
      - If rolling_R <= threshold_r -> FAIL
      - Else OK
    """
    try:
        d = pd.read_csv(trades_csv)
    except Exception as e:
        return False, f"killswitch: cannot read csv: {e}"

    if "symbol" not in d.columns or "timestamp" not in d.columns or "R" not in d.columns:
        return True, "killswitch: missing columns -> skip"

    sub = d[d["symbol"].astype(str) == str(symbol)].copy()
    if sub.empty:
        return True, "killswitch: no trades -> skip"

    sub["timestamp"] = pd.to_datetime(sub["timestamp"], utc=True, errors="coerce")
    sub = sub.dropna(subset=["timestamp"])
    if sub.empty:
        return True, "killswitch: no valid timestamps -> skip"

    sub["R"] = pd.to_numeric(sub["R"], errors="coerce").fillna(0.0).astype(float)

    last_ts = sub["timestamp"].max()
    cutoff = last_ts - pd.Timedelta(days=int(window_days))
    roll_r = float(sub.loc[sub["timestamp"] >= cutoff, "R"].sum())

    if roll_r <= float(threshold_r):
        return False, f"killswitch: rolling {window_days}d R={roll_r:.2f} <= {float(threshold_r):.2f}"
    return True, f"killswitch: rolling {window_days}d R={roll_r:.2f} > {float(threshold_r):.2f}"
