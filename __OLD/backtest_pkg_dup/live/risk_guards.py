from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np

def _first_existing_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return None

@dataclass
class GuardResult:
    allowed: bool
    reason: str

def monthly_bad_month_guard(monthly_csv: str, symbol: str, bad_month_r: float = -10.0, min_trades: int = 20) -> GuardResult:
    """Return allowed=False if symbol has any month with total_R<=bad_month_r and trades>=min_trades."""
    df = pd.read_csv(monthly_csv)
    sym_col = _first_existing_col(df, ["symbol", "pair", "ticker"])
    if sym_col is None:
        return GuardResult(True, "monthly_guard: no symbol col -> skip")
    d = df[df[sym_col].astype(str) == str(symbol)].copy()
    if d.empty:
        return GuardResult(True, "monthly_guard: no rows for symbol -> skip")
    r_col = _first_existing_col(d, ["total_R", "sum_R", "total_r", "sum_r", "R", "r"])
    t_col = _first_existing_col(d, ["trades", "n_trades", "count", "total"])
    if r_col is None or t_col is None:
        return GuardResult(True, "monthly_guard: missing cols -> skip")
    d[r_col] = pd.to_numeric(d[r_col], errors="coerce")
    d[t_col] = pd.to_numeric(d[t_col], errors="coerce").fillna(0).astype(int)
    bad = d[(d[t_col] >= int(min_trades)) & (d[r_col] <= float(bad_month_r))]
    if not bad.empty:
        worst = bad.sort_values(r_col).iloc[0]
        return GuardResult(False, f"monthly_guard: bad month {worst.get('period', worst.get('month','?'))} R={worst[r_col]:.2f} trades={int(worst[t_col])}")
    return GuardResult(True, "monthly_guard: ok")

def _max_drawdown(cum: np.ndarray) -> float:
    peak = np.maximum.accumulate(cum)
    dd = cum - peak
    return float(dd.min()) if len(dd) else 0.0

def maxdd_guard(trades_csv: str, symbol: str, maxdd_threshold: float = -25.0) -> GuardResult:
    """Compute max drawdown of cumulative R for symbol across trades_csv. If maxDD <= threshold -> block."""
    df = pd.read_csv(trades_csv)
    sym_col = _first_existing_col(df, ["symbol", "pair", "ticker"])
    if sym_col is None:
        # trades_simulated might not have symbol; then skip
        return GuardResult(True, "maxdd_guard: no symbol col -> skip")
    d = df[df[sym_col].astype(str) == str(symbol)].copy()
    if d.empty:
        return GuardResult(True, "maxdd_guard: no trades -> skip")
    r_col = _first_existing_col(d, ["R", "r"])
    ts_col = _first_existing_col(d, ["timestamp", "entry_timestamp", "time"])
    if r_col is None:
        return GuardResult(True, "maxdd_guard: no R col -> skip")
    d[r_col] = pd.to_numeric(d[r_col], errors="coerce").fillna(0.0)
    if ts_col and ts_col in d.columns:
        d[ts_col] = pd.to_datetime(d[ts_col], errors="coerce", utc=True)
        d = d.sort_values(ts_col)
    cum = d[r_col].cumsum().to_numpy()
    mdd = _max_drawdown(cum)
    if mdd <= float(maxdd_threshold):
        return GuardResult(False, f"maxdd_guard: maxDD={mdd:.2f} <= {maxdd_threshold}")
    return GuardResult(True, f"maxdd_guard: maxDD={mdd:.2f} ok")
