"""backtest.live.kill_switch

Small helpers to stop (or pause) trading when recent performance is too negative.

This is meant for *signal emission* gating (prop-firm style guardrails), not for
order execution.

CSV expectations:
- trades_csv has columns: timestamp, R
- optional: symbol
- timestamp may include timezone; parsed with utc=True
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Tuple

import pandas as pd


@dataclass(frozen=True)
class RollingGuardResult:
    ok: bool
    window_days: int
    threshold_r: float
    total_r: float
    rows: int
    reason: str


def rolling_r_guard(
    trades_csv: str | Path,
    *,
    threshold_r: float,
    window_days: int = 7,
    symbols: Optional[Iterable[str]] = None,
) -> RollingGuardResult:
    """Return ok=False if sum(R) over last window_days <= threshold_r.

    threshold_r is typically negative (e.g. -10).

    symbols: if provided, filter trades to those symbols.
    """
    p = Path(trades_csv)
    if not p.exists():
        return RollingGuardResult(
            ok=True,
            window_days=window_days,
            threshold_r=threshold_r,
            total_r=0.0,
            rows=0,
            reason=f"trades_csv not found -> skip ({p})",
        )

    df = pd.read_csv(p)
    if df.empty:
        return RollingGuardResult(True, window_days, threshold_r, 0.0, 0, "empty trades -> skip")

    if "timestamp" not in df.columns or "R" not in df.columns:
        return RollingGuardResult(True, window_days, threshold_r, 0.0, len(df), "missing timestamp/R -> skip")

    d = df.copy()
    d["timestamp"] = pd.to_datetime(d["timestamp"], utc=True, errors="coerce")
    d = d.dropna(subset=["timestamp", "R"])

    if symbols is not None and "symbol" in d.columns:
        symset = {str(s).upper() for s in symbols}
        d = d[d["symbol"].astype(str).str.upper().isin(symset)]

    if d.empty:
        return RollingGuardResult(True, window_days, threshold_r, 0.0, 0, "no rows after filters -> skip")

    end_ts = d["timestamp"].max()
    start_ts = end_ts - pd.Timedelta(days=int(window_days))
    w = d[d["timestamp"] >= start_ts]

    total_r = float(w["R"].sum())
    ok = total_r > float(threshold_r)
    reason = (
        f"rolling {window_days}d R={total_r:.2f} > {threshold_r:.2f}"
        if ok
        else f"KILL: rolling {window_days}d R={total_r:.2f} <= {threshold_r:.2f}"
    )
    return RollingGuardResult(ok, window_days, threshold_r, total_r, len(w), reason)
