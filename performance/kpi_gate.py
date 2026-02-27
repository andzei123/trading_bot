from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import numpy as np


DEFAULT_THRESHOLDS = {
    "sharpe": 1.5,
    "max_dd": 0.20,
    "pf": 1.3,
    "expectancy": 0.0,
    "positive_months": 0.60,
}


def _safe_float(x, default: float = 0.0) -> float:
    try:
        v = float(x)
        if np.isfinite(v):
            return v
    except Exception:
        pass
    return float(default)


def _compute_r(row: pd.Series) -> float:
    """
    Compute per-trade return in R units using entry/sl/exit and side.
    Robust to missing data; falls back to outcome/rr when possible.
    """
    side = str(row.get("side", "")).upper()
    entry = _safe_float(row.get("entry", np.nan), np.nan)
    sl = _safe_float(row.get("sl", np.nan), np.nan)
    exit_price = _safe_float(row.get("exit_price", np.nan), np.nan)

    # Primary: compute from prices (preferred)
    if np.isfinite(entry) and np.isfinite(sl) and np.isfinite(exit_price):
        risk = abs(entry - sl)
        if risk > 1e-12:
            if side == "LONG":
                return (exit_price - entry) / risk
            if side == "SHORT":
                return (entry - exit_price) / risk

    # Secondary: outcome + rr (common backtest export)
    outcome = str(row.get("outcome", "")).upper()
    rr = _safe_float(row.get("rr", 0.0), 0.0)
    if outcome == "WIN":
        return float(rr) if rr > 0 else 1.0
    if outcome == "LOSS":
        return -1.0

    # Fallback: score as 0 return
    return 0.0


def evaluate_strategy_kpis(trades_df: pd.DataFrame, *, thresholds: Optional[dict[str, float]] = None) -> Dict[str, Any]:
    """
    Returns dict with computed KPIs and pass flag.

    Fail-open policy:
      - if trades_df empty -> pass=False
      - if cannot compute -> pass=False
    """
    th = dict(DEFAULT_THRESHOLDS)
    if thresholds:
        th.update({k: float(v) for k, v in thresholds.items() if v is not None})

    out: Dict[str, Any] = {
        "sharpe": 0.0,
        "max_dd": 1.0,
        "pf": 0.0,
        "expectancy": 0.0,
        "positive_months": 0.0,
        "n_trades": 0,
        "pass": False,
    }

    if trades_df is None or len(trades_df) == 0:
        return out

    df = trades_df.copy()

    # timestamp
    ts_col = "timestamp" if "timestamp" in df.columns else ("timestamp_utc" if "timestamp_utc" in df.columns else None)
    if ts_col:
        try:
            df[ts_col] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
        except Exception:
            pass

    # per-trade R
    df["R"] = df.apply(_compute_r, axis=1).astype(float)
    r = df["R"].replace([np.inf, -np.inf], np.nan).dropna()

    out["n_trades"] = int(len(r))
    if len(r) == 0:
        return out

    expectancy = float(r.mean())
    out["expectancy"] = expectancy

    # Profit factor
    pos = r[r > 0].sum()
    neg = r[r < 0].sum()
    out["pf"] = float(pos / abs(neg)) if abs(neg) > 1e-12 else (999.0 if pos > 0 else 0.0)

    # Equity curve & max DD (in R units).
    # IMPORTANT: include a starting equity point (1.0) so peak is never 0 when the first trade is -1R.
    equity = 1.0 + r.cumsum()
    equity0 = pd.concat([pd.Series([1.0]), equity], ignore_index=True)
    peak = equity0.cummax()
    dd = (peak - equity0) / peak.replace(0, np.nan)
    out["max_dd"] = float(min(dd.max(), 1.0)) if len(dd) else 1.0

    # Sharpe on per-trade returns (simple, consistent proxy)
    # Note: this is not time-annualized Sharpe; it's a stability proxy for gating.
    mu = float(r.mean())
    sigma = float(r.std(ddof=1)) if len(r) > 1 else 0.0
    out["sharpe"] = float(mu / sigma) if sigma > 1e-12 else (999.0 if mu > 0 else 0.0)

    # Positive months
    if ts_col:
        try:
            df2 = df.dropna(subset=[ts_col]).copy()
            if len(df2):
                _ts = df2[ts_col]
                try:
                    if getattr(_ts.dt, "tz", None) is not None:
                        _ts = _ts.dt.tz_convert(None)
                except Exception:
                    pass
                df2["month"] = _ts.dt.to_period("M").astype(str)
                m = df2.groupby("month")["R"].sum()
                out["positive_months"] = float((m > 0).mean()) if len(m) else 0.0
        except Exception:
            pass

    out["pass"] = bool(
        (out["sharpe"] > th["sharpe"])
        and (out["max_dd"] < th["max_dd"])
        and (out["pf"] > th["pf"])
        and (out["expectancy"] > th["expectancy"])
        and (out["positive_months"] > th["positive_months"])
    )
    return out


def load_trades_csv(path: str | Path) -> pd.DataFrame:
    """
    Reads trades CSV robustly (notes column may contain commas in older exports).
    """
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(p)
    except Exception:
        try:
            return pd.read_csv(p, engine="python", on_bad_lines="skip")
        except Exception:
            return pd.DataFrame()
