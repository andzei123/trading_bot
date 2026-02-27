from __future__ import annotations

"""Equity curve tracker.

Goal for Phase-1:
- No NaT timestamps in exported equity_curve.csv
- Robust to partially-filled / dirty trades.csv (tolerant read)
"""

from pathlib import Path
from typing import Optional

import pandas as pd


def _outcome_to_r(outcome: pd.Series, rr: pd.Series) -> pd.Series:
    """Map outcome + rr to R per trade.

    Supported:
    - outcome in {WIN, LOSS, BE/BREAKEVEN}
    - numeric outcome (already R)
    """
    out = outcome.astype(str).str.upper().str.strip()
    r = pd.Series(0.0, index=outcome.index, dtype=float)

    r.loc[out.isin(["WIN", "W"])] = rr.loc[out.isin(["WIN", "W"])].astype(float)
    r.loc[out.isin(["LOSS", "L"])] = -1.0
    r.loc[out.isin(["BE", "BREAKEVEN", "BREAK EVEN"])] = 0.0

    # numeric outcomes override
    out_num = pd.to_numeric(outcome, errors="coerce")
    r.loc[out_num.notna()] = out_num.loc[out_num.notna()].astype(float)
    return r


def update_equity_curve_from_trades(
    *,
    trades_csv: str,
    out_csv: str = "exports_live/equity_curve.csv",
    initial_equity: float = 10_000.0,
    window_trades: int = 60,
) -> None:
    """Rolling equity curve from last N trades.

    Output columns:
      timestamp, equity, peak_equity, drawdown_pct

    Notes:
    - We prefer exit_timestamp if present & parseable; else entry timestamp.
    - We always drop NaT rows from output (no 'NaT' strings).
    """

    out_p = Path(out_csv)
    out_p.parent.mkdir(parents=True, exist_ok=True)

    p = Path(trades_csv)
    if (not p.exists()) or p.stat().st_size == 0:
        pd.DataFrame(columns=["timestamp", "equity", "peak_equity", "drawdown_pct"]).to_csv(out_p, index=False)
        return

    # trades.csv can be a bit dirty -> tolerant read
    df = pd.read_csv(p, engine="python", on_bad_lines="skip")
    if df is None or df.empty:
        pd.DataFrame(columns=["timestamp", "equity", "peak_equity", "drawdown_pct"]).to_csv(out_p, index=False)
        return

    # --- timestamp selection ---
    ts_col: Optional[str] = None
    if "exit_timestamp" in df.columns:
        # Support ISO like: 2025-01-01T00:00:00+00:00 (and other common variants)
        ts_try = pd.to_datetime(df["exit_timestamp"], errors="coerce", utc=True)
        if ts_try.notna().any():
            ts_col = "exit_timestamp"

    if ts_col is None:
        if "timestamp" in df.columns:
            ts_col = "timestamp"
        else:
            pd.DataFrame(columns=["timestamp", "equity", "peak_equity", "drawdown_pct"]).to_csv(out_p, index=False)
            return

    ts = pd.to_datetime(df[ts_col], errors="coerce", utc=True)

    # Fill missing timestamps so export doesn't start with NaT
    if ts.isna().all():
        # fail-open: empty export (better than NaT spam)
        pd.DataFrame(columns=["timestamp", "equity", "peak_equity", "drawdown_pct"]).to_csv(out_p, index=False)
        return

    ts = ts.ffill().bfill()
    df = df.copy()
    df["_ts"] = ts
    df = df.dropna(subset=["_ts"]).sort_values("_ts", ascending=True).reset_index(drop=True)

    if df.empty:
        pd.DataFrame(columns=["timestamp", "equity", "peak_equity", "drawdown_pct"]).to_csv(out_p, index=False)
        return

    # last N trades
    if window_trades is not None and int(window_trades) > 0:
        df = df.tail(int(window_trades)).reset_index(drop=True)

    rr = pd.to_numeric(df.get("rr"), errors="coerce").fillna(0.0)
    outcome = df.get("outcome")
    if outcome is None:
        r = pd.Series(0.0, index=df.index, dtype=float)
    else:
        r = _outcome_to_r(outcome, rr)

    equity = float(initial_equity) + r.cumsum()
    peak = equity.cummax()
    dd_pct = ((equity - peak) / peak.replace(0.0, pd.NA) * 100.0).fillna(0.0)

    out_df = pd.DataFrame(
        {
            "timestamp": df["_ts"].dt.strftime("%Y-%m-%d %H:%M:%S%z"),
            "equity": equity.astype(float).round(6),
            "peak_equity": peak.astype(float).round(6),
            "drawdown_pct": dd_pct.astype(float).round(12),
        }
    )

    # Never emit NaT
    out_df = out_df[out_df["timestamp"].astype(str).str.lower().ne("nat")]
    # --- minimal dedupe: keep last equity per timestamp ---
    out_df = out_df.drop_duplicates(subset=["timestamp"], keep="last")
    out_df.to_csv(out_p, index=False)
