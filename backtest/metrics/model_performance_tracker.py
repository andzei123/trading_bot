# backtest/metrics/model_performance_tracker.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd


@dataclass(frozen=True)
class ModelPerfRow:
    model: str
    trades: int
    R_sum: float
    winrate: float


def _pick_col(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return None


def compute_model_performance(
    trades_csv: str | Path,
    *,
    window_trades: int = 60,
) -> pd.DataFrame:
    """
    Compute rolling model performance from last `window_trades` CLOSED trades.

    Expects a CSV with at least:
      - model
      - R (or r / total_R / R_realized / etc.)
      - timestamp (optional but preferred; if missing, uses file order)

    Returns DF columns: model, trades, R_sum, winrate
    """
    p = Path(trades_csv)
    if not p.exists():
        return pd.DataFrame(columns=["model", "trades", "R_sum", "winrate"])

    try:
        df = pd.read_csv(p)
    except Exception:
        return pd.DataFrame(columns=["model", "trades", "R_sum", "winrate"])

    if df is None or df.empty:
        return pd.DataFrame(columns=["model", "trades", "R_sum", "winrate"])

    model_col = _pick_col(df, ["model", "strategy", "signal_model"])
    r_col = _pick_col(df, ["R", "r", "total_R", "r_sum", "R_realized", "R_net"])
    ts_col = _pick_col(df, ["timestamp", "close_ts", "exit_ts", "filled_ts", "ts"])

    if model_col is None or r_col is None:
        # Not enough info to compute performance
        return pd.DataFrame(columns=["model", "trades", "R_sum", "winrate"])

    # Normalize
    d = df.copy()
    d[model_col] = d[model_col].astype(str)

    # Parse R
    d[r_col] = pd.to_numeric(d[r_col], errors="coerce")
    d = d.dropna(subset=[r_col])

    if d.empty:
        return pd.DataFrame(columns=["model", "trades", "R_sum", "winrate"])

    # Order by timestamp if available, else keep file order
    if ts_col is not None:
        d[ts_col] = pd.to_datetime(d[ts_col], errors="coerce", utc=True)
        # if all NaT -> fallback to index order
        if d[ts_col].notna().any():
            d = d.sort_values(ts_col)

    # rolling window: last N trades
    d = d.tail(int(window_trades)).copy()

    # Aggregate per model within the rolling window
    grp = d.groupby(model_col, dropna=False)

    out = pd.DataFrame({
        "model": grp.size().index.astype(str),
        "trades": grp.size().values.astype(int),
        "R_sum": grp[r_col].sum().values.astype(float),
        "winrate": (grp[r_col].apply(lambda s: float((s > 0).mean()))).values.astype(float),
    })

    # Stable order: best R_sum first, then trades
    out = out.sort_values(["R_sum", "trades"], ascending=[False, False]).reset_index(drop=True)

    return out


def write_model_performance(
    trades_csv: str | Path,
    *,
    out_csv: str | Path = "exports_live/model_performance.csv",
    window_trades: int = 60,
) -> Path:
    """
    Overwrites `out_csv` with rolling performance table.
    """
    out_p = Path(out_csv)
    out_p.parent.mkdir(parents=True, exist_ok=True)

    df_perf = compute_model_performance(trades_csv, window_trades=window_trades)

    # Always write header, even if empty
    df_perf = df_perf.reindex(columns=["model", "trades", "R_sum", "winrate"])
    df_perf.to_csv(out_p, index=False)

    return out_p
