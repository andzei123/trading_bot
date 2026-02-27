"""Utilities for selecting which symbols to trade live.

This module is intentionally small and dependency-light so it can be reused by:
- backtest.journal.live_signal_runner_auto
- any future schedulers / orchestrators

Expected leaderboard schema (CSV):
- symbol: str
- metrics columns such as: total_R, exp_R, trades, maxDD_R, etc.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import pandas as pd


def load_leaderboard(path: str) -> pd.DataFrame:
    """Load leaderboard CSV and standardize column names.

    We accept either 'symbol' or 'bybit_symbol' as the symbol column (legacy).
    """
    df = pd.read_csv(path)

    # Normalize symbol column
    if "symbol" not in df.columns:
        if "bybit_symbol" in df.columns:
            df = df.rename(columns={"bybit_symbol": "symbol"})
        elif "Symbol" in df.columns:
            df = df.rename(columns={"Symbol": "symbol"})

    if "symbol" not in df.columns:
        raise ValueError(f"Leaderboard must contain a 'symbol' column. Got columns={list(df.columns)}")

    # Trim / normalize symbol values
    df["symbol"] = df["symbol"].astype(str).str.strip()

    return df


def normalize_leaderboard(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    """Ensure the metric column exists and is numeric; drop bad rows."""
    if metric not in df.columns:
        raise ValueError(f"Metric '{metric}' not found in leaderboard columns={list(df.columns)}")

    out = df.copy()
    out[metric] = pd.to_numeric(out[metric], errors="coerce")
    out = out.dropna(subset=["symbol", metric])
    return out


def pick_top_symbols(leaderboard: pd.DataFrame, metric: str = "total_R", top_n: int = 5, min_trades: int = 0) -> List[str]:
    """Pick top N symbols by a metric.

    If 'trades' column exists, you can require min_trades.
    """
    lb = normalize_leaderboard(leaderboard, metric=metric)

    if min_trades and "trades" in lb.columns:
        lb["trades"] = pd.to_numeric(lb["trades"], errors="coerce").fillna(0).astype(int)
        lb = lb[lb["trades"] >= int(min_trades)]

    lb = lb.sort_values(metric, ascending=False)
    return lb["symbol"].head(int(top_n)).tolist()
