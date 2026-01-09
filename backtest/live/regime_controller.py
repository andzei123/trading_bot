from __future__ import annotations

import pandas as pd
from dataclasses import dataclass
from pathlib import Path


@dataclass
class RegimeDecision:
    profile: str  # "OFFENSIVE" | "NEUTRAL" | "DEFENSIVE"
    enable_trend: bool
    enable_range_short: bool
    enable_range_long: bool
    reason: str


def _monthly_total_r(trades_csv: str) -> pd.Series:
    p = Path(trades_csv)
    if not p.exists():
        return pd.Series(dtype=float)

    df = pd.read_csv(trades_csv)
    if df.empty or ("timestamp" not in df.columns) or ("R" not in df.columns):
        return pd.Series(dtype=float)

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df = df.dropna(subset=["timestamp"])
    df["ym"] = df["timestamp"].dt.to_period("M").astype(str)

    return df.groupby("ym")["R"].sum().sort_index()


def decide_profile_from_performance(
    trades_csv: str,
    offensive_thr: float = 5.0,
    defensive_thr: float = 0.0,
) -> RegimeDecision:
    s = _monthly_total_r(trades_csv)

    if s.empty:
        return RegimeDecision(
            profile="NEUTRAL",
            enable_trend=True,
            enable_range_short=True,
            enable_range_long=False,
            reason="no performance history -> default NEUTRAL",
        )

    last_ym = s.index[-1]
    last_r = float(s.iloc[-1])

    if last_r < defensive_thr:
        return RegimeDecision(
            profile="DEFENSIVE",
            enable_trend=True,
            enable_range_short=True,
            enable_range_long=False,
            reason=f"last_month {last_ym} total_R={last_r:.2f} < {defensive_thr}",
        )

    if last_r > offensive_thr:
        return RegimeDecision(
            profile="OFFENSIVE",
            enable_trend=True,
            enable_range_short=True,
            enable_range_long=True,
            reason=f"last_month {last_ym} total_R={last_r:.2f} > {offensive_thr}",
        )

    return RegimeDecision(
        profile="NEUTRAL",
        enable_trend=True,
        enable_range_short=True,
        enable_range_long=False,
        reason=f"last_month {last_ym} total_R={last_r:.2f} in [{defensive_thr},{offensive_thr}]",
    )
