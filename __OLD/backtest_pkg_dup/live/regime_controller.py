# backtest/live/regime_controller.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import pandas as pd


@dataclass
class RegimeDecision:
    profile: str  # "NORMAL" | "DEFENSIVE" | "OFF" | "NEUTRAL"
    reason: str

    # --- backward compatible flags (older live_signal_runner expects these) ---
    enable_trend: bool = True
    enable_range: bool = True
    enable_range_long: bool = False   # your design: range longs usually disabled
    enable_range_short: bool = True

    # --- new allow/block filters (preferred) ---
    allow_models: Optional[List[str]] = None     # None => allow all
    block_models: Optional[List[str]] = None

    allow_sides: Optional[List[str]] = None
    block_sides: Optional[List[str]] = None

    allow_phases: Optional[List[str]] = None
    block_phases: Optional[List[str]] = None

    max_positions: int = 3


def _read_trades_csv(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()

    df = pd.read_csv(p)

    if "timestamp" not in df.columns or "R" not in df.columns:
        return pd.DataFrame()

    for col in ["model", "side", "phase", "ctx_sub_label", "symbol"]:
        if col not in df.columns:
            df[col] = ""

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df = df.dropna(subset=["timestamp"])

    df["R"] = pd.to_numeric(df["R"], errors="coerce").fillna(0.0)

    # month key WITHOUT tz warning: remove tz -> to_period
    ts = df["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)
    df["year_month"] = ts.dt.to_period("M").astype(str)

    df["model"] = df["model"].astype(str).str.upper()
    df["side"] = df["side"].astype(str).str.upper()
    df["phase"] = df["phase"].astype(str).str.upper()

    return df


def decide_profile_from_performance(
    trades_csv: str = "backtest/journal/exports_trades/trades_simulated.csv",
    perf_csv: str | None = None,
    min_trades_month: int = 20,
    window_months: int = 3,
    min_trades_window: int | None = None,
    defensive_threshold_R: float = 0.0,
    allow_range_in_defensive: bool = True,
) -> RegimeDecision:
    """
    Auto regime switch based on LAST MONTH attribution.
    Backward compatible: returns enable_trend/enable_range flags too.
    """

    # Backward/forward compatible alias: prefer perf_csv if provided
    if perf_csv is not None and str(perf_csv).strip():
        trades_csv = str(perf_csv)

    df = _read_trades_csv(trades_csv)
    if df.empty:
        return RegimeDecision(
            profile="NEUTRAL",
            reason=f"no history -> default NEUTRAL | src={trades_csv}",
            enable_trend=False,
            enable_range=False,
            enable_range_long=False,
            enable_range_short=False,
            max_positions=1,
        )

    months = sorted(df["year_month"].unique())
    w = max(1, int(window_months))
    max_ts = df["timestamp"].max()
    cutoff = max_ts - pd.DateOffset(months=w)
    d = df[df["timestamp"] >= cutoff].copy()

    # reporting label
    win_months = sorted(d["year_month"].unique()) if not d.empty else []
    window_label = f"{win_months[0]}..{win_months[-1]}" if win_months else "UNKNOWN_WINDOW"

    trades_n = len(d)
    total_R = float(d["R"].sum())

    # not enough data -> conservative neutral
    min_trades_req = int(min_trades_window) if (min_trades_window is not None) else int(min_trades_month)

    if trades_n < int(min_trades_req):
        return RegimeDecision(
            profile="NEUTRAL",
            reason=f"window_label {window_label} trades={trades_n} < {min_trades_req} -> NEUTRAL | total_R={total_R:.2f} | src={trades_csv}",
            enable_trend=False,
            enable_range=False,
            enable_range_long=False,
            enable_range_short=False,
            max_positions=1,
        )

    by_model = d.groupby("model")["R"].sum().sort_values()
    losing_models = [m for m, r in by_model.items() if float(r) < 0]

    # define range models (extend anytime)
    range_models = [m for m in d["model"].unique() if "RANGE" in m]

    # ---------------- DEFENSIVE / OFF ----------------
    if total_R < float(defensive_threshold_R):
        # prefer blocking trend model(s) that lost money (typically TDP_REENTRY)
        block_trend = []
        for m in losing_models:
            if ("TDP" in m) or ("REENTRY" in m):
                block_trend.append(m)

        # safe fallback: if negative month and TDP exists -> block it
        if not block_trend and ("TDP_REENTRY" in d["model"].unique()):
            block_trend = ["TDP_REENTRY"]

        if allow_range_in_defensive and range_models:
            return RegimeDecision(
                profile="DEFENSIVE",
                reason=f"window_label {window_label} total_R={total_R:.2f} < {defensive_threshold_R:.2f} | block_trend={block_trend} | allow_range={range_models} | src={trades_csv}",
                enable_trend=False,
                enable_range=True,
                enable_range_long=False,   # your design
                enable_range_short=True,
                allow_models=[m.upper() for m in range_models],
                block_models=[m.upper() for m in block_trend] if block_trend else None,
                max_positions=1,
            )

        # no safe range -> OFF
        return RegimeDecision(
            profile="OFF",
            reason=f"window_label {window_label} total_R={total_R:.2f} < {defensive_threshold_R:.2f} | no safe modules -> OFF | src={trades_csv}",
            enable_trend=False,
            enable_range=False,
            enable_range_long=False,
            enable_range_short=False,
            allow_models=[],
            max_positions=0,
        )

    # ---------------- NORMAL ----------------
    return RegimeDecision(
        profile="NORMAL",
        reason=f"window_label {window_label} total_R={total_R:.2f} >= {defensive_threshold_R:.2f} | NORMAL | src={trades_csv}",
        enable_trend=True,
        enable_range=True,
        enable_range_long=False,   # keep your current policy unless you turn it on later
        enable_range_short=True,
        max_positions=3,
    )
