from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import pandas as pd


@dataclass
class RegimeDecision:
    profile: str = "NEUTRAL"   # NORMAL | DEFENSIVE | NEUTRAL | OFF
    reason: str = ""
    # router flags (keep it simple for now)
    enable_trend: bool = True
    enable_range_short: bool = True
    enable_range_long: bool = False
    allow_models: Optional[list[str]] = None
    block_models: Optional[list[str]] = None


def _to_utc_ts(x) -> Optional[pd.Timestamp]:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    ts = pd.to_datetime(x, errors="coerce", utc=True)
    if ts is pd.NaT:
        return None
    return ts


def _window_filter(df: pd.DataFrame, window_months: int) -> Tuple[pd.DataFrame, str]:
    """Return last `window_months` months slice using df['timestamp'] (UTC-aware)."""
    if df is None or df.empty:
        return df, "empty"

    d = df.copy()
    d["timestamp"] = pd.to_datetime(d["timestamp"], errors="coerce", utc=True)
    d = d.dropna(subset=["timestamp"])
    if d.empty:
        return d, "bad_ts"

    end_ts = d["timestamp"].max()  # tz-aware UTC
    # IMPORTANT: end_ts is already tz-aware; do NOT pass tz=... into pd.Timestamp
    start_ts = (end_ts - pd.DateOffset(months=int(window_months))).tz_convert("UTC")
    sub = d[(d["timestamp"] >= start_ts) & (d["timestamp"] <= end_ts)].copy()

    label = f"{start_ts.strftime('%Y-%m')}..{end_ts.strftime('%Y-%m')}"
    return sub, label


def decide_profile_from_df(
    trades_df: pd.DataFrame,
    window_months: int = 12,
    min_trades: int = 30,
    total_r_threshold: float = 0.0,
) -> RegimeDecision:
    """Very simple regime decision based on rolling window total_R."""
    sub, label = _window_filter(trades_df, window_months=window_months)
    if sub is None or sub.empty:
        return RegimeDecision(profile="NEUTRAL", reason=f"window_label {label} empty")

    # ensure R exists
    if "R" not in sub.columns:
        # allow fallback to 'r' or 'result_r'
        for alt in ("r", "result_r", "pnl_r"):
            if alt in sub.columns:
                sub["R"] = pd.to_numeric(sub[alt], errors="coerce")
                break

    if "R" not in sub.columns:
        return RegimeDecision(profile="NEUTRAL", reason=f"window_label {label} missing_R")

    sub["R"] = pd.to_numeric(sub["R"], errors="coerce")
    sub = sub.dropna(subset=["R"])
    n = int(len(sub))
    total_r = float(sub["R"].sum()) if n else 0.0

    if n < int(min_trades):
        return RegimeDecision(profile="NEUTRAL", reason=f"window_label {label} trades={n} < {min_trades}")

    if total_r < float(total_r_threshold):
        # defensive: allow only range short in your system; keep as a placeholder
        return RegimeDecision(
            profile="DEFENSIVE",
            enable_trend=False,
            enable_range_short=True,
            enable_range_long=False,
            reason=f"window_label {label} total_R={total_r:.2f} < {total_r_threshold:.2f}",
        )

    return RegimeDecision(
        profile="NORMAL",
        enable_trend=True,
        enable_range_short=True,
        enable_range_long=False,
        reason=f"window_label {label} total_R={total_r:.2f} >= {total_r_threshold:.2f}",
    )


def decide_profiles_by_symbol(
    trades_csv: str,
    window_months: int = 12,
    min_trades: int = 30,
    total_r_threshold: float = 0.0,
) -> Dict[str, RegimeDecision]:
    """Return per-symbol regime decision if `symbol` column exists, else {'ALL': decision}."""
    df = pd.read_csv(trades_csv)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    if "symbol" not in df.columns:
        return {"ALL": decide_profile_from_df(df, window_months, min_trades, total_r_threshold)}

    out: Dict[str, RegimeDecision] = {}
    for sym, g in df.groupby(df["symbol"].astype(str).str.upper().str.strip()):
        out[sym] = decide_profile_from_df(g, window_months, min_trades, total_r_threshold)
    return out
