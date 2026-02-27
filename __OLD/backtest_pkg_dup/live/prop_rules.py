"""Prop-firm style rule checks.

This module is intentionally dependency-light (pandas only) so it can be reused
from both backtests and live runners.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import pandas as pd


@dataclass
class PropRules:
    """Common prop constraints (configurable).

    All percentages are expressed as fractions (e.g. 0.05 = 5%).
    """

    initial_equity: float = 10_000.0

    # Hard limits
    max_total_loss_pct: float = 0.10  # e.g. 10% max loss (relative to initial)
    max_daily_loss_pct: float = 0.05  # e.g. 5% max daily loss (relative to day start)
    max_drawdown_pct: float = 0.10  # max peak-to-trough DD (relative to peak)

    # Optional targets/requirements
    profit_target_pct: Optional[float] = None  # e.g. 0.08 for 8% target
    min_trading_days: Optional[int] = None


@dataclass
class RuleResult:
    ok: bool
    reason: str
    details: Dict


def _to_utc_series(ts: pd.Series) -> pd.Series:
    """Parse timestamps and force UTC tz-aware."""
    t = pd.to_datetime(ts, errors="coerce", utc=True)
    return t


def simulate_equity_from_trades(
    trades: pd.DataFrame,
    *,
    equity: float,
    risk_per_trade: float,
    time_col: str = "exit_timestamp",
) -> pd.DataFrame:
    """Build an equity curve from a trades DataFrame that contains column `R`.

    Risk model: fixed fraction of current equity per trade.
    """
    if "R" not in trades.columns:
        raise ValueError("trades must contain column 'R'")

    df = trades.copy()
    if time_col not in df.columns:
        # fallback
        if "timestamp" in df.columns:
            time_col = "timestamp"
        else:
            raise ValueError(f"No time column found. Expected '{time_col}' or 'timestamp'.")

    df["_t"] = _to_utc_series(df[time_col])
    df = df.dropna(subset=["_t"]).sort_values("_t")

    eq = float(equity)
    out = []
    for _, r in df.iterrows():
        risk_amt = eq * float(risk_per_trade)
        rr = float(r.get("R", 0.0) or 0.0)
        pnl = risk_amt * rr
        eq += pnl
        out.append(
            {
                "timestamp": r["_t"],
                "symbol": r.get("symbol", ""),
                "R": rr,
                "risk": risk_amt,
                "pnl": pnl,
                "equity": eq,
            }
        )

    curve = pd.DataFrame(out)
    if not curve.empty:
        curve["timestamp"] = pd.to_datetime(curve["timestamp"], utc=True)
    return curve


def compute_daily_summary(equity_curve: pd.DataFrame) -> pd.DataFrame:
    if equity_curve.empty:
        return pd.DataFrame(columns=[
            "day",
            "trades",
            "start_equity",
            "end_equity",
            "day_pnl",
            "day_pnl_pct",
        ])

    d = equity_curve.copy()
    d["day"] = d["timestamp"].dt.floor("D")

    g = d.groupby("day", as_index=False)
    daily = g.agg(
        trades=("R", "size"),
        start_equity=("equity", "first"),
        end_equity=("equity", "last"),
        day_pnl=("pnl", "sum"),
    )
    daily["day_pnl_pct"] = daily["day_pnl"] / daily["start_equity"].replace(0, pd.NA)
    return daily


def compute_max_drawdown(equity_curve: pd.DataFrame) -> Tuple[float, Dict]:
    """Return max drawdown pct (negative value) and diagnostics."""
    if equity_curve.empty:
        return 0.0, {"peak": None, "trough": None, "max_dd_pct": 0.0}

    eq = equity_curve["equity"].astype(float)
    peak = eq.cummax()
    dd = (eq - peak) / peak.replace(0, pd.NA)
    max_dd = float(dd.min())

    idx = dd.idxmin()
    info = {
        "peak_equity": float(peak.loc[idx]) if idx in peak.index else None,
        "trough_equity": float(eq.loc[idx]) if idx in eq.index else None,
        "max_dd_pct": max_dd,
    }
    return max_dd, info


def check_prop_rules(
    equity_curve: pd.DataFrame,
    daily: pd.DataFrame,
    rules: PropRules,
) -> RuleResult:
    """Evaluate equity curve against prop rules."""
    initial = float(rules.initial_equity)
    ok = True
    reasons = []
    details: Dict = {}

    # Max total loss from initial
    if not equity_curve.empty:
        min_eq = float(equity_curve["equity"].min())
        total_loss_pct = (min_eq - initial) / initial
    else:
        min_eq = initial
        total_loss_pct = 0.0
    details["min_equity"] = min_eq
    details["total_loss_pct"] = total_loss_pct
    if total_loss_pct <= -abs(float(rules.max_total_loss_pct)):
        ok = False
        reasons.append(f"max_total_loss breached: {total_loss_pct:.2%} <= -{rules.max_total_loss_pct:.2%}")

    # Max daily loss
    if not daily.empty:
        worst_day = float(daily["day_pnl_pct"].min())
        details["worst_day_pnl_pct"] = worst_day
        if worst_day <= -abs(float(rules.max_daily_loss_pct)):
            ok = False
            reasons.append(f"max_daily_loss breached: {worst_day:.2%} <= -{rules.max_daily_loss_pct:.2%}")
    else:
        details["worst_day_pnl_pct"] = 0.0

    # Max drawdown
    max_dd, dd_info = compute_max_drawdown(equity_curve)
    details.update({"max_drawdown_pct": max_dd, **dd_info})
    if max_dd <= -abs(float(rules.max_drawdown_pct)):
        ok = False
        reasons.append(f"max_drawdown breached: {max_dd:.2%} <= -{rules.max_drawdown_pct:.2%}")

    # Profit target
    if rules.profit_target_pct is not None:
        target_eq = initial * (1.0 + float(rules.profit_target_pct))
        details["profit_target_equity"] = target_eq
        if equity_curve.empty or float(equity_curve["equity"].iloc[-1]) < target_eq:
            ok = False
            reasons.append("profit_target not reached")

    # Min trading days
    if rules.min_trading_days is not None:
        unique_days = int(daily["day"].nunique()) if not daily.empty else 0
        details["trading_days"] = unique_days
        if unique_days < int(rules.min_trading_days):
            ok = False
            reasons.append(f"min_trading_days not met: {unique_days} < {rules.min_trading_days}")

    reason = "; ".join(reasons) if reasons else "OK"
    return RuleResult(ok=ok, reason=reason, details=details)
