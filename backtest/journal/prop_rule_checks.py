#!/usr/bin/env python
"""Generate equity curve + prop-firm style rule checks from trades CSV.

Works with your `trades_simulated.csv` (must contain at least: timestamp, symbol, R).
If `exit_timestamp` exists, it will be used by default (more realistic).

Outputs into out_dir:
- equity_curve.csv
- daily_summary.csv
- prop_rules.json
- prop_rules.txt

Example:
  python -m backtest.journal.prop_rule_checks \
    --trades backtest/journal/exports_trades/trades_simulated.csv \
    --out_dir reports/prop_rules \
    --equity 10000 --risk 0.01 \
    --preset ftmo
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from backtest.live.prop_rules import PropRules, check_prop_rules


def _pct_to_frac(x: float | None) -> float | None:
    """Normalize CLI percent inputs to internal fraction format.

    Internally we store rule limits as fractions (0.05 = 5%).
    But from CLI it's natural to type "5" for 5%.
    So we auto-normalize:
      - if x > 1 -> treat as percent and divide by 100
      - else -> assume already a fraction
    """
    if x is None:
        return None
    return (x / 100.0) if x > 1.0 else float(x)


def _parse_ts(s: pd.Series) -> pd.Series:
    ts = pd.to_datetime(s, utc=True, errors="coerce")
    # make sure tz-aware
    if getattr(ts.dt, "tz", None) is None:
        ts = ts.dt.tz_localize("UTC")
    return ts


def load_trades(trades_csv: str, use_exit_ts: bool = True) -> pd.DataFrame:
    df = pd.read_csv(trades_csv)
    if df.empty:
        return df

    # figure timestamp column
    ts_col = "exit_timestamp" if use_exit_ts and "exit_timestamp" in df.columns else "timestamp"
    if ts_col not in df.columns:
        raise ValueError(f"trades csv must contain 'timestamp' (and optionally 'exit_timestamp'), got columns={list(df.columns)}")

    df["_ts"] = _parse_ts(df[ts_col])
    df = df.dropna(subset=["_ts"])

    # normalize R
    if "R" not in df.columns:
        raise ValueError("trades csv must contain column 'R'")
    df["R"] = pd.to_numeric(df["R"], errors="coerce")
    df = df.dropna(subset=["R"])

    if "symbol" not in df.columns:
        # backward compatibility
        df["symbol"] = df.get("bybit_symbol", "UNKNOWN")

    df = df.sort_values("_ts").reset_index(drop=True)
    return df


def simulate_equity(df: pd.DataFrame, initial_equity: float, risk_frac: float) -> pd.DataFrame:
    """Convert trade-level R into an equity curve.

    Important: we prepend an *initial* point so downstream daily PnL /
    drawdown calculations are correct.
    """
    equity = float(initial_equity)
    rows: list[dict] = []

    # Initial point (same timestamp as first trade to anchor the day).
    if not df.empty:
        ts0 = df["_ts"].iloc[0]
        rows.append(
            {
                "i": -1,
                "is_trade": 0,
                "timestamp": ts0.isoformat(),
                "symbol": "",
                "side": "",
                "model": "",
                "R": 0.0,
                "risk_usd": 0.0,
                "pnl": 0.0,
                "equity": equity,
            }
        )

    for i, r in df.iterrows():
        risk_usd = equity * float(risk_frac)
        pnl = risk_usd * float(r["R"])
        equity = equity + pnl
        rows.append(
            {
                "i": int(i),
                "is_trade": 1,
                "timestamp": r["_ts"].isoformat(),
                "symbol": r.get("symbol", "UNKNOWN"),
                "side": r.get("side", ""),
                "model": r.get("model", ""),
                "R": float(r["R"]),
                "risk_usd": float(risk_usd),
                "pnl": float(pnl),
                "equity": float(equity),
            }
        )

    return pd.DataFrame(rows)


def daily_summary(equity_curve: pd.DataFrame) -> pd.DataFrame:
    if equity_curve.empty:
        return pd.DataFrame(columns=["day", "start_equity", "end_equity", "day_pnl", "day_pnl_pct", "trades"])

    d = equity_curve.copy()
    d["ts"] = pd.to_datetime(d["timestamp"], utc=True)
    d["day"] = d["ts"].dt.to_period("D").astype(str)

    grp = d.groupby("day", as_index=False)
    out = grp.agg(
        start_equity=("equity", "first"),
        end_equity=("equity", "last"),
        trades=("is_trade", "sum"),
    )
    out["day_pnl"] = out["end_equity"] - out["start_equity"]
    out["day_pnl_pct"] = out["day_pnl"] / out["start_equity"].replace(0, pd.NA)
    return out


def preset_rules(name: str, initial_equity: float) -> PropRules:
    n = name.lower().strip()

    # NOTE: presets are *generic* and meant to be edited per your prop.
    if n in {"ftmo", "funded", "ftmo_like"}:
        return PropRules(
            initial_equity=initial_equity,
            max_daily_loss_pct=0.05,
            max_total_loss_pct=0.10,
            max_drawdown_pct=0.10,
            profit_target_pct=None,
            min_trading_days=None,
        )

    if n in {"mff", "thefundedtrader", "tff", "generic"}:
        return PropRules(
            initial_equity=initial_equity,
            max_daily_loss_pct=0.05,
            max_total_loss_pct=0.10,
            max_drawdown_pct=0.10,
            profit_target_pct=None,
            min_trading_days=None,
        )

    if n in {"strict"}:
        return PropRules(
            initial_equity=initial_equity,
            max_daily_loss_pct=0.03,
            max_total_loss_pct=0.06,
            max_drawdown_pct=0.06,
            profit_target_pct=None,
            min_trading_days=None,
        )

    raise ValueError(f"Unknown preset: {name}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Equity curve + prop-firm rule checks")
    ap.add_argument("--trades", required=True, help="CSV with trades (needs timestamp, symbol, R)")
    ap.add_argument("--out_dir", required=True, help="Output directory")
    ap.add_argument("--equity", type=float, default=10000.0, help="Initial equity")
    ap.add_argument("--risk", type=float, default=0.01, help="Risk per trade as fraction of current equity")
    ap.add_argument("--use_entry_ts", action="store_true", help="Use entry timestamp instead of exit_timestamp")

    # Rules
    ap.add_argument("--preset", default="ftmo", help="Rule preset: ftmo | generic | strict")
    ap.add_argument("--max_daily_loss_pct", type=float, default=None)
    ap.add_argument("--max_total_loss_pct", type=float, default=None)
    ap.add_argument("--max_drawdown_pct", type=float, default=None)
    ap.add_argument("--profit_target_pct", type=float, default=None)
    ap.add_argument("--min_trading_days", type=int, default=None)

    args = ap.parse_args()

    trades = load_trades(args.trades, use_exit_ts=not args.use_entry_ts)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    eq = simulate_equity(trades, initial_equity=args.equity, risk_frac=args.risk)
    daily = daily_summary(eq)

    rules = preset_rules(args.preset, initial_equity=args.equity)
    # override if user provided
    # CLI takes percent-style numbers ("5" == 5%), while internal rules use fractions (0.05).
    if args.max_daily_loss_pct is not None:
        rules.max_daily_loss_pct = float(_pct_to_frac(float(args.max_daily_loss_pct)))
    if args.max_total_loss_pct is not None:
        rules.max_total_loss_pct = float(_pct_to_frac(float(args.max_total_loss_pct)))
    if args.max_drawdown_pct is not None:
        rules.max_drawdown_pct = float(_pct_to_frac(float(args.max_drawdown_pct)))
    if args.profit_target_pct is not None:
        rules.profit_target_pct = float(_pct_to_frac(float(args.profit_target_pct)))
    if args.min_trading_days is not None:
        rules.min_trading_days = int(args.min_trading_days)

    res = check_prop_rules(eq, daily, rules)

    eq_path = out_dir / "equity_curve.csv"
    daily_path = out_dir / "daily_summary.csv"
    json_path = out_dir / "prop_rules.json"
    txt_path = out_dir / "prop_rules.txt"

    eq.to_csv(eq_path, index=False)
    daily.to_csv(daily_path, index=False)

    payload = {
        "ok": bool(res.ok),
        "reason": res.reason,
        "rules": {
            "initial_equity": rules.initial_equity,
            "max_daily_loss_pct": rules.max_daily_loss_pct,
            "max_total_loss_pct": rules.max_total_loss_pct,
            "max_drawdown_pct": rules.max_drawdown_pct,
            "profit_target_pct": rules.profit_target_pct,
            "min_trading_days": rules.min_trading_days,
        },
        "details": res.details,
    }
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = [
        f"OK: {payload['ok']}",
        f"Reason: {payload['reason']}",
        "",
        "Rules:",
        f"  initial_equity={rules.initial_equity}",
        f"  max_daily_loss_pct={rules.max_daily_loss_pct}",
        f"  max_total_loss_pct={rules.max_total_loss_pct}",
        f"  max_drawdown_pct={rules.max_drawdown_pct}",
        f"  profit_target_pct={rules.profit_target_pct}",
        f"  min_trading_days={rules.min_trading_days}",
        "",
        "Diagnostics:",
    ]
    for k, v in payload["details"].items():
        lines.append(f"  {k}: {v}")
    txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"Wrote: {eq_path}")
    print(f"Wrote: {daily_path}")
    print(f"Wrote: {json_path}")
    print(f"Wrote: {txt_path}")
    print(f"RESULT: ok={payload['ok']} reason={payload['reason']}")
    return 0 if payload["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
