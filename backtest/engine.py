#!/usr/bin/env python
"""Simple equity curve generator.

Reads a trades/signals CSV that contains column 'R' (risk multiples).
Optionally uses 'symbol' column if present.

Usage:
  python engine.py <csv_path> <out_dir> [--equity 10000] [--risk 0.01]

Outputs:
  <out_dir>/equity_curve.csv
"""

from __future__ import annotations

import argparse
import os
import sys
import pandas as pd


def run(signals_csv: str, out_dir: str, risk_per_trade: float = 0.01, equity: float = 10000.0) -> str:
    df = pd.read_csv(signals_csv)

    if "R" not in df.columns:
        raise ValueError(f"Input CSV must have column 'R'. Got columns: {list(df.columns)}")

    eq = float(equity)
    rows = []
    for _, r in df.iterrows():
        risk = eq * float(risk_per_trade)
        R = float(r.get("R", 0.0) or 0.0)
        pnl = risk * R
        eq += pnl
        rows.append({
            "timestamp": r.get("timestamp", ""),
            "symbol": r.get("symbol", ""),
            "model": r.get("model", ""),
            "side": r.get("side", ""),
            "R": R,
            "pnl": pnl,
            "equity": eq,
        })

    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "equity_curve.csv")
    pd.DataFrame(rows).to_csv(out_path, index=False)
    return out_path


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate equity curve from trades/signals CSV with an 'R' column.")
    p.add_argument("csv", help="Path to input CSV (must contain column 'R').")
    p.add_argument("out_dir", help="Output directory to write equity_curve.csv into.")
    p.add_argument("--equity", type=float, default=10000.0, help="Starting equity (default: 10000).")
    p.add_argument("--risk", dest="risk_per_trade", type=float, default=0.01, help="Risk per trade as fraction of equity (default: 0.01).")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ns = _parse_args(sys.argv[1:] if argv is None else argv)
    out_path = run(ns.csv, ns.out_dir, risk_per_trade=ns.risk_per_trade, equity=ns.equity)
    print(f"Wrote: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
