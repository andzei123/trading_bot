"""Upgrade legacy trades.csv schema (fail-open).

Adds:
  - symbol column (default GLOBAL) if missing
  - optional R column if --add_R is used

Usage:
  python scripts/upgrade_trades_csv.py
  python scripts/upgrade_trades_csv.py --path backtest/journal/trades.csv --add_R
"""

from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def _compute_R(df: pd.DataFrame) -> pd.Series:
    """Best-effort realized R.

    Priority:
      1) If outcome exists:
         WIN -> +rr (if rr exists else +1)
         LOSS -> -1
         BE -> 0
      2) If entry/sl/exit_price/side exist -> compute sign-adjusted PnL / risk
    """
    # 1) outcome-based
    if "outcome" in df.columns:
        out = df["outcome"].astype(str).str.upper()
        if "rr" in df.columns:
            rr = pd.to_numeric(df["rr"], errors="coerce").fillna(0.0)
            R = pd.Series(0.0, index=df.index, dtype=float)
            R[out == "WIN"] = rr[out == "WIN"].astype(float)
            R[out == "LOSS"] = -1.0
            R[out == "BE"] = 0.0
            return R
        else:
            R = pd.Series(0.0, index=df.index, dtype=float)
            R[out == "WIN"] = 1.0
            R[out == "LOSS"] = -1.0
            R[out == "BE"] = 0.0
            return R

    # 2) price-based
    needed = {"entry", "sl", "exit_price", "side"}
    if needed.issubset(set(df.columns)):
        entry = pd.to_numeric(df["entry"], errors="coerce")
        sl = pd.to_numeric(df["sl"], errors="coerce")
        ex = pd.to_numeric(df["exit_price"], errors="coerce")
        side = df["side"].astype(str).str.upper()
        risk = (entry - sl).abs()
        pnl = ex - entry
        pnl = pnl.where(side == "LONG", -pnl)  # for SHORT invert
        R = (pnl / risk.replace(0, pd.NA)).fillna(0.0)
        return pd.to_numeric(R, errors="coerce").fillna(0.0)

    return pd.Series(0.0, index=df.index, dtype=float)


def upgrade(path: str, add_R: bool = False, default_symbol: str = "GLOBAL") -> None:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(str(p))

    # Read robustly (legacy file sometimes has malformed lines)
    df = pd.read_csv(p, engine="python", on_bad_lines="skip")

    changed = False

    if "symbol" not in df.columns:
        df["symbol"] = str(default_symbol).upper()
        changed = True
    else:
        df["symbol"] = df["symbol"].astype(str).fillna(str(default_symbol)).str.upper()
        changed = True  # normalize

    if add_R and "R" not in df.columns:
        df["R"] = _compute_R(df)
        changed = True

    if not changed:
        return

    # Preserve column order: append new cols at the end
    cols = list(df.columns)
    # ensure symbol last if it was inserted in middle by pandas
    if "symbol" in cols:
        cols = [c for c in cols if c != "symbol"] + ["symbol"]
    if add_R and "R" in cols:
        cols = [c for c in cols if c != "R"] + ["R"]

    df = df[cols]

    # Backup original
    backup = p.with_suffix(p.suffix + ".bak")
    if not backup.exists():
        p.replace(backup)
    else:
        # if backup exists, keep original and overwrite
        pass

    # Write upgraded
    df.to_csv(p, index=False)
    print(f"[UPGRADE] wrote {p} rows={len(df)} cols={len(df.columns)} (backup={backup})")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", default="backtest/journal/trades.csv")
    ap.add_argument("--add_R", action="store_true")
    ap.add_argument("--default_symbol", default="GLOBAL")
    args = ap.parse_args()
    upgrade(path=str(args.path), add_R=bool(args.add_R), default_symbol=str(args.default_symbol))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
