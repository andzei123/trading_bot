# performance/walk_forward.py
from __future__ import annotations

import math
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional
import re
# No hardcoded dates. We infer min/max from file.


_SYMBOL_RE = re.compile(r"^[A-Z0-9]{3,15}USDT$")


@dataclass
class TradeRow:
    ts: datetime
    side: str
    entry: float
    exit_price: float
    symbol: str


def _parse_ts(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    # trades.csv uses "YYYY-MM-DD HH:MM:SS"
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception:
        pass
    # fallback (best-effort)
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _safe_float(s: str) -> Optional[float]:
    try:
        return float(str(s).strip())
    except Exception:
        return None


def _is_symbol(token: str) -> bool:
    token = (token or "").strip()
    return bool(_SYMBOL_RE.match(token))


def _iter_trades_csv(path: Path) -> Iterable[TradeRow]:
    """
    Robust parser for two known formats:

    A) 15 cols (header):
       idx,timestamp,reason,side,entry,sl,tp,rr,score,notes,outcome,exit_price,exit_idx,bars_held,symbol

    B) 16 cols (legacy-ish):
       idx,timestamp,symbol,reason,side,entry,sl,tp,rr,score,notes,outcome,exit_price,exit_idx,bars_held,<extra>
       (where notes may contain commas, thus row can exceed 16 fields)

    We DO NOT rely on pandas tokenization because notes can contain commas.
    We always reconstruct 'notes' from the middle part.
    """
    with path.open("r", encoding="utf-8", errors="replace") as f:
        header = f.readline()  # skip
        for line_no, line in enumerate(f, start=2):
            line = line.rstrip("\n")
            if not line.strip():
                continue

            parts = line.split(",")
            # Minimum fields to be even considered
            if len(parts) < 15:
                continue

            # Detect format
            if _is_symbol(parts[2]):
                # Format B: symbol at parts[2], score at parts[9]
                if len(parts) < 16:
                    continue
                ts = _parse_ts(parts[1])
                symbol = parts[2].strip()
                side = parts[4].strip().upper()
                entry = _safe_float(parts[5])
                exit_price = _safe_float(parts[-4])  # outcome, exit_price, exit_idx, bars_held, extra
                # In B, tail is (outcome, exit_price, exit_idx, bars_held, extra)
            else:
                # Format A: symbol at last column
                ts = _parse_ts(parts[1])
                symbol = parts[-1].strip()
                side = parts[3].strip().upper()
                entry = _safe_float(parts[4])
                exit_price = _safe_float(parts[-4])  # outcome, exit_price, exit_idx, bars_held, symbol

            if ts is None or entry is None or exit_price is None:
                continue
            if entry == 0:
                continue
            if side not in ("LONG", "SHORT"):
                continue
            if not symbol:
                symbol = "NA"

            yield TradeRow(ts=ts, side=side, entry=entry, exit_price=exit_price, symbol=symbol)


def _trade_return(tr: TradeRow) -> float:
    # Simple realized return based on entry/exit and side (no leverage assumed).
    if tr.side == "LONG":
        return (tr.exit_price - tr.entry) / tr.entry
    else:  # SHORT
        return (tr.entry - tr.exit_price) / tr.entry


def _sharpe_per_trade(returns: list[float]) -> float:
    n = len(returns)
    if n < 2:
        return float("nan")
    mu = sum(returns) / n
    var = sum((x - mu) ** 2 for x in returns) / (n - 1)
    sd = math.sqrt(var)
    if sd == 0:
        return float("inf") if mu > 0 else 0.0
    # per-trade Sharpe (annualization unknown; use sqrt(n) scaling for comparability)
    return (mu / sd) * math.sqrt(n)


def _max_drawdown_pct(returns: list[float]) -> float:
    # Equity curve starts at 1.0, multiplicative compounding.
    eq = 1.0
    peak = 1.0
    max_dd = 0.0
    for r in returns:
        eq *= (1.0 + r)
        if eq > peak:
            peak = eq
        dd = (peak - eq) / peak
        if dd > max_dd:
            max_dd = dd
    return max_dd * 100.0


def _equity_slope(returns: list[float]) -> float:
    # Slope of equity curve vs trade index (simple linear regression).
    if len(returns) < 2:
        return float("nan")
    eq = 1.0
    y = []
    for r in returns:
        eq *= (1.0 + r)
        y.append(eq)
    n = len(y)
    x_mean = (n - 1) / 2.0
    y_mean = sum(y) / n
    num = sum((i - x_mean) * (y[i] - y_mean) for i in range(n))
    den = sum((i - x_mean) ** 2 for i in range(n))
    if den == 0:
        return 0.0
    return num / den  # equity units per trade


def _split_3_segments(trades: list[TradeRow]) -> list[tuple[datetime, datetime, list[TradeRow]]]:
    # Split by time into 3 contiguous segments with ~equal trade counts (robust).
    trades_sorted = sorted(trades, key=lambda t: t.ts)
    n = len(trades_sorted)
    if n == 0:
        return []
    i1 = n // 3
    i2 = (2 * n) // 3
    segs = [
        trades_sorted[:i1],
        trades_sorted[i1:i2],
        trades_sorted[i2:],
    ]
    out = []
    for seg in segs:
        if not seg:
            continue
        out.append((seg[0].ts, seg[-1].ts, seg))
    return out


def run_walk_forward(trades_path: str) -> bool:
    path = Path(trades_path)
    if not path.exists():
        print(f"[WALK_FORWARD] RESULT FAIL reason=FILE_NOT_FOUND path={path}")
        return False

    trades = list(_iter_trades_csv(path))
    if len(trades) < 10:
        print(f"[WALK_FORWARD] RESULT FAIL reason=TOO_FEW_TRADES n={len(trades)} path={path}")
        return False

    segs = _split_3_segments(trades)
    if len(segs) != 3:
        print(f"[WALK_FORWARD] RESULT FAIL reason=BAD_SPLIT segments={len(segs)} n={len(trades)}")
        return False

    fails: list[str] = []
    for (a, b, seg) in segs:
        rets = [_trade_return(t) for t in seg]
        sharpe = _sharpe_per_trade(rets)
        slope = _equity_slope(rets)
        dd = _max_drawdown_pct(rets)
        n = len(rets)
        print(f"[WALK_FORWARD] {a.date()}..{b.date()} Sharpe={sharpe:.2f} slope={slope:.6f} DD={dd:.2f}% n={n}")

        if sharpe < 1.0:
            fails.append(f"SHARPE<1.0 ({a.date()}..{b.date()}={sharpe:.2f})")
        if dd > 25.0:
            fails.append(f"DD>25% ({a.date()}..{b.date()}={dd:.2f}%)")

    if fails:
        print("[WALK_FORWARD] RESULT FAIL reason=" + ";".join(fails))
        return False

    print("[WALK_FORWARD] RESULT PASS")
    return True


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python performance/walk_forward.py <path/to/trades.csv>")
        raise SystemExit(2)
    ok = run_walk_forward(sys.argv[1])
    raise SystemExit(0 if ok else 1)
