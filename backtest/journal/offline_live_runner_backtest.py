from __future__ import annotations

"""Offline "live runner" backtest engine.

DEV4 PHASE-1 — 1:1 pipeline.

This runner replays historical candles bar-by-bar and calls the same
production decision layer as live:

  backtest.live_pipeline.pipeline_core.run_pipeline_once

Only differences vs live:
  - Data source: CSV candles directory
  - Execution: backtest.engine.execution.ExecutionSimulator

Outputs (required):
  - backtest/journal/trades.csv (must include symbol column)
  - backtest/journal/exports_live/equity_curve.csv
  - backtest/journal/exports_live/symbol_performance.csv
"""

import argparse
import csv
from pathlib import Path
from typing import Dict, List

import pandas as pd

from backtest.engine.execution import ExecutionSimulator
from backtest.live_pipeline.pipeline_core import run_pipeline_once
from backtest.metrics.equity_curve_tracker import update_equity_curve_from_trades
from backtest.metrics.symbol_performance_tracker import update_symbol_performance
from backtest.portfolio.portfolio_exposure import load_portfolio_exposure


def _parse_dt(s: str) -> pd.Timestamp:
    """Accept YYYY-MM-DD or ISO; always returns UTC timestamp."""
    ts = pd.to_datetime(s, utc=True, errors="coerce")
    if pd.isna(ts):
        raise ValueError(f"Bad datetime: {s}")
    return ts


def _load_symbol_candles(candles_dir: Path, symbol: str) -> pd.DataFrame:
    """Load candles for one symbol.

    Expected file patterns (first match wins):
      - {symbol}.csv
      - {symbol}_15m.csv
      - {symbol}_15.csv

    Required columns: timestamp, open, high, low, close
    """
    candidates = [
        candles_dir / f"{symbol}.csv",
        candles_dir / f"{symbol}_15m.csv",
        candles_dir / f"{symbol}_15.csv",
    ]
    p = None
    for c in candidates:
        if c.exists():
            p = c
            break
    if p is None:
        raise FileNotFoundError(f"No candles CSV for {symbol} in {candles_dir}")

    df = pd.read_csv(p)

    # Normalize timestamp column name
    if "timestamp" not in df.columns:
        for alt in ("time", "date", "ts"):
            if alt in df.columns:
                df = df.rename(columns={alt: "timestamp"})
                break

    required = {"timestamp", "open", "high", "low", "close"}
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{p} missing columns: {missing}")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    # --- ATR(14) + ATR% (required for phase + entry logic) ---
    # Keep NaNs for warmup (rolling), but ensure columns exist.
    try:
        high_low = df["high"] - df["low"]
        high_close = (df["high"] - df["close"].shift()).abs()
        low_close = (df["low"] - df["close"].shift()).abs()

        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = tr.rolling(14).mean()

        df["atr"] = atr
        df["atr_pct"] = atr / df["close"]
    except Exception:
        df["atr"] = 0.0
        df["atr_pct"] = 0.0

    return df


def _ensure_trades_header(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size > 0:
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "idx",
                "timestamp",
                "reason",
                "side",
                "entry",
                "sl",
                "tp",
                "rr",
                "score",
                "notes",
                "outcome",
                "exit_price",
                "exit_idx",
                "bars_held",
                "exit_timestamp",
                "symbol",
            ]
        )


def _append_trade(path: Path, row: Dict) -> None:
    """
    Append 1 trade row with stable schema.
    Guarantees column order and prevents column shift.
    """

    FIELDNAMES = [
        "idx",
        "timestamp",
        "reason",
        "side",
        "entry",
        "sl",
        "tp",
        "rr",
        "score",
        "notes",
        "outcome",
        "exit_price",
        "exit_idx",
        "bars_held",
        "exit_timestamp",
        "symbol",
    ]

    file_exists = path.exists()

    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)

        # write header only once
        if not file_exists:
            writer.writeheader()

        clean_row = {
            "idx": row.get("idx"),
            "timestamp": row.get("timestamp"),
            "reason": row.get("reason"),
            "side": row.get("side"),
            "entry": row.get("entry"),
            "sl": row.get("sl"),
            "tp": row.get("tp"),
            "rr": row.get("rr"),
            "score": row.get("score"),
            "notes": row.get("notes"),
            "outcome": row.get("outcome"),
            "exit_price": row.get("exit_price"),
            "exit_idx": row.get("exit_idx"),
            "bars_held": row.get("bars_held"),
            "exit_timestamp": row.get("exit_timestamp") or "",
            "symbol": row.get("symbol"),
        }

        writer.writerow(clean_row)

def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Offline bar-by-bar backtest that reuses the live decision pipeline (1:1)."
    )

    ap.add_argument(
        "--symbols",
        required=False,
        default="BTCUSDT",
        help="Comma-separated symbols, e.g. BTCUSDT,ETHUSDT (default: BTCUSDT)",
    )

    # --- Gate compatibility: support both --from/--to and --start/--end ---
    ap.add_argument("--from", dest="dt_from", required=False, help="Start date/time (UTC), e.g. 2025-01-01")
    ap.add_argument("--start", dest="dt_from", required=False, help="Alias for --from (UTC), e.g. 2025-01-01")

    ap.add_argument("--to", dest="dt_to", required=False, help="End date/time (UTC), e.g. 2025-03-01")
    ap.add_argument("--end", dest="dt_to", required=False, help="Alias for --to (UTC), e.g. 2025-01-02")

    ap.add_argument(
        "--candles_dir",
        required=False,
        default="backtest/data",
        help="Directory with per-symbol candle CSVs (default: backtest/data)",
    )

    ap.add_argument("--macro_root", default="data/macro", help="Macro root (reserved; fail-open)")
    ap.add_argument(
        "--portfolio_state",
        default="backtest/journal/exports_live/portfolio_state.json",
        help="Portfolio exposure JSON (read-only)",
    )
    ap.add_argument(
        "--out_trades",
        default="backtest/journal/trades.csv",
        help="Output trades.csv (will be appended/created)",
    )
    ap.add_argument(
        "--debug_force_entries",
        action="store_true",
        help="Force synthetic LONG/SHORT entries when no setups matched (debug only).",
    )
    ap.add_argument("--window", type=int, default=200, help="Candle window length passed to pipeline")
    ap.add_argument("--bybit_interval", type=int, default=15, help="Candle interval minutes")
    ap.add_argument("--debug", action="store_true", help="Verbose pipeline logs")
    args = ap.parse_args(argv)

    # Enforce date range presence (but via either alias)
    if not args.dt_from or not args.dt_to:
        ap.error("Missing date range: provide --from/--to or --start/--end")

    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
    if not symbols:
        raise SystemExit("--symbols empty")

    dt_from = _parse_dt(args.dt_from)
    dt_to = _parse_dt(args.dt_to)
    if dt_to <= dt_from:
        raise SystemExit("--to/--end must be > --from/--start")

    candles_dir = Path(args.candles_dir)
    out_trades = Path(args.out_trades)
    pf_path = Path(args.portfolio_state)

    _ensure_trades_header(out_trades)

    candles_by_symbol: Dict[str, pd.DataFrame] = {s: _load_symbol_candles(candles_dir, s) for s in symbols}

    timelines = []
    for s, df in candles_by_symbol.items():
        sub = df[(df["timestamp"] >= dt_from) & (df["timestamp"] <= dt_to)].copy()
        timelines.append(sub["timestamp"])
        candles_by_symbol[s] = sub.reset_index(drop=True)

    if not timelines or all(t.empty for t in timelines):
        print("[OFFLINE] no candles in requested range")
        return 0

    all_ts = pd.Index(sorted(set().union(*[set(t.tolist()) for t in timelines if not t.empty])))

    trade_idx = 0

    for ts in all_ts:
        portfolio_state = load_portfolio_exposure(pf_path)

        for sym in symbols:
            df_full = candles_by_symbol[sym]
            if df_full.empty:
                continue

            df_up_to = df_full[df_full["timestamp"] <= ts]
            if df_up_to.empty:
                continue
            window = df_up_to.tail(int(args.window)).copy().reset_index(drop=True)

            # Compat ctx keys: pipeline variants may check different flags
            force_flag = bool(args.debug_force_entries)
            ctx = {
                "latest_ts": ts,
                "bybit_interval": int(args.bybit_interval),
                "macro_bias": "NEUTRAL",
                "debug": bool(args.debug),

                "debug_force_entries": force_flag,
                "force_entries": force_flag,
                "debug_entry_force": force_flag,
                "DEBUG_FORCE_ENTRIES": force_flag,
            }

            df_e = run_pipeline_once(
                symbol=sym,
                candles_df=window,
                ctx=ctx,
                portfolio_state=portfolio_state,
                debug=bool(args.debug),
            )

            if df_e is None or df_e.empty:
                continue

            sim = ExecutionSimulator(df_full)

            for _, r in df_e.iterrows():
                side = str(r.get("side", "")).upper()
                entry = float(r.get("entry"))
                sl = float(r.get("sl"))
                tp = float(r.get("tp"))

                setup_ts = pd.to_datetime(r.get("timestamp"), utc=True, errors="coerce")
                if pd.isna(setup_ts):
                    continue

                try:
                    entry_idx = int(df_full.index[df_full["timestamp"] == setup_ts][0])
                except Exception:
                    entry_idx = int(df_full[df_full["timestamp"] <= setup_ts].index.max())

                res = sim.simulate(entry_idx=entry_idx, side=side, entry=entry, sl=sl, tp=tp)

                exit_ts_str = ""
                try:
                    if res is not None and res.exit_idx is not None:
                        exi = int(res.exit_idx)
                        if 0 <= exi < len(df_full) and "timestamp" in df_full.columns:
                            exit_ts = df_full["timestamp"].iloc[exi]
                            if isinstance(exit_ts, pd.Timestamp) and not pd.isna(exit_ts):
                                exit_ts_str = exit_ts.isoformat()
                except Exception:
                    exit_ts_str = ""

                rr = float(r.get("rr", 0.0) or 0.0)
                score = float(r.get("score", rr) or rr)

                _append_trade(
                    out_trades,
                    {
                        "idx": trade_idx,
                        "timestamp": setup_ts.isoformat(),
                        "reason": str(r.get("model", "")),
                        "side": side,
                        "entry": entry,
                        "sl": sl,
                        "tp": tp,
                        "rr": rr,
                        "score": score,
                        "notes": "offline_live_runner",
                        "outcome": getattr(res, "outcome", "") if res is not None else "",
                        "exit_price": getattr(res, "exit_price", "") if res is not None else "",
                        "exit_idx": getattr(res, "exit_idx", "") if res is not None else "",
                        "bars_held": getattr(res, "bars_held", "") if res is not None else "",
                        "exit_timestamp": exit_ts_str,
                        "symbol": sym,
                    },
                )
                trade_idx += 1

    try:
        update_equity_curve_from_trades(
            trades_csv=str(out_trades),
            out_csv="backtest/journal/exports_live/equity_curve.csv",
            initial_equity=10_000.0,
            window_trades=60,
        )
    except Exception as e:
        print(f"[OFFLINE][EQUITY][WARN] {repr(e)}")

    try:
        update_symbol_performance(
            trades_csv=str(out_trades),
            out_csv="backtest/journal/exports_live/symbol_performance.csv",
        )
    except Exception as e:
        print(f"[OFFLINE][SYMBOL_PERF][WARN] {repr(e)}")

    print(f"[OFFLINE] done trades_appended={trade_idx}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
