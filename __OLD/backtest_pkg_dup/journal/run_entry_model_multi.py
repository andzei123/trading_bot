
"""
Multi-symbol backtest runner (MVP)

Generates entries via entry_model.generate_entries_from_ctx() and simulates TP/SL outcomes on OHLC candles.
Outputs:
- exports_trades/entries_generated_<SYMBOL>.csv
- exports_trades/trades_simulated_<SYMBOL>.csv
- exports_trades/trades_simulated_multi.csv
- exports_trades/summary_by_symbol.csv

NOTE: This is a simple TP/SL simulator (no break-even / partials).
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import numpy as np
import pandas as pd

from backtest.journal import filter_trades as ft
from backtest.engine import entry_model as em


def _parse_symbols(s: str) -> List[str]:
    parts = [x.strip().upper() for x in (s or "").split(",")]
    return [p for p in parts if p]


def _ensure_utc(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    return df


def _simulate_trades_simple(candles: pd.DataFrame, entries: pd.DataFrame, max_hold_candles: int = 2000) -> pd.DataFrame:
    """
    For each entry:
      - walk forward candle-by-candle up to max_hold_candles
      - TP/SL hit when candle high/low crosses the level
      - if both TP and SL in same candle -> conservative (assume SL hit first)
    Returns trades df with:
      timestamp, symbol, model, side, entry, sl, tp, exit_time, exit_price, outcome, R
    """
    if entries is None or entries.empty:
        return entries.iloc[0:0].copy()

    c = _ensure_utc(candles)
    e = _ensure_utc(entries)

    # index candles by timestamp for fast slicing
    c = c.set_index("timestamp", drop=False)

    out: List[Dict[str, Any]] = []

    for _, row in e.iterrows():
        ts = row["timestamp"]
        if ts not in c.index:
            # find the next candle
            try:
                pos = c.index.searchsorted(ts)
                if pos >= len(c.index):
                    continue
                ts0 = c.index[pos]
            except Exception:
                continue
        else:
            ts0 = ts

        side = str(row.get("side", "")).upper()
        entry = float(row.get("entry"))
        sl = float(row.get("sl"))
        tp = float(row.get("tp"))

        if not np.isfinite(entry) or not np.isfinite(sl) or not np.isfinite(tp):
            continue

        risk = (entry - sl) if side == "LONG" else (sl - entry)
        if risk <= 0:
            # invalid geometry
            continue

        # walk forward
        idx0 = c.index.get_loc(ts0)
        idx1 = min(len(c.index), idx0 + int(max_hold_candles))

        exit_time = None
        exit_price = None
        outcome = "NO_HIT"

        for i in range(idx0, idx1):
            bar = c.iloc[i]
            hi = float(bar["high"])
            lo = float(bar["low"])

            if side == "LONG":
                hit_sl = lo <= sl
                hit_tp = hi >= tp
                if hit_sl or hit_tp:
                    # conservative tie-break: SL first if both
                    if hit_sl:
                        outcome = "SL"
                        exit_price = sl
                    else:
                        outcome = "TP"
                        exit_price = tp
                    exit_time = bar["timestamp"]
                    break
            elif side == "SHORT":
                hit_sl = hi >= sl
                hit_tp = lo <= tp
                if hit_sl or hit_tp:
                    if hit_sl:
                        outcome = "SL"
                        exit_price = sl
                    else:
                        outcome = "TP"
                        exit_price = tp
                    exit_time = bar["timestamp"]
                    break
            else:
                break

        if exit_time is None:
            exit_time = c.iloc[idx1 - 1]["timestamp"]
            exit_price = float(c.iloc[idx1 - 1]["close"])

        # R calc (signed by side)
        if side == "LONG":
            R = (exit_price - entry) / risk
        else:
            R = (entry - exit_price) / risk

        out.append({
            "timestamp": ts,
            "symbol": str(row.get("symbol", "")).upper(),
            "model": str(row.get("model", "")).upper(),
            "side": side,
            "entry": entry,
            "sl": sl,
            "tp": tp,
            "exit_time": exit_time,
            "exit_price": exit_price,
            "outcome": outcome,
            "R": float(R),
            "ctx_sub_label": row.get("ctx_sub_label"),
            "phase": row.get("phase"),
            "regime": row.get("regime"),
            "trend_dir": row.get("trend_dir"),
            "trend_strength": row.get("trend_strength"),
            "atr_pct": row.get("atr_pct"),
        })

    return pd.DataFrame(out)


def _wl_summary(sim: pd.DataFrame) -> Dict[str, Any]:
    if sim is None or sim.empty:
        return dict(total=0, win=0, loss=0, no_hit=0, winrate=0.0, expectancy=0.0, total_R=0.0, maxDD_R=0.0)

    s = sim.copy()
    s["R"] = pd.to_numeric(s["R"], errors="coerce")
    s = s.dropna(subset=["R"])
    total = len(s)
    win = int((s["R"] > 0).sum())
    loss = int((s["R"] < 0).sum())
    no_hit = int((s["outcome"] == "NO_HIT").sum())
    winrate = (win / (win + loss) * 100.0) if (win + loss) else 0.0
    expectancy = float(s["R"].mean()) if total else 0.0
    total_R = float(s["R"].sum()) if total else 0.0

    # equity curve in R and max drawdown in R
    eq = s.sort_values("timestamp")["R"].cumsum()
    dd = eq - eq.cummax()
    maxDD_R = float(dd.min()) if len(dd) else 0.0

    return dict(total=total, win=win, loss=loss, no_hit=no_hit, winrate=winrate, expectancy=expectancy, total_R=total_R, maxDD_R=maxDD_R)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--symbols", type=str, required=True, help="Comma separated, e.g. BTCUSDT,ETHUSDT,SOLUSDT")
    p.add_argument("--source", type=str, default="bybit", choices=["bybit", "csv"])
    p.add_argument("--bybit_category", type=str, default="linear")
    p.add_argument("--bybit_interval", type=str, default="30")
    p.add_argument("--bybit_candles", type=int, default=3000)

    p.add_argument("--mode", type=str, default="combined")
    p.add_argument("--rr", type=float, default=2.0)
    p.add_argument("--sl_atr_buffer", type=float, default=0.15)
    p.add_argument("--require_impulse_before_tdp", action="store_true")
    p.add_argument("--impulse_lookback", type=int, default=10)
    p.add_argument("--impulse_size_atr", type=float, default=1.0)
    p.add_argument("--tdp_dev_lookback", type=int, default=8)
    p.add_argument("--tts_retest_lookback", type=int, default=24)

    p.add_argument("--max_hold_candles", type=int, default=2000)

    p.add_argument("--out_dir", type=str, default="backtest/journal/exports_trades")
    args = p.parse_args()

    symbols = _parse_symbols(args.symbols)
    if not symbols:
        raise SystemExit("No symbols")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    all_trades = []
    summary_rows = []

    for sym in symbols:
        # Load candles per symbol
        if args.source == "bybit":
            candles = ft.load_bybit_latest(args.bybit_category, sym, args.bybit_interval, args.bybit_candles)
        else:
            candles, _meta = ft.load_inputs(source=args.source)  # fallback; depends on your csv loader
        if candles is None or candles.empty:
            print(f"[{sym}] no candles")
            continue

        candles = _ensure_utc(candles)
        ctx = ft.build_ctx(candles)

        entries = em.generate_entries_from_ctx(
            ctx,
            rr=float(args.rr),
            sl_atr_buffer=float(args.sl_atr_buffer),
            require_impulse_before_tdp=bool(args.require_impulse_before_tdp),
            impulse_lookback=int(args.impulse_lookback),
            impulse_size_atr=float(args.impulse_size_atr),
            tdp_dev_lookback=int(args.tdp_dev_lookback),
            tts_retest_lookback=int(args.tts_retest_lookback),
            debug_long_funnel=False,
        )

        if not entries:
            print(f"[{sym}] entries=0")
            continue

        df_e = pd.DataFrame([e.__dict__ for e in entries])
        if "symbol" not in df_e.columns:
            df_e["symbol"] = sym

        df_e = _ensure_utc(df_e)

        # save entries
        entries_path = out_dir / f"entries_generated_{sym}.csv"
        df_e.to_csv(entries_path, index=False)

        sim = _simulate_trades_simple(candles, df_e, max_hold_candles=int(args.max_hold_candles))
        # ensure symbol column for downstream multi-symbol analysis
        if 'symbol' not in sim.columns:
            sim['symbol'] = sym
        else:
            sim['symbol'] = sim['symbol'].fillna(sym).replace('', sym)
        sim_path = out_dir / f"trades_simulated_{sym}.csv"
        sim.to_csv(sim_path, index=False)

        all_trades.append(sim)

        sm = _wl_summary(sim)
        sm["symbol"] = sym
        summary_rows.append(sm)

        print(f"[{sym}] trades={sm['total']} exp={sm['expectancy']:.4f} total_R={sm['total_R']:.2f} maxDD_R={sm['maxDD_R']:.2f}")

    if all_trades:
        df_all = pd.concat(all_trades, ignore_index=True)
        # Keep both names for compatibility:
        df_all.to_csv(out_dir / "trades_simulated_multi.csv", index=False)
        df_all.to_csv(out_dir / "trades_simulated.csv", index=False)

    if summary_rows:
        df_s = pd.DataFrame(summary_rows).sort_values(["total_R"], ascending=False)
        df_s.to_csv(out_dir / "summary_by_symbol.csv", index=False)

    print("Done.")


if __name__ == "__main__":
    main()
