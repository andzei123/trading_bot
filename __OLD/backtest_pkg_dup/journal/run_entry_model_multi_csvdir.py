"""
History multi-symbol backtest runner from local CSV directory (SAFE MODE)

- Does NOT touch live_signal_runner.py
- Does NOT overwrite exports_trades/trades_simulated.csv
- Writes outputs into a user-specified out_dir

Input CSV naming convention:
  <SYMBOL>_<INTERVAL>.csv   e.g. BTCUSDT_30.csv
Columns required:
  timestamp, open, high, low, close, volume
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Dict, Any

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


def _load_symbol_csv(csv_dir: Path, symbol: str, interval: str) -> pd.DataFrame:
    p = csv_dir / f"{symbol}_{interval}.csv"
    if not p.exists():
        raise FileNotFoundError(f"Missing candles CSV: {p}")

    df = pd.read_csv(p)

    need = {"timestamp", "open", "high", "low", "close"}
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise ValueError(f"{p} missing columns: {missing}")

    if "volume" not in df.columns:
        df["volume"] = 0.0

    df = df[["timestamp", "open", "high", "low", "close", "volume"]].copy()
    df["open"] = pd.to_numeric(df["open"], errors="coerce")
    df["high"] = pd.to_numeric(df["high"], errors="coerce")
    df["low"] = pd.to_numeric(df["low"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)

    df = _ensure_utc(df)
    return df


def _simulate_trades_simple(candles: pd.DataFrame, entries: pd.DataFrame, max_hold_candles: int = 2000) -> pd.DataFrame:
    if entries is None or entries.empty:
        return entries.iloc[0:0].copy()

    c = _ensure_utc(candles).set_index("timestamp", drop=False)
    e = _ensure_utc(entries)

    out: List[Dict[str, Any]] = []

    for _, row in e.iterrows():
        ts = row["timestamp"]
        if ts not in c.index:
            pos = c.index.searchsorted(ts)
            if pos >= len(c.index):
                continue
            ts0 = c.index[pos]
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
            continue

        rr = abs(tp - entry) / abs(entry - sl) if abs(entry - sl) > 0 else np.nan

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
            "rr": float(rr) if np.isfinite(rr) else np.nan,
            "exit_time": exit_time,
            "exit_price": float(exit_price),
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


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--symbols", type=str, required=True, help="Comma separated, e.g. BTCUSDT,ETHUSDT,XRPUSDT")
    p.add_argument("--csv_dir", type=str, required=True, help="Folder with <SYMBOL>_<INTERVAL>.csv files")
    p.add_argument("--interval", type=str, default="30", help="Interval suffix used in filenames, e.g. 30 -> BTCUSDT_30.csv")

    # model params (match live)
    p.add_argument("--mode", type=str, default="combined")
    p.add_argument("--rr", type=float, default=2.0)
    p.add_argument("--sl_atr_buffer", type=float, default=0.15)
    p.add_argument("--require_impulse_before_tdp", action="store_true")
    p.add_argument("--impulse_lookback", type=int, default=10)
    p.add_argument("--impulse_size_atr", type=float, default=1.0)
    p.add_argument("--tdp_dev_lookback", type=int, default=8)
    p.add_argument("--tts_retest_lookback", type=int, default=24)

    p.add_argument("--max_hold_candles", type=int, default=2000)
    p.add_argument("--out_dir", type=str, required=True, help="Output folder (SAFE: use exports_history/...)")
    args = p.parse_args()

    symbols = _parse_symbols(args.symbols)
    if not symbols:
        raise SystemExit("No symbols")

    csv_dir = Path(args.csv_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    all_trades = []
    summary_rows = []

    for sym in symbols:
        candles = _load_symbol_csv(csv_dir, sym, str(args.interval))
        if candles.empty:
            print(f"[{sym}] no candles")
            continue

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

        # Some project versions return list[dict] instead of DataFrame
        if entries is None:
            print(f"[{sym}] no entries")
            continue

        if isinstance(entries, list):
            entries = pd.DataFrame(entries)

        # If still not a DataFrame, try best-effort conversion
        if not isinstance(entries, pd.DataFrame):
            try:
                entries = pd.DataFrame(entries)
            except Exception:
                print(f"[{sym}] entries returned unexpected type: {type(entries)}")
                continue

        if entries.empty:
            print(f"[{sym}] no entries")
            continue

        entries = entries.copy()
        entries["symbol"] = sym

        entries_path = out_dir / f"entries_generated_{sym}.csv"
        entries.to_csv(entries_path, index=False)

        sim = _simulate_trades_simple(candles, entries, max_hold_candles=int(args.max_hold_candles))
        sim_path = out_dir / f"trades_simulated_{sym}.csv"
        sim.to_csv(sim_path, index=False)

        # quick summary per symbol
        if not sim.empty:
            s = sim.copy()
            s["R"] = pd.to_numeric(s["R"], errors="coerce")
            s = s.dropna(subset=["R"])
            eq = s.sort_values("timestamp")["R"].cumsum()
            dd = eq - eq.cummax()
            summary_rows.append({
                "symbol": sym,
                "trades": int(len(s)),
                "winrate_pct": float((s["R"] > 0).mean() * 100.0) if len(s) else 0.0,
                "expectancy_R": float(s["R"].mean()) if len(s) else 0.0,
                "total_R": float(s["R"].sum()) if len(s) else 0.0,
                "maxDD_R": float(dd.min()) if len(dd) else 0.0,
            })

        all_trades.append(sim)

        print(f"[{sym}] candles={len(candles)} entries={len(entries)} trades={len(sim)} -> {sim_path}")

    if all_trades:
        df_all = pd.concat(all_trades, ignore_index=True)
    else:
        df_all = pd.DataFrame()

    (out_dir / "trades_simulated_multi.csv").write_text("", encoding="utf-8")
    (out_dir / "trades_simulated.csv").write_text("", encoding="utf-8")

    if not df_all.empty:
        df_all.to_csv(out_dir / "trades_simulated_multi.csv", index=False)
        df_all.to_csv(out_dir / "trades_simulated.csv", index=False)

    if summary_rows:
        pd.DataFrame(summary_rows).sort_values("total_R", ascending=False).to_csv(out_dir / "summary_by_symbol.csv", index=False)

    print("DONE. Outputs in:", out_dir)


if __name__ == "__main__":
    main()
