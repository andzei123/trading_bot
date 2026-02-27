"""
V2: TRUE multi-year runner that builds market_regime from candles per symbol (SAFE)

Why V2?
- backtest.journal.filter_trades.build_ctx() merges market_regime.csv from exports_trades/ (global),
  which may be short-range and causes older history to have PHASE_UNKNOWN -> no entries.
- V2 builds market regime directly from the candles you backtest, per symbol, per full history window.

Inputs:
  CSV naming: <SYMBOL>_<INTERVAL>.csv  e.g. BTCUSDT_15m.csv
  Columns: timestamp, open, high, low, close, volume

Outputs (out_dir):
  entries_generated_<SYMBOL>.csv
  trades_simulated_<SYMBOL>.csv
  trades_simulated.csv (multi)
  summary_by_symbol.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional

import numpy as np
import pandas as pd

from backtest.journal import filter_trades as ft
from backtest.journal.market_regime import build_market_regime
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
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = _ensure_utc(df)
    df = df.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)
    return df


def _build_ctx_v2(candles: pd.DataFrame, htf: str = "4h") -> pd.DataFrame:
    """
    Build ctx from candles, but merge market_regime computed from SAME candles (per symbol, full history).
    This avoids dependency on exports_trades/market_regime.csv.
    """
    # 1) base ctx from your labeling pipeline
    ctx = ft.label_tts_tdp(candles)

    ctx_m = ctx.rename(columns={
        "label": "ctx_label",
        "sub_label": "ctx_sub_label",
        "tts_dir": "ctx_tts_dir",
        "tdp_dir": "ctx_tdp_dir",
    })

    # entry_model expects 'sub_label' key as well
    if "ctx_sub_label" in ctx_m.columns:
        ctx_m["sub_label"] = ctx_m["ctx_sub_label"]

    ctx_m = _ensure_utc(ctx_m)

    # defaults (always present)
    for col, default in {
        "regime": "",
        "trend_dir": "",
        "trend_strength": 0.0,
        "atr_pct": 0.0,
    }.items():
        if col not in ctx_m.columns:
            ctx_m[col] = default

    # 2) compute market regime from same candles (per symbol)
    mr = build_market_regime(candles, htf=htf)

    # --- normalize market_regime output across project versions ---
    if isinstance(mr, pd.Series):
        mr = mr.to_frame()

    if not isinstance(mr, pd.DataFrame):
        raise ValueError(f"build_market_regime returned unexpected type: {type(mr)}")

    # If timestamp is index, bring it back as a column
    if "timestamp" not in mr.columns:
        if isinstance(mr.index, pd.DatetimeIndex):
            mr = mr.reset_index().rename(columns={"index": "timestamp"})
        else:
            mr = mr.reset_index()

    # Rename common time column variants -> timestamp
    for c in ["time", "ts", "open_time", "datetime", "date"]:
        if "timestamp" not in mr.columns and c in mr.columns:
            mr = mr.rename(columns={c: "timestamp"})

    # Rename common regime/trend column variants to what add_phase expects
    rename_map = {}
    if "regime" not in mr.columns:
        for c in ["regime_label", "market_regime", "regime_name"]:
            if c in mr.columns:
                rename_map[c] = "regime"
                break

    if "trend_dir" not in mr.columns:
        for c in ["htf_trend", "trend", "trend_direction", "trend_dir_htf"]:
            if c in mr.columns:
                rename_map[c] = "trend_dir"
                break

    if "trend_strength" not in mr.columns:
        for c in ["trend_strength_pct", "trend_strength_score", "trend_strength_htf"]:
            if c in mr.columns:
                rename_map[c] = "trend_strength"
                break

    if "atr_pct" not in mr.columns:
        for c in ["atr_pct_htf", "atr_percent", "atrp"]:
            if c in mr.columns:
                rename_map[c] = "atr_pct"
                break

    if rename_map:
        mr = mr.rename(columns=rename_map)

    mr = _ensure_utc(mr)

    # If still empty, fail loudly (otherwise you'll silently get PHASE_UNKNOWN everywhere)
    if mr.empty:
        raise ValueError("market_regime is empty after normalization (timestamp mapping failed?)")

    # Ensure required columns exist (even if some are missing in this project version)
    for col, default in {
        "regime": "",
        "trend_dir": "",
        "trend_strength": 0.0,
        "atr_pct": 0.0,
    }.items():
        if col not in mr.columns:
            mr[col] = default

    # --- FIX: align market_regime to ctx timestamps (robust, avoids merge_asof NaNs) ---
    mr2 = mr[["timestamp", "regime", "trend_dir", "trend_strength", "atr_pct"]].copy()
    mr2["timestamp"] = pd.to_datetime(mr2["timestamp"], utc=True, errors="coerce")
    mr2 = (
        mr2.dropna(subset=["timestamp"])
        .sort_values("timestamp")
        .drop_duplicates(subset=["timestamp"], keep="last")
    )
    mr2 = mr2.set_index("timestamp").sort_index()

    # Ensure ctx timestamps are clean/sorted
    ctx_m = ctx_m.copy()
    ctx_m["timestamp"] = pd.to_datetime(ctx_m["timestamp"], utc=True, errors="coerce")
    ctx_m = ctx_m.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    # Reindex to ctx timestamps with forward-fill (HTF step function)
    mr_aligned = mr2.reindex(ctx_m["timestamp"], method="ffill")

    # Assign back + normalize
    ctx_m["regime"] = mr_aligned["regime"].astype(str).str.upper().fillna("")
    ctx_m["trend_dir"] = mr_aligned["trend_dir"].astype(str).str.upper().fillna("")
    ctx_m["trend_strength"] = pd.to_numeric(mr_aligned["trend_strength"], errors="coerce").fillna(0.0)
    ctx_m["atr_pct"] = pd.to_numeric(mr_aligned["atr_pct"], errors="coerce").fillna(0.0)

    # DEBUG: what values are we feeding into add_phase?
    print("[MR DEBUG] regime sample:", ctx_m["regime"].astype(str).str.upper().value_counts().head(10).to_dict())
    print("[MR DEBUG] trend_dir sample:", ctx_m["trend_dir"].astype(str).str.upper().value_counts().head(10).to_dict())
    print("[MR DEBUG] mr cols:", list(mr.columns))
    print("[MR DEBUG] mr time span:", mr["timestamp"].min(), "->", mr["timestamp"].max())

    # 4) add phase using your existing logic
    ctx_m = ft.add_phase(ctx_m)

    # IMPORTANT: do NOT trim columns here.
    # Different project versions of entry_model may require additional feature columns.
    return ctx_m.copy()



def _simulate_trades_simple(candles: pd.DataFrame, entries: pd.DataFrame, max_hold_candles: int = 2000) -> pd.DataFrame:
    """
    Simple TP/SL hit simulator on OHLC (no intrabar ordering / no fees / no slippage).
    Safe for regime-behavior analysis.
    """
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
    p.add_argument("--interval", type=str, default="15m", help="Interval suffix used in filenames, e.g. 15m -> BTCUSDT_15m.csv")

    # regime builder
    p.add_argument("--htf", type=str, default="4h", help="HTF resample for market_regime (default 4h)")

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

    print(f"[BOOT] symbols={symbols} interval={args.interval} htf={args.htf} out_dir={out_dir}")

    all_trades = []
    summary_rows = []

    for sym in symbols:
        candles = _load_symbol_csv(csv_dir, sym, str(args.interval))
        if candles.empty:
            print(f"[{sym}] no candles")
            continue

        # V2 ctx with per-symbol market regime over full history
        ctx = _build_ctx_v2(candles, htf=str(args.htf))
        print(f"[{sym}] ctx cols={len(ctx.columns)} rows={len(ctx):,}")
        if "phase" in ctx.columns:
            print(f"[{sym}] phase top:\n{ctx['phase'].value_counts().head(10)}")
        if "sub_label" in ctx.columns:
            print(f"[{sym}] sub_label top:\n{ctx['sub_label'].value_counts().head(10)}")

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
                "start": str(s["timestamp"].min()) if len(s) else "",
                "end": str(s["timestamp"].max()) if len(s) else "",
            })

        all_trades.append(sim)

        print(f"[{sym}] candles={len(candles)} ctx={len(ctx)} entries={len(entries)} trades={len(sim)} -> {sim_path}")

    df_all = pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()

    if not df_all.empty:
        df_all.to_csv(out_dir / "trades_simulated.csv", index=False)

    if summary_rows:
        pd.DataFrame(summary_rows).sort_values("total_R", ascending=False).to_csv(out_dir / "summary_by_symbol.csv", index=False)

    print("DONE. Outputs in:", out_dir)


if __name__ == "__main__":
    main()
