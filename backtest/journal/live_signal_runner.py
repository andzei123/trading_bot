# backtest/journal/live_signal_runner.py
# Run pipeline and emit entries periodically into CSV.
from __future__ import annotations

import argparse
import time
import random
from pathlib import Path
from typing import Optional, Callable, Tuple

import importlib
import pandas as pd
import numpy as np
import requests

import backtest.journal.filter_trades as ft
from backtest.live.regime_controller import decide_profile_from_performance

BYBIT_REST = "https://api.bybit.com"

# ============================================================
# A2: Rate-limit backoff helpers (Bybit retCode 10006)
# ============================================================

class BybitRateLimitError(RuntimeError):
    pass


def _is_bybit_rate_limit_payload(j: dict) -> bool:
    try:
        code = int(j.get("retCode", -1))
    except Exception:
        code = -1
    msg = str(j.get("retMsg", "")).lower()
    return (code == 10006) or ("too many visits" in msg) or ("rate limit" in msg)


def _is_bybit_rate_limit_exception(e: Exception) -> bool:
    s = str(e).lower()
    return ("10006" in s) or ("too many visits" in s) or ("rate limit" in s) or isinstance(e, BybitRateLimitError)


# ============================================================
# Small helpers
# ============================================================

def now_utc_str() -> str:
    return pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _read_state(state_path: Path) -> Optional[pd.Timestamp]:
    if not state_path.exists():
        return None
    try:
        s = state_path.read_text(encoding="utf-8").strip()
        if not s:
            return None
        return pd.to_datetime(s, utc=True)
    except Exception:
        return None


def _write_state(state_path: Path, ts: pd.Timestamp) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(str(pd.Timestamp(ts).tz_convert("UTC")), encoding="utf-8")


# ============================================================
# Bybit candles
# ============================================================

def _bybit_get_kline(category, symbol, interval, start_ms, end_ms, limit=1000) -> pd.DataFrame:
    url = f"{BYBIT_REST}/v5/market/kline"
    params = dict(
        category=category,
        symbol=symbol,
        interval=interval,
        start=int(start_ms),
        end=int(end_ms),
        limit=int(limit),
    )

    # A2: local retry/backoff on retCode=10006
    sleep_s = 10
    while True:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        j = r.json()

        if _is_bybit_rate_limit_payload(j):
            jitter = random.uniform(0.0, 1.0)
            print(f"[{now_utc_str()}] BYBIT 10006 rate limit -> sleep {sleep_s:.0f}s")
            time.sleep(min(300, sleep_s) + jitter)
            sleep_s = min(300, sleep_s * 2)
            continue

        if j.get("retCode") != 0:
            raise RuntimeError(j)

        rows = []
        for it in (j.get("result", {}).get("list") or []):
            rows.append(
                dict(
                    timestamp=pd.to_datetime(int(it[0]), unit="ms", utc=True),
                    open=float(it[1]),
                    high=float(it[2]),
                    low=float(it[3]),
                    close=float(it[4]),
                    volume=float(it[5]),
                )
            )
        return pd.DataFrame(rows)


def load_bybit_latest(
    category: str,
    symbol: str,
    interval: str,
    candles: int,
) -> pd.DataFrame:
    end = int(pd.Timestamp.utcnow().timestamp() * 1000)
    # conservative: fetch window with some buffer
    ms_per_bar = {
        "1": 60_000,
        "3": 180_000,
        "5": 300_000,
        "15": 900_000,
        "30": 1_800_000,
        "60": 3_600_000,
        "120": 7_200_000,
        "240": 14_400_000,
        "D": 86_400_000,
    }.get(str(interval), 60_000)

    start = end - int(ms_per_bar * max(10, candles + 10))
    df = _bybit_get_kline(category, symbol, interval, start, end, limit=1000)
    if df.empty:
        return df

    df = df.sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)
    if len(df) > candles:
        df = df.iloc[-candles:].reset_index(drop=True)
    return df


# ============================================================
# Pipeline call
# ============================================================

def _load_gen_from_entry_model(entry_model_module: str = "backtest.engine.entry_model"):
    em = importlib.import_module(entry_model_module)
    return em.generate_entries_from_ctx


def _ensure_parent(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def _append_csv(path: Path, df: pd.DataFrame) -> None:
    _ensure_parent(path)
    if not path.exists():
        df.to_csv(path, index=False)
        return
    df.to_csv(path, mode="a", index=False, header=False)


def run_once(
    out_csv: Path,
    state_path: Path,
    mode: str,
    generate_entries_from_ctx: Callable,
    rr: float,
    sl_atr_buffer: float,
    require_impulse_before_tdp: bool,
    impulse_lookback: int,
    impulse_size_atr: float,
    tdp_dev_lookback: int,
    tts_retest_lookback: int,
    live_keep: int,
    source: str,
    bybit_category: str,
    bybit_symbol: str,
    bybit_interval: str,
    bybit_candles: int,
) -> int:
    # load candles
    if source == "bybit":
        candles = load_bybit_latest(bybit_category, bybit_symbol, bybit_interval, bybit_candles)
    else:
        candles, _ = ft.load_inputs(source=source)

    if candles is None or candles.empty:
        print(f"[{now_utc_str()}] No candles")
        return 0

    # normalize
    candles = candles.copy()
    candles["timestamp"] = pd.to_datetime(candles["timestamp"], utc=True)
    candles = candles.sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)
    # ------------------------------------------------------------
    # Production hygiene: skip full pipeline if no new candle
    # ------------------------------------------------------------
    last_ts = _read_state(state_path)
    latest_ts = candles["timestamp"].iloc[-1]

    if last_ts is not None and latest_ts <= last_ts:
        print(f"[{now_utc_str()}] no new candle ({latest_ts})")
        return 0

    # build ctx and generate entries
    ctx = ft.build_ctx(candles)
    entries = generate_entries_from_ctx(
        ctx,
        rr=rr,
        sl_atr_buffer=sl_atr_buffer,
        require_impulse_before_tdp=require_impulse_before_tdp,
        impulse_lookback=impulse_lookback,
        impulse_size_atr=impulse_size_atr,
        tdp_dev_lookback=tdp_dev_lookback,
        tts_retest_lookback=tts_retest_lookback,
        debug_long_funnel=True,
    )
    if not entries:
        return 0

    df_e = pd.DataFrame([e.__dict__ for e in entries])
    if df_e.empty:
        return 0

    # keep only new entries since last state
    last_ts = _read_state(state_path)
    if last_ts is not None and "timestamp" in df_e.columns:
        df_e["timestamp"] = pd.to_datetime(df_e["timestamp"], utc=True, errors="coerce")
        df_e = df_e[df_e["timestamp"] > last_ts].copy()

    if df_e.empty:
        return 0

    # keep last N rows only
    df_e = df_e.sort_values("timestamp").reset_index(drop=True)
    if live_keep and len(df_e) > int(live_keep):
        df_e = df_e.iloc[-int(live_keep):].reset_index(drop=True)

    _append_csv(out_csv, df_e)

    # update state with newest timestamp
    newest = pd.to_datetime(df_e["timestamp"].iloc[-1], utc=True)
    _write_state(state_path, newest)

    print(f"[{now_utc_str()}] Wrote {len(df_e)} entries -> {out_csv}")
    return int(len(df_e))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", type=str, default="backtest/journal/live_entries.csv")
    p.add_argument("--state", type=str, default="backtest/journal/live_state.txt")
    p.add_argument("--interval", type=int, default=30)
    p.add_argument("--once", action="store_true")
    p.add_argument("--mode", type=str, default="combined")

    # model params (subset)
    p.add_argument("--rr", type=float, default=2.0)
    p.add_argument("--sl_atr_buffer", type=float, default=0.15)
    p.add_argument("--require_impulse_before_tdp", action="store_true")
    p.add_argument("--impulse_lookback", type=int, default=10)
    p.add_argument("--impulse_size_atr", type=float, default=1.0)
    p.add_argument("--tdp_dev_lookback", type=int, default=8)
    p.add_argument("--tts_retest_lookback", type=int, default=24)

    # live runner params
    p.add_argument("--live_keep", type=int, default=50)

    # data source
    p.add_argument("--source", type=str, default="bybit", choices=["bybit", "csv"])

    # bybit params
    p.add_argument("--bybit_category", type=str, default="linear")
    p.add_argument("--bybit_symbol", type=str, default="BTCUSDT")
    p.add_argument("--bybit_interval", type=str, default="15")
    p.add_argument("--bybit_candles", type=int, default=300)

    args = p.parse_args()

    out_csv = Path(args.out)
    state_path = Path(args.state)

    gen = _load_gen_from_entry_model()
    generate_entries_from_ctx = gen

    if args.once:
        run_once(
            out_csv=out_csv,
            state_path=state_path,
            mode=args.mode,
            generate_entries_from_ctx=gen,
            rr=args.rr,
            sl_atr_buffer=args.sl_atr_buffer,
            require_impulse_before_tdp=args.require_impulse_before_tdp,
            impulse_lookback=args.impulse_lookback,
            impulse_size_atr=args.impulse_size_atr,
            tdp_dev_lookback=args.tdp_dev_lookback,
            tts_retest_lookback=args.tts_retest_lookback,
            source=args.source,
            bybit_category=args.bybit_category,
            bybit_symbol=args.bybit_symbol,
            bybit_interval=args.bybit_interval,
            bybit_candles=args.bybit_candles,
            live_keep=args.live_keep,
        )
        return

    backoff_s = 10

    while True:
        try:
            n = run_once(
                out_csv,
                state_path,
                args.mode,
                generate_entries_from_ctx,
                args.rr,
                args.sl_atr_buffer,
                args.require_impulse_before_tdp,
                args.impulse_lookback,
                args.impulse_size_atr,
                args.tdp_dev_lookback,
                args.tts_retest_lookback,
                args.live_keep,
                args.source,
                args.bybit_category,
                args.bybit_symbol,
                args.bybit_interval,
                args.bybit_candles,
            )
            # jei viskas OK — reset backoff
            backoff_s = 10

        except KeyboardInterrupt:
            break

        except Exception as e:
            if _is_bybit_rate_limit_exception(e):
                jitter = random.uniform(0.0, 1.0)
                print(f"[{now_utc_str()}] RATE_LIMIT -> sleep {backoff_s:.0f}s | {e}")
                time.sleep(min(300, backoff_s) + jitter)
                backoff_s = min(300, backoff_s * 2)
                continue

            print(f"[{now_utc_str()}] ERROR: {e}")

        time.sleep(max(1, args.interval))


if __name__ == "__main__":
    main()
