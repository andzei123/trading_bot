from __future__ import annotations

"""
Minimal live observation shell.

STRICT bounded step:
    1) load live candles
    2) assemble upstream ctx
    3) call pipeline_core.run_pipeline_once(...)
    4) only then perform live-specific emission/logging

Authority:
    offline_live_runner_backtest.py -> pipeline_core.py

Non-goals:
    - no extra phase routing outside pipeline_core
    - no macro/news/liquidity integration
    - no second filter/risk/invalidation path
    - no strategy redesign
"""

import argparse
import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd
import requests

from backtest.live_pipeline.pipeline_core import run_pipeline_once
from backtest.portfolio.portfolio_exposure import load_portfolio_exposure
from backtest.utils.wait_confirmation import apply_wait_confirmation

BYBIT_REST = "https://api.bybit.com"


def _parse_symbols(s: str) -> List[str]:
    return [x.strip().upper() for x in str(s).split(",") if x.strip()]


def _read_state(state_path: Path) -> Optional[pd.Timestamp]:
    if not state_path.exists():
        return None
    try:
        raw = state_path.read_text(encoding="utf-8").strip()
        if not raw:
            return None
        return pd.to_datetime(raw, utc=True, errors="coerce")
    except Exception:
        return None


def _write_state(state_path: Path, ts: pd.Timestamp) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(str(pd.Timestamp(ts).tz_convert("UTC")), encoding="utf-8")


def _bybit_get_kline(
    category: str,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1000,
) -> pd.DataFrame:
    url = f"{BYBIT_REST}/v5/market/kline"
    params = {
        "category": category,
        "symbol": symbol,
        "interval": interval,
        "start": int(start_ms),
        "end": int(end_ms),
        "limit": int(limit),
    }

    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    j = r.json()

    if j.get("retCode") != 0:
        raise RuntimeError(f"Bybit error for {symbol}: {j}")

    rows = []
    for it in (j.get("result", {}).get("list") or []):
        rows.append(
            {
                "timestamp": pd.to_datetime(int(it[0]), unit="ms", utc=True),
                "open": float(it[1]),
                "high": float(it[2]),
                "low": float(it[3]),
                "close": float(it[4]),
                "volume": float(it[5]),
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = df.sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)

    # Keep ATR columns available, same as offline runner.
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


def load_bybit_latest(
    category: str,
    symbol: str,
    interval: str,
    candles: int,
) -> pd.DataFrame:
    end = int(pd.Timestamp.utcnow().timestamp() * 1000)
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

    if len(df) > candles:
        df = df.iloc[-candles:].reset_index(drop=True)
    return df


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _ensure_output_csv(path: Path) -> None:
    _ensure_parent(path)


def _load_open_positions(position_state_csv: Path) -> Set[str]:
    if not position_state_csv.exists():
        return set()
    try:
        df = pd.read_csv(position_state_csv)
    except Exception:
        return set()
    if df.empty or "symbol" not in df.columns:
        return set()
    if "status" in df.columns:
        df = df[df["status"].astype(str).str.upper() == "OPEN"]
    return {str(x).upper() for x in df["symbol"].dropna().astype(str)}


def _position_is_open(symbol: str, position_state_csv: Path) -> bool:
    return str(symbol).upper() in _load_open_positions(position_state_csv)


def _mark_position_open(symbol: str, setup_id: str, opened_ts: pd.Timestamp, position_state_csv: Path) -> None:
    _ensure_parent(position_state_csv)
    row = pd.DataFrame([
        {
            "symbol": str(symbol).upper(),
            "setup_id": str(setup_id),
            "opened_ts": pd.to_datetime(opened_ts, utc=True, errors="coerce"),
            "status": "OPEN",
        }
    ])
    if not position_state_csv.exists() or position_state_csv.stat().st_size == 0:
        row.to_csv(position_state_csv, index=False)
        return
    cols = ["symbol", "setup_id", "opened_ts", "status"]

    try:
        existing = pd.read_csv(position_state_csv)
        if existing.empty:
            existing = pd.DataFrame(columns=cols)
        else:
            # užtikrinam, kad visos kolonos yra
            for c in cols:
                if c not in existing.columns:
                    existing[c] = pd.NA
            existing = existing[cols]
    except Exception:
        existing = pd.DataFrame(columns=cols)
    existing = existing.copy()
    if not existing.empty and "symbol" in existing.columns and "status" in existing.columns:
        mask = (existing["symbol"].astype(str).str.upper() == str(symbol).upper()) & (existing["status"].astype(str).str.upper() == "OPEN")
        existing = existing.loc[~mask].copy()
    combined = pd.concat([existing, row], ignore_index=True)
    combined.to_csv(position_state_csv, index=False)


def _load_fired_setup_ids(path: Path) -> Set[str]:
    if not path.exists():
        return set()
    try:
        df = pd.read_csv(path)
    except Exception:
        return set()
    if df.empty or "setup_id" not in df.columns:
        return set()
    return {str(x) for x in df["setup_id"].dropna().astype(str)}


def _append_fired_setup_ids(path: Path, rows: pd.DataFrame) -> None:
    if rows is None or rows.empty:
        return
    use_cols = [c for c in ("setup_id", "symbol", "timestamp", "model", "side", "signal_ts", "observed_ts") if c in rows.columns]
    if not use_cols:
        return
    out = rows[use_cols].copy()
    _ensure_parent(path)
    if not path.exists() or path.stat().st_size == 0:
        out.to_csv(path, index=False)
    else:
        out.to_csv(path, mode="a", header=False, index=False)


def _build_setup_ids(df: pd.DataFrame, symbol: str) -> pd.Series:
    ts = pd.to_datetime(df.get("timestamp"), utc=True, errors="coerce").astype(str)
    model = df.get("model", pd.Series("", index=df.index)).astype(str)
    side = df.get("side", pd.Series("", index=df.index)).astype(str)
    sym = pd.Series(str(symbol).upper(), index=df.index)
    return sym + "|" + ts + "|" + model + "|" + side


def _append_df(path: Path, df: pd.DataFrame) -> None:
    _ensure_parent(path)
    if df is None:
        return
    if not path.exists() or path.stat().st_size == 0:
        df.to_csv(path, index=False)
    else:
        df.to_csv(path, mode="a", header=False, index=False)


def _emit_observation_rows(
    *,
    out_csv: Path,
    symbol: str,
    latest_ts: pd.Timestamp,
    df_e: pd.DataFrame,
) -> int:
    if df_e is None or df_e.empty:
        return 0

    out = df_e.copy()

    # Stable live-observation tags only; no new selection logic.
    out["observed_ts"] = pd.to_datetime(latest_ts, utc=True, errors="coerce")
    out["symbol"] = symbol

    if "signal_ts" not in out.columns or out["signal_ts"].isna().all():
        out["signal_ts"] = pd.to_datetime(latest_ts, utc=True, errors="coerce")

    _append_df(out_csv, out)
    return int(len(out))


def run_symbol_once(
    *,
    symbol: str,
    category: str,
    interval: str,
    candles_n: int,
    window_n: int,
    portfolio_state_path: Path,
    out_csv: Path,
    state_dir: Path,
    position_state_csv: Path,
    fired_setups_csv: Path,
    debug: bool,
    debug_force_entries: bool,
    use_wait_confirmation: bool,
    candidate_pressure_csv: str,
    cluster_score_mode: Optional[str],
    cluster_max_per_group: Optional[int],
    cluster_rank_signal_score: bool,
    rr: float,
    sl_atr_buffer: float,
    require_impulse_before_tdp: bool,
    impulse_lookback: int,
    impulse_size_atr: float,
    tdp_dev_lookback: int,
    tts_retest_lookback: int,
) -> int:
    candles_df = load_bybit_latest(category, symbol, interval, candles_n)
    if candles_df is None or candles_df.empty:
        print(f"[OBSERVE][{symbol}] no candles")
        return 0

    candles_df = candles_df.copy()
    candles_df["timestamp"] = pd.to_datetime(candles_df["timestamp"], utc=True, errors="coerce")
    candles_df = candles_df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    if candles_df.empty:
        print(f"[OBSERVE][{symbol}] candles empty after normalization")
        return 0

    latest_ts = pd.to_datetime(candles_df["timestamp"].iloc[-1], utc=True, errors="coerce")
    state_path = state_dir / f"{symbol}_{interval}.txt"
    last_seen = _read_state(state_path)

    if _position_is_open(symbol, position_state_csv):
        _write_state(state_path, latest_ts)
        print(f"[OBSERVE][{symbol}] skipped open position latest_ts={latest_ts}")
        return 0

    # Minimal live observation behavior: skip already-seen candle.
    if last_seen is not None and latest_ts <= last_seen:
        print(f"[OBSERVE][{symbol}] no new candle latest_ts={latest_ts}")
        return 0

    window = candles_df.tail(int(window_n)).copy().reset_index(drop=True)
    portfolio_state = load_portfolio_exposure(portfolio_state_path)

    # Mirror the validated offline ctx assembly as closely as possible.
    ctx: Dict[str, object] = {
        "latest_ts": latest_ts,
        "bybit_interval": int(interval) if str(interval).isdigit() else interval,
        "macro_bias": "NEUTRAL",
        "debug": bool(debug),
        "use_wait_confirmation": bool(use_wait_confirmation),
        "candidate_pressure_csv": candidate_pressure_csv,
        "rr": float(rr),
        "sl_atr_buffer": float(sl_atr_buffer),
        "require_impulse_before_tdp": bool(require_impulse_before_tdp),
        "impulse_lookback": int(impulse_lookback),
        "impulse_size_atr": float(impulse_size_atr),
        "tdp_dev_lookback": int(tdp_dev_lookback),
        "tts_retest_lookback": int(tts_retest_lookback),
        "disable_invalidation": True,
        "debug_force_entries": bool(debug_force_entries),
        "force_entries": bool(debug_force_entries),
        "debug_entry_force": bool(debug_force_entries),
        "DEBUG_FORCE_ENTRIES": bool(debug_force_entries),
    }

    if cluster_score_mode is not None:
        ctx["cluster_score_mode"] = cluster_score_mode
        if cluster_score_mode == "SIGNAL_SCORE":
            ctx["cluster_rank_signal_score"] = True

    if cluster_rank_signal_score:
        ctx["cluster_rank_signal_score"] = True

    if cluster_max_per_group is not None:
        ctx["cluster_max_per_group"] = int(cluster_max_per_group)

    df_e = run_pipeline_once(
        symbol=symbol,
        candles_df=window,
        ctx=ctx,
        portfolio_state=portfolio_state,
        debug=bool(debug),
    )

    if df_e is None or df_e.empty:
        _write_state(state_path, latest_ts)
        print(f"[OBSERVE][{symbol}] pipeline rows=0 latest_ts={latest_ts}")
        return 0

    entries = df_e.to_dict("records")

    # Preserve offline post-pipeline behavior if explicitly requested.
    if bool(use_wait_confirmation):
        entries = apply_wait_confirmation(entries, candles_df)

    if not entries:
        _write_state(state_path, latest_ts)
        print(f"[OBSERVE][{symbol}] post_pipeline rows=0 after wait confirmation latest_ts={latest_ts}")
        return 0

    out_df = pd.DataFrame(entries)
    out_df["setup_id"] = _build_setup_ids(out_df, symbol)

    fired_ids = _load_fired_setup_ids(fired_setups_csv)
    out_df = out_df.loc[~out_df["setup_id"].isin(fired_ids)].copy()
    if out_df.empty:
        _write_state(state_path, latest_ts)
        print(f"[OBSERVE][{symbol}] post_pipeline rows=0 after idempotency latest_ts={latest_ts}")
        return 0

    out_df = out_df.sort_values("timestamp").head(1).copy()

    written = _emit_observation_rows(
        out_csv=out_csv,
        symbol=symbol,
        latest_ts=latest_ts,
        df_e=out_df,
    )
    _append_fired_setup_ids(fired_setups_csv, out_df)

    if written > 0:
        first_setup_id = str(out_df["setup_id"].iloc[0])
        _mark_position_open(symbol, first_setup_id, latest_ts, position_state_csv)

    _write_state(state_path, latest_ts)

    try:
        preview = out_df[
            [c for c in ("timestamp", "signal_ts", "model", "side", "entry", "sl", "tp", "rr", "phase") if c in out_df.columns]
        ].copy()
        print(f"[OBSERVE][{symbol}] wrote={written} latest_ts={latest_ts}")
        print(preview.tail(min(len(preview), 5)).to_string(index=False))
    except Exception:
        print(f"[OBSERVE][{symbol}] wrote={written} latest_ts={latest_ts}")

    return written


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Minimal live observation shell using pipeline_core as decision authority.")

    ap.add_argument("--symbols", default="BTCUSDT", help="Comma-separated symbols")
    ap.add_argument("--bybit_category", default="linear", help="Bybit category")
    ap.add_argument("--bybit_interval", default="15", help="Bybit candle interval")
    ap.add_argument("--bybit_candles", type=int, default=260, help="How many live candles to fetch")
    ap.add_argument("--window", type=int, default=200, help="Window length passed to pipeline_core")
    ap.add_argument("--portfolio_state", default="backtest/journal/exports_live/portfolio_state.json")
    ap.add_argument("--out_csv", default="backtest/journal/exports_live/live_observation_entries.csv")
    ap.add_argument("--state_dir", default="backtest/journal/exports_live/live_observation_state")
    ap.add_argument("--position_state_csv", default="backtest/journal/exports_live/position_state.csv")
    ap.add_argument("--fired_setups_csv", default="backtest/journal/fired_setups.csv")
    ap.add_argument("--poll_seconds", type=int, default=30, help="Loop sleep when not using --once")
    ap.add_argument("--once", action="store_true", help="Run one cycle only")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--debug_force_entries", action="store_true")
    ap.add_argument("--use_wait_confirmation", action="store_true")
    ap.add_argument("--candidate_pressure_csv", default="backtest/journal/exports_live/candidate_pressure.csv")

    ap.add_argument("--cluster_score_mode", choices=("LEGACY", "SIGNAL_SCORE"), default=None)
    ap.add_argument("--cluster_max_per_group", type=int, choices=(1, 2, 3), default=None)
    ap.add_argument("--cluster_rank_signal_score", action="store_true")

    # Keep the same baseline defaults as offline runner ctx.
    ap.add_argument("--rr", type=float, default=2.0)
    ap.add_argument("--sl_atr_buffer", type=float, default=0.15)
    ap.add_argument("--require_impulse_before_tdp", action="store_true")
    ap.add_argument("--impulse_lookback", type=int, default=10)
    ap.add_argument("--impulse_size_atr", type=float, default=1.0)
    ap.add_argument("--tdp_dev_lookback", type=int, default=8)
    ap.add_argument("--tts_retest_lookback", type=int, default=24)

    args = ap.parse_args(argv)

    symbols = _parse_symbols(args.symbols)
    if not symbols:
        raise SystemExit("--symbols empty")

    out_csv = Path(args.out_csv)
    state_dir = Path(args.state_dir)
    portfolio_state_path = Path(args.portfolio_state)
    position_state_csv = Path(args.position_state_csv)
    fired_setups_csv = Path(args.fired_setups_csv)

    _ensure_output_csv(out_csv)

    total_written = 0

    while True:
        cycle_written = 0
        cycle_ts = pd.Timestamp.utcnow()

        for symbol in symbols:
            try:
                cycle_written += run_symbol_once(
                    symbol=symbol,
                    category=str(args.bybit_category),
                    interval=str(args.bybit_interval),
                    candles_n=int(args.bybit_candles),
                    window_n=int(args.window),
                    portfolio_state_path=portfolio_state_path,
                    out_csv=out_csv,
                    state_dir=state_dir,
                    position_state_csv=position_state_csv,
                    fired_setups_csv=fired_setups_csv,
                    debug=bool(args.debug),
                    debug_force_entries=bool(args.debug_force_entries),
                    use_wait_confirmation=bool(args.use_wait_confirmation),
                    candidate_pressure_csv=str(args.candidate_pressure_csv),
                    cluster_score_mode=args.cluster_score_mode,
                    cluster_max_per_group=args.cluster_max_per_group,
                    cluster_rank_signal_score=bool(args.cluster_rank_signal_score),
                    rr=float(args.rr),
                    sl_atr_buffer=float(args.sl_atr_buffer),
                    require_impulse_before_tdp=bool(args.require_impulse_before_tdp),
                    impulse_lookback=int(args.impulse_lookback),
                    impulse_size_atr=float(args.impulse_size_atr),
                    tdp_dev_lookback=int(args.tdp_dev_lookback),
                    tts_retest_lookback=int(args.tts_retest_lookback),
                )
            except Exception as e:
                print(f"[OBSERVE][{symbol}] error={type(e).__name__}: {e}")

        total_written += cycle_written
        print(
            json.dumps(
                {
                    "event": "cycle_done",
                    "cycle_ts": str(cycle_ts),
                    "written_this_cycle": int(cycle_written),
                    "written_total": int(total_written),
                    "symbols": symbols,
                },
                ensure_ascii=False,
            )
        )

        if args.once:
            break

        time.sleep(max(1, int(args.poll_seconds)))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())