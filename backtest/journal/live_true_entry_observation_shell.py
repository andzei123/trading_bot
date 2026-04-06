from __future__ import annotations

"""
Minimal live shell that observes TRUE ENTRY EVENTS.

Bounded contract:
- HTF layer produces setup candidates
- LT layer confirms real entry events
- setup candidate != entry event
- setup candidates may remain alive across multiple candles
- only confirmed entry events are emitted and consumed once

Authority constraints:
- does NOT modify pipeline_core
- does NOT modify entry_model
- does NOT add macro/router/risk/context authority
- uses HTF/LT lifecycle modules as observation truth only
"""

import argparse
import json
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import requests

from backtest.journal.filter_trades import build_ctx
from backtest.journal.htf_ltf_pipeline import (
    SetupEvent,
    build_ctx_htf,
    extract_setup_events,
    pass_smart_gate,
)
from backtest.journal.lt_entry import find_retest_entry

BYBIT_REST = "https://api.bybit.com"


def _parse_symbols(raw: str) -> List[str]:
    return [x.strip().upper() for x in str(raw).split(",") if x.strip()]


def _ts(v: Any) -> pd.Timestamp:
    return pd.to_datetime(v, utc=True, errors="coerce")


def _ts_str(v: Any) -> str:
    ts = _ts(v)
    return "" if pd.isna(ts) else ts.isoformat()


def _json_default(v: Any) -> Any:
    if isinstance(v, pd.Timestamp):
        return _ts_str(v)
    return str(v)


def _bybit_get_kline(
    *,
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

    rows: List[Dict[str, Any]] = []
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

    # ATR fields reused by upstream logic.
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


def load_bybit_latest(*, category: str, symbol: str, interval: str, candles: int) -> pd.DataFrame:
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
    df = _bybit_get_kline(
        category=category,
        symbol=symbol,
        interval=interval,
        start_ms=start,
        end_ms=end,
        limit=1000,
    )
    if df.empty:
        return df
    if len(df) > candles:
        df = df.iloc[-candles:].reset_index(drop=True)
    return df


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


class SymbolState:
    """Shell-local lifecycle state for one symbol/interval."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.last_seen_ts: Optional[pd.Timestamp] = None
        self.active_candidates: Dict[str, Dict[str, Any]] = {}
        self.consumed_entries: Dict[str, Dict[str, Any]] = {}

    def load(self) -> None:
        if not self.path.exists():
            return
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return

        self.last_seen_ts = _ts(raw.get("last_seen_ts"))
        if pd.isna(self.last_seen_ts):
            self.last_seen_ts = None

        active = raw.get("active_candidates") or {}
        consumed = raw.get("consumed_entries") or {}
        if isinstance(active, dict):
            self.active_candidates = active
        if isinstance(consumed, dict):
            self.consumed_entries = consumed

    def save(self) -> None:
        _ensure_parent(self.path)
        payload = {
            "last_seen_ts": _ts_str(self.last_seen_ts),
            "active_candidates": self.active_candidates,
            "consumed_entries": self.consumed_entries,
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")


def _setup_candidate_key(symbol: str, setup: SetupEvent) -> str:
    return "|".join(
        [
            symbol.upper(),
            _ts_str(setup.timestamp_htf),
            str(setup.model).upper(),
            str(setup.side).upper(),
            str(setup.ctx_sub_label).upper(),
        ]
    )


def _entry_event_key(symbol: str, candidate_key: str, entry: Any) -> str:
    return "|".join(
        [
            symbol.upper(),
            candidate_key,
            _ts_str(getattr(entry, "timestamp", None)),
            str(getattr(entry, "model", "")).upper(),
            str(getattr(entry, "side", "")).upper(),
        ]
    )


def _candidate_expires_at(setup: SetupEvent, *, entry_window_hours: float) -> pd.Timestamp:
    return _ts(setup.timestamp_htf) + pd.Timedelta(hours=float(entry_window_hours))


def _setup_to_state_record(setup: SetupEvent, *, latest_ts: pd.Timestamp, entry_window_hours: float) -> Dict[str, Any]:
    return {
        "setup": {
            "timestamp_htf": _ts_str(setup.timestamp_htf),
            "model": setup.model,
            "side": setup.side,
            "ctx_sub_label": setup.ctx_sub_label,
            "regime": setup.regime,
            "trend_dir": setup.trend_dir,
            "trend_strength": float(setup.trend_strength),
            "atr_pct": float(setup.atr_pct),
            "range_hi": setup.range_hi,
            "range_lo": setup.range_lo,
            "meta": setup.meta,
        },
        "first_seen_ts": _ts_str(latest_ts),
        "last_checked_ts": _ts_str(latest_ts),
        "expires_at": _ts_str(_candidate_expires_at(setup, entry_window_hours=entry_window_hours)),
        "status": "ACTIVE",
    }


def _state_record_to_setup(record: Dict[str, Any]) -> SetupEvent:
    s = record["setup"]
    return SetupEvent(
        timestamp_htf=_ts(s.get("timestamp_htf")),
        model=str(s.get("model", "")),
        side=str(s.get("side", "")),
        ctx_sub_label=str(s.get("ctx_sub_label", "")),
        regime=str(s.get("regime", "")),
        trend_dir=str(s.get("trend_dir", "")),
        trend_strength=float(s.get("trend_strength", 0.0) or 0.0),
        atr_pct=float(s.get("atr_pct", 0.0) or 0.0),
        range_hi=s.get("range_hi"),
        range_lo=s.get("range_lo"),
        meta=str(s.get("meta", "")),
    )


def _collect_htf_setup_candidates(
    *,
    candles_df: pd.DataFrame,
    htf: str,
    trend_min: float,
    need_atr: bool,
    atr_min: float,
) -> List[SetupEvent]:
    ctx_m = build_ctx(candles_df)
    ctx_htf = build_ctx_htf(ctx_m, htf=htf)
    setups = extract_setup_events(ctx_htf)
    gated = [
        s
        for s in setups
        if pass_smart_gate(
            s,
            trend_min=float(trend_min),
            need_atr=bool(need_atr),
            atr_min=float(atr_min),
        )
    ]
    gated.sort(key=lambda s: _ts(s.timestamp_htf))
    return gated


def _append_rows(path: Path, df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return
    _ensure_parent(path)
    if not path.exists() or path.stat().st_size == 0:
        df.to_csv(path, index=False)
    else:
        df.to_csv(path, mode="a", header=False, index=False)


def _entry_to_emit_row(
    *,
    symbol: str,
    latest_ts: pd.Timestamp,
    candidate_key: str,
    setup: SetupEvent,
    entry: Any,
    entry_key: str,
) -> Dict[str, Any]:
    return {
        "observed_ts": _ts_str(latest_ts),
        "symbol": symbol,
        "candidate_key": candidate_key,
        "entry_key": entry_key,
        "setup_timestamp_htf": _ts_str(setup.timestamp_htf),
        "setup_model": setup.model,
        "setup_side": setup.side,
        "setup_ctx_sub_label": setup.ctx_sub_label,
        "setup_regime": setup.regime,
        "setup_trend_dir": setup.trend_dir,
        "setup_trend_strength": setup.trend_strength,
        "setup_atr_pct": setup.atr_pct,
        "setup_range_hi": setup.range_hi,
        "setup_range_lo": setup.range_lo,
        "entry_timestamp": _ts_str(getattr(entry, "timestamp", None)),
        "entry_model": getattr(entry, "model", None),
        "entry_side": getattr(entry, "side", None),
        "entry": getattr(entry, "entry", None),
        "sl": getattr(entry, "sl", None),
        "tp": getattr(entry, "tp", None),
        "meta": getattr(entry, "meta", None),
        "ctx_sub_label": getattr(entry, "ctx_sub_label", None),
        "regime": getattr(entry, "regime", None),
        "trend_dir": getattr(entry, "trend_dir", None),
        "trend_strength": getattr(entry, "trend_strength", None),
        "atr_pct": getattr(entry, "atr_pct", None),
    }


def run_symbol_once(
    *,
    symbol: str,
    category: str,
    interval: str,
    candles_n: int,
    state_dir: Path,
    out_csv: Path,
    htf: str,
    entry_window_hours: float,
    tol_atr_mult: float,
    rr: float,
    sl_atr_buffer: float,
    trend_min: float,
    need_atr: bool,
    atr_min: float,
    debug: bool,
) -> int:
    candles_df = load_bybit_latest(category=category, symbol=symbol, interval=interval, candles=candles_n)
    if candles_df is None or candles_df.empty:
        print(f"[TRUE_ENTRY][{symbol}] no candles")
        return 0

    candles_df = candles_df.copy()
    candles_df["timestamp"] = pd.to_datetime(candles_df["timestamp"], utc=True, errors="coerce")
    candles_df = candles_df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    if candles_df.empty:
        print(f"[TRUE_ENTRY][{symbol}] normalized candles empty")
        return 0

    latest_ts = _ts(candles_df["timestamp"].iloc[-1])
    state = SymbolState(state_dir / f"{symbol}_{interval}.json")
    state.load()

    if state.last_seen_ts is not None and latest_ts <= state.last_seen_ts:
        print(f"[TRUE_ENTRY][{symbol}] no new candle latest_ts={latest_ts}")
        return 0

    # 1) Collect authoritative HTF setup candidates.
    setup_candidates = _collect_htf_setup_candidates(
        candles_df=candles_df,
        htf=htf,
        trend_min=trend_min,
        need_atr=need_atr,
        atr_min=atr_min,
    )

    # 2) Sync candidate state.
    live_candidate_keys = set()
    for setup in setup_candidates:
        ck = _setup_candidate_key(symbol, setup)
        live_candidate_keys.add(ck)
        rec = state.active_candidates.get(ck)
        if rec is None:
            state.active_candidates[ck] = _setup_to_state_record(
                setup,
                latest_ts=latest_ts,
                entry_window_hours=entry_window_hours,
            )
        else:
            rec["last_checked_ts"] = _ts_str(latest_ts)
            rec.setdefault("expires_at", _ts_str(_candidate_expires_at(setup, entry_window_hours=entry_window_hours)))
            rec.setdefault("status", "ACTIVE")

    # 3) Expire candidates whose window is over.
    for ck, rec in list(state.active_candidates.items()):
        exp = _ts(rec.get("expires_at"))
        if pd.notna(exp) and latest_ts > exp:
            rec["status"] = "EXPIRED"
            state.active_candidates.pop(ck, None)

    # 4) Confirm LT entry events from still-active candidates.
    emitted_rows: List[Dict[str, Any]] = []
    for ck, rec in list(state.active_candidates.items()):
        if rec.get("status") != "ACTIVE":
            continue

        setup = _state_record_to_setup(rec)
        entry = find_retest_entry(
            candles=candles_df,
            setup=setup,
            window_hours=float(entry_window_hours),
            tol_atr_mult=float(tol_atr_mult),
            rr=float(rr),
            sl_atr_buffer=float(sl_atr_buffer),
        )
        rec["last_checked_ts"] = _ts_str(latest_ts)

        if entry is None:
            continue

        entry_ts = _ts(getattr(entry, "timestamp", None))
        if pd.isna(entry_ts) or entry_ts > latest_ts:
            continue

        ek = _entry_event_key(symbol, ck, entry)
        if ek in state.consumed_entries:
            rec["status"] = "CONSUMED"
            state.active_candidates.pop(ck, None)
            continue

        emitted_rows.append(
            _entry_to_emit_row(
                symbol=symbol,
                latest_ts=latest_ts,
                candidate_key=ck,
                setup=setup,
                entry=entry,
                entry_key=ek,
            )
        )
        state.consumed_entries[ek] = {
            "candidate_key": ck,
            "consumed_at": _ts_str(latest_ts),
            "entry_timestamp": _ts_str(entry_ts),
        }
        rec["status"] = "CONSUMED"
        state.active_candidates.pop(ck, None)

    written = 0
    if emitted_rows:
        out_df = pd.DataFrame(emitted_rows)
        _append_rows(out_csv, out_df)
        written = int(len(out_df))

    state.last_seen_ts = latest_ts
    state.save()

    if debug:
        print(
            json.dumps(
                {
                    "event": "symbol_cycle_done",
                    "symbol": symbol,
                    "latest_ts": _ts_str(latest_ts),
                    "setup_candidates_seen": len(setup_candidates),
                    "active_candidates": len(state.active_candidates),
                    "written_confirmed_entries": written,
                    "consumed_entries_total": len(state.consumed_entries),
                },
                ensure_ascii=False,
            )
        )

    return written


def main(argv: Optional[Iterable[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Minimal live shell for TRUE ENTRY EVENT observation.")
    ap.add_argument("--symbols", default="BTCUSDT")
    ap.add_argument("--bybit_category", default="linear")
    ap.add_argument("--bybit_interval", default="15")
    ap.add_argument("--bybit_candles", type=int, default=500)
    ap.add_argument("--state_dir", default="backtest/journal/exports_live/true_entry_state")
    ap.add_argument("--out_csv", default="backtest/journal/exports_live/live_true_entry_events.csv")
    ap.add_argument("--poll_seconds", type=int, default=30)
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--debug", action="store_true")

    # HTF -> LT lifecycle knobs reused from validated modules.
    ap.add_argument("--htf", default="4h")
    ap.add_argument("--entry-window-hours", type=float, default=48.0)
    ap.add_argument("--tol-atr-mult", type=float, default=0.25)
    ap.add_argument("--rr", type=float, default=3.0)
    ap.add_argument("--sl-atr-buffer", type=float, default=0.25)

    # Optional HTF gate knobs from the same HTF/LTF pipeline.
    ap.add_argument("--trend-min", type=float, default=0.0)
    ap.add_argument("--need-atr", action="store_true")
    ap.add_argument("--atr-min", type=float, default=0.0015)

    args = ap.parse_args(list(argv) if argv is not None else None)

    symbols = _parse_symbols(args.symbols)
    if not symbols:
        raise SystemExit("--symbols empty")

    state_dir = Path(args.state_dir)
    out_csv = Path(args.out_csv)
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
                    state_dir=state_dir,
                    out_csv=out_csv,
                    htf=str(args.htf),
                    entry_window_hours=float(args.entry_window_hours),
                    tol_atr_mult=float(args.tol_atr_mult),
                    rr=float(args.rr),
                    sl_atr_buffer=float(args.sl_atr_buffer),
                    trend_min=float(args.trend_min),
                    need_atr=bool(args.need_atr),
                    atr_min=float(args.atr_min),
                    debug=bool(args.debug),
                )
            except Exception as e:
                print(f"[TRUE_ENTRY][{symbol}] error={type(e).__name__}: {e}")

        total_written += cycle_written
        print(
            json.dumps(
                {
                    "event": "cycle_done",
                    "cycle_ts": _ts_str(cycle_ts),
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