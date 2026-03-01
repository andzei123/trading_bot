from __future__ import annotations

"""
Liquidation context collector (NO API KEY).

- Connects to Bybit v5 Public WebSocket: allLiquidation.{symbol}
- Aggregates liquidation events into candle buckets (default 15m)
- Exposes:
    start_liquidation_stream(symbols) -> starts background WS (thread + asyncio loop)
    get_liquidation_context_sync(symbol, since_ts, until_ts) -> dict
    async get_liquidation_context(symbol, since_ts, until_ts) -> dict
"""

import asyncio
import json
import time
import threading
from dataclasses import dataclass, field
from typing import Dict, Iterable, Optional, Tuple

BYBIT_WS_PUBLIC_LINEAR = "wss://stream.bybit.com/v5/public/linear"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _normalize_ts_to_ms(ts: int) -> int:
    if ts < 10_000_000_000:
        return ts * 1000
    return ts


def _bucket_start_ms(ts_ms: int, bucket_seconds: int) -> int:
    bucket_ms = bucket_seconds * 1000
    return (ts_ms // bucket_ms) * bucket_ms


def _liq_side_to_position_bias(event_side: str) -> str:
    s = (event_side or "").lower()
    if s == "buy":
        return "SHORT"
    if s == "sell":
        return "LONG"
    return "UNKNOWN"


@dataclass
class _Agg:
    count: int = 0
    vol_base: float = 0.0
    vol_quote: float = 0.0
    short_count: int = 0
    long_count: int = 0
    short_vol_quote: float = 0.0
    long_vol_quote: float = 0.0

    def add(self, side_bias: str, v: float, p: float) -> None:
        self.count += 1
        self.vol_base += v
        q = v * p
        self.vol_quote += q
        if side_bias == "SHORT":
            self.short_count += 1
            self.short_vol_quote += q
        elif side_bias == "LONG":
            self.long_count += 1
            self.long_vol_quote += q


@dataclass
class LiquidationCache:
    bucket_seconds: int = 15 * 60
    max_age_seconds: int = 7 * 24 * 60 * 60
    _by_symbol: Dict[str, Dict[int, _Agg]] = field(default_factory=dict)
    # Thread lock: cache is accessed from async WS tasks and sync callers.
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def add_event(self, symbol: str, ts_ms: int, side: str, v: float, p: float) -> None:
        ts_ms = _normalize_ts_to_ms(int(ts_ms))
        b = _bucket_start_ms(ts_ms, self.bucket_seconds)
        side_bias = _liq_side_to_position_bias(side)

        with self._lock:
            sym_map = self._by_symbol.setdefault(symbol, {})
            agg = sym_map.get(b)
            if agg is None:
                agg = _Agg()
                sym_map[b] = agg
            agg.add(side_bias=side_bias, v=v, p=p)
            self._prune_locked(symbol=symbol, now_ms=_now_ms())

    def _prune_locked(self, symbol: str, now_ms: int) -> None:
        cutoff_ms = now_ms - (self.max_age_seconds * 1000)
        sym_map = self._by_symbol.get(symbol)
        if not sym_map:
            return
        for k in [k for k in list(sym_map.keys()) if k < cutoff_ms]:
            sym_map.pop(k, None)

    def get_context(self, symbol: str, since_ms: int, until_ms: int) -> dict:
        start_b = _bucket_start_ms(since_ms, self.bucket_seconds)
        end_b = _bucket_start_ms(until_ms, self.bucket_seconds)

        out = _Agg()
        step = self.bucket_seconds * 1000

        with self._lock:
            sym_map = self._by_symbol.get(symbol, {})
            b = start_b
            while b <= end_b:
                agg = sym_map.get(b)
                if agg:
                    out.count += agg.count
                    out.vol_base += agg.vol_base
                    out.vol_quote += agg.vol_quote
                    out.short_count += agg.short_count
                    out.long_count += agg.long_count
                    out.short_vol_quote += agg.short_vol_quote
                    out.long_vol_quote += agg.long_vol_quote
                b += step

        if out.short_vol_quote > out.long_vol_quote:
            bias = "SHORT"
        elif out.long_vol_quote > out.short_vol_quote:
            bias = "LONG"
        else:
            bias = "NEUTRAL"

        return {
            "symbol": symbol,
            "since_ts_ms": since_ms,
            "until_ts_ms": until_ms,
            "liq_count": out.count,
            "liq_volume_quote": out.vol_quote,
            "liq_volume_base": out.vol_base,
            "liq_short_count": out.short_count,
            "liq_long_count": out.long_count,
            "liq_short_volume_quote": out.short_vol_quote,
            "liq_long_volume_quote": out.long_vol_quote,
            "liq_side_bias": bias,
        }


class BybitLiquidationWS:
    def __init__(
        self,
        symbols: Iterable[str],
        cache: LiquidationCache,
        ws_url: str = BYBIT_WS_PUBLIC_LINEAR,
        reconnect_backoff_s: Tuple[float, float] = (1.0, 20.0),
    ) -> None:
        self.symbols = [str(s).strip().upper() for s in symbols if str(s).strip()]
        self.cache = cache
        self.ws_url = ws_url
        self.reconnect_backoff_s = reconnect_backoff_s
        self._stop = asyncio.Event()

    async def run_forever(self) -> None:
        try:
            import websockets  # type: ignore
        except ModuleNotFoundError:
            # Dependency missing. Fail-open: exit the WS task quietly.
            return

        backoff = self.reconnect_backoff_s[0]
        backoff_max = self.reconnect_backoff_s[1]

        while not self._stop.is_set():
            try:
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20) as ws:
                    sub = {"op": "subscribe", "args": [f"allLiquidation.{s}" for s in self.symbols]}
                    await ws.send(json.dumps(sub))
                    backoff = self.reconnect_backoff_s[0]

                    while not self._stop.is_set():
                        raw = await ws.recv()
                        msg = json.loads(raw)
                        data = msg.get("data")
                        if not data or not isinstance(data, list):
                            continue
                        for e in data:
                            try:
                                ts_ms = int(e.get("T") or msg.get("ts") or 0)
                                symbol = str(e.get("s") or "").upper()
                                side = str(e.get("S") or "")
                                v = float(e.get("v") or 0.0)
                                p = float(e.get("p") or 0.0)
                                if not symbol or ts_ms <= 0 or v <= 0 or p <= 0:
                                    continue
                                # Cache is synchronous (thread-safe via Lock)
                                self.cache.add_event(symbol=symbol, ts_ms=ts_ms, side=side, v=v, p=p)
                            except Exception:
                                continue
            except Exception:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.6, backoff_max)

    async def stop(self) -> None:
        self._stop.set()


# ---------------------------
# Global singleton + background thread
# ---------------------------

_CACHE = LiquidationCache(bucket_seconds=15 * 60)

_BG_LOOP: Optional[asyncio.AbstractEventLoop] = None
_BG_THREAD: Optional[threading.Thread] = None
_BG_READY = threading.Event()
_BG_CLIENT: Optional[BybitLiquidationWS] = None

_WS_START_MS: int = 0


def _thread_main(symbols: list[str]) -> None:
    global _BG_LOOP, _BG_CLIENT, _WS_START_MS
    _WS_START_MS = int(time.time() * 1000)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _BG_LOOP = loop
    _BG_CLIENT = BybitLiquidationWS(symbols=symbols, cache=_CACHE)

    async def _runner():
        _BG_READY.set()
        await _BG_CLIENT.run_forever()

    loop.run_until_complete(_runner())


def start_liquidation_stream(symbols: Iterable[str]) -> bool:
    """Start Bybit liquidation WS background thread (best-effort).

    Returns:
        True  - WS thread is already running, or started successfully
        False - WS cannot start (e.g., websockets missing) or init failed
    """
    global _BG_THREAD

    # If already running, keep it.
    if _BG_THREAD and _BG_THREAD.is_alive():
        return True

    syms = [str(s).strip().upper() for s in symbols if str(s).strip()]
    if not syms:
        return False

    # Preflight dependency check to avoid thread crashes.
    try:
        import importlib.util as _importlib_util
        if _importlib_util.find_spec("websockets") is None:
            return False
    except Exception:
        # Fail-open: if the checker itself fails, don't block the runner.
        return False

    _BG_READY.clear()
    t = threading.Thread(
        target=_thread_main,
        args=(syms,),
        name="bybit_liquidation_ws_bg",
        daemon=True,
    )
    _BG_THREAD = t
    t.start()

    # Wait briefly for init.
    _BG_READY.wait(timeout=5.0)

    # Confirm init succeeded (loop/client exist and thread still alive).
    if not t.is_alive():
        return False
    if _BG_LOOP is None or _BG_CLIENT is None:
        return False

    return True

async def get_liquidation_context(symbol: str, since_ts: int, until_ts: int) -> dict:
    global _WS_START_MS
    since_ms = _normalize_ts_to_ms(int(since_ts))
    until_ms = _normalize_ts_to_ms(int(until_ts))
    if until_ms < since_ms:
        since_ms, until_ms = until_ms, since_ms
    if _WS_START_MS and since_ms < _WS_START_MS:
        since_ms = _WS_START_MS
    # Cache is synchronous; keep async wrapper for compatibility.
    return _CACHE.get_context(symbol=str(symbol).strip().upper(), since_ms=since_ms, until_ms=until_ms)


def get_liquidation_context_sync(symbol: str, since_ts: int, until_ts: int, timeout_s: float = 2.0) -> dict:
    global _WS_START_MS
    symbol = str(symbol).strip().upper()

    since_ms = _normalize_ts_to_ms(int(since_ts))
    until_ms = _normalize_ts_to_ms(int(until_ts))
    if until_ms < since_ms:
        since_ms, until_ms = until_ms, since_ms
    if _WS_START_MS and since_ms < _WS_START_MS:
        since_ms = _WS_START_MS

    if _BG_LOOP is None or not _BG_READY.is_set():
        return {
            "symbol": symbol,
            "since_ts_ms": since_ms,
            "until_ts_ms": until_ms,
            "liq_count": 0,
            "liq_volume_quote": 0.0,
            "liq_volume_base": 0.0,
            "liq_short_count": 0,
            "liq_long_count": 0,
            "liq_short_volume_quote": 0.0,
            "liq_long_volume_quote": 0.0,
            "liq_side_bias": "NEUTRAL",
        }
    # Cache read is synchronous; avoid coroutine APIs here.
    return _CACHE.get_context(symbol=symbol, since_ms=since_ms, until_ms=until_ms)


def _simplify_ctx(ctx: dict) -> dict:
    # Stable keys required by MVP context features
    return {
        "liq_count": int(ctx.get("liq_count", 0) or 0),
        "liq_volume_quote": float(ctx.get("liq_volume_quote", 0.0) or 0.0),
        "liq_bias": str(ctx.get("liq_side_bias", "NEUTRAL") or "NEUTRAL").upper(),
    }


async def get_liquidation_features(symbol: str, since_ts: int, until_ts: int) -> dict:
    """Return liquidation context features (context-only)."""
    ctx = await get_liquidation_context(symbol, since_ts, until_ts)
    return _simplify_ctx(ctx)


def get_liquidation_features_sync(symbol: str, since_ts: int, until_ts: int, timeout_s: float = 2.0) -> dict:
    """Sync version of get_liquidation_features."""
    ctx = get_liquidation_context_sync(symbol, since_ts, until_ts, timeout_s=timeout_s)
    return _simplify_ctx(ctx)
