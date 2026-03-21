"""Sprint-5 DEV1 — Execution Quality telemetry (fail-open).

Provides a proxy for execution "expensiveness" (spread + slippage estimate)
and a 0..1 quality score.

Fail-open contract:
- If candles are missing -> exec_quality_score=1.0 (caller should still wrap in try/except).
- Orderbook is optional; when missing, candles proxy is used.

Stage-0 deterministic policy:
- Candle-derived execution quality never uses the live tail candle.
- When at least 2 candles are available, the last fully closed candle is cdf.iloc[-2].
- Spread / liquidity / ATR keep the same formulas; only candle selection is stabilized.
"""

from __future__ import annotations

from typing import Any, Optional
import math

import pandas as pd
import numpy as np


def _clip01(x: float) -> float:
    try:
        if x != x or x == float("inf") or x == float("-inf"):
            return 0.0
        return float(min(1.0, max(0.0, x)))
    except Exception:
        return 0.0


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if v != v or v == float("inf") or v == float("-inf"):
            return float(default)
        return v
    except Exception:
        return float(default)


def _compute_atr_pct(candles_df: pd.DataFrame, period: int = 14) -> float:
    """Compute ATR% proxy from candles (best-effort)."""
    try:
        if candles_df is None or candles_df.empty:
            return 0.0
        c = candles_df.copy()
        for col in ("high", "low", "close"):
            if col not in c.columns:
                return 0.0
        c = c.tail(max(period * 3, period + 2)).copy()
        high = pd.to_numeric(c["high"], errors="coerce")
        low = pd.to_numeric(c["low"], errors="coerce")
        close = pd.to_numeric(c["close"], errors="coerce")
        prev_close = close.shift(1)
        tr = pd.concat([(high - low).abs(), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
        atr = tr.rolling(int(period)).mean().iloc[-1]
        last_close = float(close.iloc[-1])
        if last_close <= 0 or atr != atr:
            return 0.0
        return float(atr / last_close)
    except Exception:
        return 0.0


def estimate_execution_quality(
    df_e: pd.DataFrame,
    candles_df: Optional[pd.DataFrame],
    orderbook_snapshot: Optional[dict[str, Any]] = None,
) -> pd.DataFrame:
    """Estimate execution quality for each entry row in df_e.

    Returns one row per entry with:
      timestamp_utc, symbol, model, side,
      spread_bps, slippage_bps_est, liq_score, exec_quality_score
    """
    if df_e is None or df_e.empty:
        return pd.DataFrame()

    out = df_e.copy()

    for c in ("symbol", "model", "side"):
        if c not in out.columns:
            out[c] = ""

    # Fail-open if no candles
    if candles_df is None or getattr(candles_df, "empty", True):
        out["spread_bps"] = 0.0
        out["slippage_bps_est"] = 0.0
        out["liq_score"] = 1.0
        out["exec_quality_score"] = 1.0
        out["timestamp_utc"] = pd.Timestamp.utcnow()
        return out[[
            "timestamp_utc", "symbol", "model", "side",
            "spread_bps", "slippage_bps_est", "liq_score", "exec_quality_score",
        ]]

    cdf = candles_df.copy()
    try:
        cdf["timestamp"] = pd.to_datetime(cdf["timestamp"], utc=True, errors="coerce")
        cdf = cdf.sort_values("timestamp").dropna(subset=["timestamp"]).reset_index(drop=True)
    except Exception:
        pass

    # Deterministic candle policy: use last fully closed candle instead of the live tail candle.
    used_closed_candle_policy = False
    candle_row_idx = -1
    cdf_for_exec = cdf
    if len(cdf) >= 2:
        cdf_for_exec = cdf.iloc[:-1].copy()
        used_closed_candle_policy = True
        candle_row_idx = len(cdf_for_exec) - 1
    elif len(cdf) == 1:
        cdf_for_exec = cdf.copy()
        candle_row_idx = 0

    used_candle_ts = None
    try:
        if "timestamp" in cdf_for_exec.columns and len(cdf_for_exec):
            _ts = cdf_for_exec["timestamp"].iloc[-1]
            used_candle_ts = "" if pd.isna(_ts) else str(_ts)
    except Exception:
        used_candle_ts = None

    last_close = _safe_float(cdf_for_exec["close"].iloc[-1] if "close" in cdf_for_exec.columns and len(cdf_for_exec) else 0.0, 0.0)
    last_high = _safe_float(cdf_for_exec["high"].iloc[-1] if "high" in cdf_for_exec.columns and len(cdf_for_exec) else last_close, last_close)
    last_low = _safe_float(cdf_for_exec["low"].iloc[-1] if "low" in cdf_for_exec.columns and len(cdf_for_exec) else last_close, last_close)
    last_vol = _safe_float(cdf_for_exec["volume"].iloc[-1] if "volume" in cdf_for_exec.columns and len(cdf_for_exec) else 0.0, 0.0)

    # Stage-0.1 deterministic policy:
    # Do not use live orderbook snapshots for execution-quality scoring.
    # The live top-of-book is inherently mutable across identical --once runs.
    # For strict determinism at this layer, execution quality is always derived
    # from the last fully closed candle. We still log incoming bid/ask for audit.
    spread_bps = 0.0
    used_orderbook = False
    orderbook_present = False
    orderbook_ignored_by_policy = False
    bid = 0.0
    ask = 0.0
    mid = 0.0
    try:
        if orderbook_snapshot:
            orderbook_present = True
            bid = orderbook_snapshot.get("bid", orderbook_snapshot.get("best_bid", None))
            ask = orderbook_snapshot.get("ask", orderbook_snapshot.get("best_ask", None))
            bid = _safe_float(bid, 0.0)
            ask = _safe_float(ask, 0.0)
            if bid > 0 and ask > 0 and ask >= bid:
                mid = (bid + ask) / 2.0
                orderbook_ignored_by_policy = True
    except Exception:
        orderbook_present = False
        orderbook_ignored_by_policy = False

    if not used_orderbook:
        if last_close > 0:
            spread_bps = ((last_high - last_low) / last_close) * 10_000.0
            spread_bps *= 0.20  # damp range->spread
        else:
            spread_bps = 0.0

    atr_pct = _compute_atr_pct(cdf_for_exec, period=14)
    atr_bps = float(atr_pct * 10_000.0)

    # Liquidity proxy from notional volume (log-scaled 0..1)
    notional = max(0.0, last_close * last_vol)
    liq_score = _clip01((math.log10(notional + 1.0) - 5.0) / 5.0)

    slippage_bps_est = float(0.5 * spread_bps + (0.15 * atr_bps) * (1.0 - liq_score))

    cost_bps = spread_bps + slippage_bps_est
    cost_pen = _clip01(cost_bps / 100.0)
    exec_quality_score = _clip01(1.0 - 0.75 * cost_pen - 0.25 * (1.0 - liq_score))

    _btc = out[out["symbol"].astype(str) == "BTCUSDT"].head(1)
    if len(_btc):
        _r = _btc.iloc[0]
        print(
            "[EXEC_CANDLE_POLICY] "
            f"symbol={_r.get('symbol','')} "
            f"model={_r.get('model','')} "
            f"side={_r.get('side','')} "
            f"used_orderbook={used_orderbook} "
            f"orderbook_present={orderbook_present} "
            f"orderbook_ignored_by_policy={orderbook_ignored_by_policy} "
            f"used_closed_candle_policy={used_closed_candle_policy} "
            f"candle_row_idx={candle_row_idx} "
            f"candle_ts={used_candle_ts} "
            f"last_close={last_close:.10f} "
            f"last_high={last_high:.10f} "
            f"last_low={last_low:.10f} "
            f"last_volume={last_vol:.10f} "
            f"spread_bps={spread_bps:.10f} "
            f"slippage_bps_est={slippage_bps_est:.10f} "
            f"exec_quality_score={exec_quality_score:.10f}"
        )



    _dbg = out.head(1)
    if len(_dbg):
        _r = _dbg.iloc[0]
        print(
            "[EXEC_DEBUG_FULL] "
            f"symbol={_r.get('symbol','')} "
            f"model={_r.get('model','')} "
            f"side={_r.get('side','')} "
            f"last_close={last_close:.10f} "
            f"last_high={last_high:.10f} "
            f"last_low={last_low:.10f} "
            f"last_volume={last_vol:.10f} "
            f"spread_bps={spread_bps:.10f} "
            f"slippage_bps_est={slippage_bps_est:.10f} "
            f"liq_score={liq_score:.10f} "
            f"cost_bps={cost_bps:.10f} "
            f"exec_quality_score={exec_quality_score:.10f}"
        )

    out["spread_bps"] = float(spread_bps)
    out["slippage_bps_est"] = float(slippage_bps_est)
    out["liq_score"] = float(liq_score)
    out["exec_quality_score"] = float(exec_quality_score)
    out["timestamp_utc"] = pd.Timestamp.utcnow()

    return out[[
        "timestamp_utc", "symbol", "model", "side",
        "spread_bps", "slippage_bps_est", "liq_score", "exec_quality_score",
    ]]
