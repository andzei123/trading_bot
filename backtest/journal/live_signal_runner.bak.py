# backtest/journal/live_signal_runner.py
# Run pipeline and emit entries periodically into CSV.
from __future__ import annotations

import argparse
import time
import random
import inspect
import re
import json
from pathlib import Path
from typing import Optional, Callable, Any

import importlib
import pandas as pd
import numpy as np
import requests
from backtest.live.regime_controller import decide_profile_from_performance
from backtest.live.liquidation import start_liquidation_stream, get_liquidation_context_sync, get_liquidation_features_sync
from backtest.live.context_gate import GateDecision as ContextGateDecision, compute_context_gate


from backtest.live.portfolio import PortfolioConfig, PortfolioState, filter_signals_portfolio
import backtest.journal.filter_trades as ft


def _utc_month_str(ts: pd.Timestamp) -> str:
    """Return YYYY-MM string (UTC)."""
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.strftime("%Y-%m")


def load_monthly_risk_guard_csv(
    csv_path: str,
    *,
    month: Optional[str],
    bad_month_threshold_r: float,
    min_trades: int,
) -> dict[str, str]:
    """Return {symbol: 'OK'|'DEFENSIVE'|'OFF'} for the selected month.

    Expected columns: symbol, period, total_R, trades
    - period: 'YYYY-MM'
    - total_R: sum of R in that month
    - trades: count
    """
    if not csv_path:
        return {}
    p = Path(csv_path)
    if not p.exists():
        return {}

    df = pd.read_csv(p)
    if df.empty:
        return {}

    # Normalize column names
    cols = {c.lower(): c for c in df.columns}
    need = {"symbol", "period", "total_r", "trades"}
    if not need.issubset(set(cols.keys())):
        return {}

    df = df.rename(columns={
        cols["symbol"]: "symbol",
        cols["period"]: "period",
        cols["total_r"]: "total_R",
        cols["trades"]: "trades",
    })

    if month is None or month == "":
        # pick latest period present in file
        month = sorted(df["period"].astype(str).unique())[-1]

    cur = df[df["period"].astype(str) == str(month)].copy()
    if cur.empty:
        return {}

    out: dict[str, str] = {}
    for _, r in cur.iterrows():
        sym = str(r["symbol"]).upper()
        try:
            trades = int(r["trades"])
            total_r = float(r["total_R"])
        except Exception:
            continue

        if trades >= int(min_trades) and total_r <= float(bad_month_threshold_r):
            # MVP: mark defensive (not hard OFF), portfolio will throttle
            out[sym] = "DEFENSIVE"
        else:
            out[sym] = "OK"
    return out


def append_csv(path: str, df: pd.DataFrame) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    header = not p.exists()
    df.to_csv(p, mode="a", header=header, index=False)


# ============================================================
# DASHBOARD STATUS (Streamlit UI)
# Writes a lightweight snapshot for ui/dashboard.py
# ============================================================

LIVE_STATUS_DEFAULT = Path("backtest/journal/live_status.json")
LIVE_CONTROLS_DEFAULT = Path("backtest/journal/live_controls.json")

def _read_live_controls(path: Path) -> dict[str, Any]:
    """Read controls written by Streamlit UI. Safe defaults if file missing/bad."""
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {
        "risk_multiplier": 1.0,
        "freeze_new_signals": False,
        "notes": "",
    }

def _write_live_status(path: Path, payload: dict[str, Any]) -> None:
    """Atomic-ish status write (best effort)."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"[WARN] failed to write live status {path}: {e}")

def _trace(trace_on: bool, msg: str) -> None:
    if trace_on:
        print(f"[{now_utc_str()}] [TRACE] {msg}")


BYBIT_REST = "https://api.bybit.com"

# ============================================================
# D1: manual symbol universe (easy edit)
# ============================================================
MANUAL_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT"]


# ============================================================
# REGIME: one safe decision, no noisy fallback prints
# ============================================================

def _safe_regime_decision(trades_csv: str, window_months: int, min_trades: int, symbol: Optional[str] = None):
    """Return a regime decision.

    If `symbol` is provided and the trades CSV has a `symbol` column, we compute
    the decision on that symbol subset (per-symbol regime).
    """
    try:
        if symbol:
            df = pd.read_csv(trades_csv)
            if "symbol" in df.columns:
                df = df[df["symbol"].astype(str) == str(symbol)]
            # Preferred: a direct DF-based decider if available.
            try:
                from backtest.live.regime_controller_multi import decide_profile_from_df  # type: ignore

                return decide_profile_from_df(df, window_months=int(window_months), min_trades=int(min_trades))
            except Exception:
                # Fallback: write a temp CSV subset and reuse decide_profile_from_performance.
                import tempfile

                with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="") as f:
                    tmp_path = f.name
                    df.to_csv(tmp_path, index=False)
                return decide_profile_from_performance(
                    trades_csv=str(tmp_path),
                    min_trades_month=int(min_trades),
                    window_months=int(window_months),
                    min_trades_window=int(min_trades),
                    defensive_threshold_R=0.0,
                    allow_range_in_defensive=True,
                )

        # Default: global regime
        return decide_profile_from_performance(
            trades_csv=str(trades_csv),
            min_trades_month=int(min_trades),
            window_months=int(window_months),
            min_trades_window=int(min_trades),
            defensive_threshold_R=0.0,
            allow_range_in_defensive=True,
        )
    except Exception as e:
        # Hard-safe fallback: allow everything but keep max_positions=1
        class _Fallback:
            profile = "FALLBACK"
            reason = f"FALLBACK_EXCEPTION (repr {e})"
            enable_trend = True
            enable_range = True
            enable_range_long = False
            enable_range_short = True
            allow_models = None
            block_models = None
            allow_sides = None
            block_sides = None
            allow_phases = None
            block_phases = None
            max_positions = 1

        return _Fallback()


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


# Stable schema for live entries output.
# Even if a run produces 0 rows, we still write an empty CSV with this header
# so downstream tooling can always read the file.
LIVE_ENTRIES_COLUMNS = [
    "timestamp",
    "model",
    "side",
    "entry",
    "sl",
    "tp",
    "rr",
    "ctx_sub_label",
    "regime",
    "trend_dir",
    "trend_strength",
    "atr_pct",
    "phase",
    "symbol",
    "liq_bias",
    "liq_risk_multiplier",
    "risk_multiplier",
    "block_reason",
    "context_allow",

    # --- context gate breakdown (B2 audit/dashboard) ---
    "macro_allow",
    "macro_reason",
    "news_allow",
    "news_reason",
    "liq_allow",
    "liq_reason",
]



def _ensure_live_entries_csv(path: Path) -> None:
    """Ensure live_entries.csv exists and has the locked schema header.

    NOTE: Do NOT truncate if file already exists (multi-symbol safe).
    """
    try:
        _ensure_parent(path)
        # Only create header if file is missing or empty
        if (not path.exists()) or (path.stat().st_size == 0):
            pd.DataFrame(columns=LIVE_ENTRIES_COLUMNS).to_csv(path, index=False)
    except Exception:
        pass


def _build_portfolio_cfg():
    """
    Build PortfolioConfig safely across different versions of PortfolioConfig.

    This runner is used across multiple repo versions where PortfolioConfig fields may differ.
    We inspect the real __init__ signature and only pass supported kwargs.
    """
    try:
        sig = inspect.signature(PortfolioConfig.__init__)
        params = set(sig.parameters.keys()) - {"self"}
    except Exception:
        params = set()

    # Desired values (MVP)
    desired = {
        # current portfolio config (your output)
        "max_signals_per_cycle": 3,
        "per_symbol_cooldown_candles": 6,
        "max_1_signal_per_candle_per_symbol": True,

        # common aliases across versions
        "max_open_positions": 3,
        "max_positions": 3,
        "defensive_max_positions": 1,
        "per_symbol_cooldown": 6,
        "cooldown_candles": 6,
        "max_signals_per_symbol": 1,
        "max_per_symbol_per_candle": 1,
    }

    kwargs = {k: v for k, v in desired.items() if (not params) or (k in params)}

    # Try instantiate; if something slips through, remove unknown keys and retry.
    def _try_make(kw: dict):
        try:
            return PortfolioConfig(**kw)
        except TypeError as e:
            msg = str(e)
            m = re.search(r"unexpected keyword argument '([^']+)'", msg)
            if m:
                bad = m.group(1)
                kw2 = dict(kw)
                kw2.pop(bad, None)
                if kw2 != kw:
                    return _try_make(kw2)
            raise

    if kwargs:
        try:
            return _try_make(kwargs)
        except Exception:
            pass

    # Last resort: no-arg constructor
    return PortfolioConfig()


def _load_portfolio_state(path: str) -> PortfolioState:
    """Load PortfolioState from a JSON file path (project implementation).

    This repo's PortfolioState expects a `path` in the constructor and exposes an
    instance `.load()` method that mutates `self` and returns None.
    """
    st = PortfolioState(path)
    st.load()
    return st


def _diag_no_entries(symbol: str, ctx: "pd.DataFrame", lookback: int = 200) -> None:
    try:
        if ctx is None or len(ctx) == 0:
            print(f"[{now_utc_str()}] [DIAG:{symbol}] ctx empty")
            return

        tail = ctx.tail(int(lookback) if lookback else 200).copy()

        last = tail.iloc[-1]
        last_ts = last.get("timestamp", None)
        last_phase = last.get("phase", None)
        last_sub = last.get("ctx_sub_label", None)
        last_trend = last.get("trend_dir", None)

        phase_top = tail["phase"].value_counts().head(3).to_dict() if "phase" in tail.columns else {}
        sub_top = tail["ctx_sub_label"].value_counts().head(5).to_dict() if "ctx_sub_label" in tail.columns else {}

        sub_vals = set(str(x) for x in tail["ctx_sub_label"].dropna().unique()) if "ctx_sub_label" in tail.columns else set()
        has_tdp = any(s.startswith("TDP_") for s in sub_vals)
        has_tts = any(s.startswith("TTS_") for s in sub_vals)

        hint = []
        if not has_tdp and not has_tts:
            hint.append("no TDP/TTS labels in lookback -> no setups expected")
        else:
            if has_tdp:
                hint.append("TDP labels seen")
            if has_tts:
                hint.append("TTS labels seen")
            hint.append("but entry_model returned 0 -> likely filters (impulse/trend/retest/rr/sl)")

        print(
            f"[{now_utc_str()}] [DIAG:{symbol}] "
            f"last_ts={last_ts} phase={last_phase} sub={last_sub} trend={last_trend} "
            f"| phase_top={phase_top} sub_top={sub_top} | " + "; ".join(hint)
        )
    except Exception as e:
        print(f"[{now_utc_str()}] [DIAG:{symbol}] failed: {e}")



def _entry_to_row(e, symbol: str) -> dict:
    """Map an Entry-like object to a locked CSV schema row dict.

    This prevents column shifts (e.g., meta ending up in rr) by using explicit keys.
    """
    # timestamp must be tz-aware UTC
    ts = pd.to_datetime(getattr(e, "timestamp", None), utc=True, errors="coerce")

    entry = float(getattr(e, "entry", float("nan")))
    sl = float(getattr(e, "sl", float("nan")))
    tp = float(getattr(e, "tp", float("nan")))
    side = str(getattr(e, "side", "")).upper()

    # Robust RR from entry/sl/tp
    rr_val = float("nan")
    try:
        if side == "LONG":
            risk = entry - sl
            reward = tp - entry
        else:  # SHORT
            risk = sl - entry
            reward = entry - tp
        if risk > 0:
            rr_val = reward / risk
    except Exception:
        pass

    return {
        "timestamp": ts,
        "model": getattr(e, "model", None),
        "side": side,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "rr": rr_val,
        "ctx_sub_label": getattr(e, "ctx_sub_label", None),
        "regime": getattr(e, "regime", None),
        "trend_dir": getattr(e, "trend_dir", None),
        "trend_strength": getattr(e, "trend_strength", None),
        "atr_pct": getattr(e, "atr_pct", None),
        "phase": getattr(e, "phase", None),
        "symbol": symbol,
        "liq_bias": None,
        "liq_risk_multiplier": None,
    }


def _liq_gate_decision(bybit_symbol: str, candles: pd.DataFrame, latest_ts: pd.Timestamp, bybit_interval: str) -> ContextGateDecision:
    """Create a GateDecision from liquidation context (MVP).
    allow_trade is always True (liq can reduce risk; directional blocks stay in existing liq filter logic).
    """
    try:
        until_ms = int(pd.to_datetime(latest_ts, utc=True).timestamp() * 1000)
        since_ms = until_ms - (int(bybit_interval) * 60 * 1000)

        liq_f = get_liquidation_features_sync(str(bybit_symbol), since_ms, until_ms)
        liq_bias = str(liq_f.get("liq_bias", "NEUTRAL")).upper()
        liq_vol_q = float(liq_f.get("liq_volume_quote", 0.0) or 0.0)

        candle_notional = 0.0
        try:
            candle_notional = float(candles["close"].iloc[-1]) * float(candles["volume"].iloc[-1])
        except Exception:
            candle_notional = 0.0

        risk_multiplier = 1.0
        if candle_notional > 0 and liq_vol_q >= 0.02 * candle_notional:
            risk_multiplier = 0.5

        return ContextGateDecision(
            allow_trade=True,
            risk_multiplier=float(risk_multiplier),
            reason=f"LIQ_GATE: {liq_bias} vol_q={liq_vol_q:.0f}",
        )
    except Exception:
        return ContextGateDecision(allow_trade=True, risk_multiplier=1.0, reason="LIQ_GATE: fallback")

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
    decision,
    risk_guard_status: str = "OK",
    risk_guard_action: str = "defensive",
    emit_last_candles: int = 0,
    from_ts: str = "",
    portfolio_state_path: str = "backtest/journal/exports_live/portfolio_state.json",
    disable_portfolio: bool = False,
    debug_regime: bool = False,
    once: bool = False,
    diag: bool = False,
    diag_lookback: int = 200,
    debug_entry_filters: bool = False,
    # dashboard/trace
    status_sink: Optional[Callable[[dict[str, Any]], None]] = None,
    trace_on: bool = False,
    status_candles_n: int = 96,
    live_controls: Optional[dict[str, Any]] = None,
) -> int:
    # load candles
    if source == "bybit":
        candles = load_bybit_latest(bybit_category, bybit_symbol, bybit_interval, bybit_candles)
    else:
        candles, _ = ft.load_inputs(source=source)

    if candles is None or candles.empty:
        print(f"[{now_utc_str()}] No candles")
        _ensure_live_entries_csv(out_csv)
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

    # --- DASHBOARD/TRACE base ---
    if live_controls is None:
        live_controls = {}
    try:
        n_c = int(status_candles_n or 96)
    except Exception:
        n_c = 96
    n_c = max(1, min(500, n_c))
    candles_tail = candles.tail(n_c).copy()
    candles_last: list[dict[str, Any]] = []
    try:
        for _, r in candles_tail.iterrows():
            candles_last.append({
                "timestamp": str(pd.to_datetime(r["timestamp"], utc=True)),
                "open": float(r["open"]) if "open" in candles_tail.columns and pd.notna(r.get("open")) else None,
                "high": float(r["high"]) if "high" in candles_tail.columns and pd.notna(r.get("high")) else None,
                "low": float(r["low"]) if "low" in candles_tail.columns and pd.notna(r.get("low")) else None,
                "close": float(r["close"]) if "close" in candles_tail.columns and pd.notna(r.get("close")) else None,
                "volume": float(r["volume"]) if "volume" in candles_tail.columns and pd.notna(r.get("volume")) else None,
                # optional ctx columns if present (for dashboard overlays)
                "phase": str(r["phase"]) if "phase" in candles_tail.columns and pd.notna(r.get("phase")) else None,
                "sub_label": str(r["sub_label"]) if "sub_label" in candles_tail.columns and pd.notna(r.get("sub_label")) else None,
                "ctx_sub_label": str(r["ctx_sub_label"]) if "ctx_sub_label" in candles_tail.columns and pd.notna(r.get("ctx_sub_label")) else None,
            })
    except Exception:
        candles_last = []

    trace_counts: dict[str, Any] = {
        "entries_from_model": 0,
        "after_latest_candle_filter": None,
        "after_context": None,
        "after_regime": None,
        "after_emit_last_candles": None,
        "after_portfolio": None,
        "after_last_seen": None,
        "written_to_csv": 0,
        "skip_reason": "",
    }

    def _status(event: str, **extra: Any) -> None:
        if status_sink is None:
            return
        payload = {
            "symbol": str(bybit_symbol),
            "event": str(event),
            "updated_at_utc": now_utc_str(),
            "latest_ts": str(latest_ts),
            "controls": dict(live_controls or {}),
            "trace": dict(trace_counts),
            "candles_last": candles_last,
        }
        payload.update(extra)
        try:
            status_sink(payload)
        except Exception:
            pass



    # Heartbeat/state: update last_seen as soon as we detect a NEW candle.
    if last_ts is None or latest_ts > last_ts:
        _write_state(state_path, latest_ts)

    # If this candle was already processed, skip full pipeline in LIVE mode.
    # In --once backfill/debug mode (emit_last_candles > 1) we still want to run.
    if last_ts is not None and latest_ts <= last_ts:
        # Backfill/debug mode: if user asked to emit more than 1 candle,
        # we still run the pipeline even if latest candle already processed.
        if int(emit_last_candles or 0) > 1:
            pass
        else:
            # still update last_seen to latest_ts (recreate file if missing)
            _write_state(state_path, latest_ts)
            # emit heartbeat so dashboard keeps candles/ctx visible even without new signals
            _status(event="no_new_candle", latest_ts=str(latest_ts), candles_last=candles_last, trace={"note":"no new candle"})
            print(f"[{now_utc_str()}] no new candle ({latest_ts})")
            _ensure_live_entries_csv(out_csv)
            return 0

    # ------------------------------------------------------------
    # Generate all candidate entries; RegimeDecision filters decide what survives.
    # ------------------------------------------------------------

    # build ctx and generate entries
    ctx = ft.build_ctx(candles)
    entries = generate_entries_from_ctx(
        ctx,
        enable_trend=True,
        enable_range_short=True,
        enable_range_long=False,

        rr=rr,
        sl_atr_buffer=sl_atr_buffer,
        require_impulse_before_tdp=require_impulse_before_tdp,
        impulse_lookback=impulse_lookback,
        impulse_size_atr=impulse_size_atr,
        tdp_dev_lookback=tdp_dev_lookback,
        tts_retest_lookback=tts_retest_lookback,
        debug_long_funnel=bool(debug_entry_filters),
    )
    if debug_regime:
        try:
            models = sorted({str(getattr(e, 'model', '')).upper() for e in (entries or [])})
            print(f"[{now_utc_str()}] [DEBUG] raw entries={len(entries)} models={models}")
        except Exception as _e:
            print(f"[{now_utc_str()}] [DEBUG] raw entries debug failed: {_e}")
    # TRACE/STATUS: after entry_model
    try:
        models = sorted({str(getattr(e, "model", "")).upper() for e in (entries or [])})
    except Exception:
        models = []
    trace_counts["entries_from_model"] = int(len(entries or []))
    _trace(trace_on, f"entries_from_model={len(entries or [])} models={models}")

    # Small preview for dashboard (JSON-serializable)
    entries_preview = []
    try:
        for _e in (entries or [])[:50]:
            if isinstance(_e, dict):
                d = _e
            else:
                d = getattr(_e, "__dict__", {}) or {}
            item = {}
            for k in ("timestamp", "model", "side", "entry", "sl", "tp", "rr", "block_reason"):
                if k in d:
                    item[k] = d.get(k)
            if "timestamp" in item and item["timestamp"] is not None:
                item["timestamp"] = str(item["timestamp"])
            entries_preview.append(item)
    except Exception:
        entries_preview = []

    _status("entries_from_model", models=models, entries_preview=entries_preview)


    if not entries:
        if diag:
            _diag_no_entries(symbol=str(bybit_symbol), ctx=ctx, lookback=diag_lookback)

        if debug_regime:
            print(f"[{now_utc_str()}] [DEBUG] no entries generated by entry_model")

        # status for dashboard (so chart still updates even when no signals)
        ctx_tail = {}
        try:
            if "timestamp" in ctx.columns:
                ctx_tail = {
                    "last_ts": str(pd.to_datetime(ctx["timestamp"].iloc[-1], utc=True)),
                    "last_phase": str(ctx["phase"].iloc[-1]) if "phase" in ctx.columns else None,
                    "last_ctx_sub_label": str(ctx["ctx_sub_label"].iloc[-1]) if "ctx_sub_label" in ctx.columns else None,
                }
        except Exception:
            ctx_tail = {}
        _status(
            "no_entries",
            symbol=str(bybit_symbol),
            last_ts=ctx_tail.get("last_ts"),
            candles_last=candles_last,
            ctx_last=ctx_tail,
        )

        _ensure_live_entries_csv(out_csv)
        return 0

    df_e = pd.DataFrame([_entry_to_row(e, str(bybit_symbol)) for e in entries])
    # --- normalize symbol field ---
    if "symbol" not in df_e.columns and "sym" in df_e.columns:
        df_e["symbol"] = df_e["sym"]

    # enforce types (stable comparisons / CSV)
    if not df_e.empty:
        df_e["timestamp"] = pd.to_datetime(df_e.get("timestamp"), utc=True, errors="coerce")
        if "rr" in df_e.columns:
            df_e["rr"] = pd.to_numeric(df_e["rr"], errors="coerce")

    # normalize timestamp early (UTC)
    if "timestamp" in df_e.columns:
        df_e["timestamp"] = pd.to_datetime(df_e["timestamp"], utc=True, errors="coerce")

    # --- LIVE sanity filter: keep only entries on latest candle ---
    if (not once) and ("timestamp" in df_e.columns):
        df_e = df_e[df_e["timestamp"] == latest_ts].copy()
    trace_counts["after_latest_candle_filter"] = int(len(df_e))
    _trace(trace_on, f"after_latest_candle_filter={len(df_e)}")
    _status("after_latest_candle_filter")


    if df_e.empty:
        _ensure_live_entries_csv(out_csv)
        return 0

    from_ts_removed_all = False
    cutoff_ts = None

    # --- AUDIT cutoff: apply --from_ts only in --once mode ---

    if once and from_ts and ("timestamp" in df_e.columns):
        cutoff_ts = pd.to_datetime(str(from_ts).strip(), utc=True, errors="coerce")
        if pd.notna(cutoff_ts):
            before_from = len(df_e)
            df_e = df_e[df_e["timestamp"] >= cutoff_ts].copy()
            after_from = len(df_e)

            from_ts_removed_all = (before_from > 0 and after_from == 0)

            if debug_regime:
                print(
                    f"[{now_utc_str()}] [DEBUG] from_ts filter: from_ts={cutoff_ts} kept={after_from} (from {before_from})")

            # IMPORTANT: if from_ts removed all, do NOT run context/regime/portfolio logs
            if from_ts_removed_all:
                if debug_regime:
                    print(f"[{now_utc_str()}] [DEBUG] df_e=0 because from_ts removed all entries (from_ts={cutoff_ts})")
                _ensure_live_entries_csv(out_csv)
                return 0

    # ============================================================
    # CONTEXT GATE (MACRO + NEWS + LIQ) - MVP integrator
    # ============================================================
    try:
        macro_dir = Path("data")  # expects BTC.D_4h.csv + TOTAL3_4h.csv
        news_csv = Path("data/news_events.csv")

        liq_gate = _liq_gate_decision(
            str(bybit_symbol),
            candles=candles,
            latest_ts=latest_ts,
            bybit_interval=str(bybit_interval),
        )

        ctx_gate = compute_context_gate(
            macro_dir=str(macro_dir),
            news_events_csv=str(news_csv),
            liq_gate=ContextGateDecision(
                liq_gate.allow_trade,
                liq_gate.risk_multiplier,
                liq_gate.reason,
            ),
        )

        print(
            f"[{now_utc_str()}] [CONTEXT] "
            f"allow={ctx_gate.allow_trade} "
            f"risk={ctx_gate.risk_multiplier} "
            f"reason={ctx_gate.reason}"
        )
        if debug_regime:
            try:
                print(
                    f"[{now_utc_str()}] [CONTEXT] macro_allow={getattr(ctx_gate, 'macro_allow', None)} macro_reason={getattr(ctx_gate, 'macro_reason', None)}")
                print(
                    f"[{now_utc_str()}] [CONTEXT] news_allow={getattr(ctx_gate, 'news_allow', None)} news_reason={getattr(ctx_gate, 'news_reason', None)}")
                print(
                    f"[{now_utc_str()}] [CONTEXT] liq_allow={getattr(ctx_gate, 'liq_allow', None)} liq_reason={getattr(ctx_gate, 'liq_reason', None)}")
            except Exception:
                pass

        trace_counts["after_context"] = int(len(df_e))
        trace_counts["skip_reason"] = "" if ctx_gate.allow_trade else str(ctx_gate.reason)
        _trace(trace_on, f"after_context={len(df_e)} allow={ctx_gate.allow_trade} risk={ctx_gate.risk_multiplier} reason={ctx_gate.reason}")
        _status("after_context", context_allow=bool(ctx_gate.allow_trade), context_reason=str(ctx_gate.reason), context_risk=float(ctx_gate.risk_multiplier))
        # always stamp context fields
        df_e["risk_multiplier"] = float(getattr(ctx_gate, "risk_multiplier", 1.0))
        df_e["block_reason"] = "" if ctx_gate.allow_trade else str(getattr(ctx_gate, "reason", "CONTEXT_BLOCK"))




        # If blocked, we STILL write rows in --once (audit), so we can inspect why.
        if not ctx_gate.allow_trade:
            if once:
                # continue to CSV write (blocked rows will be written)
                pass
            else:
                _ensure_live_entries_csv(out_csv)
                return 0

        df_e["risk_multiplier"] = float(ctx_gate.risk_multiplier)
        # Only mark block_reason when trade is actually blocked
        df_e["block_reason"] = str(ctx_gate.reason) if (not ctx_gate.allow_trade) else ""

        # --- context gate breakdown (for audit / dashboard B2) ---
        df_e["context_allow"] = bool(getattr(ctx_gate, "allow_trade", True))

        df_e["macro_allow"] = getattr(ctx_gate, "macro_allow", None)
        df_e["macro_reason"] = getattr(ctx_gate, "macro_reason", None)

        df_e["news_allow"] = getattr(ctx_gate, "news_allow", None)
        df_e["news_reason"] = getattr(ctx_gate, "news_reason", None)

        df_e["liq_allow"] = getattr(ctx_gate, "liq_allow", None)
        df_e["liq_reason"] = getattr(ctx_gate, "liq_reason", None)


    except Exception as _e:

        print(f"[{now_utc_str()}] [CONTEXT][WARN] fallback: {repr(_e)}")

        # safe fallback context

        df_e["risk_multiplier"] = 1.0

        df_e["block_reason"] = ""

        df_e["context_allow"] = True

        df_e["macro_allow"] = None

        df_e["macro_reason"] = "CTX_FALLBACK"

        df_e["news_allow"] = None

        df_e["news_reason"] = "CTX_FALLBACK"

        df_e["liq_allow"] = None

        df_e["liq_reason"] = "CTX_FALLBACK"

    # ============================================================
    # Filtravimas pagal decision (RegimeDecision = vienas šaltinis)
    # ============================================================

    # normalize
    df_e["model"] = df_e["model"].astype(str).str.upper()
    df_e["side"] = df_e["side"].astype(str).str.upper()
    df_e["phase"] = df_e["phase"].astype(str).str.upper()

    def _upper_list(v):
        if v is None:
            return []
        try:
            return [str(x).strip().upper() for x in list(v) if str(x).strip()]
        except Exception:
            return []

    allow_models = _upper_list(getattr(decision, "allow_models", None))
    block_models = _upper_list(getattr(decision, "block_models", None))
    allow_sides = _upper_list(getattr(decision, "allow_sides", None))
    block_sides = _upper_list(getattr(decision, "block_sides", None))
    allow_phases = _upper_list(getattr(decision, "allow_phases", None))
    block_phases = _upper_list(getattr(decision, "block_phases", None))

    # === RegimeDecision hard filters ===
    if allow_models:
        df_e = df_e[df_e["model"].isin(allow_models)].copy()

    if block_models:
        df_e = df_e[~df_e["model"].isin(block_models)].copy()

    if allow_sides:
        df_e = df_e[df_e["side"].isin(allow_sides)].copy()

    if block_sides:
        df_e = df_e[~df_e["side"].isin(block_sides)].copy()

    if allow_phases:
        df_e = df_e[df_e["phase"].isin(allow_phases)].copy()

    if block_phases:
        df_e = df_e[~df_e["phase"].isin(block_phases)].copy()

    
    trace_counts["after_regime"] = int(len(df_e))
    _trace(trace_on, f"after_regime={len(df_e)}")
    _status("after_regime", allow_models=allow_models, block_models=block_models, allow_sides=allow_sides, block_sides=block_sides, allow_phases=allow_phases, block_phases=block_phases)
    if df_e.empty:
        if debug_regime:
            print(f"[{now_utc_str()}] [DEBUG] after allow/block df_e=0 (regime filtered everything)")
        print(f"[{now_utc_str()}] [REGIME] blocked all signals")
        _ensure_live_entries_csv(out_csv)
        return 0

    # ------------------------------------------------------------
    # emit_last_candles: keep only signals from the most recent N candles (including the latest).
    # Convention: N=1 -> only latest candle, N=6 -> latest 6 candles, etc.
    emit_n = int(emit_last_candles or 0)
    if emit_n > 0 and "timestamp" in df_e.columns:
        try:
            latest_candle_ts = pd.to_datetime(candles["timestamp"].iloc[-1], utc=True)
            span = max(0, emit_n - 1)
            cutoff = latest_candle_ts - pd.Timedelta(minutes=int(bybit_interval) * span)
            before = len(df_e)
            df_e["timestamp"] = pd.to_datetime(df_e["timestamp"], utc=True, errors="coerce")
            df_e = df_e.dropna(subset=["timestamp"])
            df_e = df_e[df_e["timestamp"] >= cutoff].copy()
            if debug_regime:
                print(f"[{now_utc_str()}] [DEBUG] emit_last_candles={emit_n} cutoff={cutoff} kept={len(df_e)}")

            trace_counts["after_emit_last_candles"] = int(len(df_e))
            _trace(trace_on, f"after_emit_last_candles={len(df_e)} cutoff={cutoff}")
            _status("after_emit_last_candles", emit_last_candles=int(emit_n), cutoff=str(cutoff))
            if df_e.empty:
                if once and debug_regime:
                    print(f"[{now_utc_str()}] [DEBUG] emit_last_candles removed all -> skip portfolio")
                _ensure_live_entries_csv(out_csv)
                return 0
        except Exception as _e:
            if debug_regime:
                print(f"[{now_utc_str()}] [DEBUG] emit_last_candles filter failed: {_e}")

    # ============================================================
    # D2: Portfolio / risk manager (MVP)
    # ============================================================
    if not disable_portfolio:
        try:
            cfg = _build_portfolio_cfg()

            # Monthly risk guard (per-symbol). Defaults:
            # - DEFENSIVE: tighter throttles (still allows signals)
            # - OFF:      drop all signals for this symbol (if action=off)
            if risk_guard_status in ("DEFENSIVE", "OFF"):
                print(f"[{now_utc_str()}] [RISK_GUARD] {bybit_symbol}: {risk_guard_status} (action={risk_guard_action})")
                if risk_guard_status == "OFF" and str(risk_guard_action).lower() == "off":
                    # Block all signals for this symbol.
                    _ensure_live_entries_csv(out_csv)
                    return 0
                if risk_guard_status == "DEFENSIVE":
                    # Prop-safe throttles (conservative but keeps signal flow).
                    cfg.max_signals_per_cycle = min(int(getattr(cfg, "max_signals_per_cycle", 1)), 1)
                    cfg.per_symbol_cooldown_candles = max(int(getattr(cfg, "per_symbol_cooldown_candles", 0)), 12)
                    cfg.max_1_signal_per_candle_per_symbol = True

            # If we're backfilling (emit_last_candles > 1), we must not let a "future"
            # live state block historical timestamps. Use a dedicated state file and
            # relax throttles so you can observe the raw signal flow.
            state_path_use = str(portfolio_state_path)
            if emit_n > 1:
                if state_path_use.endswith(".json"):
                    state_path_use = state_path_use[:-5] + "_backfill.json"
                else:
                    state_path_use = state_path_use + "_backfill.json"

                # relax throttles for test mode
                cfg.per_symbol_cooldown_candles = 0
                cfg.max_1_signal_per_candle_per_symbol = False
                cfg.max_signals_per_cycle = max(int(getattr(cfg, "max_signals_per_cycle", 1)), len(df_e))

            # IMPORTANT:
            # In backfill mode (--emit_last_candles > 1) we want to *inspect* a batch of signals
            # without historical portfolio state blocking older timestamps (negative delta).
            # So we start from a fresh in-memory state and we do NOT persist it to disk.
            backfill_mode = emit_n > 1
            if backfill_mode:
                state = PortfolioState(state_path_use)  # empty state
            else:
                state = _load_portfolio_state(state_path_use)

            # annotate symbol if missing (from runner loop)
            if "symbol" not in df_e.columns:
                df_e["symbol"] = bybit_symbol

            df_e = filter_signals_portfolio(
                signals_df=df_e,
                cfg=cfg,
                state=state,
                bybit_interval_min=int(bybit_interval),
            )
            # Persist portfolio state only in live mode.
            if not backfill_mode:
                state.save()


            trace_counts["after_portfolio"] = int(len(df_e))
            _trace(trace_on, f"after_portfolio={len(df_e)}")
            _status("after_portfolio")
            if df_e.empty:
                if debug_regime:
                    print(f"[{now_utc_str()}] [DEBUG] portfolio filtered everything")
                print(f"[{now_utc_str()}] Portfolio blocked all signals")
                _ensure_live_entries_csv(out_csv)
                return 0



        except Exception as e:
            print(f"[{now_utc_str()}] Portfolio filter error -> fallback: {e}")

    # keep only new entries since last state
    # (skip when backfilling with --emit_last_candles)
    if not emit_last_candles:
        last_ts = _read_state(state_path)
        if last_ts is not None and "timestamp" in df_e.columns:
            df_e["timestamp"] = pd.to_datetime(df_e["timestamp"], utc=True, errors="coerce")
            df_e = df_e[df_e["timestamp"] > last_ts].copy()
    # TRACE/STATUS: after last_seen filter
    if emit_last_candles:
        trace_counts["after_last_seen"] = "SKIP (emit_last_candles/backfill mode)"
    else:
        trace_counts["after_last_seen"] = int(len(df_e))
    _trace(trace_on, f"after_last_seen={trace_counts['after_last_seen']}")
    _status("after_last_seen")


    if df_e.empty:
        _ensure_live_entries_csv(out_csv)
        return 0

    # keep last N rows only
    df_e = df_e.sort_values("timestamp").reset_index(drop=True)
    if live_keep and len(df_e) > int(live_keep):
        df_e = df_e.iloc[-int(live_keep):].reset_index(drop=True)
    # --- force rr numeric ---
    if "rr" in df_e.columns:
        df_e["rr"] = pd.to_numeric(df_e["rr"], errors="coerce")


    # ============================================================
    # LIQUIDATION CONTEXT (MVP)
    # Context-only: adds liq_bias and liq_risk_multiplier
    # Does NOT block or drop trades (blocking handled by CONTEXT GATE above)
    # ============================================================
    try:
        # Use the last full candle window
        until_ms = int(pd.to_datetime(latest_ts, utc=True).timestamp() * 1000)
        since_ms = until_ms - (int(bybit_interval) * 60 * 1000)

        liq_f = get_liquidation_features_sync(str(bybit_symbol), since_ms, until_ms)
        liq_bias = str(liq_f.get("liq_bias", "NEUTRAL")).upper()
        liq_vol_q = float(liq_f.get("liq_volume_quote", 0.0) or 0.0)

        # Candle notional proxy (simple, hardcoded heuristic)
        candle_notional = 0.0
        try:
            candle_notional = float(candles["close"].iloc[-1]) * float(candles["volume"].iloc[-1])
        except Exception:
            candle_notional = 0.0

        risk_multiplier = 1.0
        # Soft risk adjustment only (no trade blocking)
        if candle_notional > 0 and liq_vol_q >= 0.02 * candle_notional:
            risk_multiplier = 0.5

        df_e["liq_bias"] = liq_bias
        df_e["liq_risk_multiplier"] = float(risk_multiplier)



    except Exception:
        # Hard-safe: if liquidation context fails, do not block trades; just fill defaults
        df_e["liq_bias"] = "NEUTRAL"
        df_e["liq_risk_multiplier"] = 1.0

    
    # Apply dashboard controls (optional)
    try:
        rm = float((live_controls or {}).get("risk_multiplier", 1.0))
    except Exception:
        rm = 1.0
    freeze = bool((live_controls or {}).get("freeze_new_signals", False))
    if "risk_multiplier" in df_e.columns:
        df_e["risk_multiplier"] = pd.to_numeric(df_e["risk_multiplier"], errors="coerce").fillna(1.0) * rm
    else:
        df_e["risk_multiplier"] = float(rm)
    df_e["freeze_new_signals"] = bool(freeze)

    if freeze:
        trace_counts["written_to_csv"] = 0
        trace_counts["skip_reason"] = "FREEZE_NEW_SIGNALS"
        _trace(trace_on, "freeze_new_signals=1 -> skip CSV write")
        _status("frozen", freeze_new_signals=True)
        _ensure_live_entries_csv(out_csv)
        return 0

    trace_counts["written_to_csv"] = int(len(df_e))
    _trace(trace_on, f"written_to_csv={len(df_e)} out={out_csv}")
    entries_tail = []
    try:
        cols_keep = [c for c in ["timestamp","symbol","model","side","entry","sl","tp","rr","ctx_sub_label","phase","risk_multiplier","block_reason"] if c in df_e.columns]
        if cols_keep:
            tmp = df_e[cols_keep].tail(10).copy()
            # ensure json serializable
            for c in tmp.columns:
                tmp[c] = tmp[c].astype(str)
            entries_tail = tmp.to_dict("records")
    except Exception:
        entries_tail = []
    _status("written_to_csv", out=str(out_csv), rows=int(len(df_e)), entries_tail=entries_tail)
    df_e = df_e.reindex(columns=LIVE_ENTRIES_COLUMNS)
    _append_csv(out_csv, df_e)

    # update state with newest timestamp
    newest = pd.to_datetime(df_e["timestamp"].iloc[-1], utc=True)
    _write_state(state_path, latest_ts)

    print(f"[{now_utc_str()}] Wrote {len(df_e)} entries -> {out_csv}")
    return int(len(df_e))


def _parse_symbols_arg(s: str) -> list[str]:
    if not s:
        return []
    parts = [x.strip().upper() for x in s.split(",")]
    return [p for p in parts if p]


def _get_symbols(args) -> list[str]:
    # 1) explicit CLI overrides all
    cli_syms = _parse_symbols_arg(getattr(args, "symbols", ""))
    if cli_syms:
        return cli_syms

    # 2) manual list
    if isinstance(MANUAL_SYMBOLS, list) and len(MANUAL_SYMBOLS) > 0:
        return [str(x).strip().upper() for x in MANUAL_SYMBOLS if str(x).strip()]

    # 3) fallback single symbol
    return [str(args.bybit_symbol).strip().upper()]

def _state_path_for_symbol(state_path: str, source: str, sym: str, itv: int) -> Path:
    """Per-symbol+interval last_seen state (legacy folder)."""
    out_dir = Path("backtest/journal/exports_live/state")
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"{source}_{str(sym).strip().upper()}_{int(itv)}_last_seen.txt"




def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", type=str, default="backtest/journal/live_entries.csv")
    p.add_argument("--status_path", type=str, default="backtest/journal/live_status.json", help="Dashboard status JSON (for ui/dashboard.py).")
    p.add_argument("--controls_path", type=str, default="backtest/journal/live_controls.json", help="Dashboard controls JSON (written by ui/dashboard.py).")
    p.add_argument("--status_candles_n", type=int, default=96, help="How many last candles to include in status JSON.")
    p.add_argument("--trace_live", action="store_true", help="Print TRACE stages and write per-symbol status snapshots.")

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

    p.add_argument(
        "--symbols",
        type=str,
        default="",
        help="Comma-separated symbols list, overrides MANUAL_SYMBOLS and --bybit_symbol. Example: BTCUSDT,ETHUSDT,SOLUSDT",
    )

    p.add_argument("--bybit_interval", type=str, default="15")
    p.add_argument("--bybit_candles", type=int, default=300)

    p.add_argument(
        "--regime_perf_csv",
        type=str,
        default="backtest/journal/exports_trades/trades_simulated.csv",
        help="CSV with simulated trades (R units) used for regime decision (e.g. trades_simulated.csv).",
    )

    p.add_argument(
        "--regime_window_months",
        type=int,
        default=3,
        help="How many recent months to use for regime decision (rolling window).",
    )
    p.add_argument(
        "--regime_min_trades",
        type=int,
        default=20,
        help="Minimum trades in rolling window to avoid NEUTRAL.",
    )

    p.add_argument(
        "--debug_regime",
        action="store_true",
        help="Print extra debug for entries->regime->portfolio filtering",
    )
    p.add_argument(
        "--debug_entry_filters",
        action="store_true",
        help="Print why entry_model candidates are rejected (entry filters/debug)"
    )

    p.add_argument(
        "--diag",
        action="store_true",
        help="Print compact diagnostics when no entries are generated.",
    )
    p.add_argument(
        "--diag_lookback",
        type=int,
        default=200,
        help="How many last candles to use in DIAG summary.",
    )

    p.add_argument(
        "--regime_per_symbol",
        action="store_true",
        help="If set, compute regime decision separately per symbol using trades_simulated.csv (requires a 'symbol' column).",
    )

    # STEP 4 (prop-safety): monthly risk guard (per symbol)
    p.add_argument(
        "--risk_guard_csv",
        type=str,
        default="",
        help="Optional CSV with per-symbol monthly totals (from step3). If provided, bot can go DEFENSIVE/OFF per symbol.",
    )
    p.add_argument(
        "--risk_guard_month",
        type=str,
        default="",
        help="Month to evaluate guard for (YYYY-MM). Default: current UTC month.",
    )
    p.add_argument("--risk_guard_bad_month_r", type=float, default=-10.0)
    p.add_argument("--risk_guard_min_trades", type=int, default=20)
    p.add_argument(
        "--risk_guard_action",
        type=str,
        default="defensive",
        choices=["defensive", "off", "none"],
        help="What to do when a symbol is flagged as BAD month: defensive=throttle portfolio; off=block new signals; none=ignore guard.",
    )

    # testing helpers
    p.add_argument(
        "--emit_last_candles",
        type=int,
        default=0,
        help="TEST MODE: emit signals from the last N candles (instead of only newest). Useful to observe flow.",
    )
    p.add_argument(
        "--from_ts",
        default="",
        help='UTC cutoff, e.g. "2026-01-23 15:45:00Z" (if no Z -> still treated as UTC)',
    )
    p.add_argument(
        "--portfolio_state_path",
        type=str,
        default="backtest/journal/exports_live/portfolio_state.json",
        help="Portfolio state JSON path. In emit_last_candles mode a _backfill.json suffix is used.",
    )
    p.add_argument(
        "--disable_portfolio",
        action="store_true",
        help="Disable portfolio filter entirely (TEST ONLY).",
    )

    args = p.parse_args()

    status_path = Path(getattr(args, "status_path", LIVE_STATUS_DEFAULT))
    controls_path = Path(getattr(args, "controls_path", LIVE_CONTROLS_DEFAULT))
    trace_live = bool(getattr(args, "trace_live", False))
    status_candles_n = int(getattr(args, "status_candles_n", 96) or 96)

    # per-loop status collector (aggregated for multi-symbol dashboard)
    def _make_status_sink(status_map: dict[str, Any]):
        def _sink(payload: dict[str, Any]) -> None:
            sym = str(payload.get("symbol", ""))
            if sym:
                status_map[sym] = payload
        return _sink


    symbols = _get_symbols(args)
    if not symbols:
        raise SystemExit("No symbols provided (MANUAL_SYMBOLS empty and no --bybit_symbol).")

    print(f"[BOOT] symbols={symbols} interval={args.bybit_interval} source={args.source}")

    # === LIQUIDATION CONTEXT (Bybit public WS, context-only) ===
    try:
        start_liquidation_stream(symbols)
        print(f"[{now_utc_str()}] [LIQ] WS started for {len(symbols)} symbols")
    except Exception as e:
        print(f"[{now_utc_str()}] [LIQ] WS start failed (ignored): {e}")

    # Ensure state file exists even if no signals are emitted
    if args.state:
        try:
            sp = Path(args.state)
            sp.parent.mkdir(parents=True, exist_ok=True)
            if not sp.exists():
                sp.write_text("", encoding="utf-8")
        except Exception as e:
            print(f"[WARN] could not create state file {args.state}: {e}")

    # STEP4: optional per-symbol monthly risk guard (from step3 CSV)
    risk_guard_map = {}
    if str(getattr(args, "risk_guard_csv", "")).strip():
        try:
            if str(getattr(args, "risk_guard_month", "auto")).lower() == "auto":
                month = _utc_month_str(pd.Timestamp.utcnow().tz_localize("UTC"))
            else:
                month = str(args.risk_guard_month)
            risk_guard_map = load_monthly_risk_guard_csv(
                args.risk_guard_csv,
                month=month,
                bad_month_threshold_r=float(args.risk_guard_bad_month_r),
                min_trades=int(args.risk_guard_min_trades),
            )
            if risk_guard_map:
                print(f"[{now_utc_str()}] [RISK_GUARD] loaded {len(risk_guard_map)} symbol statuses for month={month} from {args.risk_guard_csv}")
        except Exception as e:
            print(f"[{now_utc_str()}] [RISK_GUARD] failed to load CSV -> ignore: {e}")

    out_csv = Path(args.out)
    state_path_base = args.state

    # --- CSV init for --once audits ---
    if args.once:
        try:
            out_csv.parent.mkdir(parents=True, exist_ok=True)
            if out_csv.exists():
                out_csv.unlink()
        except Exception as e:
            print(f"[WARN] could not reset audit CSV {out_csv}: {e}")
    # --------------------------------

    gen = _load_gen_from_entry_model()
    generate_entries_from_ctx = gen

    # Decision cache (for non-per-symbol mode)
    decision_global = None
    if args.once and not args.regime_per_symbol:
        decision_global = _safe_regime_decision(args.regime_perf_csv, args.regime_window_months, args.regime_min_trades)
        print(f"[{now_utc_str()}] [REGIME] {decision_global.profile} | {decision_global.reason}")

    if args.once:
        status_map: dict[str, Any] = {}
        sink = _make_status_sink(status_map)
        live_controls = _read_live_controls(controls_path)

        for sym in symbols:
            args.bybit_symbol = sym
            state_path = _state_path_for_symbol(state_path_base, args.source, sym, args.bybit_interval)

            decision_sym = decision_global if not args.regime_per_symbol else _safe_regime_decision(
                args.regime_perf_csv, args.regime_window_months, args.regime_min_trades, symbol=sym
            )
            if args.regime_per_symbol:
                print(f"[{now_utc_str()}] [REGIME:{sym}] {decision_sym.profile} | {decision_sym.reason}")

            guard_status = risk_guard_map.get(sym, "OK")

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
                decision=decision_sym,
                risk_guard_status=str(guard_status),
                risk_guard_action=str(getattr(args, "risk_guard_action", "defensive")),
                emit_last_candles=int(getattr(args, "emit_last_candles", 0) or 0),
                from_ts=str(getattr(args, "from_ts", "")),
                portfolio_state_path=str(getattr(args, "portfolio_state_path", "backtest/journal/exports_live/portfolio_state.json")),
                disable_portfolio=bool(getattr(args, "disable_portfolio", False)),
                debug_regime=bool(getattr(args, 'debug_regime', False)),
                once=bool(getattr(args, "once", False)),
                diag=bool(getattr(args, "diag", False)),
                diag_lookback=int(getattr(args, "diag_lookback", 200)),
                debug_entry_filters=bool(getattr(args, "debug_entry_filters", False)),
                status_sink=sink,
                trace_on=trace_live,
                status_candles_n=status_candles_n,
                live_controls=live_controls,
            )

        # write aggregated status for dashboard
        _write_live_status(status_path, {
            "updated_at_utc": now_utc_str(),
            "mode": "once",
            "symbols": symbols,
            "controls": live_controls,
            "per_symbol": status_map,
        })
        return

    backoff_s = 10


    while True:
        status_map: dict[str, Any] = {}
        sink = _make_status_sink(status_map)
        live_controls = _read_live_controls(controls_path)

        # decision once per loop (global), unless --regime_per_symbol
        decision_global = None
        if not args.regime_per_symbol:
            decision_global = _safe_regime_decision(args.regime_perf_csv, args.regime_window_months, args.regime_min_trades)
            print(f"[{now_utc_str()}] [REGIME] {decision_global.profile} | {decision_global.reason}")

        for sym in symbols:
            try:
                args.bybit_symbol = sym
                state_path = _state_path_for_symbol(state_path_base, args.source, sym, args.bybit_interval)

                decision_sym = decision_global if not args.regime_per_symbol else _safe_regime_decision(
                    args.regime_perf_csv, args.regime_window_months, args.regime_min_trades, symbol=sym
                )
                if args.regime_per_symbol:
                    print(f"[{now_utc_str()}] [REGIME:{sym}] {decision_sym.profile} | {decision_sym.reason}")

                guard_status = risk_guard_map.get(sym, "OK")

                _ = run_once(
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
                    decision_sym,
                    risk_guard_status=str(guard_status),
                    risk_guard_action=str(getattr(args, "risk_guard_action", "defensive")),
                    emit_last_candles=int(getattr(args, "emit_last_candles", 0) or 0),
                    from_ts=str(getattr(args, "from_ts", "")),
                    portfolio_state_path=str(getattr(args, "portfolio_state_path", "backtest/journal/exports_live/portfolio_state.json")),
                    disable_portfolio=bool(getattr(args, "disable_portfolio", False)),
                    debug_regime=bool(getattr(args, 'debug_regime', False)),
                    once=bool(getattr(args, "once", False)),
                    diag=bool(getattr(args, "diag", False)),
                    diag_lookback=int(getattr(args, "diag_lookback", 200)),
                    debug_entry_filters=bool(getattr(args, "debug_entry_filters", False)),
                    status_sink=sink,
                    trace_on=trace_live,
                    status_candles_n=status_candles_n,
                    live_controls=live_controls,
                )
                backoff_s = 10

            except KeyboardInterrupt:
                return

            except Exception as e:
                if _is_bybit_rate_limit_exception(e):
                    jitter = random.uniform(0.0, 1.0)
                    print(f"[{now_utc_str()}] RATE_LIMIT -> sleep {backoff_s:.0f}s | {e}")
                    time.sleep(min(300, backoff_s) + jitter)
                    backoff_s = min(300, backoff_s * 2)
                    continue

                print(f"[{now_utc_str()}] ERROR: {e}")

        
        # write aggregated status for dashboard (once per loop)
        _write_live_status(status_path, {
            "updated_at_utc": now_utc_str(),
            "mode": "live",
            "symbols": symbols,
            "controls": live_controls,
            "per_symbol": status_map,
        })

        time.sleep(max(1, args.interval))


if __name__ == "__main__":
    main()
