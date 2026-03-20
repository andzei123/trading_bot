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
from datetime import datetime, timezone
from typing import Optional, Callable, Any
import os
import importlib
import pandas as pd
import numpy as np
import requests
from backtest.runtime.context_builder import build_context
from backtest.strategy.macro.service import evaluate_macro
from backtest.strategy.router.router import route_phase, route_model
from backtest.execution.signal_emitter import emit_signals
from backtest.execution.handoff import handoff_signals
from backtest.metrics.equity_curve_tracker import update_equity_curve_from_trades
from backtest.live.regime_controller import decide_profile_from_performance
from backtest.live.liquidation import start_liquidation_stream, get_liquidation_context_sync, get_liquidation_features_sync
from backtest.live.context_gate import GateDecision as ContextGateDecision, compute_context_gate
from backtest.live.phase_router import decide_phase
from backtest.live.tts_context import annotate_tts_context, get_tts_context_at
from backtest.risk.policy_engine import (
    evaluate_policy_asset,
    evaluate_policy_budget,
    evaluate_policy_corr_cap,
    evaluate_policy_kill_switch,
    evaluate_policy_portfolio,
    evaluate_policy_sizing,
)
from backtest.execution.idempotency import enforce_idempotency
from backtest.metrics.symbol_performance_tracker import update_symbol_performance
try:
    from backtest.contracts.models import df_to_risk_decisions, df_to_router_decisions
except Exception:
    df_to_risk_decisions = None  # type: ignore
    df_to_router_decisions = None  # type: ignore

from backtest.filters.signal_cluster_filter import apply_signal_cluster_filter
from backtest.risk.policy_engine import evaluate_policy_budget, evaluate_policy_corr_cap
# DEV A (PHASE2): PYRAMID bootstrap telemetry (no side-effects, fail-open)
try:
    from backtest.risk import pyramiding as _pyr  # noqa: F401
    _PYRAMID_OK = True
    _PYRAMID_IMPORT_ERR: str | None = None
except Exception as _e:
    _PYRAMID_OK = False
    _PYRAMID_IMPORT_ERR = repr(_e)

from backtest.live.portfolio import PortfolioConfig, PortfolioState, filter_signals_portfolio
import backtest.journal.filter_trades as ft

# DEV2: Equity Governor (drawdown throttle)
from backtest.risk.equity_governor import EquityGovernor

from ops.feed_watchdog import check_feed_watchdog


# DEV4: macro gate snapshot (safe import across layouts)
try:
    from live.macro_gate import compute_macro_gate  # type: ignore
except Exception:
    try:
        from backtest.live.macro_gate import compute_macro_gate  # type: ignore
    except Exception:
        compute_macro_gate = None  # type: ignore

# DEV4: cross-asset regime (safe import across layouts)
try:
    from live.cross_asset_regime import compute_cross_asset_regime  # type: ignore
except Exception:
    try:
        from backtest.live.cross_asset_regime import compute_cross_asset_regime  # type: ignore
    except Exception:
        compute_cross_asset_regime = None  # type: ignore

# DEV4: cross-asset telemetry should log only once per cycle (not per symbol)
_CROSS_ASSET_LOGGED_TS: str | None = None

# S5 DEV4: volatility regime detector (safe import across layouts)
try:
    from live.volatility_regime_detector import detect_volatility_regime  # type: ignore
except Exception:
    try:
        from backtest.live.volatility_regime_detector import detect_volatility_regime  # type: ignore
    except Exception:
        detect_volatility_regime = None  # type: ignore



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

LIVE_STATUS_DEFAULT = Path("backtest/journal/exports_live/live_status.json")
LIVE_CONTROLS_DEFAULT = Path("backtest/journal/live_controls.json")

# DEV2: equity curve source of truth (written by DEV3 tracker)
EQUITY_CURVE_CSV = Path("backtest/journal/exports_live/equity_curve.csv")
EQUITY_GOVERNOR = EquityGovernor(EQUITY_CURVE_CSV)

DIAG_ENABLED = True
DIAG_FILE = Path("backtest/journal/runner_diagnostics.jsonl")

def _json_safe(v: Any) -> Any:
    try:
        if isinstance(v, (pd.Timestamp, np.datetime64)):
            return str(pd.to_datetime(v, utc=True, errors="coerce"))
        if isinstance(v, Path):
            return str(v)
        if isinstance(v, (np.floating,)):
            x = float(v)
            return x if np.isfinite(x) else None
        if isinstance(v, (np.integer,)):
            return int(v)
        if pd.isna(v):
            return None
    except Exception:
        pass
    return v

def diag_log(event: str, **payload) -> None:
    if not DIAG_ENABLED:
        return
    try:
        rec = {"ts": datetime.now(timezone.utc).isoformat(), "event": event}
        rec.update({k: _json_safe(v) for k, v in payload.items()})
        DIAG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with DIAG_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:
        pass

def _diag_payload_from_row(row: Any, *, symbol: Optional[str] = None) -> dict[str, Any]:
    if isinstance(row, pd.Series):
        get = row.get
    elif isinstance(row, dict):
        get = row.get
    else:
        get = lambda k, default=None: getattr(row, k, default)
    return {
        "symbol": str(symbol or get("symbol")),
        "model": get("model"),
        "sub_label": get("ctx_sub_label", get("sub_label")),
        "phase": get("phase"),
        "side": get("side"),
        "entry": get("entry"),
        "tp": get("tp"),
        "sl": get("sl"),
        "setup_time": str(get("timestamp")),
        "setup_candle": str(get("timestamp")),
        "block_reason": get("block_reason"),
        "setup_phase": get("setup_phase"),
        "current_phase": get("current_phase"),
        "setup_trend_dir": get("setup_trend_dir", get("trend_dir")),
        "current_trend_dir": get("current_trend_dir"),
        "setup_macro_phase": get("setup_macro_phase", get("macro_phase")),
        "current_macro_phase": get("current_macro_phase"),
        "setup_macro_bias": get("setup_macro_bias", get("macro_bias")),
        "current_macro_bias": get("current_macro_bias"),
    }

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

def _pick_top_macro(status_map: dict) -> dict:
    """Pick first available macro snapshot from per_symbol status_map."""
    try:
        for _sym, payload in (status_map or {}).items():
            if not isinstance(payload, dict):
                continue
            mb = payload.get("macro_bias")
            mp = payload.get("macro_phase")
            ms = payload.get("macro_strength")
            if mb is not None or mp is not None or ms is not None:
                return {
                    "macro_bias": mb if mb is not None else "NEUTRAL",
                    "macro_phase": mp if mp is not None else "NA",
                    "macro_strength": ms,
                }
    except Exception:
        pass
    return {"macro_bias": "NEUTRAL", "macro_phase": "NA", "macro_strength": None}


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


def _diag_enabled(*flags: Any) -> bool:
    try:
        return any(bool(f) for f in flags)
    except Exception:
        return False


def _log_phase_authority(
    *,
    symbol: str,
    trend_phase: Any,
    context_phase: Any,
    final_phase: Any,
    authority_source: str,
    enabled: bool,
) -> None:
    if not enabled:
        return
    try:
        print(
            f"[PHASE_AUTHORITY] symbol={symbol} "
            f"trend_phase={trend_phase} "
            f"context_phase={context_phase} "
            f"final_phase={final_phase} "
            f"authority_source={authority_source}"
        )
    except Exception:
        return


def _log_invalidation_check(symbol: str, row: pd.Series, *, ordering_index: int, enabled: bool) -> None:
    if not enabled:
        return
    try:
        print(
            f"[INVALIDATION_CHECK] symbol={symbol} "
            f"model={str(row.get('model', ''))} "
            f"invalidation_reason={str(row.get('setup_close_reason', '')) or 'ACTIVE'} "
            f"ordering_index={ordering_index}"
        )
    except Exception:
        return


BYBIT_REST = "https://api.bybit.com"
BTC_KILL_THRESHOLD_R = -15.0

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


def _print_symbol_perf_contract(trades_csv: str) -> None:
    """
    Phase2 SYMBOL_PERF contract log.
    Always prints, fail-open.

    Contract:
      [SYMBOL_PERF] sharpe=... winrate=... n=... window=... status=OK|DISABLED reason=...
    """
    try:
        from pathlib import Path
        import pandas as pd
        import numpy as np
        import math

        if not trades_csv or not Path(trades_csv).exists():
            print("[SYMBOL_PERF] sharpe=None winrate=None n=0 window=60 status=DISABLED reason=NO_TRADES_FILE")
            return

        df = pd.read_csv(trades_csv, engine="python", on_bad_lines="skip")

        if df is None or df.empty:
            print("[SYMBOL_PERF] sharpe=None winrate=None n=0 window=60 status=DISABLED reason=NO_TRADES")
            return

        # ---- derive R (prefer direct R cols; fallback to outcome+rr) ----
        r = None
        for col in ("R", "r", "realized_r", "realized_R", "pnl_r"):
            if col in df.columns:
                r = pd.to_numeric(df[col], errors="coerce")
                break

        if r is None and "outcome" in df.columns:
            out = df["outcome"].astype(str).str.upper().fillna("")
            rr = pd.to_numeric(df["rr"], errors="coerce") if "rr" in df.columns else pd.Series(1.0, index=df.index)

            r = pd.Series(np.nan, index=df.index, dtype=float)
            win = out.isin(["WIN", "TP", "TP_HIT"])
            loss = out.isin(["LOSS", "SL", "SL_HIT"])
            be = out.isin(["BE", "BREAKEVEN"])

            r.loc[win] = rr.loc[win].astype(float)
            r.loc[loss] = -1.0
            r.loc[be] = 0.0

        if r is None:
            print("[SYMBOL_PERF] sharpe=None winrate=None n=0 window=60 status=DISABLED reason=NO_R_FIELDS")
            return

        r = pd.to_numeric(r, errors="coerce").dropna()
        n_total = int(len(r))
        if n_total <= 0:
            print("[SYMBOL_PERF] sharpe=None winrate=None n=0 window=60 status=DISABLED reason=NO_VALID_R")
            return

        # institutional rolling window: 120 if available else 60 else n
        window = 120 if n_total >= 120 else (60 if n_total >= 60 else n_total)
        rrw = r.tail(window).astype(float).to_numpy()
        n_win = int(len(rrw))

        if n_win <= 0:
            print(f"[SYMBOL_PERF] sharpe=None winrate=None n={n_total} window={window} status=DISABLED reason=EMPTY_WINDOW")
            return

        avg = float(rrw.mean()) if n_win else 0.0
        std = float(rrw.std(ddof=0)) if n_win else 0.0
        sharpe = float((avg / std) * math.sqrt(n_win)) if (n_win > 1 and std > 1e-12) else 0.0
        winrate = float((rrw > 0).mean()) if n_win else 0.0

        print(
            f"[SYMBOL_PERF] sharpe={sharpe:.2f} winrate={winrate:.2f} "
            f"n={n_total} window={window} status=OK reason=COMPUTED"
        )

    except Exception as e:
        print(
            "[SYMBOL_PERF] sharpe=None winrate=None "
            f"n=0 window=60 status=DISABLED reason={type(e).__name__}"
        )

# Stable schema for live entries output.
# Even if a run produces 0 rows, we still write an empty CSV with this header
# so downstream tooling can always read the file.
LIVE_ENTRIES_COLUMNS = [
    "timestamp",
    "signal_ts",
    "setup_time",
    "setup_phase",
    "setup_side",
    "setup_model",
    "setup_rr",
    "setup_expiry_ts",
    "setup_trend_dir",
    "setup_macro_bias",
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
    "macro_allow",
    "macro_reason",
    "macro_bias",
    "macro_bias_mismatch",
    "news_allow",
    "news_reason",
    "liq_allow",
    "liq_reason",
    "freeze_new_signals",
    "setup_age_hours",
    "setup_age_candles",
    "score_rr",
    "score_exec",
    "score_phase_align",
    "score_macro_align",
    "score_liq_align",
    "score_tts",
    "signal_score",
]
# Stable dtypes for live entries (to keep CSV schema consistent even when empty).
# Using pandas nullable boolean dtype for flags.
LIVE_ENTRIES_DTYPES: dict[str, str] = {
    "timestamp": "datetime64[ns, UTC]",
    "signal_ts": "datetime64[ns, UTC]",
    "setup_time": "datetime64[ns, UTC]",
    "setup_phase": "object",
    "setup_side": "object",
    "setup_model": "object",
    "setup_rr": "float64",
    "setup_expiry_ts": "datetime64[ns, UTC]",
    "setup_trend_dir": "object",
    "setup_macro_bias": "object",
    "model": "object",
    "side": "object",
    "entry": "float64",
    "sl": "float64",
    "tp": "float64",
    "rr": "float64",
    "ctx_sub_label": "object",
    "regime": "object",
    "trend_dir": "object",
    "trend_strength": "float64",
    "atr_pct": "float64",
    "phase": "object",
    "symbol": "object",
    "liq_bias": "object",
    "liq_risk_multiplier": "float64",
    "risk_multiplier": "float64",
    "block_reason": "object",
    "context_allow": "boolean",
    "macro_allow": "boolean",
    "macro_reason": "object",
    "macro_bias": "object",
    "macro_bias_mismatch": "boolean",
    "news_allow": "boolean",
    "news_reason": "object",
    "liq_allow": "boolean",
    "liq_reason": "object",
    "freeze_new_signals": "boolean",
    "setup_age_hours": "float64",
    "setup_age_candles": "float64",
    "score_rr": "float64",
    "score_exec": "float64",
    "score_phase_align": "float64",
    "score_macro_align": "float64",
    "score_liq_align": "float64",
    "score_tts": "float64",
    "signal_score": "float64",
}
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


# ------------------------------------------------------------
# Dropped signals sidecar (debug/ops)
# Writes rows that were filtered out, with drop_stage/reason, to <out>_dropped.csv
DROPPED_COLUMNS = list(LIVE_ENTRIES_COLUMNS) + ["drop_stage", "drop_reason", "drop_ts"]

def _dropped_csv_path(out_csv: Path) -> Path:
    return out_csv.with_name(out_csv.stem + "_dropped.csv")


# --- cycle metrics & dropped sidecar (pro diagnostics) ---
def _cycle_metrics_csv_path(out_csv: str) -> str:
    try:
        p = Path(str(out_csv))
        return str(p.with_name(p.stem + "_cycle_metrics.csv"))
    except Exception:
        return str(out_csv) + "_cycle_metrics.csv"

def _ensure_cycle_metrics_csv(out_csv: str) -> str:
    cm_csv = _cycle_metrics_csv_path(out_csv)
    if not Path(cm_csv).exists():
        cols = [
            "cycle_ts","latest_ts","source","interval_min","symbols",
            "once","new_candle","note",
            "raw_entries","kept_after_invalidation","dropped_invalidation",
            "kept_after_live_guard","dropped_live_guard",
            "kept_after_window",
            "after_context","after_regime","after_emit_last_candles",
            "written_entries","written_closed","written_dropped",
        ]
        pd.DataFrame(columns=cols).to_csv(cm_csv, index=False)
    return cm_csv

def _append_cycle_metrics(out_csv: str, row: dict) -> None:
    try:
        cm_csv = _ensure_cycle_metrics_csv(out_csv)
        df = pd.DataFrame([row])
        write_header = not Path(cm_csv).exists() or Path(cm_csv).stat().st_size == 0
        df.to_csv(cm_csv, mode="a", header=write_header, index=False)
    except Exception:
        pass

def _ensure_dropped_csv(out_csv: Path | str) -> Path:
    """Ensure *out*_dropped.csv exists with header."""
    out_p = Path(out_csv)
    p = _dropped_csv_path(out_p)
    try:
        if (not p.exists()) or p.stat().st_size == 0:
            p.parent.mkdir(parents=True, exist_ok=True)
            df0 = pd.DataFrame({c: [] for c in DROPPED_COLUMNS})
            df0.to_csv(p, index=False)
    except Exception:
        pass
    return p


def _append_dropped(out_csv: Path | str, df: pd.DataFrame, *, stage: str, reason: str, drop_ts) -> None:
    try:
        out_p = Path(out_csv)
        if df is None or len(df) == 0:
            _ensure_dropped_csv(out_p)
            return
        _ensure_dropped_csv(out_p)
        df_out = df.copy()

        # Ensure base schema
        for c in LIVE_ENTRIES_COLUMNS:
            if c not in df_out.columns:
                df_out[c] = np.nan

        df_out["drop_stage"] = str(stage)
        df_out["drop_reason"] = str(reason)
        df_out["drop_ts"] = pd.to_datetime(drop_ts, utc=True, errors="coerce")

        # Order columns and append
        df_out = df_out.reindex(columns=DROPPED_COLUMNS)
        df_out.to_csv(_dropped_csv_path(out_p), mode="a", header=False, index=False)
    except Exception:
        # never break live loop due to dropped sidecar
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



def _resolve_cluster_score_mode(
    entries,
    *,
    cluster_score_mode: str = "LEGACY",
    cluster_rank_signal_score: bool = False,
) -> tuple[str, str]:
    """Resolve cluster ranking mode and score field safely.

    Modes:
      - LEGACY: preserve existing behavior exactly (score if present else RR)
      - SIGNAL_SCORE: use canonical signal_score

    Invalid values fail safe to LEGACY.
    The legacy boolean flag is kept only as a backward-compatible alias.
    """
    requested_mode = str(cluster_score_mode or "").strip().upper()

    if requested_mode in ("", "LEGACY") and bool(cluster_rank_signal_score):
        requested_mode = "SIGNAL_SCORE"

    if requested_mode not in {"LEGACY", "SIGNAL_SCORE"}:
        print(
            f"[{now_utc_str()}] [CLUSTER_RANK_MODE] "
            f"invalid_score_mode={requested_mode or 'EMPTY'} fallback=LEGACY"
        )
        requested_mode = "LEGACY"

    if requested_mode == "SIGNAL_SCORE":
        return "SIGNAL_SCORE", "signal_score"

    try:
        if entries and hasattr(entries[0], "score"):
            return "LEGACY", "score"
    except Exception:
        pass
    return "LEGACY", "RR"


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        if not np.isfinite(x):
            return float(default)
        return x
    except Exception:
        return float(default)


def _score_phase_align(side: str, phase: str) -> float:
    side_u = str(side or "").upper()
    phase_u = str(phase or "").upper()
    if (side_u == "LONG") and (phase_u == "PHASE_TREND_UP"):
        return 1.0
    if (side_u == "SHORT") and (phase_u == "PHASE_TREND_DOWN"):
        return 1.0
    if (side_u == "SHORT") and (phase_u == "PHASE_RANGE"):
        return 1.0
    return 0.0


def _score_macro_align(side: str, macro_bias: str, symbol: str) -> float:
    side_u = str(side or "").upper()
    bias_u = str(macro_bias or "").upper()
    sym_u = str(symbol or "").upper()
    if bias_u in ("", "NONE", "NEUTRAL", "NA"):
        return 0.0
    if bias_u == "ALT_SHORT":
        return 1.0 if ((not sym_u.startswith("BTC")) and side_u == "SHORT") else 0.0
    if bias_u == "ALT_LONG":
        return 1.0 if ((not sym_u.startswith("BTC")) and side_u == "LONG") else 0.0
    if bias_u in ("LONG", "UP", "BTC_LONG"):
        return 1.0 if side_u == "LONG" else 0.0
    if bias_u in ("SHORT", "DOWN", "BTC_SHORT"):
        return 1.0 if side_u == "SHORT" else 0.0
    return 0.0


def _score_liq_align(side: str, liq_bias: str) -> float:
    side_u = str(side or "").upper()
    liq_u = str(liq_bias or "").upper()
    if liq_u in ("LONG", "UP", "BUY"):
        return 1.0 if side_u == "LONG" else 0.0
    if liq_u in ("SHORT", "DOWN", "SELL"):
        return 1.0 if side_u == "SHORT" else 0.0
    return 0.0


def _score_tts(side: str, allow_long: Any, allow_short: Any) -> float:
    side_u = str(side or "").upper()
    if side_u == "LONG" and bool(allow_long):
        return 1.0
    if side_u == "SHORT" and bool(allow_short):
        return 1.0
    return 0.0


def _attach_signal_scores(df_e: pd.DataFrame, *, symbol: str) -> pd.DataFrame:
    """Signal Scoring V1: telemetry-safe scoring only."""
    if df_e is None or df_e.empty:
        return df_e

    out = df_e.copy()
    rr = pd.to_numeric(out.get("rr", 0.0), errors="coerce").fillna(0.0).clip(lower=0.0, upper=4.0)
    out["score_rr"] = rr * 0.5
    out["score_exec"] = pd.to_numeric(out.get("exec_quality_score", 0.0), errors="coerce").fillna(0.0).clip(lower=0.0, upper=1.0)

    out["score_phase_align"] = [
        _score_phase_align(side=s, phase=p)
        for s, p in zip(out.get("side", pd.Series(index=out.index, dtype=object)), out.get("phase", pd.Series(index=out.index, dtype=object)))
    ]
    out["score_macro_align"] = [
        _score_macro_align(side=s, macro_bias=b, symbol=symbol)
        for s, b in zip(out.get("side", pd.Series(index=out.index, dtype=object)), out.get("macro_bias", pd.Series(index=out.index, dtype=object)))
    ]
    out["score_liq_align"] = [
        _score_liq_align(side=s, liq_bias=b)
        for s, b in zip(out.get("side", pd.Series(index=out.index, dtype=object)), out.get("liq_bias", pd.Series(index=out.index, dtype=object)))
    ]
    out["score_tts"] = [
        _score_tts(side=s, allow_long=al, allow_short=ash)
        for s, al, ash in zip(
            out.get("side", pd.Series(index=out.index, dtype=object)),
            out.get("tts_allow_long", pd.Series(False, index=out.index)),
            out.get("tts_allow_short", pd.Series(False, index=out.index)),
        )
    ]
    out["signal_score"] = (
        pd.to_numeric(out["score_rr"], errors="coerce").fillna(0.0)
        + pd.to_numeric(out["score_exec"], errors="coerce").fillna(0.0)
        + pd.to_numeric(out["score_phase_align"], errors="coerce").fillna(0.0)
        + pd.to_numeric(out["score_macro_align"], errors="coerce").fillna(0.0)
        + pd.to_numeric(out["score_liq_align"], errors="coerce").fillna(0.0)
        + pd.to_numeric(out["score_tts"], errors="coerce").fillna(0.0)
    ).round(6)

    try:
        if len(out) > 0:
            best_idx = pd.to_numeric(out["signal_score"], errors="coerce").fillna(-1e9).idxmax()
            r = out.loc[best_idx]
            print(
                f"[SIGNAL_SCORE] symbol={symbol} n={len(out)} "
                f"best_model={str(r.get('model', ''))} best_side={str(r.get('side', ''))} "
                f"best_signal_score={_safe_float(r.get('signal_score', 0.0)):.2f} "
                f"rr={_safe_float(r.get('score_rr', 0.0)):.2f} exec={_safe_float(r.get('score_exec', 0.0)):.2f} "
                f"phase={_safe_float(r.get('score_phase_align', 0.0)):.1f} macro={_safe_float(r.get('score_macro_align', 0.0)):.1f} "
                f"liq={_safe_float(r.get('score_liq_align', 0.0)):.1f} tts={_safe_float(r.get('score_tts', 0.0)):.1f}"
            )
    except Exception:
        pass

    return out

def _diag_no_entries(symbol: str, ctx: "pd.DataFrame", lookback: int = 200) -> None:
    try:
        if ctx is None or len(ctx) == 0:
            print(f"[{now_utc_str()}] [DIAG:{symbol}] ctx empty")
            return

        tail = ctx.tail(int(lookback) if lookback else 200).copy()

        last = tail.iloc[-1]
        last_ts = last.get("timestamp", None)
        last_phase = last.get("phase", None)
        last_sub = last.get("ctx_sub_label", last.get("sub_label", None))
        last_trend = last.get("trend_dir", None)

        phase_top = tail["phase"].value_counts().head(3).to_dict() if "phase" in tail.columns else {}

        # pick sub label column (ctx_sub_label preferred; fallback to sub_label)
        _sub_col = "ctx_sub_label" if "ctx_sub_label" in tail.columns else (
            "sub_label" if "sub_label" in tail.columns else None)
        sub_top = tail[_sub_col].value_counts().head(5).to_dict() if _sub_col else {}

        sub_vals = set(str(x) for x in tail[_sub_col].dropna().unique()) if _sub_col else set()
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
    """Normalize an entry into a row for CSV.

    Entry producers may return:
      - dataclass/objects with attributes (timestamp/model/side/...)
      - plain dicts with keys
    """
    if isinstance(e, dict):
        get = e.get
    else:
        get = lambda k, default=None: getattr(e, k, default)

    ts = get("timestamp", None)
    # some producers may use 'ts' or 'time'
    if ts is None:
        ts = get("ts", None)
    if ts is None:
        ts = get("time", None)

    return {
        "timestamp": ts,
        "signal_ts": get("signal_ts", None),
        "setup_time": get("setup_time", ts),
        "setup_phase": get("setup_phase", get("phase", None)),
        "setup_side": get("setup_side", get("side", "")),
        "setup_model": get("setup_model", get("model", "")),
        "setup_rr": get("setup_rr", get("rr", None)),
        "setup_expiry_ts": get("setup_expiry_ts", None),
        "setup_trend_dir": get("setup_trend_dir", get("trend_dir", None)),
        "setup_macro_bias": get("setup_macro_bias", get("macro_bias", None)),
        "model": get("model", ""),
        "side": get("side", ""),
        "entry": get("entry", None),
        "sl": get("sl", None),
        "tp": get("tp", None),
        "rr": get("rr", None),
        "ctx_sub_label": get("ctx_sub_label", get("sub_label", None)),
        "regime": get("regime", None),
        "trend_dir": get("trend_dir", None),
        "trend_strength": get("trend_strength", None),
        "atr_pct": get("atr_pct", None),
        "phase": get("phase", None),
        "score_rr": get("score_rr", None),
        "score_exec": get("score_exec", None),
        "score_phase_align": get("score_phase_align", None),
        "score_macro_align": get("score_macro_align", None),
        "score_liq_align": get("score_liq_align", None),
        "score_tts": get("score_tts", None),
        "signal_score": get("signal_score", None),
        "symbol": symbol,
    }


def _compute_setup_expiry_ts(
    *,
    setup_ts: Any,
    candles: pd.DataFrame,
    live_max_setup_age_candles: int,
    setup_keep_candles: int,
) -> pd.Timestamp | pd.NaT:
    try:
        sts = pd.to_datetime(setup_ts, utc=True, errors="coerce")
        if pd.isna(sts) or candles is None or candles.empty or "timestamp" not in candles.columns:
            return pd.NaT

        max_age = int(live_max_setup_age_candles or 0)
        if max_age <= 0:
            max_age = int(setup_keep_candles or 0)
        if max_age <= 0:
            return pd.NaT

        c = candles[["timestamp"]].copy()
        c["timestamp"] = pd.to_datetime(c["timestamp"], utc=True, errors="coerce")
        c = c.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
        pos = c.index[c["timestamp"] >= sts]
        if len(pos) == 0:
            return pd.NaT

        i0 = int(pos[0])
        ie = min(len(c) - 1, i0 + max_age - 1)
        if ie < i0:
            ie = i0
        return pd.to_datetime(c.iloc[ie]["timestamp"], utc=True, errors="coerce")
    except Exception:
        return pd.NaT


def _freeze_setup_contract_df(
    df_e: pd.DataFrame,
    *,
    candles: pd.DataFrame,
    live_max_setup_age_candles: int,
    setup_keep_candles: int,
) -> pd.DataFrame:
    if df_e is None or df_e.empty:
        return df_e

    out = df_e.copy()

    def _src(*cols: str, default=None):
        for c in cols:
            if c in out.columns:
                return out[c]
        return pd.Series([default] * len(out), index=out.index)

    out["setup_time"] = pd.to_datetime(_src("setup_time", "setup_ts", "timestamp"), utc=True, errors="coerce")
    out["setup_phase"] = _src("setup_phase", "phase").astype("object")
    out["setup_side"] = _src("setup_side", "side").astype("object")
    out["setup_model"] = _src("setup_model", "model").astype("object")
    out["setup_rr"] = pd.to_numeric(_src("setup_rr", "rr"), errors="coerce")
    out["setup_trend_dir"] = _src("setup_trend_dir", "trend_dir").astype("object")
    out["setup_macro_bias"] = _src("setup_macro_bias", "macro_bias").astype("object")
    out["setup_expiry_ts"] = pd.to_datetime(_src("setup_expiry_ts"), utc=True, errors="coerce")

    missing_expiry = out["setup_expiry_ts"].isna()
    if bool(missing_expiry.any()):
        out.loc[missing_expiry, "setup_expiry_ts"] = out.loc[missing_expiry, "setup_time"].apply(
            lambda ts: _compute_setup_expiry_ts(
                setup_ts=ts,
                candles=candles,
                live_max_setup_age_candles=live_max_setup_age_candles,
                setup_keep_candles=setup_keep_candles,
            )
        )
    return out


def _stamp_current_context_df(
    df_e: pd.DataFrame,
    *,
    phase_scalar: str,
    tdir: str | None,
    macro_phase_hint: Any,
    macro_bias_hint: Any,
) -> pd.DataFrame:
    if df_e is None or df_e.empty:
        return df_e
    out = df_e.copy()
    out["current_phase"] = str(phase_scalar)
    out["current_trend_dir"] = str(tdir) if tdir else None
    out["current_macro_phase"] = str(macro_phase_hint)
    out["current_macro_bias"] = str(macro_bias_hint)
    return out


def _invalidate_setups_hit_tp_sl(
    df_e: pd.DataFrame,
    candles: pd.DataFrame,
    latest_ts: pd.Timestamp,
    *,
    symbol: str = "",
    debug: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Mark setups as CLOSED if they would already be resolved historically (TP/SL hit) after the setup candle.
    This is a "professional live" guard: we should not emit stale setups that are already over.

    Conservative assumptions:
      - A setup is only eligible to close if its entry price was touched after the setup candle.
      - After entry-touch, we look for TP/SL hits using OHLC tests.
      - If TP and SL are both hit in the same candle, we assume SL first (worst-case).

    Rules (simple OHLC hit test):
      - ENTRY touched: low <= entry <= high
      - LONG: SL hit if low <= sl; TP hit if high >= tp
      - SHORT: SL hit if high >= sl; TP hit if low <= tp

    Adds columns (stable dtypes):
      - setup_status            (str)  ACTIVE / CLOSED
      - setup_close_reason      (str)  TP_HIT / SL_HIT
      - setup_entry_touch_ts    (datetime64[ns, UTC])
      - setup_close_ts          (datetime64[ns, UTC])

    Returns:
      (df_active, df_closed)
    """
    if df_e is None or df_e.empty:
        return df_e, pd.DataFrame()
    if candles is None or candles.empty:
        df_e = df_e.copy()
        df_e["setup_status"] = "ACTIVE"
        df_e["setup_close_reason"] = ""
        df_e["setup_entry_touch_ts"] = pd.Series(pd.NaT, index=df_e.index, dtype="datetime64[ns, UTC]")
        df_e["setup_close_ts"] = pd.Series(pd.NaT, index=df_e.index, dtype="datetime64[ns, UTC]")
        return df_e, pd.DataFrame()

    required_cols = {"timestamp", "high", "low"}
    if not required_cols.issubset(set(candles.columns)):
        df_e = df_e.copy()
        df_e["setup_status"] = "ACTIVE"
        df_e["setup_close_reason"] = ""
        df_e["setup_entry_touch_ts"] = pd.Series(pd.NaT, index=df_e.index, dtype="datetime64[ns, UTC]")
        df_e["setup_close_ts"] = pd.Series(pd.NaT, index=df_e.index, dtype="datetime64[ns, UTC]")
        return df_e, pd.DataFrame()

    if "timestamp" not in df_e.columns and "setup_time" not in df_e.columns:
        return df_e, pd.DataFrame()
    if "side" not in df_e.columns and "setup_side" not in df_e.columns:
        return df_e, pd.DataFrame()

    c = candles.copy()
    c["timestamp"] = pd.to_datetime(c["timestamp"], utc=True, errors="coerce")
    c = c.sort_values("timestamp").dropna(subset=["timestamp"]).reset_index(drop=True)

    latest_ts = pd.to_datetime(latest_ts, utc=True, errors="coerce")
    if pd.isna(latest_ts):
        return df_e, pd.DataFrame()

    df_e = df_e.copy()
    setup_ts_col = "setup_time" if "setup_time" in df_e.columns else "timestamp"
    side_col = "setup_side" if "setup_side" in df_e.columns else "side"
    df_e[setup_ts_col] = pd.to_datetime(df_e[setup_ts_col], utc=True, errors="coerce")

    # Normalize numeric cols on entries
    for col in ("entry", "sl", "tp"):
        if col in df_e.columns:
            df_e[col] = pd.to_numeric(df_e[col], errors="coerce")

    # Initialize columns with stable dtypes
    df_e["setup_status"] = "ACTIVE"
    df_e["setup_close_reason"] = ""
    df_e["setup_entry_touch_ts"] = pd.Series(pd.NaT, index=df_e.index, dtype="datetime64[ns, UTC]")
    df_e["setup_close_ts"] = pd.Series(pd.NaT, index=df_e.index, dtype="datetime64[ns, UTC]")

    # Deterministic evaluation order for live/backtest parity.
    # Keep mutations on the original df_e by iterating over a stable sorted view.
    invalidation_sort_cols = [c for c in (setup_ts_col, "model", side_col, "entry") if c in df_e.columns]
    if invalidation_sort_cols:
        try:
            df_e_eval = df_e.sort_values(
                by=invalidation_sort_cols,
                kind="mergesort",
                na_position="last",
            )
        except Exception:
            df_e_eval = df_e
    else:
        df_e_eval = df_e

    for idx, row in df_e_eval.iterrows():
        setup_ts = row.get(setup_ts_col)
        if pd.isna(setup_ts):
            continue

        side = str(row.get(side_col, "")).upper().strip()
        entry = row.get("entry")
        sl = row.get("sl")
        tp = row.get("tp")

        # Need entry + at least one level to close
        if entry is None or (isinstance(entry, float) and pd.isna(entry)):
            continue
        if (sl is None or (isinstance(sl, float) and pd.isna(sl))) and (tp is None or (isinstance(tp, float) and pd.isna(tp))):
            continue

        # Window after setup candle up to latest_ts
        w_all = c[(c["timestamp"] > setup_ts) & (c["timestamp"] <= latest_ts)]
        if w_all.empty:
            continue

        # Entry touch (first candle where entry is within [low, high])
        entry_hit = w_all[(w_all["low"] <= float(entry)) & (w_all["high"] >= float(entry))]
        if entry_hit.empty:
            continue
        entry_ts = entry_hit["timestamp"].iloc[0]

        # After entry touch (including the touch candle)
        w = w_all[w_all["timestamp"] >= entry_ts]
        if w.empty:
            continue

        first_sl_ts = pd.NaT
        first_tp_ts = pd.NaT

        if side == "LONG":
            if sl is not None and not (isinstance(sl, float) and pd.isna(sl)):
                hit = w[w["low"] <= float(sl)]
                if not hit.empty:
                    first_sl_ts = hit["timestamp"].iloc[0]
            if tp is not None and not (isinstance(tp, float) and pd.isna(tp)):
                hit = w[w["high"] >= float(tp)]
                if not hit.empty:
                    first_tp_ts = hit["timestamp"].iloc[0]

        elif side == "SHORT":
            if sl is not None and not (isinstance(sl, float) and pd.isna(sl)):
                hit = w[w["high"] >= float(sl)]
                if not hit.empty:
                    first_sl_ts = hit["timestamp"].iloc[0]
            if tp is not None and not (isinstance(tp, float) and pd.isna(tp)):
                hit = w[w["low"] <= float(tp)]
                if not hit.empty:
                    first_tp_ts = hit["timestamp"].iloc[0]
        else:
            continue

        if pd.isna(first_sl_ts) and pd.isna(first_tp_ts):
            continue

        # Choose earliest; if same candle, assume SL first (worst-case)
        chosen_ts = pd.NaT
        reason = ""
        if pd.isna(first_sl_ts):
            chosen_ts = first_tp_ts
            reason = "TP_HIT"
        elif pd.isna(first_tp_ts):
            chosen_ts = first_sl_ts
            reason = "SL_HIT"
        else:
            if first_sl_ts == first_tp_ts:
                chosen_ts = first_sl_ts
                reason = "SL_HIT"
            elif first_tp_ts < first_sl_ts:
                chosen_ts = first_tp_ts
                reason = "TP_HIT"
            else:
                chosen_ts = first_sl_ts
                reason = "SL_HIT"

        df_e.at[idx, "setup_status"] = "CLOSED"
        df_e.at[idx, "setup_close_reason"] = reason
        df_e.at[idx, "setup_entry_touch_ts"] = pd.to_datetime(entry_ts, utc=True, errors="coerce")
        df_e.at[idx, "setup_close_ts"] = pd.to_datetime(chosen_ts, utc=True, errors="coerce")

    df_closed = df_e[df_e["setup_status"] == "CLOSED"].copy()
    df_active = df_e[df_e["setup_status"] == "ACTIVE"].copy()

    if debug:
        try:
            for ordering_index, (_, _row) in enumerate(df_e_eval.iterrows(), start=1):
                _log_invalidation_check(str(symbol), _row, ordering_index=ordering_index, enabled=True)
        except Exception:
            pass

    return df_active, df_closed

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
    setup_keep_candles: int = 96,
    live_max_setup_age_candles: int = 0,
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
    # DEV4: global guards
    kill_window_days: int = 7,
    kill_threshold_r: float = -10.0,
    kill_min_trades: int = 0,
    kill_trades_csv: str = "",
    enable_tts_gate: bool = False,
    disable_invalidation: bool = False,
    diag_always=False,
    paper: bool = False,
    phase_guard: bool = False,
    cluster_rank_signal_score: bool = False,
    cluster_score_mode: str = "LEGACY",
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

    # --- S5 DEV3: FEED / LATENCY WATCHDOG (fail-open) ---
    try:
        _wd = check_feed_watchdog(latest_candle_ts=latest_ts)

        if not hasattr(run_once, "_watchdog_printed"):
            run_once._watchdog_printed = True
            print(_wd.log_line)

        if _wd.freeze_new_signals:
            if not hasattr(run_once, "_watchdog_froze_printed"):
                run_once._watchdog_froze_printed = True
                print("[WATCHDOG] entries=0 (freeze_new_signals=True)")
            _ensure_live_entries_csv(out_csv)
            return 0
    except Exception:
        if not hasattr(run_once, "_watchdog_printed"):
            run_once._watchdog_printed = True
            print("[WATCHDOG] ok lag_s=None")
    # -----------------------------------------------------
    # -----------------------------------------------------
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
        "after_invalidation": None,
        "dropped_invalidation": None,
        "after_live_window": None,
        "dropped_live_window": None,
        "after_live_guard": None,
        "dropped_live_guard": None,
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
    if (not once) and (last_ts is not None) and (latest_ts <= last_ts):
        # Backfill/debug mode: if user asked to emit more than 1 candle,
        # we still run the pipeline even if latest candle already processed.
        if int(emit_last_candles or 0) > 1:
            pass
        else:
            # still update last_seen to latest_ts (recreate file if missing)
            _write_state(state_path, latest_ts)

            # emit heartbeat so dashboard keeps candles/ctx visible even without new signals
            _status(
                event="no_new_candle",
                latest_ts=str(latest_ts),
                candles_last=candles_last,
                trace={"note": "no new candle"},
            )
            print(f"[{now_utc_str()}] no new candle ({latest_ts})")

            _ensure_live_entries_csv(out_csv)
            _ensure_dropped_csv(Path(out_csv))

            # --- cycle metrics (one row per symbol per cycle) ---
            try:
                _ensure_cycle_metrics_csv(str(out_csv))
                _append_cycle_metrics(str(out_csv), {
                    "cycle_ts": now_utc_str(),
                    "latest_ts": str(latest_ts),
                    "source": str(source),
                    "interval_min": int(bybit_interval) if str(bybit_interval).isdigit() else np.nan,
                    "symbol": str(bybit_symbol),
                    "once": bool(once),
                    "new_candle": False,
                    "note": "no_new_candle",
                    "raw_entries": np.nan,
                    "kept_after_invalidation": np.nan,
                    "dropped_invalidation": np.nan,
                    "kept_after_live_guard": np.nan,
                    "dropped_live_guard": np.nan,
                    "kept_after_window": np.nan,
                    "after_context": np.nan,
                    "after_regime": np.nan,
                    "after_emit_last_candles": np.nan,
                    "written_entries": 0,
                    "written_closed": np.nan,
                    "written_dropped": np.nan,
                })
            except Exception:
                pass

            # ------------------------------------------------------------
            # DEV3 (SPRINT-3 STEP-1): Equity Curve Tracker
            # ------------------------------------------------------------
            try:
                update_equity_curve_from_trades(
                    trades_csv="backtest/journal/trades.csv",
                    out_csv="backtest/journal/exports_live/equity_curve.csv",
                    initial_equity=10_000.0,
                    window_trades=60,
                )
            except Exception as _e:
                print(f"[{now_utc_str()}] [EQUITY][WARN] update_equity_curve failed: {repr(_e)}")

            # ------------------------------------------------------------
            # DEV3 (SPRINT-4 STEP-1): Symbol performance (po equity tracker)
            # ------------------------------------------------------------
            try:
                update_symbol_performance(
                    trades_csv="backtest/journal/trades.csv",
                    out_csv="backtest/journal/exports_live/symbol_performance.csv",
                )
            except Exception as _e:
                print(f"[{now_utc_str()}] [SYMBOL_PERF][WARN] update failed: {repr(_e)}")

            return 0

    # ------------------------------------------------------------
    # Generate all candidate entries; RegimeDecision filters decide what survives.
    # ------------------------------------------------------------

    # build ctx and generate entries
    ctx = build_context(
        candles,
        diag_always=bool(diag_always),
    )

    # ---- DEV4: phase hint BEFORE entry generation ----
    # goal:
    #  - decide PHASE_PRE from candles (+ macro bias hint)
    #  - map to entry_model phase format: PHASE_TREND_UP / PHASE_TREND_DOWN / PHASE_RANGE

    macro_eval = evaluate_macro(
        ctx,
        compute_macro_gate=compute_macro_gate,
        compute_cross_asset_regime=compute_cross_asset_regime,
    )
    ctx = macro_eval.ctx
    macro_dec = macro_eval.macro_dec
    macro_bias_hint = macro_eval.macro_bias_hint
    macro_phase_hint = macro_eval.macro_phase_hint
    macro_strength_hint = macro_eval.macro_strength_hint
    cross_asset_regime = macro_eval.cross_asset_regime
    cross_asset_strength = macro_eval.cross_asset_strength
    cross_asset_reason = macro_eval.cross_asset_reason
    cross_asset_status = macro_eval.cross_asset_status

    # Telemetry – CONTRACT LOG
    global _CROSS_ASSET_LOGGED_TS
    try:
        _key = str(latest_ts)
        if _CROSS_ASSET_LOGGED_TS != _key:
            print(
                f"[CROSS_ASSET] "
                f"regime={cross_asset_regime} "
                f"strength={cross_asset_strength:.2f} "
                f"reason={cross_asset_reason} "
                f"status={cross_asset_status}"
            )
            _CROSS_ASSET_LOGGED_TS = _key
    except Exception:
        pass
    #print(f"[CROSS_ASSET] regime={cross_asset_regime}")

    # Decide phase using macro bias hint (fail-open to RANGE)
    phase_eval = route_phase(
        candles,
        ctx=ctx,
        debug_regime=bool(debug_regime),
        symbol=str(bybit_symbol),
        now_utc_str_fn=now_utc_str,
    )
    ctx = phase_eval.ctx
    phase_authority_source = phase_eval.phase_authority_source
    context_phase_pre_guard = phase_eval.context_phase_pre_guard
    phase_scalar = phase_eval.phase_scalar
    trend_phase_label = phase_eval.trend_phase_label
    tdir = phase_eval.tdir

    _log_phase_authority(
        symbol=str(bybit_symbol),
        trend_phase=str(trend_phase_label),
        context_phase=str(context_phase_pre_guard),
        final_phase=str(phase_scalar),
        authority_source=str(phase_authority_source),
        enabled=_diag_enabled(debug_regime, debug_entry_filters),
    )
    diag_log(
        "PHASE_DECISION",
        symbol=str(bybit_symbol),
        trend_phase=str(trend_phase_label),
        context_phase=str(context_phase_pre_guard),
        final_phase=str(phase_scalar),
        trend_dir=str(tdir) if "tdir" in locals() else None,
        macro_phase=str(macro_phase_hint),
        macro_bias=str(macro_bias_hint),
        macro_strength=_json_safe(macro_strength_hint),
        authority_source=str(phase_authority_source),
    )

    entries = route_model(
        ctx,
        generate_entries_from_ctx=generate_entries_from_ctx,
        symbol=bybit_symbol,
        rr=rr,
        sl_atr_buffer=sl_atr_buffer,
        require_impulse_before_tdp=require_impulse_before_tdp,
        impulse_lookback=impulse_lookback,
        impulse_size_atr=impulse_size_atr,
        tdp_dev_lookback=tdp_dev_lookback,
        tts_retest_lookback=tts_retest_lookback,
        debug_entry_filters=bool(debug_entry_filters),
    )


    # ============================================================
    # DEV1 — Signal Cluster Filter (ENTRY → before risk/budget)
    # ============================================================
    dropped_cluster = []
    if entries:
        cluster_score_mode_resolved, cluster_score_field = _resolve_cluster_score_mode(
            entries,
            cluster_score_mode=cluster_score_mode,
            cluster_rank_signal_score=cluster_rank_signal_score,
        )
        print(
            f"[{now_utc_str()}] [CLUSTER_RANK_MODE] "
            f"score_mode={cluster_score_mode_resolved} score_field={cluster_score_field}"
        )
        try:
            from backtest.filters.signal_cluster_filter import apply_signal_cluster_filter
            entries, dropped_cluster = apply_signal_cluster_filter(
                entries,
                max_per_group=999,
                score=cluster_score_field,
                phase=phase_scalar,
                debug=_diag_enabled(debug_regime, debug_entry_filters),
            )
            if debug_entry_filters:
                try:
                    kept_labs = sorted({str(getattr(e, "ctx_sub_label", "") or "") for e in (entries or [])})
                    print(
                        f"[DEBUG][{bybit_symbol}] after_cluster entries={len(entries or [])} ctx_sub_label={kept_labs} dropped_cluster={len(dropped_cluster or [])}")
                except Exception:
                    pass
            print(f"[{now_utc_str()}] [CLUSTER_FILTER][{bybit_symbol}] kept={len(entries)} dropped={len(dropped_cluster)} score_field={cluster_score_field}")
            # Sidecar: persist dropped signals for auditability
            try:
                if dropped_cluster:
                    df_cluster_dropped = pd.DataFrame([_entry_to_row(e, str(bybit_symbol)) for e in dropped_cluster])
                    if not df_cluster_dropped.empty:
                        _append_dropped(Path(out_csv), df_cluster_dropped, stage="CLUSTER_FILTER", reason="CLUSTER_FILTER", drop_ts=latest_ts)
            except Exception:
                pass
        except Exception as _e:
            err_cls = _e.__class__.__name__
            err_msg = str(_e)
            print(f"[{now_utc_str()}] [CLUSTER_FILTER][{bybit_symbol}] kept={len(entries)} dropped=0 (fallback err={err_cls}: {err_msg})")
            entries = list(entries or [])

    if debug_regime:
        try:
            models = sorted({str(getattr(e, 'model', '')).upper() for e in (entries or [])})
            print(f"[{now_utc_str()}] [DEBUG][{bybit_symbol}] raw entries={len(entries)} models={models}")
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
            print(f"[{now_utc_str()}] [DEBUG][{bybit_symbol}] no entries generated by entry_model")

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
            macro_bias=str(macro_bias_hint),
            macro_phase=str(macro_phase_hint),
            macro_strength=macro_strength_hint,
            last_ts=ctx_tail.get("last_ts"),
            candles_last=candles_last,
            ctx_last=ctx_tail,
        )


    # ------------------------------------------------------------
    # DEV3 (SPRINT-3 STEP-1): Equity Curve Tracker (drawdown source of truth)
    # updates every runner cycle
    # ------------------------------------------------------------
    try:
        update_equity_curve_from_trades(
            trades_csv="backtest/journal/trades.csv",
            out_csv="backtest/journal/exports_live/equity_curve.csv",
            initial_equity=10_000.0,
            window_trades=60,
        )
    except Exception as _e:
        print(f"[{now_utc_str()}] [EQUITY][WARN] update_equity_curve failed: {repr(_e)}")

    # --- DEV3 SPRINT-4: symbol performance (after equity) ---
    try:
        update_symbol_performance(
            trades_csv="backtest/journal/trades.csv",
            out_csv="backtest/journal/exports_live/symbol_performance.csv",
        )
    except Exception as _e:
        print(f"[{now_utc_str()}] [SYMBOL_PERF][WARN] update failed: {repr(_e)}")

    _ensure_live_entries_csv(out_csv)
    df_e = pd.DataFrame([_entry_to_row(e, str(bybit_symbol)) for e in entries])
    try:
        if df_e is not None and not df_e.empty:
            df_e = _freeze_setup_contract_df(
                df_e,
                candles=candles,
                live_max_setup_age_candles=live_max_setup_age_candles,
                setup_keep_candles=setup_keep_candles,
            )
            df_e = _stamp_current_context_df(
                df_e,
                phase_scalar=str(phase_scalar),
                tdir=tdir,
                macro_phase_hint=macro_phase_hint,
                macro_bias_hint=macro_bias_hint,
            )
    except Exception:
        pass
    try:
        for _, _setup_row in df_e.iterrows():
            diag_log("SETUP_CREATED", **_diag_payload_from_row(_setup_row, symbol=str(bybit_symbol)))
    except Exception:
        pass
    # ============================================================
    # STAGE 5 / STEP 4: RouterDecision DTO adapter (no-op, fail-open)
    # Router-layer contract only; no logic / behavior changes.
    # ============================================================
    try:
        if df_to_router_decisions is not None:
            _ = df_to_router_decisions(
                df_e,
                symbol=str(bybit_symbol),
                phase=str(phase_scalar),
                trend_dir=str(tdir or ""),
            )
    except Exception:
        pass
    # --- DEV2 DF safety: schema guard for empty/no-column cycles ---
    if df_e is None:
        df_e = pd.DataFrame()
    for _c, _dtype in {
        "model": "object",
        "side": "object",
        "phase": "object",
        "timestamp": "datetime64[ns, UTC]",
        "risk_multiplier": "float",
        "dynamic_multiplier": "float",
        "equity_governor_multiplier": "float",
    }.items():
        if _c not in df_e.columns:
            # create empty column with consistent dtype
            if str(_dtype).startswith("datetime64"):
                df_e[_c] = pd.Series(pd.to_datetime([], utc=True))
            else:
                df_e[_c] = pd.Series(dtype=_dtype)



    
    
    # ============================================================
    # S5 DEV1 — EXECUTION QUALITY (telemetry only, fail-open)
    # Integrate AFTER entry normalization, BEFORE corr caps / budget.
    # ============================================================
    try:
        from backtest.execution.execution_quality import estimate_execution_quality  # S5 DEV1
        eq_df = estimate_execution_quality(df_e=df_e, candles_df=candles, orderbook_snapshot=None)
        if isinstance(eq_df, pd.DataFrame) and (not eq_df.empty):
            try:
                if "exec_quality_score" in eq_df.columns:
                    _eq = eq_df[[c for c in ("symbol", "model", "side", "exec_quality_score") if c in eq_df.columns]].copy()
                    if len(_eq) == len(df_e):
                        df_e["exec_quality_score"] = pd.to_numeric(_eq["exec_quality_score"], errors="coerce")
                    else:
                        df_e["exec_quality_score"] = pd.to_numeric(eq_df["exec_quality_score"].iloc[:len(df_e)].reset_index(drop=True), errors="coerce")
            except Exception:
                pass
            # Log at least one line per cycle (avoid spam: first row only).
            try:
                _r0 = eq_df.iloc[0]
                _sym = str(_r0.get("symbol", bybit_symbol))
                _model = str(_r0.get("model", ""))
                _spread = float(_r0.get("spread_bps", np.nan))
                _slip = float(_r0.get("slippage_bps_est", np.nan))
                _score = float(_r0.get("exec_quality_score", 1.0))
                print(f"[EXEC_QUALITY] symbol={_sym} model={_model} spread_bps={_spread:.1f} slip_bps={_slip:.1f} score={_score:.2f}")
            except Exception:
                pass

            # Append-only CSV (never blocks pipeline)
            try:
                eq_path = Path("backtest/journal/exports_live/execution_quality.csv")
                eq_path.parent.mkdir(parents=True, exist_ok=True)
                header = (not eq_path.exists()) or (eq_path.stat().st_size == 0)
                eq_df.to_csv(eq_path, mode="a", header=header, index=False)
            except Exception:
                pass
    except Exception as _e:
        # Fail-open: do nothing, keep pipeline running
        try:
            if not hasattr(run_once, "_exec_quality_warned"):
                run_once._exec_quality_warned = True
                print(f"[EXEC_QUALITY] symbol={bybit_symbol} fallback score=1.00 (err={repr(_e)})")
        except Exception:
            pass

    try:
        tts_ctx = annotate_tts_context(ctx[[c for c in ctx.columns if c in ("timestamp", "open", "high", "low", "close", "phase")]].copy())
        if isinstance(tts_ctx, pd.DataFrame) and (not tts_ctx.empty) and ("timestamp" in tts_ctx.columns):
            cols = [c for c in ("timestamp", "tts_allow_long", "tts_allow_short") if c in tts_ctx.columns]
            tts_tail = tts_ctx[cols].copy()
            tts_tail["timestamp"] = pd.to_datetime(tts_tail["timestamp"], utc=True, errors="coerce")
            if "timestamp" in df_e.columns:
                df_e["timestamp"] = pd.to_datetime(df_e["timestamp"], utc=True, errors="coerce")
                df_e = pd.merge_asof(
                    df_e.sort_values("timestamp"),
                    tts_tail.sort_values("timestamp"),
                    on="timestamp",
                    direction="backward",
                ).sort_index()
    except Exception:
        df_e["tts_allow_long"] = df_e.get("tts_allow_long", False)
        df_e["tts_allow_short"] = df_e.get("tts_allow_short", False)

    if "macro_bias" not in df_e.columns:
        try:
            df_e["macro_bias"] = str(locals().get("macro_bias_hint") or "NEUTRAL").upper()
        except Exception:
            df_e["macro_bias"] = "NEUTRAL"

    try:
        _liq_ctx = get_liquidation_context_sync(str(bybit_symbol))
        _liq_bias = str((_liq_ctx or {}).get("liq_bias", "NEUTRAL")).upper()
    except Exception:
        _liq_bias = "NEUTRAL"
    df_e["liq_bias"] = _liq_bias

    df_e = _attach_signal_scores(df_e, symbol=str(bybit_symbol))
    # S5 DEV4 — VOLATILITY REGIME (early warning, fail-open)
    # Integrate BEFORE risk sizing (corr caps / budget).
    # ============================================================
    vol_regime = 'NORMAL'
    vol_atr_pct = 0.0
    vol_z = 0.0
    vol_multiplier = 1.0
    try:
        if detect_volatility_regime is not None:
            vr = detect_volatility_regime(candles)
            vol_regime = str(getattr(vr, 'regime', 'NORMAL') or 'NORMAL').upper()
            vol_atr_pct = float(getattr(vr, 'atr_pct', 0.0) or 0.0)
            vol_z = float(getattr(vr, 'z', 0.0) or 0.0)
    except Exception:
        vol_regime, vol_atr_pct, vol_z = 'NORMAL', 0.0, 0.0

    try:
        print(f"[VOL_REGIME] symbol={bybit_symbol} regime={vol_regime} atr_pct={vol_atr_pct:.4f} z={vol_z:.2f}")
    except Exception:
        pass

    if vol_regime == 'SHOCK':
        vol_multiplier = 0.5

    try:
        if df_e is not None and not df_e.empty:
            if 'risk_multiplier' not in df_e.columns:
                df_e['risk_multiplier'] = 1.0
            rm0 = pd.to_numeric(df_e['risk_multiplier'], errors='coerce').fillna(1.0).astype(float)
            df_e['risk_multiplier'] = (rm0 * float(vol_multiplier)).astype(float)
    except Exception:
        pass

    try:
        ctx['vol_regime'] = vol_regime
        ctx['vol_atr_pct'] = float(vol_atr_pct)
        ctx['vol_z'] = float(vol_z)
        ctx['vol_multiplier'] = float(vol_multiplier)
    except Exception:
        pass

# ===== CORR_CAP (SOFT + DEBUG) =====

    CAP_BTC = 0.02
    CAP_ALT = 0.02
    CAP_MEME = 0.01
    BASE_RISK = 0.002

    corr_cap_decision = evaluate_policy_corr_cap(
        df_e,
        portfolio_state_path=portfolio_state_path,
        base_risk=BASE_RISK,
        cap_btc=CAP_BTC,
        cap_alt=CAP_ALT,
        cap_meme=CAP_MEME,
    )

    df_e = corr_cap_decision["df_kept"]
    df_corr_dropped = corr_cap_decision["df_drop"]

    if not df_corr_dropped.empty:
        _append_dropped(Path(out_csv), df_corr_dropped, stage="CORR_CAP", reason="CORR_CAP", drop_ts=latest_ts)

    print(f"[{now_utc_str()}] [CORR_CAP][{bybit_symbol}] kept={len(df_e)} dropped={len(df_corr_dropped)}")

   # _append_dropped(Path(out_csv), df_corr_dropped, stage="CORR_CAP", reason="CORR_CAP", drop_ts=latest_ts)

   # print(f"[{now_utc_str()}] [CORR_CAP][{bybit_symbol}] kept={len(df_e)} dropped={len(df_corr_dropped)}")

    
# ============================================================
    # DEV2: BUDGET_CAP enforcement + telemetry
    # ============================================================
    BASE_RISK_PER_TRADE = 0.002
    BUCKET_CAP = 0.006
    GLOBAL_CAP = 0.012

    budget_decision = evaluate_policy_budget(
        df_e,
        base_risk_per_trade=BASE_RISK_PER_TRADE,
        bucket_cap=BUCKET_CAP,
        global_cap=GLOBAL_CAP,
    )

    long_used = float(budget_decision["long_used"])
    range_used = float(budget_decision["range_used"])
    short_used = float(budget_decision["short_used"])
    global_used = float(budget_decision["global_used"])

    if df_e is not None and not df_e.empty:
        df_kept = budget_decision["df_kept"]
        df_drop = budget_decision["df_drop"]
        df_e = df_kept

        print(
            f"[{now_utc_str()}] [BUDGET][{bybit_symbol}] kept={len(df_kept)} dropped={len(df_drop)} "
            f"long_used={long_used:.4f} range_used={range_used:.4f} short_used={short_used:.4f} global_used={global_used:.4f}"
        )

        # soft warning 80%
        if long_used > 0.8 * BUCKET_CAP:
            print(f"[{now_utc_str()}] [BUDGET_WARN] LONG nearing cap used={long_used:.4f} cap={BUCKET_CAP:.4f}")
        if range_used > 0.8 * BUCKET_CAP:
            print(f"[{now_utc_str()}] [BUDGET_WARN] RANGE nearing cap used={range_used:.4f} cap={BUCKET_CAP:.4f}")
        if short_used > 0.8 * BUCKET_CAP:
            print(f"[{now_utc_str()}] [BUDGET_WARN] SHORT nearing cap used={short_used:.4f} cap={BUCKET_CAP:.4f}")
        if global_used > 0.8 * GLOBAL_CAP:
            print(f"[{now_utc_str()}] [BUDGET_WARN] GLOBAL nearing cap used={global_used:.4f} cap={GLOBAL_CAP:.4f}")

        if not df_drop.empty:
            _append_dropped(Path(out_csv), df_drop, stage="BUDGET_CAP", reason="BUDGET_CAP", drop_ts=latest_ts)

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
        if "signal_ts" in df_e.columns:

            df_e["signal_ts"] = pd.to_datetime(df_e["signal_ts"], utc=True, errors="coerce")


    # --- setup invalidation: drop setups where TP/SL already hit after setup candle ---
    try:
        df_before_invalidation = df_e.copy()
        before_inv = len(df_e)
        if (not disable_invalidation) and (not once):
            df_active, df_closed = _invalidate_setups_hit_tp_sl(
                df_e,
                candles=candles,
                latest_ts=latest_ts,
                symbol=str(bybit_symbol),
                debug=_diag_enabled(debug_regime, debug_entry_filters),
            )
            df_e = df_active
            try:
                if df_closed is not None and (not df_closed.empty):
                    df_closed = _freeze_setup_contract_df(
                        df_closed,
                        candles=candles,
                        live_max_setup_age_candles=live_max_setup_age_candles,
                        setup_keep_candles=setup_keep_candles,
                    )
                    df_closed = _stamp_current_context_df(
                        df_closed,
                        phase_scalar=str(phase_scalar),
                        tdir=tdir,
                        macro_phase_hint=macro_phase_hint,
                        macro_bias_hint=macro_bias_hint,
                    )
                    for _, _row in df_closed.iterrows():
                        _reason = str(_row.get("setup_close_reason", "unknown") or "unknown").strip().lower()
                        diag_log(
                            "SETUP_INVALIDATED",
                            **_diag_payload_from_row(_row, symbol=str(bybit_symbol)),
                            reason=_reason,
                        )
            except Exception:
                pass
        else:
            df_closed = pd.DataFrame()


        try:
            trace_counts["after_invalidation"] = int(len(df_e)) if df_e is not None else 0
            trace_counts["dropped_invalidation"] = int(len(df_closed)) if df_closed is not None else 0
        except Exception:
            pass
        if debug_regime:
            dropped = int(len(df_closed)) if df_closed is not None else 0
            kept = int(len(df_e)) if df_e is not None else 0
            print(f"[{now_utc_str()}] [DEBUG][{bybit_symbol}] setup invalidation: dropped={dropped} kept={kept}")

            # DEV1-1: if invalidation dropped everything, print top close reasons (TP/SL) to explain why
            if debug_entry_filters and dropped > 0 and kept == 0:
                try:
                    if isinstance(df_closed, pd.DataFrame) and (not df_closed.empty) and ("setup_close_reason" in df_closed.columns):
                        vc = (
                            df_closed["setup_close_reason"]
                            .fillna("")
                            .astype(str)
                            .replace({"": "UNKNOWN"})
                            .value_counts()
                        )
                        top = list(vc.head(3).items())
                        print(f"[{now_utc_str()}] [INVALIDATION_DIAG][{bybit_symbol}] dropped_all top_reasons={top}")
                    else:
                        print(f"[{now_utc_str()}] [INVALIDATION_DIAG][{bybit_symbol}] dropped_all reason=NO_CLOSE_REASON_COL")
                except Exception as _e:
                    print(f"[{now_utc_str()}] [INVALIDATION_DIAG][{bybit_symbol}] dropped_all diag_failed err={repr(_e)}")


        # Sidecar CSV for CLOSED setups (audit/UI)
        if df_closed is not None and (not df_closed.empty) and out_csv:
            p_out = Path(out_csv)
            closed_csv = str(p_out.with_name(p_out.stem + "_closed.csv"))

            df_closed_out = df_closed.copy()
            # Ensure minimal columns exist
            for _c in [
                "timestamp", "signal_ts", "model", "side", "entry", "sl", "tp", "symbol",
                "setup_status", "setup_close_reason", "setup_entry_touch_ts", "setup_close_ts",
            ]:
                if _c not in df_closed_out.columns:
                    df_closed_out[_c] = np.nan

            # --- fill signal_ts for CLOSED rows (audit run / emit candle) ---
            try:
                if "signal_ts" not in df_closed_out.columns:
                    df_closed_out["signal_ts"] = pd.NaT

                # treat empty strings as missing
                df_closed_out["signal_ts"] = df_closed_out["signal_ts"].replace("", pd.NaT)

                # normalize dtype
                df_closed_out["signal_ts"] = pd.to_datetime(df_closed_out["signal_ts"], utc=True, errors="coerce")

                # fill missing -> latest_ts (audit cycle / emit candle)
                _fill_ts = pd.to_datetime(latest_ts, utc=True, errors="coerce")
                df_closed_out["signal_ts"] = df_closed_out["signal_ts"].fillna(_fill_ts)
            except Exception:
                pass
            # --- setup age metrics (professional audit fields) ---
            try:
                interval_min = int(bybit_interval) if bybit_interval else 15

                ts_setup = pd.to_datetime(df_closed_out["timestamp"], utc=True, errors="coerce")
                ts_close = pd.to_datetime(df_closed_out["setup_close_ts"], utc=True, errors="coerce")

                age_hours = (ts_close - ts_setup).dt.total_seconds() / 3600.0
                df_closed_out["setup_age_hours"] = age_hours

                df_closed_out["setup_age_candles"] = (
                        (age_hours * 60.0) / interval_min
                ).round(2)

            except Exception:
                pass

            closed_cols = [
                "timestamp", "signal_ts", "symbol", "model", "side", "entry", "sl", "tp",
                "setup_status", "setup_close_reason", "setup_entry_touch_ts", "setup_close_ts",
                "setup_age_hours", "setup_age_candles",
            ]

            df_closed_out = df_closed_out[closed_cols]
            write_header = not Path(closed_csv).exists()
            df_closed_out.to_csv(closed_csv, mode="a", header=write_header, index=False)

    except Exception:
        # Never break the live loop due to invalidation/audit sidecar
        pass

    # --- LIVE emission window ---
    # In LIVE we want to *emit on the latest candle*, but keep the original setup timestamp for audit.
    # So we keep setup_ts=original timestamp, set signal_ts=latest_ts, and use signal_ts as working timestamp
    # for downstream state/portfolio/last_seen logic.
    cutoff_live = None
    _setup_min = pd.NaT
    _setup_max = pd.NaT
    cutoff_live = None
    _setup_min = pd.NaT
    _setup_max = pd.NaT

    emit_n = int(emit_last_candles or 0)
    if (not once) and ("timestamp" in df_e.columns):
        # Preserve setup timestamp
        df_e["setup_ts"] = pd.to_datetime(df_e["setup_time"], utc=True, errors="coerce") if "setup_time" in df_e.columns else pd.to_datetime(df_e["timestamp"], utc=True, errors="coerce")

        # Candle size (minutes)
        try:
            interval_min = int(bybit_interval)
        except Exception:
            interval_min = 15

        # Keep setups from a recent window so previous-candle setups can trigger on the newest candle.
        live_window_candles = max(2, int(setup_keep_candles or 96))
        cutoff_live = pd.to_datetime(latest_ts, utc=True) - pd.Timedelta(minutes=interval_min * live_window_candles)

        before_window = len(df_e)
        df_e = df_e[df_e["setup_ts"] >= cutoff_live].copy()
        print(
            f"[POST_REGIME_LIVE_WINDOW] before={before_window} after={len(df_e)} "
            f"cutoff_live={cutoff_live}"
        )
        try:
            trace_counts["after_live_window"] = int(len(df_e))
            trace_counts["dropped_live_window"] = int(before_window - len(df_e))
        except Exception:
            pass

        # Use latest_ts as emit candle (signal_ts)
        df_e["signal_ts"] = pd.to_datetime(latest_ts, utc=True)

        # Age metrics (for UI + guard)
        try:
            df_e["setup_age_hours"] = (df_e["signal_ts"] - df_e["setup_ts"]).dt.total_seconds() / 3600.0
            df_e["setup_age_candles"] = (df_e["setup_age_hours"] * 60.0) / float(interval_min)
        except Exception:
            pass

        # --- LIVE-only emission guard: emit only if setup_age_candles <= X (0 disables) ---
        try:
            max_age = int(live_max_setup_age_candles or 0)
            before_age = len(df_e)
            if max_age > 0 and "setup_age_candles" in df_e.columns:
                df_e = df_e[df_e["setup_age_candles"] <= float(max_age)].copy()
                try:
                    trace_counts["after_live_guard"] = int(len(df_e))
                    trace_counts["dropped_live_guard"] = int(before_age - len(df_e))
                except Exception:
                    pass
                if debug_regime:
                    print(
                        f"[{now_utc_str()}] [DEBUG] live age guard: max_age={max_age} "
                        f"kept={len(df_e)} (from {before_age})"
                    )
            print(
                f"[POST_REGIME_MAX_AGE] before={before_age} after={len(df_e)} "
                f"max_age={live_max_setup_age_candles}"
            )
        except Exception:
            pass

        # Use signal_ts as working timestamp for downstream filters/state, but keep setup_ts for audit
        df_e["timestamp"] = df_e["signal_ts"]

        if debug_regime:
            try:
                _setup_min = pd.to_datetime(df_e["setup_ts"], utc=True, errors="coerce").min()
                _setup_max = pd.to_datetime(df_e["setup_ts"], utc=True, errors="coerce").max()
            except Exception:
                _setup_min = pd.NaT
                _setup_max = pd.NaT
            print(
                f"[{now_utc_str()}] [DEBUG] live window: setup_keep_candles={int(setup_keep_candles or 96)} cutoff={cutoff_live} "
                f"kept={len(df_e)} (from {before_window}) | setup_ts_min={_setup_min} setup_ts_max={_setup_max}"
            )
    else:
        # --once mode (audit/backfill): keep timestamp as-is; add signal_ts for a stable schema
        if ("timestamp" in df_e.columns) and ("signal_ts" not in df_e.columns):
            df_e["signal_ts"] = pd.to_datetime(df_e["timestamp"], utc=True, errors="coerce")

    trace_counts["after_latest_candle_filter"] = int(len(df_e))
    _trace(trace_on, f"after_latest_candle_filter={len(df_e)}")
    _status("after_latest_candle_filter")

    if df_e.empty:
        if debug_regime or trace_on:
            try:
                print(f"[{now_utc_str()}] [DEBUG][{bybit_symbol}] live filter removed all: setup_ts_min={_setup_min} setup_ts_max={_setup_max} cutoff={cutoff_live}")
            except Exception:
                pass
        _ensure_live_entries_csv(out_csv)
    # --- AUDIT cutoff: apply --from_ts only in --once mode ---
    if once and from_ts and ("timestamp" in df_e.columns):
        cutoff_ts = pd.to_datetime(str(from_ts).strip(), utc=True, errors="coerce")
        if pd.notna(cutoff_ts):
            before_from = len(df_e)
            df_e = df_e[df_e["timestamp"] >= cutoff_ts].copy()
            if debug_regime:
                print(f"[{now_utc_str()}] [DEBUG] from_ts filter: from_ts={cutoff_ts} kept={len(df_e)} (from {before_from})")

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
        # DEBUG: disable context blocking in --once diagnostics
        if once:
            try:
                ctx_gate.allow_trade = True
                if hasattr(ctx_gate, "macro_allow"):
                    ctx_gate.macro_allow = True
                if hasattr(ctx_gate, "news_allow"):
                    ctx_gate.news_allow = True
                if hasattr(ctx_gate, "liq_allow"):
                    ctx_gate.liq_allow = True
            except Exception:
                pass

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
                # still write blocked rows so the dashboard can show why signals were blocked
                pass

        df_e["risk_multiplier"] = float(ctx_gate.risk_multiplier)
        # ============================================================
        # DEV2: Equity Governor (drawdown throttle)
        sizing_decision = evaluate_policy_sizing(
            df_e,
            context_risk_multiplier=float(ctx_gate.risk_multiplier),
            equity_governor=EQUITY_GOVERNOR,
        )

        df_e = sizing_decision["df_e"]

        # log lieka runner'yje (identiškas)
        if sizing_decision["logged"]:
            print(
                f"[{now_utc_str()}] [EQUITY_GOVERNOR] "
                f"dd={sizing_decision['equity_dd']:.4f} "
                f"multiplier={sizing_decision['equity_governor_multiplier']}"
            )

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

        # --- macro bias: prefer LONG/SHORT depending on BTC dominance vs TOTAL3 ---
        # We don't want macro to hard-block all trades; instead it biases allowed sides and/or risk.
        macro_bias = getattr(getattr(ctx_gate, "macro", None), "bias", None) or getattr(ctx_gate, "macro_bias", None)
        if macro_bias and "side" in df_e.columns:
            sym_u = str(bybit_symbol).upper()
            is_alt = not sym_u.startswith("BTC")
            if is_alt and macro_bias in ("ALT_SHORT", "ALT_LONG"):
                side_u = df_e["side"].astype(str).str.upper()
                if macro_bias == "ALT_SHORT":
                    side_ok = side_u.eq("SHORT")
                else:  # ALT_LONG
                    side_ok = side_u.eq("LONG")

                # Do NOT block trades based on macro side-bias; only annotate.
                # Macro bias is informational: it can be used later for sizing / preference rules.
                df_e["macro_allow"] = True
                df_e["macro_bias"] = macro_bias
                df_e["macro_bias_mismatch"] = (~side_ok)
                base_mr = df_e.get("macro_reason", "")
                df_e["macro_reason"] = base_mr.astype(str) + f" | bias={macro_bias}"

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
    # ============================================================
    # DEV4: KILL SWITCH (rolling R) - global guardrail (fail-open)
    ks_decision = evaluate_policy_kill_switch(
        symbol=str(bybit_symbol),
        kill_threshold_r=float(kill_threshold_r),
        btc_kill_threshold_r=float(BTC_KILL_THRESHOLD_R),
        kill_window_days=int(kill_window_days),
        kill_trades_csv=str(kill_trades_csv),
        out_csv=str(out_csv),
    )

    if ks_decision["error"] is not None:
        print(f"[{now_utc_str()}] [KILL_SWITCH] skip (error={ks_decision['error']})")
    else:
        print(
            f"[{now_utc_str()}] [KILL_SWITCH] symbol={ks_decision['symbol']} "
            f"threshold_used={ks_decision['threshold_used']:.2f} reason={ks_decision['reason']}"
        )

        if not ks_decision["ok"]:
            df_e["context_allow"] = False
            df_e["block_reason"] = (
                    df_e.get("block_reason", "").astype(str)
                    + " | "
                    + str(ks_decision["reason"])
            ).str.strip()

            df_e = df_e.iloc[0:0].copy()

    # ============================================================
    # PHASE ENFORCE — use ctx phase (scalar from last candle) to filter models
    #   ctx is a DataFrame, so ctx.get("phase") returns a Series -> we must take last value.
    # ============================================================
    try:
        def _last_scalar_from_ctx(col: str, default=None):
            try:
                if col not in ctx.columns:
                    return default
                s = ctx[col]
                # Series -> take last non-null
                if hasattr(s, "dropna"):
                    s2 = s.dropna()
                    return (s2.iloc[-1] if len(s2) else default)
                # fallback
                return default
            except Exception:
                return default

        # ctx["phase"] must be one of: PHASE_TREND_UP / PHASE_TREND_DOWN / PHASE_RANGE
        phase_ctx = str(_last_scalar_from_ctx("phase", "PHASE_RANGE") or "PHASE_RANGE").upper()
        if phase_ctx not in ("PHASE_TREND_UP", "PHASE_TREND_DOWN", "PHASE_RANGE"):
            phase_ctx = "PHASE_RANGE"

        # macro_bias only for telemetry (do not recompute phase here)
        macro_bias = str(
            _last_scalar_from_ctx("macro_bias", None) or (locals().get("macro_bias_hint") or "NEUTRAL")).upper()

        # Router view (just for printing)
        if phase_ctx == "PHASE_TREND_UP":
            ph_raw = "LONG"
        elif phase_ctx == "PHASE_TREND_DOWN":
            ph_raw = "SHORT"
        else:
            ph_raw = "RANGE"

        # Filter df_e to match phase contract
        if df_e is not None and len(df_e) > 0:
            model_u = df_e["model"].astype(str).str.upper()
            side_u = df_e["side"].astype(str).str.upper()

            if phase_ctx == "PHASE_TREND_UP":
                # only trend LONGs
                mask = (model_u == "TDP_REENTRY") & (side_u == "LONG")

            elif phase_ctx == "PHASE_TREND_DOWN":
                # only trend SHORTs
                mask = (model_u == "TDP_REENTRY") & (side_u == "SHORT")

            else:
                # PHASE_RANGE: only range shorts (MVP lock)
                mask = (model_u == "RANGE_TOP_SHORT_V2") & (side_u == "SHORT")

            df_e = df_e.loc[mask].copy()

        # enforce scalar phase for output schema
        if df_e is not None and len(df_e) > 0:
            df_e["phase"] = phase_ctx
            if "phase_reason" not in df_e.columns:
                df_e["phase_reason"] = ""

        print(f"[{now_utc_str()}] [PHASE][{bybit_symbol}] {ph_raw} | macro_bias={macro_bias} | ctx_phase={phase_ctx}")

    except Exception as _pe:
        print(f"[{now_utc_str()}] [PHASE][{bybit_symbol}] skip (error={repr(_pe)})")

# Filtravimas pagal decision (RegimeDecision = vienas šaltinis)
    # ============================================================

    # ------------------------------------------------------------
    # SCHEMA GUARD + SAFE NORMALIZE (prevents KeyError when df_e empty/no columns)
    # ------------------------------------------------------------
    for _c, _dtype in [
        ("timestamp", "datetime64[ns, UTC]"),
        ("signal_ts", "datetime64[ns, UTC]"),
        ("symbol", "object"),
        ("model", "object"),
        ("side", "object"),
        ("phase", "object"),
        ("entry", "float64"),
        ("sl", "float64"),
        ("tp", "float64"),
    ]:
        if _c not in df_e.columns:
            df_e[_c] = pd.Series(dtype=_dtype)

    # If at this point nothing is left (e.g. CORR_CAP dropped all) -> exit cleanly
    if df_e.empty:
        _ensure_live_entries_csv(out_csv)
        if paper:
            # still touch signals_live.csv + paper_trades.csv so ops can see this cycle ran
            try:
                from backtest.journal.paper_executor import run_paper_executor  # type: ignore
                signals_path = Path("backtest/journal/exports_live/signals_live.csv")
                signals_path.parent.mkdir(parents=True, exist_ok=True)
                # In paper mode, signals_live.csv is a per-cycle snapshot.
                # Always write the header (empty snapshot) so LastWriteTime updates and paper executor can run.
                try:
                    pd.DataFrame(columns=[
                        "timestamp","signal_ts","symbol","model","side","entry","sl","tp","rr",
                        "ctx_sub_label","phase","regime","trend_dir","status"
                    ]).to_csv(signals_path, index=False)
                except Exception:
                    pass
                # ensure paper_trades exists (even if no signals)
                run_paper_executor(in_csv=str(signals_path), out_csv="backtest/journal/exports_live/paper_trades.csv")
            except Exception:
                pass
        return 0

    # normalize (only when non-empty)
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
    print(
        f"[{now_utc_str()}] [DEBUG] before_regime df_e={len(df_e)} models={df_e['model'].unique().tolist() if len(df_e) else []} sides={df_e['side'].unique().tolist() if len(df_e) else []}")
    print(
        f"[{now_utc_str()}] [DEBUG] regime_decision "
        f"profile={getattr(decision, 'profile', None)} "
        f"reason={getattr(decision, 'reason', None)} "
        f"allow_models={getattr(decision, 'allow_models', None)} "
        f"block_models={getattr(decision, 'block_models', None)} "
        f"allow_sides={getattr(decision, 'allow_sides', None)} "
        f"block_sides={getattr(decision, 'block_sides', None)} "
        f"allow_phases={getattr(decision, 'allow_phases', None)} "
        f"block_phases={getattr(decision, 'block_phases', None)}"
    )

    df_before_regime = df_e.copy()

    def _none_if_empty(x):
        return None if (x is not None and len(x) == 0) else x

    allow_models = _none_if_empty(allow_models)
    block_models = _none_if_empty(block_models)
    allow_sides = _none_if_empty(allow_sides)
    block_sides = _none_if_empty(block_sides)
    allow_phases = _none_if_empty(allow_phases)
    block_phases = _none_if_empty(block_phases)

    if os.getenv("BYPASS_REGIME_FILTERS", "0") == "1":
        allow_models = None
        block_models = None
        allow_sides = None
        block_sides = None
        allow_phases = None
        block_phases = None

    # === RegimeDecision hard filters ===
    if allow_models:
        df_e = df_e[df_e["model"].isin(allow_models)].copy()

    if block_models:
        df_e = df_e[~df_e["model"].isin(block_models)].copy()

    if allow_sides:
        df_e = df_e[df_e["side"].isin(allow_sides)].copy()

    if block_sides:
        df_e = df_e[~df_e["side"].isin(block_sides)].copy()

    _phase_col = "setup_phase" if "setup_phase" in df_e.columns else "phase"
    if allow_phases:
        df_e = df_e[df_e[_phase_col].astype(str).str.upper().isin(allow_phases)].copy()

    if block_phases:
        df_e = df_e[~df_e[_phase_col].astype(str).str.upper().isin(block_phases)].copy()

    print(
        f"[{now_utc_str()}] [DEBUG] after_regime "
        f"df_e={len(df_e)} "
        f"models={df_e['model'].unique().tolist() if len(df_e) else []} "
        f"sides={df_e['side'].unique().tolist() if len(df_e) else []}"
    )

    trace_counts["after_regime"] = int(len(df_e))
    _trace(trace_on, f"after_regime={len(df_e)}")
    _status("after_regime", allow_models=allow_models, block_models=block_models, allow_sides=allow_sides, block_sides=block_sides, allow_phases=allow_phases, block_phases=block_phases)
    if df_e.empty:
        why = []
        if allow_models is not None and len(allow_models) == 0:
            why.append("allow_models=[]")
        if allow_sides is not None and len(allow_sides) == 0:
            why.append("allow_sides=[]")
        if allow_phases is not None and len(allow_phases) == 0:
            why.append("allow_phases=[]")

        why_str = " ".join(why) if why else "unknown/other-filters"

        if len(df_before_regime) == 0:
            reason = "No signals before regime (earlier filters removed all)"
        else:
            reason = f"RegimeDecision filtered all (before={len(df_before_regime)} after=0 ..."

        if df_e.empty:
            if len(df_before_regime) == 0:
                if debug_regime:
                    print(f"[{now_utc_str()}] [REGIME] skip (no entries before regime)")
                _ensure_live_entries_csv(out_csv)
                return 0

            # tik tada laikom, kad regime/filteriai išmetė viską
            _append_dropped(out_csv, df_before_regime, stage="REGIME", reason="RegimeDecision filtered everything", drop_ts=latest_ts)
            if debug_regime:
                print(f"...filtered all (before={len(df_before_regime)} after=0 ...)")
            _ensure_live_entries_csv(out_csv)
            return 0

    # ------------------------------------------------------------
    # emit_last_candles: keep only signals from the most recent N candles (including the latest).
    # IMPORTANT: this is based on signal_ts (emit candle), NOT on the setup timestamp.
    tf_minutes = int(bybit_interval)  # pvz. "15" -> 15

    if emit_last_candles and "signal_ts" in df_e.columns:
        df_before_emit_last = df_e.copy()
        try:
            emit_n = int(emit_last_candles)
        except Exception:
            emit_n = None
        if emit_n and emit_n > 0 and (not once):
            cutoff = latest_ts - pd.Timedelta(minutes=tf_minutes * emit_n)
            if "signal_ts" in df_e.columns:

                df_e["signal_ts"] = pd.to_datetime(df_e["signal_ts"], utc=True, errors="coerce")
            df_e = df_e.dropna(subset=["signal_ts"])
            before = len(df_e)
            _emit_rejected = df_e[df_e["signal_ts"] < cutoff].copy()
            try:
                for _, _row in _emit_rejected.iterrows():
                    diag_log(
                        "SETUP_FILTERED",
                        **_diag_payload_from_row(_row, symbol=str(bybit_symbol)),
                        filter_name="emit_last_candles",
                        reason=f"signal_ts < cutoff ({cutoff})",
                    )
                    diag_log(
                        "SETUP_EMIT_CHECK",
                        **_diag_payload_from_row(_row, symbol=str(bybit_symbol)),
                        current_time=str(latest_ts),
                        setup_age_candles=_row.get("setup_age_candles"),
                        live_max_setup_age_candles=int(live_max_setup_age_candles or 0),
                        emit_allowed=False,
                    )
            except Exception:
                pass
            df_e = df_e[df_e["signal_ts"] >= cutoff]
            if len(df_e) == 0 and len(df_before_emit_last) > 0:
                _append_dropped(out_csv, df_before_emit_last, stage="EMIT_LAST_CANDLES", reason=f"signal_ts < cutoff ({cutoff})", drop_ts=latest_ts)
            if trace_on:

                print(f"[TRACE] after_emit_last_candles={len(df_e)} cutoff={cutoff} (from {before})")

    if not disable_portfolio:
        portfolio_policy = evaluate_policy_portfolio(
            df_e,
            symbol=str(bybit_symbol),
            risk_guard_status=str(risk_guard_status),
            risk_guard_action=str(risk_guard_action),
            portfolio_state_path=str(portfolio_state_path),
            emit_n=int(emit_n),
            bybit_interval=bybit_interval,
            build_portfolio_cfg=_build_portfolio_cfg,
            load_portfolio_state=_load_portfolio_state,
            portfolio_state_cls=PortfolioState,
            filter_signals_portfolio_fn=filter_signals_portfolio,
        )
        if portfolio_policy["ok"]:
            asset_policy = portfolio_policy["asset_policy"]
            if asset_policy["log"]:
                print(
                    f"[{now_utc_str()}] [RISK_GUARD] "
                    f"{bybit_symbol}: {asset_policy['status']} (action={asset_policy['action']})"
                )

                if asset_policy["block_new_signals"]:
                    _ensure_live_entries_csv(out_csv)

            df_e = portfolio_policy["df_e"]
            state = portfolio_policy["state"]
            backfill_mode = bool(portfolio_policy["backfill_mode"])
            before_portfolio = int(portfolio_policy["before_portfolio"])

            print(f"[POST_REGIME_PORTFOLIO] before={before_portfolio} after={len(df_e)}")
            # Persist portfolio state only in live mode.
            if not backfill_mode:
                state.save()

            trace_counts["after_portfolio"] = int(len(df_e))
            _trace(trace_on, f"after_portfolio={len(df_e)}")
            _status("after_portfolio")
            if df_e.empty:
                if debug_regime:
                    print(f"[{now_utc_str()}] [PORTFOLIO] blocked all signals")
                _ensure_live_entries_csv(out_csv)
        else:
            print(f"[{now_utc_str()}] Portfolio filter error -> fallback: {portfolio_policy['error']}")

    # keep only new entries since last state
    # apply only in continuous live loop; skip in --once and backfill/debug
    before_last_seen = len(df_e)
    df_e, last_ts = enforce_idempotency(
        df_e,
        once=once,
        emit_last_candles=emit_last_candles,
        state_path=state_path,
        read_state_fn=_read_state,
    )
    print(
        f"[POST_REGIME_LAST_SEEN] before={before_last_seen} after={len(df_e)} "
        f"last_ts={last_ts}"
    )
    # TRACE/STATUS: after last_seen filter
    if once:
        trace_counts["after_last_seen"] = "SKIP (--once mode)"
    elif emit_last_candles:
        trace_counts["after_last_seen"] = "SKIP (emit_last_candles/backfill mode)"
    else:
        trace_counts["after_last_seen"] = int(len(df_e))
    _trace(trace_on, f"after_last_seen={trace_counts['after_last_seen']}")
    _status("after_last_seen")


    if df_e.empty:
        _ensure_live_entries_csv(out_csv)
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
    # restore setup timestamp for audit (keep signal_ts as emit timestamp)
    if "setup_ts" in df_e.columns:
        df_e["timestamp"] = pd.to_datetime(df_e["setup_ts"], utc=True, errors="coerce")
        df_e = df_e.drop(columns=["setup_ts"])



    # ============================================================
    # STAGE 5 / STEP 3: RiskDecision DTO adapter (no-op, fail-open)
    # Policy-layer contract only; no logic / behavior changes.
    # ============================================================
    try:
        if df_to_risk_decisions is not None:
            _ = df_to_risk_decisions(df_e, symbol=str(bybit_symbol))
    except Exception:
        pass

    # ============================================================
    # WRITE live entries CSV (stable schema)
    # ============================================================
    emit_signals(
        df_e=df_e,
        out_csv=out_csv,
        bybit_symbol=str(bybit_symbol),
        live_entries_columns=LIVE_ENTRIES_COLUMNS,
        live_entries_dtypes=LIVE_ENTRIES_DTYPES,
        ensure_live_entries_csv=_ensure_live_entries_csv,
        append_csv=_append_csv,
        diag_log=diag_log,
        diag_payload_from_row=_diag_payload_from_row,
        now_utc_str=now_utc_str,
    )

    try:
        diag_log(
            "POST_EMIT_HANDOFF_START",
            symbol=str(bybit_symbol),
            count=int(len(df_e)),
            paper=bool(paper),
            once=bool(once),
        )
    except Exception:
        pass

    handoff_signals(
        df_e=df_e,
        paper=paper,
        bybit_symbol=str(bybit_symbol),
        latest_ts=latest_ts,
        rr=rr,
        once=once,
        diag_log=diag_log,
        now_utc_str=now_utc_str,
    )

    # ------------------------------------------------------------
    # DEV3 (SPRINT-3 STEP-1): Equity Curve Tracker (drawdown source of truth)
    # updates every runner cycle
    # ------------------------------------------------------------
    try:
        update_equity_curve_from_trades(
            trades_csv="backtest/journal/trades.csv",
            out_csv="backtest/journal/exports_live/equity_curve.csv",
            initial_equity=10_000.0,
            window_trades=60,
        )
    except Exception as _e:
        print(f"[{now_utc_str()}] [EQUITY][WARN] update_equity_curve failed: {repr(_e)}")
    # --- DEV3 SPRINT-4: symbol performance (after equity) ---
    try:
        update_symbol_performance(
            trades_csv="backtest/journal/trades.csv",
            out_csv="backtest/journal/exports_live/symbol_performance.csv",
        )
    except Exception as _e:
        print(f"[{now_utc_str()}] [SYMBOL_PERF][WARN] update failed: {repr(_e)}")

    # update state with newest timestamp
    newest = None
    try:
        if df_e is not None and (not df_e.empty) and ("timestamp" in df_e.columns):
            _ts = df_e["timestamp"].iloc[-1]
            newest = pd.to_datetime(_ts, utc=True, errors="coerce")
    except Exception:
        newest = None

    _write_state(state_path, latest_ts)

    # cycle metrics (one row per symbol per cycle)
    try:
        _ensure_dropped_csv(Path(out_csv))
        _append_cycle_metrics(out_csv, {
            "cycle_ts": now_utc_str(),
            "latest_ts": str(latest_ts),
            "source": str(source),
            "interval_min": int(bybit_interval) if str(bybit_interval).isdigit() else np.nan,
            "symbol": str(bybit_symbol),
            "once": bool(once),
            "new_candle": True,
            "note": "",
            "raw_entries": trace_counts.get("entries_from_model", np.nan),
            "kept_after_invalidation": trace_counts.get("after_invalidation", np.nan),
            "dropped_invalidation": trace_counts.get("dropped_invalidation", np.nan),
            "kept_after_live_guard": trace_counts.get("after_live_guard", np.nan),
            "dropped_live_guard": trace_counts.get("dropped_live_guard", np.nan),
            "kept_after_window": trace_counts.get("after_live_window", np.nan),
            "after_context": trace_counts.get("after_context", np.nan),
            "after_regime": trace_counts.get("after_regime", np.nan),
            "after_emit_last_candles": trace_counts.get("after_emit_last_candles", np.nan),
            "written_entries": int(len(df_e)),
            "written_closed": np.nan,
            "written_dropped": np.nan,
        })

    except Exception:
        pass

    print(f"[{now_utc_str()}] Wrote {len(df_e)} entries -> live_entries {out_csv}")
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
    p.add_argument("--out", type=str, default="backtest/journal/exports_live/live_entries.csv")
    p.add_argument("--status_path", type=str, default=str(LIVE_STATUS_DEFAULT), help="Dashboard status JSON (for ui/dashboard.py).")
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
        "--phase_guard",
        action="store_true",
        help="DEV: if phase=PHASE_TREND_UP but trend_dir=DOWN, override phase->PHASE_RANGE (opt-in)",
    )
    p.add_argument(
        "--diag_always",
        action="store_true",
        help="DIAG: print ENTRY_DIAG drop reasons even when entries are generated (debug only).",
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

    # --- DEV4: global kill switch (rolling R) ---
    p.add_argument("--kill_window_days", type=int, default=7, help="Kill-switch rolling window in days (sum of R).")
    p.add_argument("--kill_threshold_r", type=float, default=-10.0, help="Kill-switch threshold in R (trigger if rolling R <= threshold).")
    p.add_argument("--kill_min_trades", type=int, default=0, help="Kill-switch min trades in window (0 = ignore).")

    # --- DEV4: TTS gate toggle (trend alignment filter for TDP_REENTRY) ---
    p.add_argument("--enable_tts_gate", action="store_true", help="Enable TTS gate for TDP_REENTRY long/short.")
    p.add_argument(
        "--cluster_score_mode",
        type=str,
        default="LEGACY",
        help="Cluster ranking mode: LEGACY (default) or SIGNAL_SCORE. Invalid values fall back to LEGACY.",
    )
    p.add_argument(
        "--cluster_rank_signal_score",
        action="store_true",
        help="Backward-compatible alias for --cluster_score_mode SIGNAL_SCORE.",
    )


    # testing helpers
    p.add_argument(
        "--emit_last_candles",
        type=int,
        default=0,
        help="TEST MODE: emit signals from the last N candles (instead of only newest). Useful to observe flow.",
    )
    p.add_argument("--setup_keep_candles", type=int, default=96, help="LIVE: keep setups from last N candles (setup timestamp window). Helps when setup forms earlier than entry candle.")
    p.add_argument("--live_max_setup_age_candles", type=int, default=0, help="LIVE: extra guard. Emit only if setup_age_candles <= X (0=disabled). Standard execution hygiene to avoid stale setups.")
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

    p.add_argument(
        "--paper",
        action="store_true",
        help="After emitting entries, write exports_live/signals_live.csv and run paper executor (updates paper_trades.csv).",
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

    # ------------------------------------------------------------
    # RUN MODE contract (Phase3 KPI hard lock)
    # ------------------------------------------------------------
    RUN_MODE = (os.environ.get("RUN_MODE") or "KPI_VALIDATION").strip().upper()
    if RUN_MODE not in ("KPI_VALIDATION", "DEV_DEBUG"):
        RUN_MODE = "KPI_VALIDATION"

    print(f"[RUN_MODE] mode={RUN_MODE}")
    IS_KPI = (RUN_MODE == "KPI_VALIDATION")

    # KPI_VALIDATION: force-disable debug toggles (fail-open)
    if IS_KPI:
        try:
            args.debug_regime = False
        except Exception:
            pass
        try:
            args.debug_entry_filters = False
        except Exception:
            pass
        try:
            args.emit_last_candles = 0
        except Exception:
            pass


    # --- [SYMBOL_PERF] contract tag (must appear every run, fail-open) ---
    try:
        # IMPORTANT: čia turi būti TRADES failas, ne regime/perf CSV.
        # Jei turi args.trades_csv ar panašų — naudok jį. Jei ne, palik default.
        trades_csv = getattr(args, "trades_csv", None) or "backtest/journal/trades.csv"
        _print_symbol_perf_contract(trades_csv)
    except Exception:
        # never block runner on telemetry
        try:
            print("[SYMBOL_PERF] sharpe=None winrate=None n=0 window=0 status=DISABLED reason=EXC")
        except Exception:
            pass

    # --- [PYRAMID] contract tag (must appear every run) ---
    # Printed immediately after [BOOT], no strategy/edge logic touched.
    try:
        if _PYRAMID_OK:
            print("[PYRAMID] status=ACTIVE reason=module_ok")
        else:
            print(f"[PYRAMID] status=DISABLED reason=IMPORT_ERROR err={_PYRAMID_IMPORT_ERR}")
    except Exception:
        # never block runner on telemetry
        pass

    # ============================================================
    # PHASE2 — PORTFOLIO_CAP TELEMETRY (1x per run, fail-open)
    # ============================================================
    try:
        from backtest.portfolio.portfolio_exposure import load_portfolio_exposure

        exp = load_portfolio_exposure(args.portfolio_state_path)
        bu = (exp or {}).get("bucket_used", {}) or {}
        positions = (exp or {}).get("positions", []) or []

        btc = float(bu.get("BTC", 0.0) or 0.0)
        alt = float(bu.get("ALT", 0.0) or 0.0)
        meme = float(bu.get("MEME", 0.0) or 0.0)
        total = float(bu.get("GLOBAL", btc + alt + meme) or (btc + alt + meme))

        if isinstance(positions, list) and len(positions) > 0:
            source = "positions"
            reason = "OK"
            status = "ENABLED"
        else:
            source = "state"
            reason = "NO_POSITIONS"
            status = "ENABLED"

        print(
            "[PORTFOLIO_CAP] "
            f"btc={btc:.2f} alt={alt:.2f} meme={meme:.2f} total={total:.2f} "
            f"source={source} status={status} reason={reason}"
        )

    except Exception as e:
        print(
            "[PORTFOLIO_CAP] "
            "btc=0.00 alt=0.00 meme=0.00 total=0.00 "
            f"source=state status=DISABLED reason={type(e).__name__}"
        )

    # === LIQUIDATION CONTEXT (Bybit public WS, context-only) ===
    # Contract: exactly-once per --once run. Runner owns the [LIQ] log (no prints inside liquidation.py).
    syms = list(symbols)

    liq_ok = False
    liq_reason = "START_FAILED"
    liq_err = ""

    try:
        liq_ok = bool(start_liquidation_stream(syms))
        liq_reason = "OK" if liq_ok else "START_FAILED"
    except Exception as e:
        liq_ok = False
        liq_reason = "EXCEPTION"
        liq_err = f" err={repr(e)}"

    if liq_ok:
        print(f"[{now_utc_str()}] [LIQ] WS started for {len(syms)} symbols")
    else:
        print(f"[{now_utc_str()}] [LIQ] WS disabled reason={liq_reason}{liq_err}")


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
                emit_last_candles=(0 if IS_KPI else int(getattr(args, "emit_last_candles", 0) or 0)),
                from_ts=str(getattr(args, "from_ts", "")),
                live_max_setup_age_candles=int(getattr(args, "live_max_setup_age_candles", 0) or 0),
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
                kill_window_days=int(getattr(args, 'kill_window_days', 7) or 7),
                kill_threshold_r=float(getattr(args, 'kill_threshold_r', -10.0) or -10.0),
                kill_min_trades=int(getattr(args, 'kill_min_trades', 0) or 0),
                kill_trades_csv=str(getattr(args, 'regime_perf_csv', '')),
                enable_tts_gate=bool(getattr(args, 'enable_tts_gate', False)),
                disable_invalidation=bool(getattr(args, 'disable_invalidation', False)),
                paper=bool(getattr(args, 'paper', False)),
                diag_always=args.diag_always,
                phase_guard=bool(args.phase_guard),
                cluster_rank_signal_score=bool(getattr(args, "cluster_rank_signal_score", False)),
                cluster_score_mode=str(getattr(args, "cluster_score_mode", "LEGACY")),
            )

        # write aggregated status for dashboard
        _write_live_status(status_path, {
            "updated_at_utc": now_utc_str(),
            "mode": "once",
            "symbols": symbols,
            "controls": live_controls,
            **_pick_top_macro(status_map),
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
                    emit_last_candles=(0 if IS_KPI else int(getattr(args, "emit_last_candles", 0) or 0)),
                    from_ts=str(getattr(args, "from_ts", "")),
                    setup_keep_candles=int(getattr(args, "setup_keep_candles", 96) or 96),
                    live_max_setup_age_candles=int(getattr(args, "live_max_setup_age_candles", 0) or 0),
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
                kill_window_days=args.kill_window_days,
                kill_threshold_r=args.kill_threshold_r,
                kill_min_trades=args.kill_min_trades,
                enable_tts_gate=args.enable_tts_gate,
                paper=args.paper,
                cluster_rank_signal_score=bool(getattr(args, "cluster_rank_signal_score", False)),
                cluster_score_mode=str(getattr(args, "cluster_score_mode", "LEGACY")),
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
            **_pick_top_macro(status_map),
            "per_symbol": status_map,
        })

        time.sleep(max(1, args.interval))


if __name__ == "__main__":
    main()
