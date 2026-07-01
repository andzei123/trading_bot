from __future__ import annotations

"""Production decision pipeline core (1:1 for live + offline).

DEV4 PHASE-1

This module provides a single function :func:`run_pipeline_once` that executes
the *production* decision layer for one symbol at one timestamp.

Guardrail:
  - live_signal_runner.py and offline_live_runner_backtest.py are orchestrators.
  - All decision/filter/cap logic changes must happen here.

Fail-open:
  - Any exception results in an empty dataframe with guaranteed schema.
"""

from dataclasses import dataclass
from pathlib import Path

from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd

from backtest.live.pipeline_helpers.diagnostics import _write_candidate_pressure_row
from backtest.live.pipeline_helpers.entry_normalization import _entries_to_df
from backtest.live.pipeline_helpers.normalization import (
    _safe_to_datetime_utc,
    _series_col_or_default,
)
from backtest.live.pipeline_helpers.schema import LIVE_ENTRIES_COLUMNS, _empty_entries_df

from backtest.filters.signal_cluster_filter import apply_signal_cluster_filter, cluster_filter_entries
from backtest.filters.cluster_score_shadow_v2 import append_cluster_score_shadow_v2, shadow_score_v2_value, shadow_score_v3_tdp_only_value, shadow_score_v4a_range_wide_value, shadow_score_v4b_range_realistic_value, shadow_score_v4c_range_high_rr_penalty_value
from backtest.live.phase_router import decide_phase
from backtest.risk.portfolio_correlation_caps import _bucket as _corr_bucket  # type: ignore
from backtest.journal.identity import _ensure_canonical_setup_key

PRE_VISIBLE_ENTRY_EXPOSURE_COLUMNS = [
    "cycle_ts",
    "latest_ts",
    "symbol",
    "model",
    "side",
    "timestamp",
    "setup_created_ts",
    "candidate_ts",
    "structural_ts",
    "canonical_setup_key",
    "setup_id",
    "entry",
    "sl",
    "tp",
    "rr",
    "candidate_source",
    "pre_visible_reason",
    "has_visible_ts_before_assignment",
    "visible_ts_before_assignment",
    "pipeline_visible_ts_before_assignment",
    "candidate_latest_ts",
    "trigger_refresh_candidate",
    "trigger_refresh_reason",
    "old_canonical_setup_key",
    "original_setup_created_ts",
    "raw_candidate_count",
    "cluster_group_count",
]


def _pre_visible_path(path_like) -> Optional[Path]:
    if path_like is None:
        return None
    path_s = str(path_like).strip()
    if not path_s:
        return None
    return Path(path_s)


def _pre_visible_get(entry, name: str, default=""):
    if isinstance(entry, dict):
        return entry.get(name, default)
    return getattr(entry, name, default)


def _pre_visible_ts(value):
    return pd.to_datetime(value, utc=True, errors="coerce")


def _pre_visible_structural_ts(entry):
    for source in ("structural_ts", "setup_created_ts", "candidate_ts", "timestamp"):
        ts = _pre_visible_ts(_pre_visible_get(entry, source, pd.NaT))
        if pd.notna(ts):
            return ts
    return pd.NaT


def _pre_visible_setup_id(symbol: str, entry) -> str:
    existing = str(_pre_visible_get(entry, "setup_id", "") or "")
    if existing:
        return existing
    ts = _pre_visible_ts(_pre_visible_get(entry, "timestamp", pd.NaT))
    model = str(_pre_visible_get(entry, "model", "") or "")
    side = str(_pre_visible_get(entry, "side", "") or "").upper()
    return f"{str(symbol).upper()}|{ts}|{model}|{side}"


def _append_pre_visible_entry_exposure_rows(
    path_like,
    *,
    entries,
    symbol: str,
    latest_ts: pd.Timestamp,
    cycle_ts,
) -> None:
    path = _pre_visible_path(path_like)
    if path is None or not entries:
        return

    latest_ts = _pre_visible_ts(latest_ts)
    cycle_ts = _pre_visible_ts(cycle_ts)
    if pd.isna(cycle_ts):
        cycle_ts = latest_ts

    rows = []
    raw_count = int(len(entries))
    for entry in entries:
        # Telemetry-only read/copy. Do not mutate entry dicts or Entry objects.
        visible_before = _pre_visible_get(entry, "visible_ts", "")
        pipeline_visible_before = _pre_visible_get(entry, "pipeline_visible_ts", "")
        visible_ts = _pre_visible_ts(visible_before)
        pipeline_visible_ts = _pre_visible_ts(pipeline_visible_before)
        timestamp = _pre_visible_ts(_pre_visible_get(entry, "timestamp", pd.NaT))
        setup_created_ts = _pre_visible_ts(_pre_visible_get(entry, "setup_created_ts", timestamp))
        candidate_ts = _pre_visible_ts(_pre_visible_get(entry, "candidate_ts", timestamp))
        structural_ts = _pre_visible_structural_ts(entry)
        rows.append({
            "cycle_ts": cycle_ts,
            "latest_ts": latest_ts,
            "symbol": str(_pre_visible_get(entry, "symbol", symbol) or symbol).upper(),
            "model": str(_pre_visible_get(entry, "model", "") or ""),
            "side": str(_pre_visible_get(entry, "side", "") or "").upper(),
            "timestamp": timestamp,
            "setup_created_ts": setup_created_ts,
            "candidate_ts": candidate_ts,
            "structural_ts": structural_ts,
            "canonical_setup_key": str(_pre_visible_get(entry, "canonical_setup_key", "") or ""),
            "setup_id": _pre_visible_setup_id(symbol, entry),
            "entry": _pre_visible_get(entry, "entry", ""),
            "sl": _pre_visible_get(entry, "sl", ""),
            "tp": _pre_visible_get(entry, "tp", ""),
            "rr": _pre_visible_get(entry, "rr", ""),
            "candidate_source": "generate_entries_from_ctx",
            "pre_visible_reason": "admitted_to_entries",
            "has_visible_ts_before_assignment": bool(pd.notna(visible_ts) or pd.notna(pipeline_visible_ts)),
            "visible_ts_before_assignment": visible_ts,
            "pipeline_visible_ts_before_assignment": pipeline_visible_ts,
            "candidate_latest_ts": _pre_visible_ts(_pre_visible_get(entry, "candidate_latest_ts", pd.NaT)),
            "trigger_refresh_candidate": bool(_pre_visible_get(entry, "trigger_refresh_candidate", False)),
            "trigger_refresh_reason": str(_pre_visible_get(entry, "trigger_refresh_reason", "") or ""),
            "old_canonical_setup_key": str(_pre_visible_get(entry, "old_canonical_setup_key", "") or ""),
            "original_setup_created_ts": _pre_visible_get(entry, "original_setup_created_ts", ""),
            "raw_candidate_count": raw_count,
            "cluster_group_count": "",
        })

    if not rows:
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    out = pd.DataFrame(rows)
    for col in PRE_VISIBLE_ENTRY_EXPOSURE_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    out = out[PRE_VISIBLE_ENTRY_EXPOSURE_COLUMNS]
    if not path.exists() or path.stat().st_size == 0:
        out.to_csv(path, index=False)
    else:
        out.to_csv(path, mode="a", header=False, index=False)


TDP_VISIBLE_ASSIGNMENT_TRACE_COLUMNS = [
    "timestamp",
    "cycle_ts",
    "symbol",
    "model",
    "canonical_setup_key",
    "setup_created_ts",
    "candidate_ts",
    "previous_visible_ts",
    "assigned_visible_ts",
    "assignment_reason",
    "assignment_callsite",
    "pre_assignment_death_stage",
    "pre_assignment_death_reason",
    "passed_wait",
    "is_emitted",
    "entry",
    "sl",
    "tp",
    "distance_to_entry_R",
    "entry_window_expires_ts",
    "wait_confirm_ts",
    "latest_ts",
    "raw_candidate_count",
    "pressure_window_id",
    "pressure_window_age_bars",
    "candidate_persistence_bars",
]


def _append_tdp_visible_assignment_trace_row(
    path_like,
    *,
    entry,
    symbol: str,
    latest_ts: pd.Timestamp,
    cycle_ts,
    previous_visible_ts,
    assigned_visible_ts,
    assignment_reason: str,
    assignment_callsite: str,
    raw_candidate_count: int,
) -> None:
    """Telemetry-only trace for TDP visible_ts assignment.

    This function copies fields only. It must not mutate candidate rows/objects
    and must not influence ranking, wait, stale, idempotency, emission, execution,
    lifecycle registry, or position state.
    """
    path = _pre_visible_path(path_like)
    if path is None:
        return

    model = str(_pre_visible_get(entry, "model", "") or "")
    if model != "TDP_REENTRY":
        return

    timestamp = _pre_visible_ts(_pre_visible_get(entry, "timestamp", pd.NaT))
    setup_created_ts = _pre_visible_ts(_pre_visible_get(entry, "setup_created_ts", timestamp))
    candidate_ts = _pre_visible_ts(_pre_visible_get(entry, "candidate_ts", timestamp))
    latest_ts = _pre_visible_ts(latest_ts)
    cycle_ts = _pre_visible_ts(cycle_ts)
    if pd.isna(cycle_ts):
        cycle_ts = latest_ts

    canonical = str(_pre_visible_get(entry, "canonical_setup_key", "") or "")
    if not canonical:
        canonical = str(_pre_visible_get(entry, "setup_id", "") or "")

    row = {
        "timestamp": timestamp,
        "cycle_ts": cycle_ts,
        "symbol": str(_pre_visible_get(entry, "symbol", symbol) or symbol).upper(),
        "model": model,
        "canonical_setup_key": canonical,
        "setup_created_ts": setup_created_ts,
        "candidate_ts": candidate_ts,
        "previous_visible_ts": _pre_visible_ts(previous_visible_ts),
        "assigned_visible_ts": _pre_visible_ts(assigned_visible_ts),
        "assignment_reason": str(assignment_reason or ""),
        "assignment_callsite": str(assignment_callsite or ""),
        "pre_assignment_death_stage": str(_pre_visible_get(entry, "death_stage", "") or ""),
        "pre_assignment_death_reason": str(_pre_visible_get(entry, "death_reason", "") or ""),
        "passed_wait": _pre_visible_get(entry, "passed_wait", ""),
        "is_emitted": bool(_pre_visible_get(entry, "is_emitted", _pre_visible_get(entry, "emitted", False))),
        "entry": _pre_visible_get(entry, "entry", ""),
        "sl": _pre_visible_get(entry, "sl", ""),
        "tp": _pre_visible_get(entry, "tp", ""),
        "distance_to_entry_R": _pre_visible_get(entry, "distance_to_entry_R", ""),
        "entry_window_expires_ts": _pre_visible_ts(_pre_visible_get(entry, "entry_window_expires_ts", pd.NaT)),
        "wait_confirm_ts": _pre_visible_ts(_pre_visible_get(entry, "wait_confirm_ts", pd.NaT)),
        "latest_ts": latest_ts,
        "raw_candidate_count": int(raw_candidate_count),
        "pressure_window_id": str(_pre_visible_get(entry, "pressure_window_id", "") or ""),
        "pressure_window_age_bars": _pre_visible_get(entry, "pressure_window_age_bars", ""),
        "candidate_persistence_bars": _pre_visible_get(entry, "candidate_persistence_bars", ""),
    }

    path.parent.mkdir(parents=True, exist_ok=True)
    out = pd.DataFrame([row])
    for col in TDP_VISIBLE_ASSIGNMENT_TRACE_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    out = out[TDP_VISIBLE_ASSIGNMENT_TRACE_COLUMNS]
    if not path.exists() or path.stat().st_size == 0:
        out.to_csv(path, index=False)
    else:
        out.to_csv(path, mode="a", header=False, index=False)


TDP_IDENTITY_RESURFACING_TRACE_COLUMNS = [
    "cycle_ts",
    "latest_ts",
    "symbol",
    "model",
    "side",
    "canonical_setup_key",
    "setup_id",
    "setup_created_ts",
    "candidate_ts",
    "timestamp",
    "entry",
    "sl",
    "tp",
    "is_first_generator_seen",
    "previous_generator_seen_ts",
    "bars_since_previous_seen",
    "generator_seen_count",
    "raw_candidate_count",
    "candidate_source",
    "candidate_accept_reason",
    "candidate_reject_reason",
]


TDP_IDENTITY_RESURFACING_SEEN = {}


def _tdp_identity_resurfacing_key(symbol: str, entry) -> str:
    canonical = str(_pre_visible_get(entry, "canonical_setup_key", "") or "")
    if canonical:
        return canonical
    setup_id = str(_pre_visible_get(entry, "setup_id", "") or "")
    if setup_id:
        return setup_id
    ts = _pre_visible_ts(_pre_visible_get(entry, "timestamp", pd.NaT))
    side = str(_pre_visible_get(entry, "side", "") or "").upper()
    return f"{str(symbol).upper()}|TDP_REENTRY|{side}|{ts}"


def _append_tdp_identity_resurfacing_trace_rows(
    path_like,
    *,
    entries,
    symbol: str,
    latest_ts: pd.Timestamp,
    cycle_ts,
) -> None:
    """Telemetry-only TDP generator-output resurfacing trace.

    This logger runs on entries returned by generate_entries_from_ctx(...)
    before visible_ts assignment. It copies fields only and never influences
    generation, ranking, wait, stale, idempotency, emission, execution,
    lifecycle registry, or position state.
    """
    path = _pre_visible_path(path_like)
    if path is None or not entries:
        return

    latest_ts = _pre_visible_ts(latest_ts)
    cycle_ts = _pre_visible_ts(cycle_ts)
    if pd.isna(cycle_ts):
        cycle_ts = latest_ts

    rows = []
    raw_count = int(len(entries))
    for entry in entries:
        model = str(_pre_visible_get(entry, "model", "") or "")
        if model != "TDP_REENTRY":
            continue

        key = _tdp_identity_resurfacing_key(symbol, entry)
        prev_state = TDP_IDENTITY_RESURFACING_SEEN.get(key)
        if prev_state is None:
            is_first = True
            previous_seen_ts = pd.NaT
            bars_since_previous = ""
            seen_count = 1
        else:
            is_first = False
            previous_seen_ts = _pre_visible_ts(prev_state.get("last_seen_ts", pd.NaT))
            seen_count = int(prev_state.get("count", 0) or 0) + 1
            if pd.notna(previous_seen_ts) and pd.notna(latest_ts):
                bars_since_previous = int(max(0, (latest_ts - previous_seen_ts).total_seconds() // (15 * 60)))
            else:
                bars_since_previous = ""

        TDP_IDENTITY_RESURFACING_SEEN[key] = {
            "last_seen_ts": latest_ts,
            "count": seen_count,
        }

        timestamp = _pre_visible_ts(_pre_visible_get(entry, "timestamp", pd.NaT))
        setup_created_ts = _pre_visible_ts(_pre_visible_get(entry, "setup_created_ts", timestamp))
        candidate_ts = _pre_visible_ts(_pre_visible_get(entry, "candidate_ts", timestamp))
        setup_id = str(_pre_visible_get(entry, "setup_id", "") or "")
        canonical = str(_pre_visible_get(entry, "canonical_setup_key", "") or "")
        if not canonical:
            canonical = setup_id or key

        rows.append({
            "cycle_ts": cycle_ts,
            "latest_ts": latest_ts,
            "symbol": str(_pre_visible_get(entry, "symbol", symbol) or symbol).upper(),
            "model": model,
            "side": str(_pre_visible_get(entry, "side", "") or "").upper(),
            "canonical_setup_key": canonical,
            "setup_id": setup_id,
            "setup_created_ts": setup_created_ts,
            "candidate_ts": candidate_ts,
            "timestamp": timestamp,
            "entry": _pre_visible_get(entry, "entry", ""),
            "sl": _pre_visible_get(entry, "sl", ""),
            "tp": _pre_visible_get(entry, "tp", ""),
            "is_first_generator_seen": bool(is_first),
            "previous_generator_seen_ts": previous_seen_ts,
            "bars_since_previous_seen": bars_since_previous,
            "generator_seen_count": int(seen_count),
            "raw_candidate_count": raw_count,
            "candidate_source": "generate_entries_from_ctx",
            "candidate_accept_reason": "returned_in_entries",
            "candidate_reject_reason": "",
        })

    if not rows:
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    out = pd.DataFrame(rows)
    for col in TDP_IDENTITY_RESURFACING_TRACE_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    out = out[TDP_IDENTITY_RESURFACING_TRACE_COLUMNS]
    if not path.exists() or path.stat().st_size == 0:
        out.to_csv(path, index=False)
    else:
        out.to_csv(path, mode="a", header=False, index=False)


def _invalidate_setups_hit_tp_sl(
    df_e: pd.DataFrame, candles: pd.DataFrame, latest_ts: pd.Timestamp
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Copy of live invalidation guard (conservative OHLC TP/SL check).

    Returns (df_active, df_closed).
    """
    if df_e is None or df_e.empty:
        return df_e, pd.DataFrame()
    if candles is None or candles.empty:
        return df_e, pd.DataFrame()

    c = candles.copy()
    if "timestamp" not in c.columns or "high" not in c.columns or "low" not in c.columns:
        return df_e, pd.DataFrame()

    c["timestamp"] = pd.to_datetime(c["timestamp"], utc=True, errors="coerce")
    c = c.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    latest_ts = pd.to_datetime(latest_ts, utc=True, errors="coerce")
    if pd.isna(latest_ts):
        return df_e, pd.DataFrame()

    df = df_e.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    for col in ("entry", "sl", "tp"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["setup_status"] = "ACTIVE"
    df["setup_close_reason"] = ""
    df["setup_entry_touch_ts"] = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns, UTC]")
    df["setup_close_ts"] = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns, UTC]")

    closed_rows = []

    for idx, row in df.iterrows():
        setup_ts = row.get("timestamp")
        if pd.isna(setup_ts):
            continue
        side = str(row.get("side", "")).upper().strip()
        entry = row.get("entry")
        sl = row.get("sl")
        tp = row.get("tp")
        if not np.isfinite(entry) or not np.isfinite(sl) or not np.isfinite(tp):
            continue

        # consider candles strictly after setup_ts
        post = c[c["timestamp"] > setup_ts]
        if post.empty:
            continue

        entry_touch_ts: Optional[pd.Timestamp] = None
        close_ts: Optional[pd.Timestamp] = None
        close_reason: str = ""

        # 1) find entry touch
        for _, r in post.iterrows():
            if float(r["low"]) <= float(entry) <= float(r["high"]):
                entry_touch_ts = pd.Timestamp(r["timestamp"])
                break
        if entry_touch_ts is None:
            continue

        # 2) after entry touch: detect SL/TP hit
        post2 = post[post["timestamp"] >= entry_touch_ts]
        for _, r in post2.iterrows():
            h = float(r["high"])
            l = float(r["low"])
            ts = pd.Timestamp(r["timestamp"])

            if side == "LONG":
                sl_hit = l <= float(sl)
                tp_hit = h >= float(tp)
                if sl_hit and tp_hit:
                    close_reason = "SL_HIT"
                    close_ts = ts
                    break
                if sl_hit:
                    close_reason = "SL_HIT"
                    close_ts = ts
                    break
                if tp_hit:
                    close_reason = "TP_HIT"
                    close_ts = ts
                    break
            else:  # SHORT
                sl_hit = h >= float(sl)
                tp_hit = l <= float(tp)
                if sl_hit and tp_hit:
                    close_reason = "SL_HIT"
                    close_ts = ts
                    break
                if sl_hit:
                    close_reason = "SL_HIT"
                    close_ts = ts
                    break
                if tp_hit:
                    close_reason = "TP_HIT"
                    close_ts = ts
                    break

        if close_ts is None:
            continue

        df.at[idx, "setup_status"] = "CLOSED"
        df.at[idx, "setup_close_reason"] = close_reason
        df.at[idx, "setup_entry_touch_ts"] = entry_touch_ts
        df.at[idx, "setup_close_ts"] = close_ts
        closed_rows.append(idx)

    if not closed_rows:
        return df, pd.DataFrame()

    df_closed = df.loc[closed_rows].copy()
    df_active = df.drop(index=closed_rows).copy()
    return df_active, df_closed


def run_pipeline_once(
    *,
    symbol: str,
    candles_df: pd.DataFrame,
    ctx: Dict[str, Any],
    portfolio_state: Dict[str, Any],
    debug: bool = False,
) -> pd.DataFrame:
    """Run the production decision pipeline for one symbol.

    Parameters
    ----------
    symbol:
        Trading symbol, e.g. BTCUSDT.
    candles_df:
        Historical candle window (must include timestamp/open/high/low/close).
    ctx:
        Context snapshot (macro, regime, liq, etc.). Must include at least:
          - latest_ts (pd.Timestamp or str)
          - bybit_interval (minutes as str/int) for age calculations (optional)
          - macro_bias (optional)
          - risk_multiplier (optional)
          - freeze_new_signals (optional)
    portfolio_state:
        Portfolio exposure snapshot. Must include bucket_used.
    debug:
        Print extra logs.
    """
    try:
        if candles_df is None or candles_df.empty:
            return _empty_entries_df()

        candles = candles_df.copy()
        candles["timestamp"] = pd.to_datetime(candles["timestamp"], utc=True, errors="coerce")
        candles = candles.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
        if candles.empty:
            return _empty_entries_df()

        latest_ts = pd.to_datetime(ctx.get("latest_ts", candles["timestamp"].iloc[-1]), utc=True, errors="coerce")
        if pd.isna(latest_ts):
            latest_ts = candles["timestamp"].iloc[-1]

        # -------------------
        # PHASE_PRE
        # -------------------
        macro_bias = str(ctx.get("macro_bias", "NEUTRAL") or "NEUTRAL").upper()
        ph_pre = decide_phase(candles=candles, macro_bias=macro_bias)

        # Single source of truth for entry-model-compatible phase string
        ph_raw = str(getattr(ph_pre, "phase", ph_pre) or "").upper()
        if ph_raw == "LONG":
            phase_authority = "PHASE_TREND_UP"
        elif ph_raw == "SHORT":
            phase_authority = "PHASE_TREND_DOWN"
        elif ph_raw == "RANGE":
            phase_authority = "PHASE_RANGE"
        else:
            phase_authority = ph_raw

        ctx["phase"] = phase_authority

        # Prefer PhaseDecision.atr_pct (works for offline too); fallback to candles column if present
        atrp = getattr(ph_pre, "atr_pct", None)
        if atrp is None:
            try:
                atrp = float(candles["atr_pct"].iloc[-1]) if "atr_pct" in candles.columns else np.nan
            except Exception:
                atrp = np.nan

        # normalize ctx for downstream telemetry
        try:
            if atrp is not None and np.isfinite(float(atrp)):
                ctx["atr_pct"] = float(atrp)
        except Exception:
            pass

        print(f"[PHASE_PRE][{symbol}] {ph_pre} | authority={phase_authority} macro_bias={macro_bias} atr%={atrp}")
        # -------------------
        # ENTRY GENERATION
        # -------------------
        try:
            from backtest.engine.entry_model import generate_entries_from_ctx
        except Exception:
            # fallback for alt layout
            from backtest.engine import entry_model as _em  # type: ignore

            generate_entries_from_ctx = _em.generate_entries_from_ctx  # type: ignore

        # build full ctx df for entry model: use journal.filter_trades ctx builder (same as live)
        import backtest.journal.filter_trades as ft

        ctx_df = ft.build_ctx(candles)
        try:
            ctx_df["phase"] = phase_authority
        except Exception:
            pass

        try:
            print(f"[CTX_DIAG][{symbol}] rows={len(ctx_df)} cols={len(ctx_df.columns)}")
            if "phase" in ctx_df.columns:
                print(f"[CTX_DIAG][{symbol}] phase_tail={ctx_df['phase'].tail(3).tolist()}")
            if "sub_label" in ctx_df.columns:
                print(
                    f"[CTX_DIAG][{symbol}] sub_label_top={ctx_df['sub_label'].astype(str).value_counts().head(10).to_dict()}")
            else:
                print(f"[CTX_DIAG][{symbol}] sub_label missing")
        except Exception as e:
            print(f"[CTX_DIAG][{symbol}] failed err={e}")

        # inject macro fields used by entry_model
        for k in ("macro_bias", "macro_phase", "macro_strength", "cross_asset_regime", "cross_asset_reason"):
            if k in ctx:
                try:
                    ctx_df[k] = ctx[k]
                except Exception:
                    pass

        # Telemetry-only bridge into entry_model. Stored in DataFrame attrs so
        # entry generation signatures and returned entries remain unchanged.
        try:
            ctx_df.attrs["entry_model_pre_admission_csv"] = str(ctx.get("entry_model_pre_admission_csv", "") or "")
            ctx_df.attrs["tdp_disappearance_trace_csv"] = str(ctx.get("tdp_disappearance_trace_csv", "") or "")
            ctx_df.attrs["tdp_true_birth_trace_csv"] = str(ctx.get("tdp_true_birth_trace_csv", "") or "")
            ctx_df.attrs["cycle_ts"] = ctx.get("cycle_ts", ctx.get("latest_ts", pd.NaT))
            ctx_df.attrs["latest_ts"] = latest_ts
        except Exception:
            pass


        entries = generate_entries_from_ctx(
            ctx_df,
            rr=float(ctx.get("rr", 2.0) or 2.0),
            sl_atr_buffer=float(ctx.get("sl_atr_buffer", 0.15) or 0.15),
            require_impulse_before_tdp=bool(ctx.get("require_impulse_before_tdp", False)),
            impulse_lookback=int(ctx.get("impulse_lookback", 10) or 10),
            impulse_size_atr=float(ctx.get("impulse_size_atr", 1.0) or 1.0),
            tdp_dev_lookback=int(ctx.get("tdp_dev_lookback", 8) or 8),
            tts_retest_lookback=int(ctx.get("tts_retest_lookback", 24) or 24),
            debug_entry_filters=bool(ctx.get("debug", False)),
            symbol=str(symbol or ""),
        )
        print(f"[ENTRY_DIAG][{symbol}] raw_entries={len(entries)}")
        _append_pre_visible_entry_exposure_rows(
            ctx.get("pre_visible_entry_exposure_csv", ""),
            entries=entries,
            symbol=symbol,
            latest_ts=latest_ts,
            cycle_ts=ctx.get("cycle_ts", latest_ts),
        )

        _append_tdp_identity_resurfacing_trace_rows(
            ctx.get("tdp_identity_resurfacing_trace_csv", ""),
            entries=entries,
            symbol=symbol,
            latest_ts=latest_ts,
            cycle_ts=ctx.get("cycle_ts", latest_ts),
        )

        _write_candidate_pressure_row(
            entries=entries,
            symbol=symbol,
            ts=latest_ts,
            out_csv=str(ctx.get("candidate_pressure_csv", "") or ""),
        )

        # -------------------
        # DEBUG FORCE (pipeline-level) тАФ to exercise corr/budget/execution layers
        # -------------------
        if debug and bool(ctx.get("debug_force_entries", False)) and (not entries):
            try:
                last = candles.iloc[-1]
                setup_ts = last["timestamp"]
                close = float(last["close"])

                forced = []
                for side in ("LONG", "SHORT"):
                    forced.append(
                        {
                            "timestamp": setup_ts,
                            "model": "DEBUG_FORCED",
                            "side": side,
                            "entry": close,
                            "sl": close * (0.99 if side == "LONG" else 1.01),
                            "tp": close * (1.02 if side == "LONG" else 0.98),
                            "rr": 2.0,
                            "score": 0.5,
                            "notes": "pipeline_debug_force",
                            # optional: keep these consistent with downstream expectations
                            "risk_multiplier": 1.0,
                            "atr_pct": ctx.get("atr_pct", np.nan),
                            "phase": str(ctx.get("phase", "")),
                        }
                    )

                entries = forced
                print(f"[DEBUG_FORCE_PIPELINE][{symbol}] forced_entries={len(entries)} ts={setup_ts} close={close:.4f}")
            except Exception as e:
                print(f"[DEBUG_FORCE_PIPELINE][{symbol}] failed: {e}")

                
        # -------------------
        # CLUSTER FILTER (works on entries list)
        # -------------------
        try:
            requested_mode = str(ctx.get("cluster_score_mode", "") or "").strip().upper()

            # backward-compatible alias
            if requested_mode in ("", "LEGACY") and bool(ctx.get("cluster_rank_signal_score", False)):
                requested_mode = "SIGNAL_SCORE"

            # default behavior unchanged when no flag is provided
            if requested_mode == "SIGNAL_SCORE":
                cluster_score = "SIGNAL_SCORE"
            elif requested_mode == "SHADOW_SCORE_V2":
                cluster_score = "SHADOW_SCORE_V2"
            elif requested_mode == "SHADOW_SCORE_V3_TDP_ONLY":
                cluster_score = "SHADOW_SCORE_V3_TDP_ONLY"
            elif requested_mode == "SHADOW_SCORE_V4A_RANGE_WIDE":
                cluster_score = "SHADOW_SCORE_V4A_RANGE_WIDE"
            elif requested_mode == "SHADOW_SCORE_V4B_RANGE_REALISTIC":
                cluster_score = "SHADOW_SCORE_V4B_RANGE_REALISTIC"
            elif requested_mode == "SHADOW_SCORE_V4C_RANGE_HIGH_RR_PENALTY":
                cluster_score = "SHADOW_SCORE_V4C_RANGE_HIGH_RR_PENALTY"
            else:
                cluster_score = str(ctx.get("cluster_score", "RR") or "RR")

            if cluster_score == "SHADOW_SCORE_V2":
                try:
                    _shadow_latest_close = float(candles_df["close"].iloc[-1])
                except Exception:
                    _shadow_latest_close = 0.0
                _shadow_res = cluster_filter_entries(
                    entries,
                    max_per_group=int(ctx.get("cluster_max_per_group", 2) or 2),
                    score_fn=lambda e: shadow_score_v2_value(
                        e,
                        cluster_score="SIGNAL_SCORE",
                        latest_close=_shadow_latest_close,
                    ),
                    ranking_source="SHADOW_SCORE_V2",
                )
                kept_entries, dropped_entries = _shadow_res.kept, _shadow_res.dropped
            elif cluster_score == "SHADOW_SCORE_V3_TDP_ONLY":
                try:
                    _shadow_latest_close = float(candles_df["close"].iloc[-1])
                except Exception:
                    _shadow_latest_close = 0.0
                _shadow_res = cluster_filter_entries(
                    entries,
                    max_per_group=int(ctx.get("cluster_max_per_group", 2) or 2),
                    score_fn=lambda e: shadow_score_v3_tdp_only_value(
                        e,
                        cluster_score="SIGNAL_SCORE",
                        latest_close=_shadow_latest_close,
                    ),
                    ranking_source="SHADOW_SCORE_V3_TDP_ONLY",
                )
                kept_entries, dropped_entries = _shadow_res.kept, _shadow_res.dropped
            elif cluster_score == "SHADOW_SCORE_V4A_RANGE_WIDE":
                try:
                    _shadow_latest_close = float(candles_df["close"].iloc[-1])
                except Exception:
                    _shadow_latest_close = 0.0
                _shadow_res = cluster_filter_entries(
                    entries,
                    max_per_group=int(ctx.get("cluster_max_per_group", 2) or 2),
                    score_fn=lambda e: shadow_score_v4a_range_wide_value(
                        e,
                        cluster_score="SIGNAL_SCORE",
                        latest_close=_shadow_latest_close,
                    ),
                    ranking_source="SHADOW_SCORE_V4A_RANGE_WIDE",
                )
                kept_entries, dropped_entries = _shadow_res.kept, _shadow_res.dropped
            elif cluster_score == "SHADOW_SCORE_V4B_RANGE_REALISTIC":
                try:
                    _shadow_latest_close = float(candles_df["close"].iloc[-1])
                except Exception:
                    _shadow_latest_close = 0.0
                _shadow_res = cluster_filter_entries(
                    entries,
                    max_per_group=int(ctx.get("cluster_max_per_group", 2) or 2),
                    score_fn=lambda e: shadow_score_v4b_range_realistic_value(
                        e,
                        cluster_score="SIGNAL_SCORE",
                        latest_close=_shadow_latest_close,
                    ),
                    ranking_source="SHADOW_SCORE_V4B_RANGE_REALISTIC",
                )
                kept_entries, dropped_entries = _shadow_res.kept, _shadow_res.dropped
            elif cluster_score == "SHADOW_SCORE_V4C_RANGE_HIGH_RR_PENALTY":
                try:
                    _shadow_latest_close = float(candles_df["close"].iloc[-1])
                except Exception:
                    _shadow_latest_close = 0.0
                _shadow_res = cluster_filter_entries(
                    entries,
                    max_per_group=int(ctx.get("cluster_max_per_group", 2) or 2),
                    score_fn=lambda e: shadow_score_v4c_range_high_rr_penalty_value(
                        e,
                        cluster_score="SIGNAL_SCORE",
                        latest_close=_shadow_latest_close,
                    ),
                    ranking_source="SHADOW_SCORE_V4C_RANGE_HIGH_RR_PENALTY",
                )
                kept_entries, dropped_entries = _shadow_res.kept, _shadow_res.dropped
            else:
                kept_entries, dropped_entries = apply_signal_cluster_filter(
                    entries,
                    max_per_group=int(ctx.get("cluster_max_per_group", 2) or 2),
                    score=cluster_score,
                    phase=str(ph_pre or "") or None,
                )
            if bool(ctx.get("cluster_score_shadow_v2", False)):
                append_cluster_score_shadow_v2(
                    path_like=ctx.get("cluster_score_shadow_v2_csv", ""),
                    candidate_entries=list(entries or []),
                    production_kept=list(kept_entries or []),
                    production_dropped=list(dropped_entries or []),
                    candles_df=candles_df,
                    cycle_ts=ctx.get("cycle_ts", latest_ts),
                    latest_ts=latest_ts,
                    symbol=symbol,
                    cluster_score=cluster_score,
                    max_per_group=int(ctx.get("cluster_max_per_group", 2) or 2),
                )
            if debug:
                print(
                    f"[CLUSTER_FILTER][{symbol}] "
                    f"mode={requested_mode or 'DEFAULT'} "
                    f"score={cluster_score} "
                    f"kept={len(kept_entries)} dropped={len(dropped_entries)}"
                )
            entries = kept_entries
        except Exception:
            pass

        # -------------------
        # LIVE VISIBILITY TIMESTAMPS
        # Minimal fix for discovery gate semantics:
        # visible_ts / pipeline_visible_ts must exist before _entries_to_df()
        # so shell discovery gate can use them instead of old setup timestamp.
        # -------------------
        if entries:
            normalized_entries = []
            for e in entries:
                if isinstance(e, dict):
                    row = dict(e)
                    if row.get("visible_ts") is None:
                        _append_tdp_visible_assignment_trace_row(
                            ctx.get("tdp_visible_assignment_trace_csv", ""),
                            entry=row,
                            symbol=symbol,
                            latest_ts=latest_ts,
                            cycle_ts=ctx.get("cycle_ts", latest_ts),
                            previous_visible_ts=row.get("visible_ts", pd.NaT),
                            assigned_visible_ts=latest_ts,
                            assignment_reason="visible_ts_missing_assigned_latest_ts",
                            assignment_callsite="pipeline_core.dict_visible_ts_assignment",
                            raw_candidate_count=len(entries),
                        )
                        row["visible_ts"] = latest_ts
                    if row.get("pipeline_visible_ts") is None:
                        row["pipeline_visible_ts"] = latest_ts
                    normalized_entries.append(row)
                else:
                    try:
                        if getattr(e, "visible_ts", None) is None:
                            _append_tdp_visible_assignment_trace_row(
                                ctx.get("tdp_visible_assignment_trace_csv", ""),
                                entry=e,
                                symbol=symbol,
                                latest_ts=latest_ts,
                                cycle_ts=ctx.get("cycle_ts", latest_ts),
                                previous_visible_ts=getattr(e, "visible_ts", pd.NaT),
                                assigned_visible_ts=latest_ts,
                                assignment_reason="visible_ts_missing_assigned_latest_ts",
                                assignment_callsite="pipeline_core.object_visible_ts_assignment",
                                raw_candidate_count=len(entries),
                            )
                            setattr(e, "visible_ts", latest_ts)
                    except Exception:
                        pass
                    try:
                        if getattr(e, "pipeline_visible_ts", None) is None:
                            setattr(e, "pipeline_visible_ts", latest_ts)
                    except Exception:
                        pass
                    normalized_entries.append(e)

            entries = normalized_entries

        df_e = _entries_to_df(entries, symbol=symbol)
        if df_e.empty:
            return _empty_entries_df()
        df_e = _ensure_canonical_setup_key(df_e, symbol=symbol)

        # ensure phase column is filled
        if "phase" in df_e.columns:
            df_e["phase"] = df_e["phase"].fillna(phase_authority)
        else:
            df_e["phase"] = phase_authority

        # -------------------
        # ENTRY QUALITY FILTERS (placeholder: keep as-is; live filters already encoded upstream)
        # -------------------

        # -------------------
        # CORR_CAP (SOFT + DEBUG) тАФ 1:1 with live
        # -------------------
        CAP_BTC = float(ctx.get("cap_btc", 0.02) or 0.02)
        CAP_ALT = float(ctx.get("cap_alt", 0.02) or 0.02)
        CAP_MEME = float(ctx.get("cap_meme", 0.01) or 0.01)
        BASE_RISK = float(ctx.get("base_risk", 0.002) or 0.002)

        bu = (portfolio_state or {}).get("bucket_used", {}) or {}
        corr_btc_used = float(bu.get("BTC", 0.0) or 0.0)
        corr_alt_used = float(bu.get("ALT", 0.0) or 0.0)
        corr_meme_used = float(bu.get("MEME", 0.0) or 0.0)

        try:
            print(f"[PORTFOLIO_EXPOSURE] bucket_used={bu}")
        except Exception:
            pass

        df_corr_kept = []
        df_corr_dropped = []

        for _, r in df_e.iterrows():
            sym = str(r.get("symbol", "") or "").upper()

            rm_raw = r.get("risk_multiplier", 1.0)
            try:
                rm = float(rm_raw)
            except Exception:
                rm = 1.0
            if rm != rm or rm in (float("inf"), float("-inf")):
                rm = 1.0

            plan_risk = float(BASE_RISK * rm)
            if plan_risk != plan_risk or plan_risk in (float("inf"), float("-inf")):
                plan_risk = 0.0

            bucket = _corr_bucket(sym)
            if bucket == "BTC":
                used = corr_btc_used
                cap = CAP_BTC
            elif bucket == "MEME":
                used = corr_meme_used
                cap = CAP_MEME
            else:
                used = corr_alt_used
                cap = CAP_ALT

            try:
                used = float(used or 0.0)
            except Exception:
                used = 0.0
            try:
                cap = float(cap or 0.0)
            except Exception:
                cap = 0.0
            if used != used:
                used = 0.0
            if cap != cap:
                cap = 0.0

            would = used + plan_risk

            print(
                f"[CORR_CAP_DEBUG] cap_btc={CAP_BTC:.4f} cap_alt={CAP_ALT:.4f} cap_meme={CAP_MEME:.4f} "
                f"used_btc={corr_btc_used:.4f} used_alt={corr_alt_used:.4f} used_meme={corr_meme_used:.4f} "
                f"plan_risk={plan_risk:.4f} bucket={bucket} would={would:.4f}"
            )

            # HARD DROP only if > 1.2x cap
            if cap > 0 and would > cap * 1.2:
                df_corr_dropped.append(r)
                continue

            # SOFT CAP: if over cap but <= 1.2x тЖТ throttle
            if cap > 0 and would > cap:
                new_rm = rm * 0.25
                try:
                    r = r.copy()
                except Exception:
                    pass
                r["risk_multiplier"] = new_rm
                plan_risk = float(BASE_RISK * new_rm)
                print(f"[CORR_CAP_SOFT] bucket={bucket} applied_multiplier=0.25")

            # update exposure
            if bucket == "BTC":
                corr_btc_used += plan_risk
            elif bucket == "MEME":
                corr_meme_used += plan_risk
            else:
                corr_alt_used += plan_risk

            df_corr_kept.append(r)

        df_e = pd.DataFrame(df_corr_kept)
        df_corr_dropped = pd.DataFrame(df_corr_dropped)
        print(f"[CORR_CAP][{symbol}] kept={len(df_e)} dropped={len(df_corr_dropped)}")

        # -------------------
        # BUDGET_CAP (1:1 with live)
        # -------------------
        BASE_RISK_PER_TRADE = float(ctx.get("base_risk_per_trade", 0.002) or 0.002)
        BUCKET_CAP = float(ctx.get("bucket_cap", 0.006) or 0.006)
        GLOBAL_CAP = float(ctx.get("global_cap", 0.012) or 0.012)

        long_used = 0.0
        range_used = 0.0
        short_used = 0.0
        global_used = 0.0

        kept_rows = []
        dropped_rows = []

        if df_e is not None and not df_e.empty:
            if "risk_multiplier" in df_e.columns:
                rm = pd.to_numeric(df_e["risk_multiplier"], errors="coerce").fillna(1.0)
            else:
                rm = pd.Series(1.0, index=df_e.index)

            dm = _series_col_or_default(df_e, "dynamic_multiplier", 1.0)
            egm = _series_col_or_default(df_e, "equity_governor_multiplier", 1.0)

            try:
                df_e["plan_risk"] = float(BASE_RISK_PER_TRADE) * rm.astype(float) * dm.astype(float) * egm.astype(float)
            except Exception:
                df_e["plan_risk"] = float(BASE_RISK_PER_TRADE) * rm.astype(float)

            for _, r in df_e.iterrows():
                side = str(r.get("side", "") or "").upper()
                plan_risk = float(r.get("plan_risk", BASE_RISK_PER_TRADE) or BASE_RISK_PER_TRADE)

                if side == "LONG":
                    if (long_used + plan_risk > BUCKET_CAP) or (global_used + plan_risk > GLOBAL_CAP):
                        dropped_rows.append(r)
                        continue
                    long_used += plan_risk
                elif side == "SHORT":
                    if (short_used + plan_risk > BUCKET_CAP) or (global_used + plan_risk > GLOBAL_CAP):
                        dropped_rows.append(r)
                        continue
                    short_used += plan_risk
                else:
                    if (range_used + plan_risk > BUCKET_CAP) or (global_used + plan_risk > GLOBAL_CAP):
                        dropped_rows.append(r)
                        continue
                    range_used += plan_risk

                global_used += plan_risk
                kept_rows.append(r)

            df_kept = pd.DataFrame(kept_rows)
            df_drop = pd.DataFrame(dropped_rows)
            df_e = df_kept

            print(
                f"[BUDGET][{symbol}] kept={len(df_kept)} dropped={len(df_drop)} "
                f"long_used={long_used:.4f} range_used={range_used:.4f} short_used={short_used:.4f} global_used={global_used:.4f}"
            )
            print(f"[POST_BUDGET][{symbol}] rows_after_budget={len(df_e)}")
        else:
            print(
                f"[BUDGET][{symbol}] kept=0 dropped=0 "
                f"long_used={long_used:.4f} range_used={range_used:.4f} short_used={short_used:.4f} global_used={global_used:.4f}"
            )

        # -------------------
        # INVALIDATION (TP/SL hit already)
        # Live-only stale setup guard. In offline replay, execution simulator
        # should own the close/outcome path, so this can be disabled via ctx.
        # -------------------
        disable_invalidation = bool(ctx.get("disable_invalidation", False))

        try:
            if not disable_invalidation:
                before_invalidation = len(df_e)

                df_e, _df_closed = _invalidate_setups_hit_tp_sl(df_e, candles, latest_ts)

                after_invalidation = len(df_e)

                if after_invalidation < before_invalidation:
                    print(
                        f"[DROP][{symbol}] stage=INVALIDATION "
                        f"before={before_invalidation} after={after_invalidation}"
                    )

                if debug and _df_closed is not None and (not _df_closed.empty):
                    print(f"[INVALIDATION][{symbol}] closed={len(_df_closed)} kept={len(df_e)}")

            else:
                if debug:
                    print(f"[INVALIDATION][{symbol}] skipped disable_invalidation=True")

        except Exception:
            pass
        print(f"[POST_INVALIDATION][{symbol}] rows_after_invalidation={len(df_e)}")
        if df_e is None or df_e.empty:
            print(f"[POST_DROP][{symbol}] stage=INVALIDATION latest_ts={latest_ts}")

        print(f"[POST_NORMALIZE_PRE][{symbol}] rows_before_normalize={len(df_e)} cols={list(df_e.columns)}")
        # -------------------
        # FINAL NORMALIZE
        # -------------------
        identity_columns = ["setup_created_ts", "canonical_setup_key", "lifecycle_state"]
        final_columns = list(LIVE_ENTRIES_COLUMNS)
        for c in identity_columns:
            if c not in final_columns:
                final_columns.append(c)
        for c in final_columns:
            if c not in df_e.columns:
                df_e[c] = np.nan

        df_e = df_e.reindex(columns=final_columns)
        print(f"[POST_NORMALIZE][{symbol}] rows_after_reindex={len(df_e)}")

        df_e["symbol"] = symbol
        df_e = _ensure_canonical_setup_key(df_e, symbol=symbol)
        if "signal_ts" not in df_e.columns or df_e["signal_ts"].isna().all():
            df_e["signal_ts"] = latest_ts

        print(f"[POST_RETURN][{symbol}] rows_before_return={len(df_e)}")
        return df_e


    except Exception as e:

        print(f"[PIPELINE_CORE_FAIL][{symbol}] fail-open exception: {repr(e)}")

        return _empty_entries_df()