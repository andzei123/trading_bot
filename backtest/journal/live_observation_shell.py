from __future__ import annotations

"""
Minimal live observation shell with bounded observability logging.

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

Observability additions in this version:
    - flow logging only
    - per-symbol/per-cycle counters for where setups die
    - no changes to pipeline, wait, stale, idempotency, or emit logic
"""

import argparse
import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd
import requests

from backtest.journal.position_closer import close_symbol_if_hit
from backtest.live_pipeline.pipeline_core import run_pipeline_once
from backtest.portfolio.portfolio_exposure import load_portfolio_exposure
from backtest.utils.wait_confirmation import apply_wait_confirmation
from backtest.journal.live_emit_guard import filter_live_emit_candidates, select_newest_live_candidate
from backtest.journal.identity import (
    LIFECYCLE_FIRED,
    LIFECYCLE_OPENED,
    LIFECYCLE_STALE,
    LIFECYCLE_WAIT_REJECTED,
    _append_terminal_lifecycle_row,
    _ensure_canonical_setup_key,
    _load_canonical_keys_from_csv,
    _load_terminal_canonical_keys,
)

try:
    from backtest.journal.live_journal_rotation_manager import (
        DEFAULT_DESIGN_CSV as LIVE_ROTATION_DESIGN_CSV,
        LiveJournalRotationManager,
    )
except Exception:
    LIVE_ROTATION_DESIGN_CSV = Path("backtest/journal/live_journal_rotation_design.csv")
    LiveJournalRotationManager = None

try:
    from backtest.journal.live_journal_rotation_controller import LiveJournalRotationController
except Exception:
    LiveJournalRotationController = None

try:
    from backtest.journal.live_rotation_plan_writer import append_live_rotation_plan_rows
except Exception:
    append_live_rotation_plan_rows = None

try:
    from backtest.journal.live_rotation_integration import (
        LiveRotationIntegrationBoundary,
        LiveRotationIntegrationConfig,
    )
except Exception:
    LiveRotationIntegrationBoundary = None
    LiveRotationIntegrationConfig = None

BYBIT_REST = "https://api.bybit.com"

FLOW_LOG_COLUMNS = [
    "cycle_ts",
    "symbol",
    "latest_ts",
    "setup_id",
    "canonical_setup_key",
    "lifecycle_state",
    "model",
    "side",
    "setup_created_ts",
    "wait_confirm_ts",
    "wait_context_source",
    "visible_ts",
    "entry_anchor_ts",
    "first_seen_ts",
    "age_candles_at_first_seen",
    "intended_entry_ts",
    "entry_window_expires_ts",
    "entry_delay_minutes",
    "entry_timing_valid",
    "execution_ts_source",
    "trigger_refresh_candidate",
    "trigger_refresh_reason",
    "old_canonical_setup_key",
    "original_setup_created_ts",
    "candidate_latest_ts",
    "range_retest_score",
    "trigger_refresh_applied",
    "age_minutes_at_first_seen",
    "phase",
    "model_summary_raw",
    "model_summary_after_wait",
    "after_freshness_count",
    "model_summary_after_stale",
    "model_summary_after_idempotency",
    "model_summary_after_per_cycle_guard",
    "sub_label_summary_raw",
    "skipped_open_position",
    "pipeline_zero_rows",
    "raw_entries_count",
    "after_wait_count",
    "after_stale_count",
    "after_idempotency_count",
    "after_per_cycle_guard_count",
    "emitted_count",
    "notes",
    "death_stage",
    "death_reason",
    "parity_filter_mode",
    "parity_filter_stage",
    "filter_input_rows",
    "filter_missing_range_width_pct",
    "filter_missing_distance_to_entry_R",
    "filter_pass_rows",
    "filter_reject_rows",
    "filter_pass_then_emitted",
    "filter_pass_then_wait_confirmation_death",
    "filter_pass_then_stale_death",
    "filter_pass_then_position_gate_death",
    "stale_anchor_current_ts",
    "stale_anchor_visible_ts",
    "stale_anchor_wait_confirm_ts",
    "stale_age_current_bars",
    "stale_age_visible_bars",
    "stale_age_wait_confirm_bars",
    "stale_window_allowed_bars",
    "current_stale_decision",
    "visible_rebase_stale_decision",
    "wait_rebase_stale_decision",
    "would_pass_if_visible_rebased",
    "would_pass_if_wait_rebased",
    "stale_authority_disagreement",
    "stale_disagreement_type",
]

# -----------------------------------------------------------------------------
# TELEMETRY ONLY: raw candidate lifecycle / pressure-window diagnostics.
# These CSVs are append/rebuild observability artifacts. They are never read by
# trading gates and must not influence ordering, filtering, idempotency, stale
# handling, lifecycle decisions, position state, or emitted trade rows.
# -----------------------------------------------------------------------------
RAW_CANDIDATE_LIFECYCLE_DIAG_COLUMNS = [
    "cycle_ts",
    "symbol",
    "latest_ts",
    "candidate_ts",
    "model",
    "side",
    "setup_id",
    "canonical_setup_key",
    "setup_created_ts",
    "timestamp",
    "visible_ts",
    "entry_anchor_ts",
    "wait_confirm_ts",
    "intended_entry_ts",
    "entry",
    "sl",
    "tp",
    "rr",
    "phase",
    "range_retest_score",
    "death_stage",
    "death_reason",
    "passed_wait",
    "wait_bypassed",
    "wait_bypass_scope",
    "would_have_failed_wait",
    "wait_shadow_decision",
    "passed_stale",
    "passed_idempotency",
    "passed_position_gate",
    "emitted",
    "pressure_raw_count",
    "pressure_group_count",
    "inside_pressure_window",
    "pressure_window_id",
    "pressure_window_age_bars",
    "pressure_window_duration_bars",
    "pressure_window_peak_raw",
    "setup_age_minutes",
    "setup_age_bars",
    "range_width_pct",
    "entry_to_range_high_pct",
    "entry_to_range_low_pct",
    "target_distance_pct",
    "stop_distance_pct",
    "planned_rr",
    "risk_pct",
    "reward_pct",
    "risk_distance",
    "reward_distance",
    "distance_to_entry_pct",
    "distance_to_entry_R",
    "stale_anchor_current_ts",
    "stale_anchor_visible_ts",
    "stale_anchor_wait_confirm_ts",
    "stale_age_current_bars",
    "stale_age_visible_bars",
    "stale_age_wait_confirm_bars",
    "stale_window_allowed_bars",
    "current_stale_decision",
    "visible_rebase_stale_decision",
    "wait_rebase_stale_decision",
    "would_pass_if_visible_rebased",
    "would_pass_if_wait_rebased",
    "stale_authority_disagreement",
    "stale_disagreement_type",
    "parity_filter_applied",
    "parity_filter_passed",
    "parity_filter_name",
    "parity_filter_reason",
    "parity_filter_stage",
    "filter_range_width_pct",
    "filter_distance_to_entry_R",
    "filter_threshold_range_width_pct",
    "filter_threshold_distance_to_entry_R",
]

PARITY_FILTER_DIAGNOSTICS_COLUMNS = [
    "timestamp",
    "cycle_ts",
    "symbol",
    "model",
    "side",
    "canonical_setup_key",
    "candidate_ts",
    "visible_ts",
    "wait_confirm_ts",
    "setup_age_bars",
    "range_width_pct",
    "distance_to_entry_R",
    "distance_to_entry_pct",
    "entry_to_range_low_pct",
    "entry_to_range_high_pct",
    "planned_rr",
    "risk_pct",
    "reward_pct",
    "filter_name",
    "filter_applied",
    "filter_passed",
    "filter_failed_condition",
    "death_stage",
    "death_reason",
    "is_emitted",
]

PARITY_FILTER_MODES = {
    "NONE",
    "RANGE_AGE_3_5",
    "COMBINED_FINGERPRINT",
    "REMOVE_RANGE_AGE_1_2",
    "RANGE_GEOMETRY_P50",
}

TDP_STALE_SHADOW_COLUMNS = [
    "cycle_ts",
    "symbol",
    "side",
    "model",
    "canonical_setup_key",
    "setup_id",
    "setup_created_ts",
    "candidate_ts",
    "visible_ts",
    "wait_confirm_ts",
    "candidate_latest_ts",
    "entry_window_expires_ts",
    "death_reason",
    "entry",
    "sl",
    "tp",
    "planned_rr",
    "setup_age_bars",
    "setup_age_minutes",
    "distance_to_entry_R",
    "target_distance_pct",
    "stop_distance_pct",
    "raw_candidate_count",
    "pressure_window_id",
    "candidate_persistence_bars",
    "would_shadow_entry_ts",
    "shadow_entry_source",
]

TDP_STALE_SHADOW_PERSISTENCE_BY_KEY: Dict[str, int] = {}

STRUCTURAL_TS_SHADOW_COLUMNS = [
    "cycle_ts",
    "structural_ts",
    "symbol",
    "side",
    "model",
    "canonical_setup_key",
    "setup_id",
    "setup_created_ts",
    "candidate_ts",
    "visible_ts",
    "wait_confirm_ts",
    "entry",
    "sl",
    "tp",
    "planned_rr",
    "setup_age_bars",
    "setup_age_minutes",
    "distance_to_entry_R",
    "target_distance_pct",
    "stop_distance_pct",
    "raw_candidate_count",
    "pressure_window_id",
    "death_reason",
    "is_emitted",
    "passed_wait",
    "wait_bypassed",
    "wait_bypass_scope",
    "would_have_failed_wait",
    "wait_shadow_decision",
    "passed_stale",
    "passed_idempotency",
    "passed_position_gate",
]



SNIPER_CANDIDATE_DIAG_COLUMNS = [
    "timestamp",
    "symbol",
    "setup_id",
    "canonical_setup_key",
    "candidate_ts",
    "visible_ts",
    "wait_confirm_ts",
    "death_stage",
    "death_reason",
    "entry",
    "sl",
    "tp",
    "raw_candidate_count",
    "prev_raw_candidate_count",
    "raw_count_delta",
    "cluster_group_count",
    "groups_gt1",
    "groups_gt2",
    "groups_gt3",
    "pressure_window_id",
    "pressure_window_age",
    "pressure_window_age_bars",
    "pressure_window_duration_bars",
    "pressure_window_peak_raw",
    "pressure_window_candidate_count",
    "setup_age_minutes",
    "setup_age_bars",
    "range_width_pct",
    "entry_to_range_high_pct",
    "entry_to_range_low_pct",
    "target_distance_pct",
    "stop_distance_pct",
    "planned_rr",
    "risk_pct",
    "reward_pct",
    "risk_distance",
    "reward_distance",
    "distance_to_entry_pct",
    "distance_to_entry_R",
    "candidate_persistence_bars",
    "is_candidate_expansion",
    "is_candidate_flat",
    "is_candidate_contraction",
    "is_emitted",
    "passed_wait",
    "wait_bypassed",
    "wait_bypass_scope",
    "would_have_failed_wait",
    "wait_shadow_decision",
    "passed_stale",
    "passed_idempotency",
    "passed_position_gate",
]

SNIPER_CANDIDATE_SUMMARY_COLUMNS = [
    "symbol",
    "model",
    "death_stage",
    "death_reason",
    "candidate_expansion_state",
    "candidate_count",
    "emitted_count",
]

SNIPER_PREV_RAW_COUNT_BY_SYMBOL: Dict[str, int] = {}
SNIPER_PERSISTENCE_BY_KEY: Dict[str, int] = {}
RAW_PRESSURE_WINDOW_BY_SYMBOL: Dict[str, Dict[str, object]] = {}

PRESSURE_WINDOW_SUMMARY_COLUMNS = [
    "symbol",
    "window_id",
    "window_start",
    "window_end",
    "duration_bars",
    "duration_minutes",
    "max_raw_candidate_count",
    "sum_raw_candidate_count",
    "avg_raw_candidate_count",
    "cluster_group_count_max",
    "groups_gt1_any",
    "groups_gt2_any",
    "groups_gt3_any",
    "emitted_count_inside_window",
    "raw_candidate_count_inside_window",
    "death_reason_counts_inside_window",
    "first_candidate_ts",
    "last_candidate_ts",
    # Hindsight diagnostics: these locate candle events after the pressure
    # window and are never used for trading decisions.
    "hindsight_window_high",
    "hindsight_window_high_ts",
    "hindsight_first_lower_high_after_window_high_ts",
    "hindsight_first_close_below_prev_low_after_window_high_ts",
]

OPPORTUNITY_MANAGER_SNAPSHOT_COLUMNS = [
    "cycle_ts",
    "symbol",
    "latest_ts",
    "candidate_ts",
    "timestamp",
    "signal_ts",
    "model",
    "side",
    "setup_id",
    "canonical_setup_key",
    "setup_created_ts",
    "visible_ts",
    "entry_anchor_ts",
    "wait_confirm_ts",
    "intended_entry_ts",
    "entry_window_expires_ts",
    "entry",
    "sl",
    "tp",
    "rr",
    "phase",
    "range_retest_score",
    "setup_age_minutes",
    "setup_age_bars",
    "range_width_pct",
    "entry_to_range_high_pct",
    "entry_to_range_low_pct",
    "target_distance_pct",
    "stop_distance_pct",
    "planned_rr",
    "risk_pct",
    "reward_pct",
    "risk_distance",
    "reward_distance",
    "distance_to_entry_pct",
    "distance_to_entry_R",
    "raw_candidate_count",
    "cluster_group_count",
    "pressure_window_id",
    "pressure_window_age_bars",
    "pressure_window_duration_bars",
    "pressure_window_peak_raw",
    "candidate_persistence_bars",
    "is_selected",
    "selection_rank",
    "selected_for_execution",
    "execution_rank",
    "selection_reason",
    "rejected_reason",
]


AUTHORITY_WATERFALL_COLUMNS = [
    "canonical_setup_key",
    "symbol",
    "model",
    "side",
    "setup_created_ts",
    "candidate_ts",
    "visible_ts",
    "wait_confirm_ts",
    "signal_ts",
    "opened_ts",
    "closed_ts",
    "reached_created",
    "reached_visible",
    "reached_wait",
    "reached_freshness",
    "reached_executable",
    "reached_opportunity_manager",
    "reached_execution",
    "reached_opened",
    "terminal_stage",
    "death_reason",
    "planned_rr",
    "risk_pct",
    "reward_pct",
    "distance_to_entry_R",
    "distance_to_entry_pct",
    "range_width_pct",
    "setup_age_bars",
    "candidate_persistence_bars",
    "opened",
    "close_reason",
    "final_R_if_known",
    "selected_for_execution",
    "rejected_reason",
    "execution_rank",
]


def _diag_path(path_like) -> Optional[Path]:
    if path_like is None:
        return None
    path_s = str(path_like).strip()
    if not path_s:
        return None
    return Path(path_s)


def _diag_row_key(row: pd.Series) -> str:
    canonical = str(row.get("canonical_setup_key", "") or "")
    if canonical:
        return canonical
    setup_id = str(row.get("setup_id", "") or "")
    if setup_id:
        return setup_id
    return "|".join([
        str(row.get("symbol", "") or ""),
        str(row.get("model", "") or ""),
        str(row.get("side", "") or ""),
        str(pd.to_datetime(row.get("timestamp", pd.NaT), utc=True, errors="coerce")),
    ])


def _diag_key_set(df: Optional[pd.DataFrame]) -> Set[str]:
    if df is None or df.empty:
        return set()
    keys: Set[str] = set()
    for _, row in df.iterrows():
        keys.add(_diag_row_key(row))
        old_key = str(row.get("old_canonical_setup_key", "") or "")
        if old_key:
            keys.add(old_key)
        setup_id = str(row.get("setup_id", "") or "")
        if setup_id:
            keys.add(setup_id)
    return keys


def _telemetry_float(value: object):
    try:
        out = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    except Exception:
        return None
    if pd.isna(out):
        return None
    return float(out)


def _telemetry_first_present(row: pd.Series, names: List[str]):
    for name in names:
        if name in row.index:
            value = row.get(name, "")
            if value is not None and str(value) != "" and str(value).lower() != "nan":
                return value
    return ""


def _telemetry_ts(value: object):
    return pd.to_datetime(value, utc=True, errors="coerce")


def _telemetry_candidate_first_seen_ts(row: pd.Series, latest_ts: pd.Timestamp):
    first_seen_ts = _telemetry_ts(_telemetry_first_present(row, ["first_seen_ts", "visible_ts", "observed_ts", "cycle_ts"]))
    if pd.isna(first_seen_ts):
        first_seen_ts = _telemetry_ts(latest_ts)
    return first_seen_ts


def _telemetry_price_at_or_before(candles_df: Optional[pd.DataFrame], ts_value):
    if candles_df is None or candles_df.empty or "timestamp" not in candles_df.columns or "close" not in candles_df.columns:
        return None
    c = candles_df[["timestamp", "close"]].copy()
    c["timestamp"] = pd.to_datetime(c["timestamp"], utc=True, errors="coerce")
    c["close"] = pd.to_numeric(c["close"], errors="coerce")
    c = c.dropna(subset=["timestamp", "close"]).sort_values("timestamp")
    if c.empty:
        return None
    ts_value = _telemetry_ts(ts_value)
    if pd.notna(ts_value):
        before = c.loc[c["timestamp"] <= ts_value]
        if not before.empty:
            return float(before.iloc[-1]["close"])
    return None


def _telemetry_range_bounds_between(candles_df: Optional[pd.DataFrame], start_ts, end_ts):
    start_ts = _telemetry_ts(start_ts)
    end_ts = _telemetry_ts(end_ts)
    if pd.isna(start_ts) or pd.isna(end_ts):
        return None, None
    if candles_df is None or candles_df.empty or "timestamp" not in candles_df.columns:
        return None, None
    high_col = "high" if "high" in candles_df.columns else "High" if "High" in candles_df.columns else ""
    low_col = "low" if "low" in candles_df.columns else "Low" if "Low" in candles_df.columns else ""
    if not high_col or not low_col:
        return None, None
    c = candles_df[["timestamp", high_col, low_col]].copy()
    c["timestamp"] = pd.to_datetime(c["timestamp"], utc=True, errors="coerce")
    c[high_col] = pd.to_numeric(c[high_col], errors="coerce")
    c[low_col] = pd.to_numeric(c[low_col], errors="coerce")
    c = c.dropna(subset=["timestamp", high_col, low_col]).sort_values("timestamp")
    if c.empty:
        return None, None
    if c["timestamp"].iloc[0] > start_ts or c["timestamp"].iloc[-1] < end_ts:
        return None, None
    window = c.loc[(c["timestamp"] >= start_ts) & (c["timestamp"] <= end_ts)]
    if window.empty:
        return None, None
    return float(window[high_col].max()), float(window[low_col].min())


def _telemetry_closed_bars_between(candles_df: Optional[pd.DataFrame], start_ts, end_ts):
    start_ts = _telemetry_ts(start_ts)
    end_ts = _telemetry_ts(end_ts)
    if pd.isna(start_ts) or pd.isna(end_ts):
        return ""
    if candles_df is not None and not candles_df.empty and "timestamp" in candles_df.columns:
        ts = pd.to_datetime(candles_df["timestamp"], utc=True, errors="coerce").dropna().sort_values()
        if not ts.empty:
            return int(((ts > start_ts) & (ts <= end_ts)).sum())
    minutes = (end_ts - start_ts).total_seconds() / 60.0
    if minutes < 0:
        return ""
    return int(minutes // 15.0)


def _stale_shadow_ts(row: Dict[str, object], names: List[str]):
    for name in names:
        ts = pd.to_datetime(row.get(name, pd.NaT), utc=True, errors="coerce")
        if pd.notna(ts):
            return ts
    return pd.NaT


def _stale_shadow_allowed_bars(row: Dict[str, object]):
    model = str(row.get("model", "") or "").upper()
    if model == "TDP_REENTRY":
        return 4
    if model.startswith("RANGE_"):
        return 1
    return ""


def _stale_shadow_timeframe_minutes(row: Dict[str, object]) -> float:
    try:
        setup_age_bars = pd.to_numeric(pd.Series([row.get("setup_age_bars", "")]), errors="coerce").iloc[0]
        setup_age_minutes = pd.to_numeric(pd.Series([row.get("setup_age_minutes", "")]), errors="coerce").iloc[0]
        if pd.notna(setup_age_bars) and float(setup_age_bars) > 0 and pd.notna(setup_age_minutes):
            tf = float(setup_age_minutes) / float(setup_age_bars)
            if tf > 0:
                return tf
    except Exception:
        pass
    return 15.0


def _stale_shadow_age_bars(now_ts, anchor_ts, timeframe_minutes: float):
    now_ts = pd.to_datetime(now_ts, utc=True, errors="coerce")
    anchor_ts = pd.to_datetime(anchor_ts, utc=True, errors="coerce")
    if pd.isna(now_ts) or pd.isna(anchor_ts):
        return ""
    minutes = float((now_ts - anchor_ts).total_seconds() / 60.0)
    if minutes < 0 or timeframe_minutes <= 0:
        return ""
    return int(minutes // float(timeframe_minutes))


def _stale_shadow_decision(age_bars, allowed_bars):
    if age_bars == "" or allowed_bars == "":
        return ""
    try:
        return bool(int(age_bars) > int(allowed_bars))
    except Exception:
        return ""


def _stale_authority_shadow_fields(row: Dict[str, object]) -> Dict[str, object]:
    """Telemetry-only stale authority matrix.

    This function does not feed any trading gate. It only compares the actual
    production stale result with alternative visible_ts / wait_confirm_ts
    anchors for diagnostics.
    """
    now_ts = _stale_shadow_ts(row, ["latest_ts", "cycle_ts"])
    current_anchor = _stale_shadow_ts(row, ["setup_created_ts", "candidate_ts", "timestamp"])
    visible_anchor = _stale_shadow_ts(row, ["visible_ts", "pipeline_visible_ts"])
    wait_anchor = _stale_shadow_ts(row, ["wait_confirm_ts"])
    allowed_bars = _stale_shadow_allowed_bars(row)
    timeframe_minutes = _stale_shadow_timeframe_minutes(row)

    current_age = _stale_shadow_age_bars(now_ts, current_anchor, timeframe_minutes)
    visible_age = _stale_shadow_age_bars(now_ts, visible_anchor, timeframe_minutes)
    wait_age = _stale_shadow_age_bars(now_ts, wait_anchor, timeframe_minutes)

    actual_current_stale = bool(str(row.get("death_reason", "") or "") == "stale_execution_window")
    visible_decision = _stale_shadow_decision(visible_age, allowed_bars)
    wait_decision = _stale_shadow_decision(wait_age, allowed_bars)

    would_pass_visible = bool(actual_current_stale and visible_decision is False)
    would_pass_wait = bool(actual_current_stale and wait_decision is False)

    disagreement_types: List[str] = []
    if visible_decision != "" and bool(visible_decision) != actual_current_stale:
        disagreement_types.append("current_stale_visible_pass" if actual_current_stale else "current_pass_visible_stale")
    if wait_decision != "" and bool(wait_decision) != actual_current_stale:
        disagreement_types.append("current_stale_wait_pass" if actual_current_stale else "current_pass_wait_stale")

    if disagreement_types:
        disagreement_type = ";".join(disagreement_types)
    elif visible_decision == "" and wait_decision == "":
        disagreement_type = "not_applicable"
    else:
        disagreement_type = "no_disagreement"

    return {
        "stale_anchor_current_ts": current_anchor,
        "stale_anchor_visible_ts": visible_anchor,
        "stale_anchor_wait_confirm_ts": wait_anchor,
        "stale_age_current_bars": current_age,
        "stale_age_visible_bars": visible_age,
        "stale_age_wait_confirm_bars": wait_age,
        "stale_window_allowed_bars": allowed_bars,
        "current_stale_decision": actual_current_stale,
        "visible_rebase_stale_decision": visible_decision,
        "wait_rebase_stale_decision": wait_decision,
        "would_pass_if_visible_rebased": would_pass_visible,
        "would_pass_if_wait_rebased": would_pass_wait,
        "stale_authority_disagreement": bool(disagreement_types),
        "stale_disagreement_type": disagreement_type,
    }


def _telemetry_pressure_window_fields(symbol: str, latest_ts: pd.Timestamp, raw_count: int) -> Dict[str, object]:
    sym = str(symbol or "").upper()
    latest_ts = _telemetry_ts(latest_ts)
    if raw_count <= 0 or pd.isna(latest_ts):
        RAW_PRESSURE_WINDOW_BY_SYMBOL.pop(sym, None)
        return {
            "pressure_window_id": "",
            "pressure_window_age_bars": "",
            "pressure_window_duration_bars": "",
            "pressure_window_peak_raw": "",
        }

    state = RAW_PRESSURE_WINDOW_BY_SYMBOL.get(sym)
    if not state:
        state = {
            "window_start": latest_ts,
            "window_id": f"{sym}|{latest_ts.isoformat()}",
            "duration_bars": 0,
            "peak_raw": 0,
            "last_ts": pd.NaT,
        }
    last_ts = _telemetry_ts(state.get("last_ts", pd.NaT))
    if pd.isna(last_ts) or last_ts != latest_ts:
        state["duration_bars"] = int(state.get("duration_bars", 0) or 0) + 1
        state["last_ts"] = latest_ts
    state["peak_raw"] = max(int(state.get("peak_raw", 0) or 0), int(raw_count))
    RAW_PRESSURE_WINDOW_BY_SYMBOL[sym] = state

    duration_bars = int(state.get("duration_bars", 0) or 0)
    return {
        "pressure_window_id": str(state.get("window_id", "") or ""),
        "pressure_window_age_bars": max(0, duration_bars - 1),
        "pressure_window_duration_bars": duration_bars,
        "pressure_window_peak_raw": int(state.get("peak_raw", raw_count) or raw_count),
    }


def _telemetry_range_and_rr_fields(row: pd.Series, *, candles_df: Optional[pd.DataFrame], setup_created_ts, first_seen_ts) -> Dict[str, object]:
    entry = _telemetry_float(_telemetry_first_present(row, ["entry", "planned_entry", "entry_price"]))
    stop = _telemetry_float(_telemetry_first_present(row, ["sl", "stop", "stop_loss"]))
    target = _telemetry_float(_telemetry_first_present(row, ["tp", "target", "take_profit"]))
    range_high, range_low = _telemetry_range_bounds_between(candles_df, setup_created_ts, first_seen_ts)
    if range_high is None or range_low is None:
        range_high = _telemetry_float(_telemetry_first_present(row, ["range_high", "range_top", "range_upper", "range_high_price", "tdp_range_high", "ctx_range_high"]))
        range_low = _telemetry_float(_telemetry_first_present(row, ["range_low", "range_bottom", "range_lower", "range_low_price", "tdp_range_low", "ctx_range_low"]))
    first_seen_price = _telemetry_price_at_or_before(candles_df, first_seen_ts)

    out: Dict[str, object] = {
        "range_width_pct": "",
        "entry_to_range_high_pct": "",
        "entry_to_range_low_pct": "",
        "target_distance_pct": "",
        "stop_distance_pct": "",
        "planned_rr": "",
        "risk_pct": "",
        "reward_pct": "",
        "risk_distance": "",
        "reward_distance": "",
        "distance_to_entry_pct": "",
        "distance_to_entry_R": "",
    }
    if entry is not None and entry != 0:
        if range_high is not None and range_low is not None:
            out["range_width_pct"] = abs(range_high - range_low) / abs(entry)
            out["entry_to_range_high_pct"] = abs(entry - range_high) / abs(entry)
            out["entry_to_range_low_pct"] = abs(entry - range_low) / abs(entry)
        if target is not None:
            out["target_distance_pct"] = abs(entry - target) / abs(entry)
        if stop is not None:
            out["stop_distance_pct"] = abs(entry - stop) / abs(entry)
        if first_seen_price is not None:
            out["distance_to_entry_pct"] = abs(first_seen_price - entry) / abs(entry)
    if entry is not None and stop is not None and target is not None:
        risk_distance = abs(entry - stop)
        reward_distance = abs(entry - target)
        out["risk_distance"] = risk_distance
        out["reward_distance"] = reward_distance
        if entry != 0:
            out["risk_pct"] = risk_distance / abs(entry)
            out["reward_pct"] = reward_distance / abs(entry)
        if risk_distance != 0:
            out["planned_rr"] = reward_distance / risk_distance
            if first_seen_price is not None:
                out["distance_to_entry_R"] = abs(first_seen_price - entry) / risk_distance
    return out


def _make_raw_candidate_diag_rows(
    *,
    candidates_df: Optional[pd.DataFrame],
    cycle_ts: pd.Timestamp,
    symbol: str,
    latest_ts: pd.Timestamp,
    candles_df: Optional[pd.DataFrame] = None,
) -> List[Dict[str, object]]:
    """Build immutable telemetry snapshots for raw candidates.

    This function copies candidate fields only. It must never mutate the
    dataframe that continues through trading gates.
    """
    if candidates_df is None or candidates_df.empty:
        _telemetry_pressure_window_fields(symbol, latest_ts, 0)
        return []

    rows: List[Dict[str, object]] = []
    raw_count = int(len(candidates_df))
    pressure_fields = _telemetry_pressure_window_fields(symbol, latest_ts, raw_count)
    for _, r in candidates_df.iterrows():
        candidate_ts = pd.to_datetime(r.get("timestamp", pd.NaT), utc=True, errors="coerce")
        setup_created_ts = pd.to_datetime(r.get("setup_created_ts", candidate_ts), utc=True, errors="coerce")
        first_seen_ts = _telemetry_candidate_first_seen_ts(r, latest_ts)
        setup_age_minutes: object = ""
        if pd.notna(setup_created_ts) and pd.notna(first_seen_ts):
            setup_age_minutes = max(0.0, float((first_seen_ts - setup_created_ts).total_seconds() / 60.0))
        setup_age_bars = _telemetry_closed_bars_between(candles_df, setup_created_ts, first_seen_ts)
        range_rr_fields = _telemetry_range_and_rr_fields(
            r,
            candles_df=candles_df,
            setup_created_ts=setup_created_ts,
            first_seen_ts=first_seen_ts,
        )
        row = {
            "cycle_ts": pd.to_datetime(cycle_ts, utc=True, errors="coerce"),
            "symbol": str(r.get("symbol", symbol) or symbol).upper(),
            "latest_ts": pd.to_datetime(latest_ts, utc=True, errors="coerce"),
            "candidate_ts": candidate_ts,
            "model": str(r.get("model", "") or ""),
            "side": str(r.get("side", "") or "").upper(),
            "setup_id": str(r.get("setup_id", "") or ""),
            "canonical_setup_key": str(r.get("canonical_setup_key", "") or ""),
            "setup_created_ts": setup_created_ts,
            "timestamp": candidate_ts,
            "visible_ts": pd.to_datetime(r.get("visible_ts", pd.NaT), utc=True, errors="coerce"),
            "entry_anchor_ts": pd.to_datetime(r.get("entry_anchor_ts", pd.NaT), utc=True, errors="coerce"),
            "wait_confirm_ts": pd.to_datetime(r.get("wait_confirm_ts", pd.NaT), utc=True, errors="coerce"),
            "candidate_latest_ts": pd.to_datetime(r.get("candidate_latest_ts", pd.NaT), utc=True, errors="coerce"),
            "entry_window_expires_ts": pd.to_datetime(r.get("entry_window_expires_ts", pd.NaT), utc=True, errors="coerce"),
            "intended_entry_ts": pd.to_datetime(r.get("intended_entry_ts", pd.NaT), utc=True, errors="coerce"),
            "entry": _telemetry_first_present(r, ["entry", "planned_entry", "entry_price"]),
            "sl": _telemetry_first_present(r, ["sl", "stop", "stop_loss"]),
            "tp": _telemetry_first_present(r, ["tp", "target", "take_profit"]),
            "rr": r.get("rr", ""),
            "phase": str(r.get("phase", "") or ""),
            "range_retest_score": r.get("range_retest_score", ""),
            "death_stage": "unknown",
            "death_reason": "",
            "passed_wait": False,
            "passed_stale": False,
            "passed_idempotency": False,
            "passed_position_gate": False,
            "emitted": False,
            # Pressure-window fields are derived later from this diagnostic CSV.
            # If upstream candidate_pressure internals are unavailable here, keep
            # group/window fields empty rather than inventing values.
            "pressure_raw_count": raw_count,
            "pressure_group_count": "",
            "inside_pressure_window": bool(raw_count > 0),
            "pressure_window_id": pressure_fields["pressure_window_id"],
            "pressure_window_age_bars": pressure_fields["pressure_window_age_bars"],
            "pressure_window_duration_bars": pressure_fields["pressure_window_duration_bars"],
            "pressure_window_peak_raw": pressure_fields["pressure_window_peak_raw"],
            "setup_age_minutes": setup_age_minutes,
            "setup_age_bars": setup_age_bars,
        }
        row.update(range_rr_fields)
        rows.append(row)
    return rows


def _mark_raw_candidate_diag_rows(
    rows: List[Dict[str, object]],
    *,
    wait_df: Optional[pd.DataFrame] = None,
    idempotency_df: Optional[pd.DataFrame] = None,
    stale_df: Optional[pd.DataFrame] = None,
    position_df: Optional[pd.DataFrame] = None,
    emitted_df: Optional[pd.DataFrame] = None,
    death_stage: str,
    death_reason: str,
) -> List[Dict[str, object]]:
    """Annotate telemetry rows using post-gate copies only.

    The returned list is written to diagnostics only and is not consumed by any
    decision path.
    """
    wait_keys = _diag_key_set(wait_df)
    idem_keys = _diag_key_set(idempotency_df)
    stale_keys = _diag_key_set(stale_df)
    pos_keys = _diag_key_set(position_df)
    emit_keys = _diag_key_set(emitted_df)

    wait_meta_by_key = {}
    try:
        if wait_df is not None and not wait_df.empty:
            for _, wr in wait_df.iterrows():
                wk = str(wr.get("canonical_setup_key") or wr.get("setup_id") or "")
                if not wk:
                    continue
                wait_meta_by_key[wk] = {
                    "wait_bypassed": bool(wr.get("wait_bypassed", False)),
                    "wait_bypass_scope": str(wr.get("wait_bypass_scope", "") or ""),
                    "would_have_failed_wait": bool(wr.get("would_have_failed_wait", False)),
                    "wait_shadow_decision": str(wr.get("wait_shadow_decision", "") or ""),
                }
    except Exception:
        wait_meta_by_key = {}

    out = []
    for row in rows:
        k = str(row.get("canonical_setup_key") or row.get("setup_id") or "")
        r = dict(row)
        r["passed_wait"] = bool(k and k in wait_keys)
        wait_meta = wait_meta_by_key.get(k, {})
        r["wait_bypassed"] = bool(wait_meta.get("wait_bypassed", False))
        r["wait_bypass_scope"] = str(wait_meta.get("wait_bypass_scope", "") or "")
        r["would_have_failed_wait"] = bool(wait_meta.get("would_have_failed_wait", False))
        r["wait_shadow_decision"] = str(wait_meta.get("wait_shadow_decision", "") or "")
        r["passed_idempotency"] = bool(k and k in idem_keys)
        r["passed_stale"] = bool(k and k in stale_keys)
        r["passed_position_gate"] = bool(k and k in pos_keys)
        r["emitted"] = bool(k and k in emit_keys)
        if r["emitted"]:
            r["death_stage"] = "emitted"
            r["death_reason"] = "passed_all_filters"
        elif r["passed_position_gate"] and death_stage == "post_guard":
            r["death_stage"] = "post_guard"
            r["death_reason"] = death_reason
        else:
            r["death_stage"] = death_stage
            r["death_reason"] = death_reason
        r.update(_stale_authority_shadow_fields(r))
        out.append(r)
    return out


def _append_raw_candidate_lifecycle_diag(path_like, rows: List[Dict[str, object]]) -> None:
    path = _diag_path(path_like)
    if path is None or not rows:
        return
    _ensure_parent(path)
    out = pd.DataFrame(rows)
    for col in RAW_CANDIDATE_LIFECYCLE_DIAG_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    out = out[RAW_CANDIDATE_LIFECYCLE_DIAG_COLUMNS]
    if not path.exists() or path.stat().st_size == 0:
        out.to_csv(path, index=False)
    else:
        out.to_csv(path, mode="a", header=False, index=False)


def _tdp_stale_shadow_entry_ts(row: Dict[str, object]):
    for source in ("candidate_latest_ts", "visible_ts", "candidate_ts", "cycle_ts"):
        ts = pd.to_datetime(row.get(source, pd.NaT), utc=True, errors="coerce")
        if pd.notna(ts):
            return ts, source
    return pd.NaT, ""


def _append_tdp_stale_shadow_rows(path_like, marked_rows: List[Dict[str, object]]) -> None:
    path = _diag_path(path_like)
    if path is None or not marked_rows:
        return

    rows: List[Dict[str, object]] = []
    for raw_row in marked_rows:
        # Work on a shallow copy only. This logger must never mutate the
        # diagnostic rows passed to other telemetry writers.
        r = dict(raw_row)

        if str(r.get("model", "")) != "TDP_REENTRY":
            continue
        if str(r.get("death_reason", "")) != "stale_execution_window":
            continue

        k = str(r.get("canonical_setup_key") or r.get("setup_id") or "")
        persistence_bars = ""
        if k:
            persistence_bars = int(TDP_STALE_SHADOW_PERSISTENCE_BY_KEY.get(k, 0) + 1)
            TDP_STALE_SHADOW_PERSISTENCE_BY_KEY[k] = persistence_bars

        would_shadow_entry_ts, shadow_entry_source = _tdp_stale_shadow_entry_ts(r)
        rows.append({
            "cycle_ts": pd.to_datetime(r.get("cycle_ts", pd.NaT), utc=True, errors="coerce"),
            "symbol": str(r.get("symbol", "") or "").upper(),
            "side": str(r.get("side", "") or "").upper(),
            "model": str(r.get("model", "") or ""),
            "canonical_setup_key": str(r.get("canonical_setup_key", "") or ""),
            "setup_id": str(r.get("setup_id", "") or ""),
            "setup_created_ts": pd.to_datetime(r.get("setup_created_ts", pd.NaT), utc=True, errors="coerce"),
            "candidate_ts": pd.to_datetime(r.get("candidate_ts", r.get("timestamp", pd.NaT)), utc=True, errors="coerce"),
            "visible_ts": pd.to_datetime(r.get("visible_ts", pd.NaT), utc=True, errors="coerce"),
            "wait_confirm_ts": pd.to_datetime(r.get("wait_confirm_ts", pd.NaT), utc=True, errors="coerce"),
            "candidate_latest_ts": pd.to_datetime(r.get("candidate_latest_ts", pd.NaT), utc=True, errors="coerce"),
            "entry_window_expires_ts": pd.to_datetime(r.get("entry_window_expires_ts", pd.NaT), utc=True, errors="coerce"),
            "death_reason": str(r.get("death_reason", "") or ""),
            "entry": r.get("entry", ""),
            "sl": r.get("sl", ""),
            "tp": r.get("tp", ""),
            "planned_rr": r.get("planned_rr", ""),
            "setup_age_bars": r.get("setup_age_bars", ""),
            "setup_age_minutes": r.get("setup_age_minutes", ""),
            "distance_to_entry_R": r.get("distance_to_entry_R", ""),
            "target_distance_pct": r.get("target_distance_pct", ""),
            "stop_distance_pct": r.get("stop_distance_pct", ""),
            "raw_candidate_count": r.get("raw_candidate_count", r.get("pressure_raw_count", "")),
            "pressure_window_id": str(r.get("pressure_window_id", "") or ""),
            "candidate_persistence_bars": persistence_bars,
            "would_shadow_entry_ts": would_shadow_entry_ts,
            "shadow_entry_source": shadow_entry_source,
        })

    if not rows:
        return

    _ensure_parent(path)
    out = pd.DataFrame(rows)
    for col in TDP_STALE_SHADOW_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    out = out[TDP_STALE_SHADOW_COLUMNS]
    if not path.exists() or path.stat().st_size == 0:
        out.to_csv(path, index=False)
    else:
        out.to_csv(path, mode="a", header=False, index=False)



def _structural_ts_from_shadow_row(row: Dict[str, object]):
    for source in ("structural_ts", "setup_created_ts", "candidate_ts", "timestamp"):
        ts = pd.to_datetime(row.get(source, pd.NaT), utc=True, errors="coerce")
        if pd.notna(ts):
            return ts
    return pd.NaT


def _append_structural_ts_shadow_rows(path_like, marked_rows: List[Dict[str, object]]) -> None:
    path = _diag_path(path_like)
    if path is None or not marked_rows:
        return

    rows: List[Dict[str, object]] = []
    for raw_row in marked_rows:
        # Work on a shallow copy only. This logger must never mutate the
        # diagnostic rows passed to other telemetry writers or trading logic.
        r = dict(raw_row)
        rows.append({
            "cycle_ts": pd.to_datetime(r.get("cycle_ts", pd.NaT), utc=True, errors="coerce"),
            "structural_ts": _structural_ts_from_shadow_row(r),
            "symbol": str(r.get("symbol", "") or "").upper(),
            "side": str(r.get("side", "") or "").upper(),
            "model": str(r.get("model", "") or ""),
            "canonical_setup_key": str(r.get("canonical_setup_key", "") or ""),
            "setup_id": str(r.get("setup_id", "") or ""),
            "setup_created_ts": pd.to_datetime(r.get("setup_created_ts", pd.NaT), utc=True, errors="coerce"),
            "candidate_ts": pd.to_datetime(r.get("candidate_ts", r.get("timestamp", pd.NaT)), utc=True, errors="coerce"),
            "visible_ts": pd.to_datetime(r.get("visible_ts", pd.NaT), utc=True, errors="coerce"),
            "wait_confirm_ts": pd.to_datetime(r.get("wait_confirm_ts", pd.NaT), utc=True, errors="coerce"),
            "entry": r.get("entry", ""),
            "sl": r.get("sl", ""),
            "tp": r.get("tp", ""),
            "planned_rr": r.get("planned_rr", ""),
            "setup_age_bars": r.get("setup_age_bars", ""),
            "setup_age_minutes": r.get("setup_age_minutes", ""),
            "distance_to_entry_R": r.get("distance_to_entry_R", ""),
            "target_distance_pct": r.get("target_distance_pct", ""),
            "stop_distance_pct": r.get("stop_distance_pct", ""),
            "raw_candidate_count": r.get("raw_candidate_count", r.get("pressure_raw_count", "")),
            "pressure_window_id": str(r.get("pressure_window_id", "") or ""),
            "death_reason": str(r.get("death_reason", "") or ""),
            "is_emitted": bool(r.get("emitted", False)),
            "passed_wait": bool(r.get("passed_wait", False)),
            "passed_stale": bool(r.get("passed_stale", False)),
            "passed_idempotency": bool(r.get("passed_idempotency", False)),
            "passed_position_gate": bool(r.get("passed_position_gate", False)),
        })

    if not rows:
        return

    _ensure_parent(path)
    out = pd.DataFrame(rows)
    for col in STRUCTURAL_TS_SHADOW_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    out = out[STRUCTURAL_TS_SHADOW_COLUMNS]
    if not path.exists() or path.stat().st_size == 0:
        out.to_csv(path, index=False)
    else:
        out.to_csv(path, mode="a", header=False, index=False)



def _sniper_candidate_key(row: pd.Series) -> str:
    canonical = str(row.get("canonical_setup_key", "") or "")
    if canonical:
        return canonical
    setup_id = str(row.get("setup_id", "") or "")
    if setup_id:
        return setup_id
    return "|".join([
        str(row.get("symbol", "") or "").upper(),
        str(row.get("candidate_ts", row.get("timestamp", "")) or ""),
    ])


def _make_sniper_candidate_diag_rows(
    *,
    marked_rows: List[Dict[str, object]],
    sniper_diag_csv: str,
) -> List[Dict[str, object]]:
    """Build RANGE_TOP_SHORT_V2 SHORT sniper telemetry rows only.

    This function consumes already-created diagnostic copies. It does not mutate
    candidate dataframes and none of its outputs are read by trading gates.
    """
    if not marked_rows:
        return []

    range_short_rows = [
        dict(r) for r in marked_rows
        if str(r.get("model", "")) == "RANGE_TOP_SHORT_V2"
        and str(r.get("side", "")).upper() == "SHORT"
    ]
    if not range_short_rows:
        return []

    symbol = str(range_short_rows[0].get("symbol", "") or "").upper()
    prev_raw_count = int(SNIPER_PREV_RAW_COUNT_BY_SYMBOL.get(symbol, 0))

    raw_candidate_count = int(range_short_rows[0].get("pressure_raw_count", len(marked_rows)) or len(marked_rows))
    raw_count_delta = int(raw_candidate_count - prev_raw_count)
    is_expansion = bool(raw_candidate_count > prev_raw_count)
    is_flat = bool(raw_candidate_count == prev_raw_count)
    is_contraction = bool(raw_candidate_count < prev_raw_count)

    out: List[Dict[str, object]] = []
    for r in range_short_rows:
        cluster_group_count = pd.to_numeric(pd.Series([r.get("pressure_group_count", "")]), errors="coerce").iloc[0]
        if pd.isna(cluster_group_count):
            cluster_group_value: object = ""
            groups_gt1: object = ""
            groups_gt2: object = ""
            groups_gt3: object = ""
        else:
            cluster_group_value = int(cluster_group_count)
            groups_gt1 = bool(cluster_group_count > 1)
            groups_gt2 = bool(cluster_group_count > 2)
            groups_gt3 = bool(cluster_group_count > 3)

        k = _sniper_candidate_key(pd.Series(r))
        persistence_bars = int(SNIPER_PERSISTENCE_BY_KEY.get(k, 0) + 1)
        out.append({
            "timestamp": pd.to_datetime(r.get("latest_ts", r.get("cycle_ts", pd.NaT)), utc=True, errors="coerce"),
            "symbol": str(r.get("symbol", symbol) or symbol).upper(),
            "setup_id": str(r.get("setup_id", "") or ""),
            "canonical_setup_key": str(r.get("canonical_setup_key", "") or ""),
            "candidate_ts": pd.to_datetime(r.get("candidate_ts", r.get("timestamp", pd.NaT)), utc=True, errors="coerce"),
            "visible_ts": pd.to_datetime(r.get("visible_ts", pd.NaT), utc=True, errors="coerce"),
            "wait_confirm_ts": pd.to_datetime(r.get("wait_confirm_ts", pd.NaT), utc=True, errors="coerce"),
            "death_stage": str(r.get("death_stage", "") or ""),
            "death_reason": str(r.get("death_reason", "") or ""),
            "entry": _telemetry_first_present(pd.Series(r), ["entry", "planned_entry", "entry_price"]),
            "sl": _telemetry_first_present(pd.Series(r), ["sl", "stop", "stop_loss"]),
            "tp": _telemetry_first_present(pd.Series(r), ["tp", "target", "take_profit"]),
            "raw_candidate_count": raw_candidate_count,
            "prev_raw_candidate_count": prev_raw_count,
            "raw_count_delta": raw_count_delta,
            "cluster_group_count": cluster_group_value,
            "groups_gt1": groups_gt1,
            "groups_gt2": groups_gt2,
            "groups_gt3": groups_gt3,
            "pressure_window_id": str(r.get("pressure_window_id", "") or ""),
            "pressure_window_age": r.get("pressure_window_age_bars", ""),
            "pressure_window_age_bars": r.get("pressure_window_age_bars", ""),
            "pressure_window_duration_bars": r.get("pressure_window_duration_bars", ""),
            "pressure_window_peak_raw": r.get("pressure_window_peak_raw", ""),
            "pressure_window_candidate_count": r.get("pressure_raw_count", raw_candidate_count),
            "setup_age_minutes": r.get("setup_age_minutes", ""),
            "setup_age_bars": r.get("setup_age_bars", ""),
            "range_width_pct": r.get("range_width_pct", ""),
            "entry_to_range_high_pct": r.get("entry_to_range_high_pct", ""),
            "entry_to_range_low_pct": r.get("entry_to_range_low_pct", ""),
            "target_distance_pct": r.get("target_distance_pct", ""),
            "stop_distance_pct": r.get("stop_distance_pct", ""),
            "planned_rr": r.get("planned_rr", ""),
            "risk_pct": r.get("risk_pct", ""),
            "reward_pct": r.get("reward_pct", ""),
            "risk_distance": r.get("risk_distance", ""),
            "reward_distance": r.get("reward_distance", ""),
            "distance_to_entry_pct": r.get("distance_to_entry_pct", ""),
            "distance_to_entry_R": r.get("distance_to_entry_R", ""),
            "candidate_persistence_bars": persistence_bars,
            "is_candidate_expansion": is_expansion,
            "is_candidate_flat": is_flat,
            "is_candidate_contraction": is_contraction,
            "is_emitted": bool(r.get("emitted", False)),
            "passed_wait": bool(r.get("passed_wait", False)),
            "passed_stale": bool(r.get("passed_stale", False)),
            "passed_idempotency": bool(r.get("passed_idempotency", False)),
            "passed_position_gate": bool(r.get("passed_position_gate", False)),
        })
        if k:
            SNIPER_PERSISTENCE_BY_KEY[k] = persistence_bars
    SNIPER_PREV_RAW_COUNT_BY_SYMBOL[symbol] = raw_candidate_count
    return out


def _append_sniper_candidate_diag(path_like, rows: List[Dict[str, object]]) -> None:
    path = _diag_path(path_like)
    if path is None or not rows:
        return
    _ensure_parent(path)
    out = pd.DataFrame(rows)
    for col in SNIPER_CANDIDATE_DIAG_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    out = out[SNIPER_CANDIDATE_DIAG_COLUMNS]
    if not path.exists() or path.stat().st_size == 0:
        out.to_csv(path, index=False)
    else:
        out.to_csv(path, mode="a", header=False, index=False)


def _opportunity_manager_snapshot_key(row: pd.Series) -> str:
    canonical = str(row.get("canonical_setup_key", "") or "")
    if canonical:
        return canonical
    setup_id = str(row.get("setup_id", "") or "")
    if setup_id:
        return setup_id
    return "|".join([
        str(row.get("symbol", "") or "").upper(),
        str(row.get("model", "") or ""),
        str(row.get("side", "") or "").upper(),
        str(row.get("timestamp", row.get("candidate_ts", "")) or ""),
    ])


def _append_opportunity_manager_snapshot(
    *,
    executable_candidates_df: Optional[pd.DataFrame],
    selected_df: Optional[pd.DataFrame],
    output_csv_path: str,
) -> None:
    """Append opportunity-manager candidate snapshot telemetry only.

    This helper copies fields from already-built candidate rows into an
    append-only CSV and must never feed ranking, filtering, selection,
    execution, risk, TP, SL, wait, stale, or idempotency logic.
    """
    path = _diag_path(output_csv_path)
    if path is None or executable_candidates_df is None or executable_candidates_df.empty:
        return

    def _snapshot_is_blank(value: object) -> bool:
        if value is None:
            return True
        try:
            if pd.isna(value):
                return True
        except Exception:
            pass
        return str(value).strip() == ""

    def _snapshot_first_present(row: pd.Series, names: List[str]):
        for name in names:
            if name not in row.index:
                continue
            value = row.get(name, "")
            if not _snapshot_is_blank(value):
                return value
        return ""

    def _snapshot_float(value: object):
        try:
            out = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        except Exception:
            return None
        if pd.isna(out):
            return None
        return float(out)

    selected_keys: Set[str] = set()
    if selected_df is not None and not selected_df.empty:
        for _, selected_row in selected_df.iterrows():
            selected_key = _opportunity_manager_snapshot_key(selected_row)
            if selected_key:
                selected_keys.add(selected_key)

    rows: List[Dict[str, object]] = []
    for rank, (_, candidate_row) in enumerate(executable_candidates_df.iterrows(), start=1):
        key = _opportunity_manager_snapshot_key(candidate_row)
        row = {col: "" for col in OPPORTUNITY_MANAGER_SNAPSHOT_COLUMNS}
        for col in OPPORTUNITY_MANAGER_SNAPSHOT_COLUMNS:
            if col in candidate_row.index:
                row[col] = candidate_row.get(col, "")

        candidate_ts = _snapshot_first_present(candidate_row, ["candidate_ts", "timestamp"])
        timestamp = _snapshot_first_present(candidate_row, ["timestamp", "candidate_ts"])
        row["candidate_ts"] = candidate_ts
        row["timestamp"] = timestamp
        row["signal_ts"] = _snapshot_first_present(candidate_row, ["signal_ts", "trade_open_ts", "opened_ts", "timestamp"])
        row["setup_created_ts"] = _snapshot_first_present(candidate_row, ["setup_created_ts", "candidate_ts", "timestamp"])
        row["visible_ts"] = _snapshot_first_present(candidate_row, ["visible_ts", "pipeline_visible_ts"])
        row["entry"] = _snapshot_first_present(candidate_row, ["entry"])
        row["sl"] = _snapshot_first_present(candidate_row, ["sl"])
        row["tp"] = _snapshot_first_present(candidate_row, ["tp"])

        if _snapshot_is_blank(row.get("planned_rr", "")):
            row["planned_rr"] = _snapshot_first_present(candidate_row, ["rr"])

        entry = _snapshot_float(row.get("entry", ""))
        sl = _snapshot_float(row.get("sl", ""))
        tp = _snapshot_float(row.get("tp", ""))
        if entry is not None and sl is not None and tp is not None:
            risk_distance = abs(entry - sl)
            reward_distance = abs(tp - entry)
            if risk_distance > 0 and entry != 0:
                if _snapshot_is_blank(row.get("risk_distance", "")):
                    row["risk_distance"] = risk_distance
                if _snapshot_is_blank(row.get("reward_distance", "")):
                    row["reward_distance"] = reward_distance
                if _snapshot_is_blank(row.get("risk_pct", "")):
                    row["risk_pct"] = risk_distance / abs(entry)
                if _snapshot_is_blank(row.get("reward_pct", "")):
                    row["reward_pct"] = reward_distance / abs(entry)
                if _snapshot_is_blank(row.get("planned_rr", "")):
                    row["planned_rr"] = reward_distance / risk_distance

        selected_for_execution = bool(key and key in selected_keys)
        row["is_selected"] = selected_for_execution
        row["selected_for_execution"] = selected_for_execution
        row["selection_rank"] = 1 if selected_for_execution else ""
        row["execution_rank"] = 1 if selected_for_execution else ""
        if selected_for_execution:
            row["selection_reason"] = "selected"
            row["rejected_reason"] = ""
        elif selected_keys:
            row["selection_reason"] = ""
            row["rejected_reason"] = "lower_rank"
        rows.append(row)

    if not rows:
        return

    _ensure_parent(path)
    out = pd.DataFrame(rows)
    for col in OPPORTUNITY_MANAGER_SNAPSHOT_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    out = out[OPPORTUNITY_MANAGER_SNAPSHOT_COLUMNS]
    if not path.exists() or path.stat().st_size == 0:
        out.to_csv(path, index=False)
    else:
        out.to_csv(path, mode="a", header=False, index=False)


def _append_sniper_candidate_outputs(
    *,
    sniper_candidate_diag_csv: str,
    sniper_candidate_summary_csv: str,
    marked_rows: List[Dict[str, object]],
) -> None:
    rows = _make_sniper_candidate_diag_rows(
        marked_rows=marked_rows,
        sniper_diag_csv=sniper_candidate_diag_csv,
    )
    _append_sniper_candidate_diag(sniper_candidate_diag_csv, rows)


def _ensure_sniper_candidate_outputs(sniper_candidate_diag_csv: str, sniper_candidate_summary_csv: str) -> None:
    diag_path = _diag_path(sniper_candidate_diag_csv)
    if diag_path is not None and (not diag_path.exists() or diag_path.stat().st_size == 0):
        _ensure_parent(diag_path)
        pd.DataFrame(columns=SNIPER_CANDIDATE_DIAG_COLUMNS).to_csv(diag_path, index=False)
    summary_path = _diag_path(sniper_candidate_summary_csv)
    if summary_path is not None and (not summary_path.exists() or summary_path.stat().st_size == 0):
        _ensure_parent(summary_path)
        pd.DataFrame(columns=SNIPER_CANDIDATE_SUMMARY_COLUMNS).to_csv(summary_path, index=False)

def _range_short_hindsight_fields(candles_df: Optional[pd.DataFrame], start_ts, end_ts) -> Dict[str, object]:
    # Hindsight-only candle-location telemetry. This function must not be called
    # from any trading decision path.
    empty = {
        "hindsight_window_high": "",
        "hindsight_window_high_ts": "",
        "hindsight_first_lower_high_after_window_high_ts": "",
        "hindsight_first_close_below_prev_low_after_window_high_ts": "",
    }
    if candles_df is None or candles_df.empty or "timestamp" not in candles_df.columns:
        return empty
    c = candles_df.copy()
    c["timestamp"] = pd.to_datetime(c["timestamp"], utc=True, errors="coerce")
    c = c.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    if c.empty or "high" not in c.columns or "low" not in c.columns or "close" not in c.columns:
        return empty
    start_ts = pd.to_datetime(start_ts, utc=True, errors="coerce")
    end_ts = pd.to_datetime(end_ts, utc=True, errors="coerce")
    if pd.isna(start_ts) or pd.isna(end_ts):
        return empty
    inside = c[(c["timestamp"] >= start_ts) & (c["timestamp"] <= end_ts)].copy()
    if inside.empty:
        return empty
    high_idx = inside["high"].astype(float).idxmax()
    window_high = float(c.loc[high_idx, "high"])
    window_high_ts = c.loc[high_idx, "timestamp"]
    after = c[c["timestamp"] > window_high_ts].copy()
    lower_high_ts = pd.NaT
    close_below_prev_low_ts = pd.NaT
    prev_low = None
    for _, r in after.iterrows():
        if pd.isna(lower_high_ts) and float(r["high"]) < window_high:
            lower_high_ts = r["timestamp"]
        if prev_low is not None and pd.isna(close_below_prev_low_ts) and float(r["close"]) < float(prev_low):
            close_below_prev_low_ts = r["timestamp"]
        prev_low = r["low"]
        if pd.notna(lower_high_ts) and pd.notna(close_below_prev_low_ts):
            break
    return {
        "hindsight_window_high": window_high,
        "hindsight_window_high_ts": window_high_ts,
        "hindsight_first_lower_high_after_window_high_ts": lower_high_ts,
        "hindsight_first_close_below_prev_low_after_window_high_ts": close_below_prev_low_ts,
    }


def _rebuild_pressure_window_summary(
    raw_diag_csv,
    summary_csv,
    candles_df: Optional[pd.DataFrame] = None,
    candles_symbol: str = "",
) -> None:
    """Rebuild summary from diagnostic rows only.

    This output is telemetry-only. Rebuilding it never touches live state,
    emitted rows, lifecycle registries, or filters.
    """
    raw_path = _diag_path(raw_diag_csv)
    summary_path = _diag_path(summary_csv)
    if raw_path is None or summary_path is None or not raw_path.exists() or raw_path.stat().st_size == 0:
        return
    try:
        df = pd.read_csv(raw_path)
    except Exception:
        return
    if df.empty or "symbol" not in df.columns or "latest_ts" not in df.columns:
        return
    df = df.copy()
    df["latest_ts"] = pd.to_datetime(df["latest_ts"], utc=True, errors="coerce")
    df["candidate_ts"] = pd.to_datetime(df.get("candidate_ts"), utc=True, errors="coerce")
    df = df.dropna(subset=["latest_ts"])
    if df.empty:
        return

    rows = []
    for sym, sdf in df.groupby(df["symbol"].astype(str).str.upper()):
        per_cycle = (
            sdf.groupby("latest_ts", dropna=True)
            .agg(
                raw_candidate_count=("canonical_setup_key", "size"),
                emitted_count=("emitted", lambda x: int(pd.Series(x).astype(str).str.lower().isin(["true", "1"]).sum())),
                cluster_group_count=("pressure_group_count", lambda x: pd.to_numeric(x, errors="coerce").max()),
            )
            .reset_index()
            .sort_values("latest_ts")
        )
        if per_cycle.empty:
            continue
        window_no = 0
        current = []
        prev_ts = pd.NaT
        for _, cyc in per_cycle.iterrows():
            ts = cyc["latest_ts"]
            if current and pd.notna(prev_ts) and (ts - prev_ts) > pd.Timedelta(minutes=16):
                window_no += 1
                rows.append((sym, window_no, list(current)))
                current = []
            current.append(cyc)
            prev_ts = ts
        if current:
            window_no += 1
            rows.append((sym, window_no, list(current)))

    out_rows = []
    for sym, window_no, cycles in rows:
        cyc_df = pd.DataFrame(cycles)
        start = cyc_df["latest_ts"].min()
        end = cyc_df["latest_ts"].max()
        window_id = f"{sym}|{start.isoformat()}"
        mask = (df["symbol"].astype(str).str.upper() == sym) & (df["latest_ts"] >= start) & (df["latest_ts"] <= end)
        inside = df.loc[mask].copy()
        raw_counts = pd.to_numeric(cyc_df["raw_candidate_count"], errors="coerce").fillna(0)
        group_counts = pd.to_numeric(cyc_df.get("cluster_group_count"), errors="coerce")
        death_counts = inside.get("death_reason", pd.Series(dtype=object)).fillna("").astype(str).value_counts().to_dict()
        duration_bars = int(len(cyc_df))
        duration_minutes = int(max(0, duration_bars - 1) * 15)
        h = {}
        try:
            range_short = inside[(inside.get("model", "").astype(str) == "RANGE_TOP_SHORT_V2") & (inside.get("side", "").astype(str).str.upper() == "SHORT")]
        except Exception:
            range_short = pd.DataFrame()
        if not range_short.empty and str(candles_symbol).upper() == sym:
            h = _range_short_hindsight_fields(candles_df, start, end)
        else:
            h = _range_short_hindsight_fields(None, start, end)
        out_rows.append({
            "symbol": sym,
            "window_id": window_id,
            "window_start": start,
            "window_end": end,
            "duration_bars": duration_bars,
            "duration_minutes": duration_minutes,
            "max_raw_candidate_count": int(raw_counts.max()) if len(raw_counts) else 0,
            "sum_raw_candidate_count": int(raw_counts.sum()) if len(raw_counts) else 0,
            "avg_raw_candidate_count": float(raw_counts.mean()) if len(raw_counts) else 0.0,
            "cluster_group_count_max": "" if group_counts.dropna().empty else int(group_counts.max()),
            "groups_gt1_any": "" if group_counts.dropna().empty else bool((group_counts > 1).any()),
            "groups_gt2_any": "" if group_counts.dropna().empty else bool((group_counts > 2).any()),
            "groups_gt3_any": "" if group_counts.dropna().empty else bool((group_counts > 3).any()),
            "emitted_count_inside_window": int(inside.get("emitted", pd.Series(dtype=object)).astype(str).str.lower().isin(["true", "1"]).sum()),
            "raw_candidate_count_inside_window": int(len(inside)),
            "death_reason_counts_inside_window": json.dumps(death_counts, sort_keys=True),
            "first_candidate_ts": inside["candidate_ts"].min() if "candidate_ts" in inside.columns else pd.NaT,
            "last_candidate_ts": inside["candidate_ts"].max() if "candidate_ts" in inside.columns else pd.NaT,
            **h,
        })
    _ensure_parent(summary_path)
    out = pd.DataFrame(out_rows)
    for col in PRESSURE_WINDOW_SUMMARY_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    out[PRESSURE_WINDOW_SUMMARY_COLUMNS].to_csv(summary_path, index=False)




VISIBLE_TS_PATH = Path("backtest/journal/visible_ts_cache.csv")


def _load_visible_ts_cache() -> Dict[str, pd.Timestamp]:
    if not VISIBLE_TS_PATH.exists():
        return {}

    try:
        df = pd.read_csv(VISIBLE_TS_PATH)
    except Exception:
        return {}

    if df.empty or "setup_id" not in df.columns or "visible_ts" not in df.columns:
        return {}

    out: Dict[str, pd.Timestamp] = {}
    for _, row in df.iterrows():
        sid = str(row.get("setup_id", "") or "")
        ts = pd.to_datetime(row.get("visible_ts"), utc=True, errors="coerce")
        if sid and pd.notna(ts):
            out[sid] = ts
    return out


def _save_visible_ts_cache(cache: Dict[str, pd.Timestamp]) -> None:
    global VISIBLE_TS_CACHE

    if len(VISIBLE_TS_CACHE) > MAX_CACHE_SIZE:
        VISIBLE_TS_CACHE = dict(
            sorted(
                VISIBLE_TS_CACHE.items(),
                key=lambda x: x[1],
                reverse=True
            )[:MAX_CACHE_SIZE]
        )

    VISIBLE_TS_PATH.parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(
        [
            {"setup_id": k, "visible_ts": pd.to_datetime(v, utc=True, errors="coerce")}
            for k, v in VISIBLE_TS_CACHE.items()
        ]
    )

    df.to_csv(VISIBLE_TS_PATH, index=False)


VISIBLE_TS_CACHE = _load_visible_ts_cache()
MAX_CACHE_SIZE = 50000

FETCH_STATUS_OK = "ok"
FETCH_STATUS_EMPTY = "empty"
FETCH_STATUS_DNS_ERROR = "dns_error"
FETCH_STATUS_CONNECTION_ERROR = "connection_error"
FETCH_STATUS_TIMEOUT = "timeout"
FETCH_STATUS_HTTP_ERROR = "http_error"
FETCH_STATUS_API_ERROR = "api_error"

LAST_BYBIT_FETCH_STATUS: Dict[str, str] = {}




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
def _normalize_ts_ns(s):
    return pd.to_datetime(s, utc=True, errors="coerce").dt.as_unit("ns")

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
) -> tuple[pd.DataFrame, str]:
    url = f"{BYBIT_REST}/v5/market/kline"
    params = {
        "category": category,
        "symbol": symbol,
        "interval": interval,
        "start": int(start_ms),
        "end": int(end_ms),
        "limit": int(limit),
    }

    last_error: Optional[str] = None
    last_status = FETCH_STATUS_EMPTY

    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            j = r.json()
            if j.get("retCode") == 0:
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
                    LAST_BYBIT_FETCH_STATUS[str(symbol).upper()] = FETCH_STATUS_EMPTY
                    return df, FETCH_STATUS_EMPTY

                df = df.sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)

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

                LAST_BYBIT_FETCH_STATUS[str(symbol).upper()] = FETCH_STATUS_OK
                return df, FETCH_STATUS_OK

            last_error = f"Bybit error for {symbol}: {j}"
            ret_code = int(j.get("retCode", -1))
            last_status = FETCH_STATUS_API_ERROR

            if ret_code == 10006 and attempt < 2:
                print(f"[NETWORK_WARN] symbol={symbol} type=api_rate_limit attempt={attempt + 1}")
                time.sleep(1.0 + attempt)
                continue

            print(f"[NETWORK_WARN] symbol={symbol} type=api_error details={last_error}")
            LAST_BYBIT_FETCH_STATUS[str(symbol).upper()] = last_status
            return pd.DataFrame(), last_status

        except requests.exceptions.Timeout as e:
            last_error = f"Timeout error for {symbol}: {e}"
            last_status = FETCH_STATUS_TIMEOUT
            print(f"[NETWORK_WARN] symbol={symbol} type=timeout attempt={attempt + 1}")
            if attempt < 2:
                time.sleep(min(4.0, 1.0 * (2 ** attempt)))
                continue
            LAST_BYBIT_FETCH_STATUS[str(symbol).upper()] = last_status
            return pd.DataFrame(), last_status

        except requests.exceptions.ConnectionError as e:
            msg = str(e)
            if "NameResolutionError" in msg or "Failed to resolve" in msg or "Temporary failure in name resolution" in msg:
                last_status = FETCH_STATUS_DNS_ERROR
                print(f"[NETWORK_WARN] symbol={symbol} type=dns_error attempt={attempt + 1}")
            else:
                last_status = FETCH_STATUS_CONNECTION_ERROR
                print(f"[NETWORK_WARN] symbol={symbol} type=connection_error attempt={attempt + 1}")
            last_error = f"Connection error for {symbol}: {e}"
            if attempt < 2:
                time.sleep(min(4.0, 1.0 * (2 ** attempt)))
                continue
            LAST_BYBIT_FETCH_STATUS[str(symbol).upper()] = last_status
            return pd.DataFrame(), last_status

        except requests.exceptions.HTTPError as e:
            last_error = f"HTTP error for {symbol}: {e}"
            last_status = FETCH_STATUS_HTTP_ERROR
            print(f"[NETWORK_WARN] symbol={symbol} type=http_error attempt={attempt + 1}")
            if attempt < 2:
                time.sleep(min(4.0, 1.0 * (2 ** attempt)))
                continue
            LAST_BYBIT_FETCH_STATUS[str(symbol).upper()] = last_status
            return pd.DataFrame(), last_status

        except requests.RequestException as e:
            last_error = f"Request error for {symbol}: {e}"
            last_status = FETCH_STATUS_CONNECTION_ERROR
            print(f"[NETWORK_WARN] symbol={symbol} type=request_error attempt={attempt + 1}")
            if attempt < 2:
                time.sleep(min(4.0, 1.0 * (2 ** attempt)))
                continue
            LAST_BYBIT_FETCH_STATUS[str(symbol).upper()] = last_status
            return pd.DataFrame(), last_status

    print(f"[BYBIT][{symbol}] {last_error or 'unknown error'}")
    LAST_BYBIT_FETCH_STATUS[str(symbol).upper()] = last_status
    return pd.DataFrame(), last_status


def load_bybit_latest(
    category: str,
    symbol: str,
    interval: str,
    candles: int,
) -> pd.DataFrame:
    end = int(pd.Timestamp.now("UTC").timestamp() * 1000)
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
    df, fetch_status = _bybit_get_kline(category, symbol, interval, start, end, limit=1000)
    LAST_BYBIT_FETCH_STATUS[str(symbol).upper()] = fetch_status
    if df.empty:
        return df

    if len(df) > candles:
        df = df.iloc[-candles:].reset_index(drop=True)
    return df


def _is_transport_fetch_status(status: str) -> bool:
    return status in {
        FETCH_STATUS_DNS_ERROR,
        FETCH_STATUS_CONNECTION_ERROR,
        FETCH_STATUS_TIMEOUT,
        FETCH_STATUS_HTTP_ERROR,
    }


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _ensure_output_csv(path: Path) -> None:
    _ensure_parent(path)


def _initialize_live_rotation_manager():
    if LiveJournalRotationManager is None:
        print("[LIVE_ROTATION] manager unavailable")
        return None
    try:
        manager = LiveJournalRotationManager(LIVE_ROTATION_DESIGN_CSV)
        count = manager.load()
        status = manager.status() if hasattr(manager, "status") else "LOADED"
        if status == "LOADED":
            print(f"[LIVE_ROTATION] design loaded: {count} policies")
            return manager
        if Path(LIVE_ROTATION_DESIGN_CSV).exists():
            error = getattr(manager, "error", "")
            if error:
                print(f"[LIVE_ROTATION] design load error: {error}")
            else:
                print("[LIVE_ROTATION] design load error: unknown")
        else:
            print("[LIVE_ROTATION] design not found")
    except Exception as exc:
        print(f"[LIVE_ROTATION] design load error: {exc}")
    return None


def _initialize_live_rotation_controller(rotation_manager):
    if rotation_manager is None or LiveJournalRotationController is None:
        return None
    try:
        return LiveJournalRotationController(rotation_manager)
    except Exception:
        return None


def _initialize_live_rotation_integration_boundary():
    if LiveRotationIntegrationBoundary is None or LiveRotationIntegrationConfig is None:
        return None
    try:
        boundary = LiveRotationIntegrationBoundary(
            LiveRotationIntegrationConfig.from_environment()
        )
        boundary.reach()
        return boundary
    except Exception:
        return None


def _observe_live_rotation_decisions(rotation_controller, *, cycle_ts, csv_paths: List[Path], debug: bool) -> None:
    if not debug or rotation_controller is None:
        return
    for csv_path in csv_paths:
        try:
            result = rotation_controller.evaluate(Path(csv_path), cycle_ts)
        except Exception as exc:
            print(f"[LIVE_ROTATION] csv={Path(csv_path).name} policy=UNKNOWN restart=UNKNOWN decision=UNKNOWN_POLICY action=NONE error={type(exc).__name__}:{exc}")
            continue
        print(
            "[LIVE_ROTATION] "
            f"csv={result.get('csv', Path(csv_path).name)} "
            f"policy={result.get('policy', 'UNKNOWN')} "
            f"restart={result.get('restart', 'UNKNOWN')} "
            f"decision={result.get('decision', 'UNKNOWN_POLICY')} "
            f"action={result.get('action', 'NONE')}"
        )


def _write_live_rotation_plan(rotation_controller, rotation_manager, *, cycle_ts, csv_paths: List[Path], plan_csv: Path) -> None:
    if rotation_controller is None or append_live_rotation_plan_rows is None:
        return
    try:
        append_live_rotation_plan_rows(
            Path(plan_csv),
            rotation_controller=rotation_controller,
            rotation_manager=rotation_manager,
            cycle_ts=cycle_ts,
            csv_paths=csv_paths,
        )
    except Exception:
        return


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


def _position_overlaps_at_open(symbol: str, intended_open_ts, position_state_csv: Path) -> bool:
    """
    Return True if intended_open_ts falls inside any persisted same-symbol
    position window using half-open interval semantics:
        opened_ts <= intended_open_ts < closed_ts

    Null closed_ts is treated as open-ended.
    """
    if not position_state_csv.exists() or position_state_csv.stat().st_size == 0:
        return False

    intended_open_ts = pd.to_datetime(intended_open_ts, utc=True, errors="coerce")
    if pd.isna(intended_open_ts):
        return False

    try:
        df = pd.read_csv(position_state_csv)
    except Exception:
        return False

    if df.empty or "symbol" not in df.columns or "opened_ts" not in df.columns:
        return False

    work = df.copy()
    work["symbol"] = work["symbol"].astype(str).str.upper()
    work = work.loc[work["symbol"] == str(symbol).upper()].copy()
    if work.empty:
        return False

    work["opened_ts"] = pd.to_datetime(work["opened_ts"], utc=True, errors="coerce")
    if "closed_ts" in work.columns:
        work["closed_ts"] = pd.to_datetime(work["closed_ts"], utc=True, errors="coerce")
    else:
        work["closed_ts"] = pd.NaT

    overlap_mask = (
        work["opened_ts"].notna()
        & (work["opened_ts"] <= intended_open_ts)
        & (
            work["closed_ts"].isna()
            | (intended_open_ts < work["closed_ts"])
        )
    )
    return bool(overlap_mask.any())


def _pending_position_overlaps_at_open(
    symbol: str,
    intended_open_ts,
    pending_open_intervals: Dict[str, List[tuple]],
) -> bool:
    """Return True if candidate open interval overlaps accepted rows in this batch."""
    intended_open_ts = pd.to_datetime(intended_open_ts, utc=True, errors="coerce")
    if pd.isna(intended_open_ts):
        return False

    candidate_symbol = str(symbol).upper()
    candidate_close_ts = pd.NaT

    for opened_ts, closed_ts in pending_open_intervals.get(candidate_symbol, []):
        opened_ts = pd.to_datetime(opened_ts, utc=True, errors="coerce")
        closed_ts = pd.to_datetime(closed_ts, utc=True, errors="coerce")
        if pd.isna(opened_ts):
            continue

        pending_ends_after_candidate_opens = (
            pd.isna(closed_ts)
            or intended_open_ts < closed_ts
        )
        candidate_ends_after_pending_opens = (
            pd.isna(candidate_close_ts)
            or opened_ts < candidate_close_ts
        )

        if pending_ends_after_candidate_opens and candidate_ends_after_pending_opens:
            return True

    return False


def _candidate_intended_open_ts(row: pd.Series):
    for ts_col in ("trade_open_ts", "signal_ts", "opened_ts", "timestamp"):
        if ts_col in row.index:
            ts = pd.to_datetime(row.get(ts_col), utc=True, errors="coerce")
            if pd.notna(ts):
                return ts
    return pd.NaT


def _filter_position_overlap_candidates(
    *,
    out_df: pd.DataFrame,
    symbol: str,
    position_state_csv: Path,
    flow_log_csv: Path,
    flow_row: Dict[str, object],
) -> tuple[pd.DataFrame, int]:
    if out_df is None or out_df.empty:
        return out_df, 0

    accepted_indices = []
    skipped_count = 0
    pending_open_intervals: Dict[str, List[tuple]] = {}

    for row_idx, candidate_row in out_df.iterrows():
        candidate_symbol = str(candidate_row.get("symbol", symbol)).upper()
        intended_open_ts = _candidate_intended_open_ts(candidate_row)

        existing_overlap = _position_overlaps_at_open(
            candidate_symbol,
            intended_open_ts,
            position_state_csv,
        )
        pending_overlap = _pending_position_overlaps_at_open(
            candidate_symbol,
            intended_open_ts,
            pending_open_intervals,
        )

        if existing_overlap or pending_overlap:
            skipped_count += 1
            print(
                f"[POSITION_GATE] symbol={candidate_symbol} "
                f"canonical={candidate_row.get('canonical_setup_key', '')} "
                f"skipped_open_position=True "
                f"intended_open_ts={intended_open_ts} "
                f"reason=open_position_exists_at_intended_open_ts"
            )

            skipped_flow_row = dict(flow_row)
            skipped_flow_row["symbol"] = candidate_symbol
            skipped_flow_row["skipped_open_position"] = True
            skipped_flow_row["death_stage"] = "position_gate"
            skipped_flow_row["death_reason"] = "open_position_exists_at_intended_open_ts"
            skipped_flow_row["notes"] = "blocked_by_open_position_at_intended_open_ts"
            skipped_flow_row["emitted_count"] = 0

            for col in (
                "setup_id",
                "canonical_setup_key",
                "lifecycle_state",
                "model",
                "side",
                "setup_created_ts",
                "wait_confirm_ts",
                "wait_context_source",
                "visible_ts",
                "entry_anchor_ts",
                "first_seen_ts",
                "timestamp",
                "signal_ts",
                "trade_open_ts",
                "opened_ts",
            ):
                if col in out_df.columns:
                    skipped_flow_row[col] = candidate_row.get(col)

            _append_flow_row(flow_log_csv, skipped_flow_row)
            continue

        accepted_indices.append(row_idx)
        if pd.notna(intended_open_ts):
            pending_open_intervals.setdefault(candidate_symbol, []).append(
                (intended_open_ts, pd.NaT)
            )

    if not accepted_indices:
        return out_df.iloc[0:0].copy(), skipped_count

    return out_df.loc[accepted_indices].copy(), skipped_count

def _mark_position_open(
    symbol: str,
    setup_id: str,
    opened_ts: pd.Timestamp,
    position_state_csv: Path,
    canonical_setup_key: str = "",
    setup_created_ts=pd.NaT,
    signal_ts=pd.NaT,
    wait_confirm_ts=pd.NaT,
    wait_context_source: str = "",
) -> None:
    _ensure_parent(position_state_csv)

    row = pd.DataFrame([
        {
            "symbol": str(symbol).upper(),
            "canonical_setup_key": str(canonical_setup_key or ""),
            "setup_id": str(setup_id),
            "setup_created_ts": pd.to_datetime(setup_created_ts, utc=True, errors="coerce"),
            "wait_confirm_ts": pd.to_datetime(wait_confirm_ts, utc=True, errors="coerce"),
            "wait_context_source": str(wait_context_source or ""),
            "signal_ts": pd.to_datetime(signal_ts, utc=True, errors="coerce"),
            "opened_ts": pd.to_datetime(opened_ts, utc=True, errors="coerce"),
            "status": "OPEN",
            "closed_ts": pd.NA,
            "close_reason": pd.NA,
            "lifecycle_state": LIFECYCLE_OPENED,
        }
    ])

    cols = [
        "symbol",
        "canonical_setup_key",
        "setup_id",
        "setup_created_ts",
        "wait_confirm_ts",
        "wait_context_source",
        "signal_ts",
        "opened_ts",
        "status",
        "closed_ts",
        "close_reason",
        "lifecycle_state",
    ]

    if not position_state_csv.exists() or position_state_csv.stat().st_size == 0:
        row[cols].to_csv(position_state_csv, index=False)
        return

    try:
        existing = pd.read_csv(position_state_csv)
        if existing.empty:
            existing = pd.DataFrame(columns=cols)
        else:
            for c in cols:
                if c not in existing.columns:
                    existing[c] = pd.NA
            existing = existing[cols]
    except Exception:
        existing = pd.DataFrame(columns=cols)

    if not existing.empty and "symbol" in existing.columns and "status" in existing.columns:
        mask = (
            existing["symbol"].astype(str).str.upper() == str(symbol).upper()
        ) & (
            existing["status"].astype(str).str.upper() == "OPEN"
        )
        existing = existing.loc[~mask].copy()

    combined = pd.concat([existing, row[cols]], ignore_index=True)
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

    rows = _ensure_canonical_setup_key(rows)

    def _created_from_canonical(k):
        try:
            return str(k).split("|", 3)[3]
        except Exception:
            return pd.NaT

    canonical_created = rows["canonical_setup_key"].apply(_created_from_canonical)

    # Preserve ORIGINAL structural setup timestamp
    rows["setup_created_ts"] = canonical_created

    signal_fallback = rows.get("signal_ts", rows.get("timestamp"))

    if "trade_open_ts" not in rows.columns:
        rows["trade_open_ts"] = signal_fallback
    else:
        rows["trade_open_ts"] = rows["trade_open_ts"].fillna(signal_fallback)
        rows.loc[
            rows["trade_open_ts"].astype(str).str.strip().eq(""),
            "trade_open_ts"
        ] = signal_fallback

    if "opened_ts" not in rows.columns:
        rows["opened_ts"] = signal_fallback
    else:
        rows["opened_ts"] = rows["opened_ts"].fillna(signal_fallback)
        rows.loc[
            rows["opened_ts"].astype(str).str.strip().eq(""),
            "opened_ts"
        ] = signal_fallback

    if "execution_ts_source" not in rows.columns:
        rows["execution_ts_source"] = (
            "signal_ts"
            if "signal_ts" in rows.columns
            else "timestamp"
        )
    else:
        fallback_source = (
            "signal_ts"
            if "signal_ts" in rows.columns
            else "timestamp"
        )

        rows["execution_ts_source"] = (
            rows["execution_ts_source"].fillna(fallback_source)
        )

        rows.loc[
            rows["execution_ts_source"].astype(str).str.strip().eq(""),
            "execution_ts_source"
        ] = fallback_source

    # Wait confirmation metadata
    if "wait_confirm_ts" not in rows.columns:
        rows["wait_confirm_ts"] = (
            pd.to_datetime(
                rows["setup_created_ts"],
                utc=True,
                errors="coerce",
            ) + pd.Timedelta(minutes=15)
        )
    else:
        fallback_wait = (
            pd.to_datetime(
                rows["setup_created_ts"],
                utc=True,
                errors="coerce",
            ) + pd.Timedelta(minutes=15)
        )

        rows["wait_confirm_ts"] = (
            rows["wait_confirm_ts"].fillna(fallback_wait)
        )

        rows.loc[
            rows["wait_confirm_ts"].astype(str).str.strip().eq(""),
            "wait_confirm_ts"
        ] = fallback_wait

    if "wait_context_source" not in rows.columns:
        rows["wait_context_source"] = (
            "setup_created_ts_plus_1_candle"
        )
    else:
        rows["wait_context_source"] = (
            rows["wait_context_source"]
            .fillna("setup_created_ts_plus_1_candle")
        )

        rows.loc[
            rows["wait_context_source"]
            .astype(str)
            .str.strip()
            .eq(""),
            "wait_context_source"
        ] = "setup_created_ts_plus_1_candle"

    use_cols = [
        c for c in (
            "canonical_setup_key",
            "setup_id",
            "symbol",
            "timestamp",
            "setup_created_ts",
            "wait_confirm_ts",
            "wait_context_source",
            "model",
            "side",
            "signal_ts",
            "observed_ts",
            "trade_open_ts",
            "opened_ts",
            "execution_ts_source",
            "lifecycle_state",
        )
        if c in rows.columns
    ]

    if not use_cols:
        return

    out = rows[use_cols].copy()

    _ensure_parent(path)

    if not path.exists() or path.stat().st_size == 0:
        out.to_csv(path, index=False)
    else:
        out.to_csv(path, mode="a", header=False, index=False)


def _build_setup_id(symbol: str, timestamp, model: str, side: str) -> str:
    ts = pd.to_datetime(timestamp, utc=True, errors="coerce")
    return f"{str(symbol).upper()}|{ts}|{str(model)}|{str(side).upper()}"


def _build_runtime_setup_ids(df: pd.DataFrame, symbol: str) -> pd.Series:
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
    out["observed_ts"] = pd.to_datetime(latest_ts, utc=True, errors="coerce")
    out["symbol"] = symbol

    # Canonical execution/open clock for emitted live rows is the emitted
    # post-wait timestamp. Do not use structural setup_created_ts as open time.
    execution_ts = pd.to_datetime(out.get("timestamp"), utc=True, errors="coerce")
    if "signal_ts" not in out.columns or out["signal_ts"].isna().all():
        out["signal_ts"] = execution_ts
    out["trade_open_ts"] = execution_ts
    out["opened_ts"] = execution_ts
    out["execution_ts_source"] = "timestamp"
    out["position_gate_reason"] = ""
    out["skipped_open_position"] = False
    out["lifecycle_state"] = LIFECYCLE_OPENED

    _append_df(out_csv, out)
    return int(len(out))


def _series_summary(df: Optional[pd.DataFrame], col: str) -> str:
    if df is None or df.empty or col not in df.columns:
        return ""
    try:
        s = df[col].dropna().astype(str)
        if s.empty:
            return ""
        vc = s.value_counts()
        return ";".join([f"{idx}:{int(val)}" for idx, val in vc.items()])
    except Exception:
        return ""


def _derive_phase(df_e: Optional[pd.DataFrame], ctx: Dict[str, object]) -> str:
    if df_e is not None and not df_e.empty and "phase" in df_e.columns:
        try:
            s = df_e["phase"].dropna().astype(str)
            if not s.empty:
                return str(s.iloc[0])
        except Exception:
            pass
    try:
        return str(ctx.get("phase", "") or "")
    except Exception:
        return ""



def _parity_filter_bool(value) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _parity_filter_num(value):
    try:
        out = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    except Exception:
        return None
    if pd.isna(out):
        return None
    return float(out)


def _parity_filter_mode(mode: str) -> str:
    mode = str(mode or "NONE").strip().upper()
    return mode if mode in PARITY_FILTER_MODES else "NONE"


def _parity_filter_enrich_fields(out_df: pd.DataFrame, candles_df: Optional[pd.DataFrame], latest_ts) -> pd.DataFrame:
    """Attach research-filter geometry fields to candidate rows if absent.

    This is parity research telemetry only. It uses the same diagnostic helpers as
    raw_candidate_lifecycle_diag and does not affect live behavior when filters
    are disabled.
    """
    if out_df is None or out_df.empty:
        return out_df
    out = out_df.copy()
    required = [
        "range_width_pct",
        "distance_to_entry_R",
        "distance_to_entry_pct",
        "entry_to_range_low_pct",
        "entry_to_range_high_pct",
        "planned_rr",
        "risk_pct",
        "reward_pct",
        "setup_age_bars",
    ]
    for col in required:
        if col not in out.columns:
            out[col] = pd.Series([pd.NA] * len(out), index=out.index, dtype="object")
        else:
            # Candidate rows can arrive with pandas StringDtype columns. Geometry
            # enrichment writes floats, so force object dtype before scalar assignment.
            out[col] = out[col].astype("object")

    for idx, row in out.iterrows():
        setup_created_ts = pd.to_datetime(row.get("setup_created_ts", row.get("timestamp", pd.NaT)), utc=True, errors="coerce")
        first_seen_ts = _telemetry_candidate_first_seen_ts(row, latest_ts)
        computed = _telemetry_range_and_rr_fields(
            row,
            candles_df=candles_df,
            setup_created_ts=setup_created_ts,
            first_seen_ts=first_seen_ts,
        )
        for col, val in computed.items():
            cur = out.at[idx, col] if col in out.columns else ""
            if str(cur) == "" or str(cur).lower() == "nan" or pd.isna(cur):
                out.at[idx, col] = val
        cur_age = out.at[idx, "setup_age_bars"] if "setup_age_bars" in out.columns else ""
        if str(cur_age) == "" or str(cur_age).lower() == "nan" or pd.isna(cur_age):
            out.at[idx, "setup_age_bars"] = _telemetry_closed_bars_between(candles_df, setup_created_ts, first_seen_ts)
    return out


def _parity_filter_failed_condition(row: pd.Series, mode: str, *, range_width_min: float, distance_min: float) -> tuple[bool, str]:
    mode = _parity_filter_mode(mode)
    if mode == "NONE":
        return True, ""

    model = str(row.get("model", "") or "")
    setup_age = _parity_filter_num(row.get("setup_age_bars", ""))
    distance_r = _parity_filter_num(row.get("distance_to_entry_R", ""))
    range_width = _parity_filter_num(row.get("range_width_pct", ""))

    if mode == "RANGE_AGE_3_5":
        if model != "RANGE_TOP_SHORT_V2":
            return False, "model_not_range"
        if setup_age is None:
            return False, "missing_setup_age_bars"
        if not (3 <= setup_age <= 5):
            return False, "setup_age_bars_outside_3_5"
        return True, ""

    if mode == "COMBINED_FINGERPRINT":
        if model == "RANGE_TOP_SHORT_V2":
            if setup_age is None:
                return False, "missing_setup_age_bars"
            if 3 <= setup_age <= 5:
                return True, ""
            return False, "range_setup_age_bars_outside_3_5"
        if model == "TDP_REENTRY":
            if distance_r is None:
                return False, "missing_distance_to_entry_R"
            if distance_r >= 8:
                return True, ""
            return False, "distance_to_entry_R_below_8"
        return False, "unsupported_model"

    if mode == "REMOVE_RANGE_AGE_1_2":
        if model == "RANGE_TOP_SHORT_V2":
            if setup_age is None:
                return False, "missing_setup_age_bars"
            if 1 <= setup_age <= 2:
                return False, "range_setup_age_bars_1_2_removed"
        return True, ""

    if mode == "RANGE_GEOMETRY_P50":
        if model != "RANGE_TOP_SHORT_V2":
            return False, "model_not_range"
        if range_width is None:
            return False, "missing_range_width_pct"
        if distance_r is None:
            return False, "missing_distance_to_entry_R"
        if range_width < float(range_width_min):
            return False, "range_width_pct_below_threshold"
        if distance_r <= float(distance_min):
            return False, "distance_to_entry_R_below_threshold"
        return True, ""

    return True, ""


def _append_parity_filter_diagnostics(path_like, rows: List[Dict[str, object]]) -> None:
    path = _diag_path(path_like)
    if path is None or not rows:
        return
    _ensure_parent(path)
    out = pd.DataFrame(rows)
    for col in PARITY_FILTER_DIAGNOSTICS_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    out = out[PARITY_FILTER_DIAGNOSTICS_COLUMNS]
    if not path.exists() or path.stat().st_size == 0:
        out.to_csv(path, index=False)
    else:
        out.to_csv(path, mode="a", header=False, index=False)


def _apply_parity_research_filter(
    *,
    out_df: pd.DataFrame,
    candles_df: Optional[pd.DataFrame],
    latest_ts,
    cycle_ts,
    filter_mode: str,
    filter_diag_csv: str,
    range_width_min: float,
    distance_min: float,
) -> tuple[pd.DataFrame, Dict[str, object]]:
    mode = _parity_filter_mode(filter_mode)
    stats: Dict[str, object] = {
        "parity_filter_mode": mode,
        "parity_filter_stage": "pre_final_selection_pre_emission",
        "filter_input_rows": 0,
        "filter_missing_range_width_pct": 0,
        "filter_missing_distance_to_entry_R": 0,
        "filter_pass_rows": 0,
        "filter_reject_rows": 0,
        "filter_pass_then_emitted": 0,
        "filter_pass_then_wait_confirmation_death": 0,
        "filter_pass_then_stale_death": 0,
        "filter_pass_then_position_gate_death": 0,
    }
    if out_df is None or out_df.empty or mode == "NONE":
        return out_df, stats

    enriched = _parity_filter_enrich_fields(out_df, candles_df, latest_ts)
    rows = []
    keep_indices = []
    for idx, row in enriched.iterrows():
        stats["filter_input_rows"] = int(stats["filter_input_rows"]) + 1
        rw = _parity_filter_num(row.get("range_width_pct", ""))
        dr = _parity_filter_num(row.get("distance_to_entry_R", ""))
        if rw is None:
            stats["filter_missing_range_width_pct"] = int(stats["filter_missing_range_width_pct"]) + 1
        if dr is None:
            stats["filter_missing_distance_to_entry_R"] = int(stats["filter_missing_distance_to_entry_R"]) + 1

        passed, failed_condition = _parity_filter_failed_condition(
            row,
            mode,
            range_width_min=float(range_width_min),
            distance_min=float(distance_min),
        )
        if passed:
            keep_indices.append(idx)
            stats["filter_pass_rows"] = int(stats["filter_pass_rows"]) + 1
        else:
            stats["filter_reject_rows"] = int(stats["filter_reject_rows"]) + 1

        rows.append({
            "timestamp": pd.to_datetime(row.get("timestamp", pd.NaT), utc=True, errors="coerce"),
            "cycle_ts": pd.to_datetime(cycle_ts, utc=True, errors="coerce"),
            "symbol": str(row.get("symbol", "") or "").upper(),
            "model": str(row.get("model", "") or ""),
            "side": str(row.get("side", "") or "").upper(),
            "canonical_setup_key": str(row.get("canonical_setup_key", "") or ""),
            "candidate_ts": pd.to_datetime(row.get("candidate_ts", row.get("timestamp", pd.NaT)), utc=True, errors="coerce"),
            "visible_ts": pd.to_datetime(row.get("visible_ts", pd.NaT), utc=True, errors="coerce"),
            "wait_confirm_ts": pd.to_datetime(row.get("wait_confirm_ts", pd.NaT), utc=True, errors="coerce"),
            "setup_age_bars": row.get("setup_age_bars", ""),
            "range_width_pct": row.get("range_width_pct", ""),
            "distance_to_entry_R": row.get("distance_to_entry_R", ""),
            "distance_to_entry_pct": row.get("distance_to_entry_pct", ""),
            "entry_to_range_low_pct": row.get("entry_to_range_low_pct", ""),
            "entry_to_range_high_pct": row.get("entry_to_range_high_pct", ""),
            "planned_rr": row.get("planned_rr", row.get("rr", "")),
            "risk_pct": row.get("risk_pct", ""),
            "reward_pct": row.get("reward_pct", ""),
            "filter_name": mode,
            "filter_applied": True,
            "filter_passed": bool(passed),
            "filter_failed_condition": failed_condition,
            "death_stage": "" if passed else "parity_filter",
            "death_reason": "" if passed else failed_condition,
            "is_emitted": False,
        })

    _append_parity_filter_diagnostics(filter_diag_csv, rows)

    if not keep_indices:
        return enriched.iloc[0:0].copy(), stats

    kept = enriched.loc[keep_indices].copy()
    kept["parity_filter_applied"] = True
    kept["parity_filter_passed"] = True
    kept["parity_filter_name"] = mode
    kept["parity_filter_reason"] = "passed"
    kept["parity_filter_stage"] = "pre_final_selection_pre_emission"
    kept["filter_range_width_pct"] = kept.get("range_width_pct", "")
    kept["filter_distance_to_entry_R"] = kept.get("distance_to_entry_R", "")
    kept["filter_threshold_range_width_pct"] = float(range_width_min)
    kept["filter_threshold_distance_to_entry_R"] = float(distance_min)
    return kept, stats

def _append_flow_row(flow_log_csv: Path, row: Dict[str, object]) -> None:
    _ensure_parent(flow_log_csv)
    flow_row = dict(row)
    flow_row.update(_stale_authority_shadow_fields(flow_row))
    out = pd.DataFrame([{c: flow_row.get(c, "") for c in FLOW_LOG_COLUMNS}])
    if not flow_log_csv.exists() or flow_log_csv.stat().st_size == 0:
        out.to_csv(flow_log_csv, index=False)
    else:
        out.to_csv(flow_log_csv, mode="a", header=False, index=False)


def _make_flow_row(*, cycle_ts: pd.Timestamp, symbol: str, latest_ts: Optional[pd.Timestamp]) -> Dict[str, object]:
    return {
        "cycle_ts": pd.to_datetime(cycle_ts, utc=True, errors="coerce"),
        "symbol": str(symbol).upper(),
        "latest_ts": pd.to_datetime(latest_ts, utc=True, errors="coerce") if latest_ts is not None else pd.NaT,

        # setup identity
        "setup_id": "",
        "canonical_setup_key": "",
        "lifecycle_state": "",
        "model": "",
        "side": "",
        "setup_created_ts": pd.NaT,
        "wait_confirm_ts": pd.NaT,
        "wait_context_source": "",

        # execution-visible semantics
        "visible_ts": pd.NaT,
        "entry_anchor_ts": pd.NaT,
        "first_seen_ts": pd.NaT,
        "age_candles_at_first_seen": 0,
        "intended_entry_ts": pd.NaT,
        "entry_window_expires_ts": pd.NaT,
        "entry_delay_minutes": 0.0,
        "entry_timing_valid": False,
        "execution_ts_source": "",
        "trigger_refresh_candidate": False,
        "trigger_refresh_reason": "",
        "old_canonical_setup_key": "",
        "original_setup_created_ts": "",
        "candidate_latest_ts": "",
        "range_retest_score": 0.0,
        "trigger_refresh_applied": False,
        "age_minutes_at_first_seen": 0.0,

        # summaries / diagnostics
        "phase": "",
        "model_summary_raw": "",
        "model_summary_after_wait": "",
        "model_summary_after_stale": "",
        "model_summary_after_idempotency": "",
        "model_summary_after_per_cycle_guard": "",
        "sub_label_summary_raw": "",

        # gates / counters
        "skipped_open_position": False,
        "pipeline_zero_rows": False,
        "raw_entries_count": 0,
        "after_wait_count": 0,
        "after_stale_count": 0,
        "after_idempotency_count": 0,
        "after_per_cycle_guard_count": 0,
        "emitted_count": 0,
        "after_freshness_count": 0,

        # outcome
        "notes": "",
        "death_stage": "no_raw",
        "death_reason": "",
    }

def _time_alignment_audit(
    *,
    symbol: str,
    candles_df: pd.DataFrame,
    latest_ts: pd.Timestamp,
    raw_df: Optional[pd.DataFrame] = None,
    visible_ts: Optional[pd.Timestamp] = None,
    pipeline_visible_ts: Optional[pd.Timestamp] = None,
) -> None:
    try:
        now_utc = pd.Timestamp.now(tz="UTC")

        c = candles_df.copy()
        c["timestamp"] = pd.to_datetime(c["timestamp"], utc=True, errors="coerce")
        c = c.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

        latest_ts = pd.to_datetime(latest_ts, utc=True, errors="coerce")

        print(f"\n[TIME_AUDIT][{symbol}] =====================================")
        print(f"[TIME_AUDIT][{symbol}] now_utc={now_utc}")
        print(f"[TIME_AUDIT][{symbol}] latest_ts={latest_ts}")

        print(f"[TIME_AUDIT][{symbol}] last_5_candles:")
        print(c[["timestamp", "open", "high", "low", "close"]].tail(5).to_string(index=False))

        last_row_ts = c["timestamp"].iloc[-1] if len(c) else pd.NaT

        visible_ts = pd.to_datetime(visible_ts, utc=True, errors="coerce")
        pipeline_visible_ts = pd.to_datetime(pipeline_visible_ts, utc=True, errors="coerce")

        entry_anchor_ts = latest_ts
        if pd.notna(visible_ts):
            entry_anchor_ts = visible_ts
        elif pd.notna(pipeline_visible_ts):
            entry_anchor_ts = pipeline_visible_ts

        last_le_latest = c.loc[c["timestamp"] <= entry_anchor_ts, "timestamp"].max() if len(c) else pd.NaT

        print(f"[TIME_AUDIT][{symbol}] last_row_ts={last_row_ts}")
        print(f"[TIME_AUDIT][{symbol}] entry_anchor_ts={entry_anchor_ts}")
        print(f"[TIME_AUDIT][{symbol}] last_ts_le_anchor={last_le_latest}")
        print(f"[TIME_AUDIT][{symbol}] last_row_eq_last_le_anchor={bool(last_row_ts == last_le_latest)}")

        sliced = c.loc[c["timestamp"] <= entry_anchor_ts].copy()
        removed = int(len(c) - len(sliced))

        print(f"[TIME_AUDIT][{symbol}] rows_before_slice={len(c)}")
        print(f"[TIME_AUDIT][{symbol}] rows_removed_by_slice={removed}")
        print(f"[TIME_AUDIT][{symbol}] max_ts_after_slice={sliced['timestamp'].max() if len(sliced) else pd.NaT}")
        print(f"[TIME_AUDIT][{symbol}] last_3_after_slice:")
        if len(sliced):
            print(sliced[["timestamp", "open", "high", "low", "close"]].tail(3).to_string(index=False))
        else:
            print("(empty)")

        if raw_df is not None and not raw_df.empty:
            r = raw_df.copy()
            if "timestamp" in r.columns:
                r["timestamp"] = pd.to_datetime(r["timestamp"], utc=True, errors="coerce")
                first_setup_ts = pd.to_datetime(r["timestamp"].iloc[0], utc=True, errors="coerce")
                print(f"[TIME_AUDIT][{symbol}] first_setup_ts={first_setup_ts}")

                match_idx = sliced.index[sliced["timestamp"] == first_setup_ts].tolist()
                if match_idx:
                    idx = match_idx[0]
                    pos = sliced.index.get_loc(idx)
                    candles_after = len(sliced) - pos - 1
                    diff_min = (latest_ts - first_setup_ts).total_seconds() / 60.0 if pd.notna(first_setup_ts) else None
                    print(f"[TIME_AUDIT][{symbol}] setup_index_in_sliced={pos}")
                    print(f"[TIME_AUDIT][{symbol}] candles_after_setup_until_latest={candles_after}")
                    print(f"[TIME_AUDIT][{symbol}] setup_to_latest_minutes={diff_min}")
                else:
                    print(f"[TIME_AUDIT][{symbol}] first_setup_ts_not_found_in_sliced_window")

            if "ctx_sub_label" in r.columns:
                tdp_rows = r.loc[r["ctx_sub_label"].isin(["TDP_TOP", "TDP_BOT"])].copy()
            elif "sub_label" in r.columns:
                tdp_rows = r.loc[r["sub_label"].isin(["TDP_TOP", "TDP_BOT"])].copy()
            else:
                tdp_rows = pd.DataFrame()

            if not tdp_rows.empty:
                print(f"[TIME_AUDIT][{symbol}] last_6_for_tdp:")
                cols = [cname for cname in ["timestamp", "dev_count", "ctx_sub_label", "sub_label"] if cname in sliced.columns]
                if cols:
                    print(sliced[cols].tail(6).to_string(index=False))

                last_tdp_ts = pd.to_datetime(tdp_rows["timestamp"].iloc[-1], utc=True, errors="coerce")
                tdp_on_last = bool(len(sliced) and last_tdp_ts == sliced["timestamp"].iloc[-1])
                print(f"[TIME_AUDIT][{symbol}] last_tdp_label_ts={last_tdp_ts}")
                print(f"[TIME_AUDIT][{symbol}] tdp_label_on_last_row={tdp_on_last}")

        print(f"[TIME_AUDIT][{symbol}] =====================================\n")

    except Exception as e:
        print(f"[TIME_AUDIT_ERROR][{symbol}] {type(e).__name__}: {e}")

def _apply_execution_window_guard(
    *,
    wait_checked: pd.DataFrame,
    latest_ts: pd.Timestamp,
    symbol: str,
    flow_log_csv: Path,
    flow_row: Dict[str, object],
) -> pd.DataFrame:
    """
    Enforce entry timing semantics after wait confirmation accepts.

    setup_created_ts / wait_confirm_ts remain immutable identity/confirmation
    timestamps. Execution is only allowed at wait_confirm_ts within a small
    model-specific window. The live/latest cycle timestamp is observability
    only and must not become signal_ts/opened_ts for an old setup.
    """
    if wait_checked is None or wait_checked.empty:
        return wait_checked

    out = wait_checked.copy()

    # Research-only: TDP_WAIT_OFF_ONLY must match global WAIT_OFF semantics
    # for bypassed TDP rows. Therefore these rows must not be evaluated by
    # the wait_confirm_ts-anchored execution-window guard.
    bypass_mask = pd.Series(False, index=out.index)
    if "wait_bypassed" in out.columns:
        bypass_mask = (
            out["wait_bypassed"]
            .astype(str)
            .str.lower()
            .isin(["true", "1", "yes"])
        )
    bypass_out = out.loc[bypass_mask].copy()
    if not bypass_out.empty:
        visible_anchor = pd.Series(pd.NaT, index=bypass_out.index)
        if "visible_ts" in bypass_out.columns:
            visible_anchor = pd.to_datetime(
                bypass_out["visible_ts"], utc=True, errors="coerce"
            )
        latest_anchor = pd.to_datetime(latest_ts, utc=True, errors="coerce")
        observable_anchor = visible_anchor.fillna(latest_anchor)

        # Causality repair for research-only TDP_WAIT_OFF_ONLY:
        # bypassed rows must not open at setup_created_ts or wait_confirm_ts.
        # They become executable only when observable in the replay cycle.
        for ts_col in (
            "signal_ts",
            "timestamp",
            "trade_open_ts",
            "opened_ts",
            "intended_entry_ts",
        ):
            bypass_out[ts_col] = observable_anchor
        bypass_out["entry_window_expires_ts"] = observable_anchor
        bypass_out["entry_delay_minutes"] = 0.0
        bypass_out["execution_ts_source"] = "tdp_wait_off_only_observable_anchor"
        bypass_out["wait_context_source"] = "tdp_wait_off_only_observable_anchor"
        bypass_out["entry_timing_valid"] = True
    out = out.loc[~bypass_mask].copy()
    if out.empty:
        return bypass_out

    out["setup_created_ts"] = pd.to_datetime(
        out.get("setup_created_ts", out.get("timestamp", pd.NaT)),
        utc=True,
        errors="coerce",
    )

    if "wait_confirm_ts" not in out.columns:
        out["wait_confirm_ts"] = pd.NaT

    out["wait_confirm_ts"] = pd.to_datetime(
        out["wait_confirm_ts"],
        utc=True,
        errors="coerce",
    )

    fallback_wait_confirm_ts = out["setup_created_ts"] + pd.Timedelta(minutes=15)
    out["wait_confirm_ts"] = out["wait_confirm_ts"].fillna(fallback_wait_confirm_ts)

    if "wait_context_source" not in out.columns:
        out["wait_context_source"] = "setup_created_ts_plus_1_candle"
    else:
        out["wait_context_source"] = (
            out["wait_context_source"]
            .fillna("setup_created_ts_plus_1_candle")
        )
        out.loc[
            out["wait_context_source"].astype(str).str.strip().eq(""),
            "wait_context_source",
        ] = "setup_created_ts_plus_1_candle"

    model_window_candles = (
        out.get("model", pd.Series("", index=out.index))
        .astype(str)
        .map({
            "RANGE_TOP_SHORT_V2": 4,
            "TDP_REENTRY": 16,
        })
        .fillna(1)
        .astype(int)
    )

    out["intended_entry_ts"] = out["wait_confirm_ts"]
    out["entry_window_expires_ts"] = (
        out["wait_confirm_ts"]
        + pd.to_timedelta(model_window_candles * 15, unit="m")
    )

    current_latest_ts = pd.to_datetime(latest_ts, utc=True, errors="coerce")
    out["entry_delay_minutes"] = (
        (current_latest_ts - out["wait_confirm_ts"]).dt.total_seconds() / 60.0
    )

    out["signal_ts"] = out["wait_confirm_ts"]
    out["timestamp"] = out["wait_confirm_ts"]
    out["trade_open_ts"] = out["wait_confirm_ts"]
    out["opened_ts"] = out["wait_confirm_ts"]
    out["execution_ts_source"] = "wait_confirm_ts"

    timing_valid_mask = current_latest_ts <= out["entry_window_expires_ts"]
    out["entry_timing_valid"] = timing_valid_mask

    stale_exec_df = out.loc[~timing_valid_mask].copy()
    if not stale_exec_df.empty:
        for _, stale_exec_row in stale_exec_df.iterrows():
            stale_flow_row = dict(flow_row)
            stale_flow_row["symbol"] = str(stale_exec_row.get("symbol", symbol)).upper()
            stale_flow_row["setup_id"] = stale_exec_row.get("setup_id", stale_flow_row.get("setup_id", ""))
            stale_flow_row["canonical_setup_key"] = stale_exec_row.get("canonical_setup_key", stale_flow_row.get("canonical_setup_key", ""))
            stale_flow_row["model"] = stale_exec_row.get("model", stale_flow_row.get("model", ""))
            stale_flow_row["side"] = str(stale_exec_row.get("side", stale_flow_row.get("side", ""))).upper()
            stale_flow_row["setup_created_ts"] = stale_exec_row.get("setup_created_ts", pd.NaT)
            stale_flow_row["wait_confirm_ts"] = stale_exec_row.get("wait_confirm_ts", pd.NaT)
            stale_flow_row["wait_context_source"] = stale_exec_row.get("wait_context_source", "setup_created_ts_plus_1_candle")
            stale_flow_row["visible_ts"] = stale_exec_row.get("visible_ts", pd.NaT)
            stale_flow_row["entry_anchor_ts"] = stale_exec_row.get("entry_anchor_ts", pd.NaT)
            stale_flow_row["first_seen_ts"] = stale_exec_row.get("visible_ts", pd.NaT)
            stale_flow_row["intended_entry_ts"] = stale_exec_row.get("intended_entry_ts", pd.NaT)
            stale_flow_row["entry_window_expires_ts"] = stale_exec_row.get("entry_window_expires_ts", pd.NaT)
            stale_flow_row["entry_delay_minutes"] = float(stale_exec_row.get("entry_delay_minutes", 0.0) or 0.0)
            stale_flow_row["entry_timing_valid"] = False
            stale_flow_row["execution_ts_source"] = "wait_confirm_ts"
            stale_flow_row["trigger_refresh_candidate"] = bool(stale_exec_row.get("trigger_refresh_candidate", False))
            stale_flow_row["trigger_refresh_reason"] = str(stale_exec_row.get("trigger_refresh_reason", ""))
            stale_flow_row["old_canonical_setup_key"] = str(stale_exec_row.get("old_canonical_setup_key", ""))
            stale_flow_row["original_setup_created_ts"] = stale_exec_row.get("original_setup_created_ts", pd.NaT)
            stale_flow_row["candidate_latest_ts"] = stale_exec_row.get("candidate_latest_ts", pd.NaT)
            stale_flow_row["range_retest_score"] = float(stale_exec_row.get("range_retest_score", 0.0) or 0.0)
            stale_flow_row["trigger_refresh_applied"] = bool(stale_exec_row.get("trigger_refresh_applied", False))
            stale_flow_row["after_wait_count"] = 0
            stale_flow_row["after_freshness_count"] = 0
            stale_flow_row["after_idempotency_count"] = 0
            stale_flow_row["after_stale_count"] = 0
            stale_flow_row["after_per_cycle_guard_count"] = 0
            stale_flow_row["emitted_count"] = 0
            stale_flow_row["notes"] = "entry_window_expired"
            stale_flow_row["death_stage"] = "stale"
            stale_flow_row["death_reason"] = "stale_execution_window"
            _append_flow_row(flow_log_csv, stale_flow_row)

    valid_out = out.loc[timing_valid_mask].copy()
    if not bypass_out.empty:
        return pd.concat([valid_out, bypass_out], ignore_index=True, sort=False)
    return valid_out

def _canonical_created_ts_for_diagnostics(canonical_setup_key: object):
    try:
        return pd.to_datetime(
            str(canonical_setup_key).split("|", 3)[3],
            utc=True,
            errors="coerce",
        )
    except Exception:
        return pd.NaT



def _range_trigger_current_candle_has_fresh_risk_anchor(
    *,
    row: pd.Series,
    candles_df: pd.DataFrame,
    candidate_latest_ts: pd.Timestamp,
) -> bool:
    """
    Return True only when the existing entry/sl/tp on the candidate row
    are plausibly based on the fresh trigger candle context.

    This is intentionally conservative. A coincidental old entry inside the
    fresh candle is not enough. The entry must be inside the candidate candle,
    close enough to that candle close, directionally valid, and either the row
    timestamps are already near candidate_latest_ts or the diagnostics explicitly
    identified candidate_latest_ts as the fresh RANGE retest trigger.
    """
    if candles_df is None or candles_df.empty or "timestamp" not in candles_df.columns:
        return False

    candidate_latest_ts = pd.to_datetime(candidate_latest_ts, utc=True, errors="coerce")
    if pd.isna(candidate_latest_ts):
        return False

    c = candles_df.copy()
    c["timestamp"] = pd.to_datetime(c["timestamp"], utc=True, errors="coerce")
    c = c.dropna(subset=["timestamp"]).sort_values("timestamp")
    hit = c.loc[c["timestamp"] == candidate_latest_ts]
    if hit.empty:
        return False

    candle = hit.iloc[-1]
    try:
        high = float(candle.get("high"))
        low = float(candle.get("low"))
        close = float(candle.get("close"))
        candle_range = high - low
        entry = float(pd.to_numeric(pd.Series([row.get("entry")]), errors="coerce").iloc[0])
        sl = float(pd.to_numeric(pd.Series([row.get("sl")]), errors="coerce").iloc[0])
        tp = float(pd.to_numeric(pd.Series([row.get("tp")]), errors="coerce").iloc[0])
    except Exception:
        return False

    if candle_range <= 0:
        return False

    if not (low <= entry <= high):
        return False

    entry_close_distance = abs(entry - close) / candle_range
    if entry_close_distance > 0.75:
        return False

    side = str(row.get("side", "")).upper()
    if side == "SHORT":
        direction_valid = bool(sl > entry and tp < entry)
    elif side == "LONG":
        direction_valid = bool(sl < entry and tp > entry)
    else:
        direction_valid = False

    if not direction_valid:
        return False

    near_candidate_ts = False
    for ts_col in ("setup_created_ts", "timestamp", "entry_anchor_ts", "visible_ts"):
        ts = pd.to_datetime(row.get(ts_col), utc=True, errors="coerce")
        if pd.notna(ts) and abs((ts - candidate_latest_ts).total_seconds()) <= 900:
            near_candidate_ts = True
            break

    try:
        score = float(row.get("range_retest_score", 0.0) or 0.0)
    except Exception:
        score = 0.0

    diagnostics_prove_fresh_trigger = (
        bool(row.get("trigger_refresh_candidate", False))
        and score >= 3.0
        and str(row.get("trigger_refresh_reason", ""))
        == "possible_fresh_range_retest_collapsed_to_old_canonical"
        and pd.notna(candidate_latest_ts)
    )

    return bool(near_candidate_ts or diagnostics_prove_fresh_trigger)


def _maybe_refresh_range_trigger_instance(
    *,
    wait_checked: pd.DataFrame,
    candles_df: pd.DataFrame,
    latest_ts: pd.Timestamp,
) -> pd.DataFrame:
    """
    RANGE_TOP_SHORT_V2 only.

    Convert a diagnostics-proven fresh range retest collision into a new
    setup instance before the execution-window guard runs. This does not
    touch TDP, lifecycle architecture, overlap gate, closer, or TP/SL logic.
    """
    if wait_checked is None or wait_checked.empty:
        return wait_checked

    out = wait_checked.copy()
    latest_ts = pd.to_datetime(latest_ts, utc=True, errors="coerce")
    if "trigger_refresh_applied" not in out.columns:
        out["trigger_refresh_applied"] = False

    for idx, row in out.iterrows():
        if str(row.get("model", "")) != "RANGE_TOP_SHORT_V2":
            continue
        if not bool(row.get("trigger_refresh_candidate", False)):
            continue

        try:
            score = float(row.get("range_retest_score", 0.0) or 0.0)
        except Exception:
            score = 0.0
        if score < 3.0:
            continue

        candidate_latest_ts = pd.to_datetime(
            row.get("candidate_latest_ts"),
            utc=True,
            errors="coerce",
        )
        if pd.isna(candidate_latest_ts):
            continue

        refreshed_wait_confirm_ts = candidate_latest_ts + pd.Timedelta(minutes=15)
        if pd.isna(latest_ts) or latest_ts < refreshed_wait_confirm_ts:
            out.at[idx, "trigger_refresh_reason"] = "refreshed_wait_not_confirmed_yet"
            out.at[idx, "trigger_refresh_applied"] = False
            continue

        old_wait_confirm_ts = pd.to_datetime(
            row.get("wait_confirm_ts"),
            utc=True,
            errors="coerce",
        )
        if pd.isna(old_wait_confirm_ts):
            old_wait_confirm_ts = (
                pd.to_datetime(row.get("setup_created_ts"), utc=True, errors="coerce")
                + pd.Timedelta(minutes=15)
            )
        if pd.isna(old_wait_confirm_ts):
            continue

        old_expiry_ts = old_wait_confirm_ts + pd.Timedelta(minutes=60)
        if candidate_latest_ts <= old_expiry_ts:
            continue

        if not _range_trigger_current_candle_has_fresh_risk_anchor(
            row=row,
            candles_df=candles_df,
            candidate_latest_ts=candidate_latest_ts,
        ):
            out.at[idx, "trigger_refresh_reason"] = "risk_anchor_not_fresh"
            out.at[idx, "trigger_refresh_applied"] = False
            continue

        side = str(row.get("side", "")).upper()
        symbol = str(row.get("symbol", "")).upper()
        if not symbol:
            setup_id = str(row.get("setup_id", ""))
            symbol = setup_id.split("|", 1)[0].upper() if "|" in setup_id else ""
        if not symbol or not side:
            continue

        out.at[idx, "old_canonical_setup_key"] = str(row.get("canonical_setup_key", ""))
        out.at[idx, "setup_created_ts"] = candidate_latest_ts
        out.at[idx, "timestamp"] = candidate_latest_ts
        out.at[idx, "canonical_setup_key"] = f"{symbol}|RANGE_TOP_SHORT_V2|{side}|{candidate_latest_ts}"
        out.at[idx, "setup_id"] = f"{symbol}|{candidate_latest_ts}|RANGE_TOP_SHORT_V2|{side}"
        out.at[idx, "wait_confirm_ts"] = refreshed_wait_confirm_ts
        out.at[idx, "wait_context_source"] = "setup_created_ts_plus_1_candle"
        out.at[idx, "trigger_refresh_applied"] = True
        out.at[idx, "trigger_refresh_reason"] = "fresh_range_retest_new_instance"

    return out

def _add_range_trigger_refresh_diagnostics(
    *,
    df: pd.DataFrame,
    candles_df: pd.DataFrame,
    latest_ts: pd.Timestamp,
) -> pd.DataFrame:
    """
    Diagnostics only.

    This must not mutate canonical_setup_key, setup_id, setup_created_ts,
    timestamp, visible_ts, entry_anchor_ts, or execution timing. It only
    annotates RANGE_TOP_SHORT_V2 rows that look like possible fresh retests
    collapsed onto an older canonical identity.
    """
    if df is None or df.empty:
        return df

    out = df.copy()
    latest_ts = pd.to_datetime(latest_ts, utc=True, errors="coerce")

    defaults = {
        "trigger_refresh_candidate": False,
        "trigger_refresh_reason": "",
        "old_canonical_setup_key": "",
        "original_setup_created_ts": "",
        "candidate_latest_ts": "",
        "range_retest_score": 0.0,
    }
    for col, default in defaults.items():
        if col not in out.columns:
            out[col] = default

    if candles_df is None or candles_df.empty or pd.isna(latest_ts):
        return out
    if "timestamp" not in candles_df.columns:
        return out

    c = candles_df.copy()
    c["timestamp"] = pd.to_datetime(c["timestamp"], utc=True, errors="coerce")
    c = c.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    if c.empty:
        return out

    hit = c.loc[c["timestamp"] == latest_ts]
    if hit.empty:
        hit = c.loc[c["timestamp"] <= latest_ts].tail(1)
    if hit.empty:
        return out

    latest_idx = int(hit.index[-1])
    latest_candle = hit.iloc[-1]
    prior = c.iloc[max(0, latest_idx - 8):latest_idx]
    if prior.empty:
        return out

    try:
        high = float(latest_candle.get("high"))
        low = float(latest_candle.get("low"))
        open_ = float(latest_candle.get("open"))
        close = float(latest_candle.get("close"))
        candle_range = high - low
        local_top = bool(high >= float(prior["high"].max()) * 0.9995)
        upper_wick = high - max(open_, close)
        rejection = bool(candle_range > 0 and (upper_wick >= candle_range * 0.15 or close < open_))
    except Exception:
        return out

    for idx, row in out.iterrows():
        if str(row.get("model", "")) != "RANGE_TOP_SHORT_V2":
            continue
        if str(row.get("side", "")).upper() != "SHORT":
            continue

        canonical_key = str(row.get("canonical_setup_key", "") or "")
        original_setup_ts = _canonical_created_ts_for_diagnostics(canonical_key)
        if pd.isna(original_setup_ts):
            original_setup_ts = pd.to_datetime(
                row.get("setup_created_ts", row.get("timestamp", pd.NaT)),
                utc=True,
                errors="coerce",
            )

        old_identity = pd.notna(original_setup_ts) and latest_ts > (original_setup_ts + pd.Timedelta(minutes=30))
        entry = pd.to_numeric(pd.Series([row.get("entry")]), errors="coerce").iloc[0]
        entry_in_candle = bool(pd.notna(entry) and low <= float(entry) <= high)

        score = 0.0
        if old_identity:
            score += 1.0
        if local_top:
            score += 1.0
        if rejection:
            score += 1.0
        if entry_in_candle:
            score += 1.0

        candidate = bool(old_identity and local_top and rejection)
        reason = ""
        if candidate:
            reason = "possible_fresh_range_retest_collapsed_to_old_canonical"
        elif old_identity:
            reason = "old_range_identity_no_fresh_retest_proven"

        out.at[idx, "trigger_refresh_candidate"] = candidate
        out.at[idx, "trigger_refresh_reason"] = reason
        out.at[idx, "old_canonical_setup_key"] = canonical_key
        out.at[idx, "original_setup_created_ts"] = (
            "" if pd.isna(original_setup_ts) else str(original_setup_ts)
        )
        out.at[idx, "candidate_latest_ts"] = (
            "" if pd.isna(latest_ts) else str(latest_ts)
        )
        out.at[idx, "range_retest_score"] = float(score)

    return out


def model_freshness_filter(df: pd.DataFrame, latest_ts: pd.Timestamp) -> pd.DataFrame:
    if df.empty:
        return df

    latest_ts = pd.to_datetime(latest_ts, utc=True, errors="coerce")
    if pd.isna(latest_ts):
        return df

    work = df.copy()

    # normalize all possible time sources
    for col in ("timestamp", "visible_ts", "pipeline_visible_ts", "entry_anchor_ts"):
        if col in work.columns:
            work[col] = pd.to_datetime(work[col], utc=True, errors="coerce")
        else:
            work[col] = pd.NaT

    rows = []

    for _, row in work.iterrows():
        model = row["model"]

        # LIVE freshness authority:
        # entry_anchor_ts -> visible_ts -> pipeline_visible_ts -> timestamp
        setup_ts = row.get("entry_anchor_ts", pd.NaT)
        if pd.isna(setup_ts):
            setup_ts = row.get("visible_ts", pd.NaT)
        if pd.isna(setup_ts):
            setup_ts = row.get("pipeline_visible_ts", pd.NaT)
        if pd.isna(setup_ts):
            setup_ts = row.get("timestamp", pd.NaT)

        if pd.isna(setup_ts):
            print(f"[FRESHNESS] model={model} age=NA keep=False reason=missing_anchor_ts")
            continue

        age_candles = int((latest_ts - setup_ts).total_seconds() / 900)

        keep = False
        reason = ""

        ALLOWED_EXTRA_BARS = 0

        if model == "RANGE_TOP_SHORT_V2":
            keep = age_candles <= 4
            reason = "range_fresh" if keep else "range_too_old"

        elif model == "TDP_REENTRY":
            keep = age_candles <= 16
            reason = "tdp_fresh" if keep else "tdp_too_old"

        else:
            keep = True
            reason = "no_filter"

        print(
            f"[FRESHNESS] model={model} "
            f"anchor_ts={setup_ts} latest_ts={latest_ts} "
            f"age={age_candles} keep={keep} reason={reason}"
        )

        if keep:
            rows.append(row)

    return pd.DataFrame(rows)

def run_symbol_once(
    *,
    cycle_ts: pd.Timestamp,
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
    terminal_lifecycle_registry_csv: Path,
    flow_log_csv: Path,
    debug: bool,
    debug_force_entries: bool,
    use_wait_confirmation: bool,
    tdp_wait_off_only: bool,
    candidate_pressure_csv: str,
    cluster_score_mode: Optional[str],
    cluster_max_per_group: Optional[int],
    cluster_rank_signal_score: bool,
    cluster_score_shadow_v2: bool = False,
    cluster_score_shadow_v2_csv: str = "",
    rr: float,
    sl_atr_buffer: float,
    require_impulse_before_tdp: bool,
    impulse_lookback: int,
    impulse_size_atr: float,
    tdp_dev_lookback: int,
    tts_retest_lookback: int,
    raw_candidate_diag_csv: str = "",
    pressure_window_summary_csv: str = "",
    sniper_candidate_diag_csv: str = "",
    sniper_candidate_summary_csv: str = "",
    tdp_stale_shadow_csv: str = "",
    structural_ts_shadow_csv: str = "",
    pre_visible_entry_exposure_csv: str = "",
    entry_model_pre_admission_csv: str = "",
    tdp_visible_assignment_trace_csv: str = "",
    tdp_identity_resurfacing_trace_csv: str = "",
    tdp_disappearance_trace_csv: str = "",
    tdp_true_birth_trace_csv: str = "",
    parity_filter_mode: str = "NONE",
    parity_filter_diagnostics_csv: str = "",
    opportunity_manager_snapshot_csv: str = "",
    authority_waterfall_csv: str = "",
    parity_range_width_pct_min: float = 0.0,
    parity_distance_to_entry_R_min: float = 0.5,
) -> int:
    candles_df = load_bybit_latest(category, symbol, interval, candles_n)
    fetch_status = LAST_BYBIT_FETCH_STATUS.get(str(symbol).upper(), FETCH_STATUS_EMPTY)
    if candles_df is None or candles_df.empty:
        print(f"[OBSERVE][{symbol}] no candles fetch_status={fetch_status}")
        flow_row = _make_flow_row(cycle_ts=cycle_ts, symbol=symbol, latest_ts=None)
        flow_row["notes"] = f"no_candles:{fetch_status}"
        flow_row["pipeline_zero_rows"] = True
        _append_flow_row(flow_log_csv, flow_row)
        return 0

    candles_df = candles_df.copy()
    candles_df["timestamp"] = pd.to_datetime(candles_df["timestamp"], utc=True, errors="coerce").dt.as_unit("us")
    candles_df = candles_df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    debug_candles_dir = Path("backtest/journal/exports_live/candles_debug")
    debug_candles_dir.mkdir(parents=True, exist_ok=True)

    candles_df.to_csv(
        debug_candles_dir / f"{symbol}_raw_live_candles.csv",
        index=False,
    )
    if candles_df.empty:
        print(f"[OBSERVE][{symbol}] candles empty after normalization fetch_status={fetch_status}")
        flow_row = _make_flow_row(cycle_ts=cycle_ts, symbol=symbol, latest_ts=None)
        flow_row["notes"] = f"candles_empty_after_normalization:{fetch_status}"
        flow_row["pipeline_zero_rows"] = True
        _append_flow_row(flow_log_csv, flow_row)
        return 0

    # paskutinė žvakė laikoma formuojama, todėl jos nenaudojam kaip latest closed bar
    if candles_df is None or len(candles_df) < 2:
        print(f"[OBSERVE][{symbol}] insufficient candles rows={0 if candles_df is None else len(candles_df)}")
        return 0

    latest_ts = pd.to_datetime(
        candles_df["timestamp"].iloc[-2],
        utc=True,
        errors="coerce",
    )
    candles_df = candles_df.iloc[:-1].copy().reset_index(drop=True)
    candles_df.to_csv(
        debug_candles_dir / f"{symbol}_closed_live_candles.csv",
        index=False,
    )
    _ensure_sniper_candidate_outputs(sniper_candidate_diag_csv, sniper_candidate_summary_csv)
    flow_row = _make_flow_row(cycle_ts=cycle_ts, symbol=symbol, latest_ts=latest_ts)
    raw_candidate_diag_rows: List[Dict[str, object]] = []

    def _append_cycle_candidate_diags(marked_rows: List[Dict[str, object]]) -> None:
        _append_raw_candidate_lifecycle_diag(raw_candidate_diag_csv, marked_rows)
        _append_tdp_stale_shadow_rows(tdp_stale_shadow_csv, marked_rows)
        _append_structural_ts_shadow_rows(structural_ts_shadow_csv, marked_rows)
        _append_sniper_candidate_outputs(
            sniper_candidate_diag_csv=sniper_candidate_diag_csv,
            sniper_candidate_summary_csv=sniper_candidate_summary_csv,
            marked_rows=marked_rows,
        )
    if debug:
        _time_alignment_audit(
            symbol=symbol,
            candles_df=candles_df,
            latest_ts=latest_ts,
            raw_df=None,
            visible_ts=None,
            pipeline_visible_ts=None,
        )
    state_path = state_dir / f"{symbol}_{interval}.txt"
    last_seen = _read_state(state_path)

    # Integration point: closer runs every cycle before position gate / signal generation.
    close_symbol_if_hit(
        symbol=symbol,
        candles_df=candles_df,
        position_state_csv=position_state_csv,
        out_csv=out_csv,
    )

    if _position_is_open(symbol, position_state_csv):
        _write_state(state_path, latest_ts)
        print(f"[OBSERVE][{symbol}] skipped open position latest_ts={latest_ts}")
        print(f"[POSITION_GATE] symbol={symbol} canonical= skipped_open_position=True reason=open_position_exists")
        flow_row["skipped_open_position"] = True
        flow_row["death_stage"] = "position_gate"
        flow_row["death_reason"] = "open_position_exists"
        flow_row["notes"] = "blocked_by_open_position"
        _append_flow_row(flow_log_csv, flow_row)
        return 0

    if last_seen is not None and latest_ts <= last_seen:
        print(f"[OBSERVE][{symbol}] no new candle latest_ts={latest_ts}")
        flow_row["notes"] = "no_new_candle"
        flow_row["pipeline_zero_rows"] = True
        flow_row["death_stage"] = "no_raw"
        flow_row["death_reason"] = "no_new_candle"
        _append_flow_row(flow_log_csv, flow_row)
        return 0

    window = candles_df.tail(int(window_n)).copy().reset_index(drop=True)
    window["timestamp"] = pd.to_datetime(window["timestamp"], utc=True, errors="coerce").dt.as_unit("us")
    portfolio_state = load_portfolio_exposure(portfolio_state_path)

    ctx: Dict[str, object] = {
        "latest_ts": latest_ts,
        "bybit_interval": int(interval) if str(interval).isdigit() else interval,
        "macro_bias": "NEUTRAL",
        "debug": bool(debug),
        "use_wait_confirmation": bool(use_wait_confirmation),
        "tdp_wait_off_only": bool(tdp_wait_off_only),
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
        "cycle_ts": pd.to_datetime(cycle_ts, utc=True, errors="coerce"),
        "pre_visible_entry_exposure_csv": str(pre_visible_entry_exposure_csv or ""),
        "entry_model_pre_admission_csv": str(entry_model_pre_admission_csv or ""),
        "tdp_visible_assignment_trace_csv": str(tdp_visible_assignment_trace_csv or ""),
        "tdp_identity_resurfacing_trace_csv": str(tdp_identity_resurfacing_trace_csv or ""),
        "tdp_disappearance_trace_csv": str(tdp_disappearance_trace_csv or ""),
        "tdp_true_birth_trace_csv": str(tdp_true_birth_trace_csv or ""),
        "DEBUG_FORCE_ENTRIES": bool(debug_force_entries),
    }

    if cluster_score_mode is not None:
        ctx["cluster_score_mode"] = cluster_score_mode
        if cluster_score_mode == "SIGNAL_SCORE":
            ctx["cluster_rank_signal_score"] = True

    if cluster_rank_signal_score:
        ctx["cluster_rank_signal_score"] = True

    if cluster_score_shadow_v2:
        ctx["cluster_score_shadow_v2"] = True
        ctx["cluster_score_shadow_v2_csv"] = str(cluster_score_shadow_v2_csv or "")

    if cluster_max_per_group is not None:
        ctx["cluster_max_per_group"] = int(cluster_max_per_group)

    df_e = run_pipeline_once(
        symbol=symbol,
        candles_df=window,
        ctx=ctx,
        portfolio_state=portfolio_state,
        debug=bool(debug),
    )

    if df_e is not None and not df_e.empty:
        df_e = _ensure_canonical_setup_key(df_e.copy(), symbol=symbol)

        df_e["timestamp"] = pd.to_datetime(df_e["timestamp"], utc=True, errors="coerce")

        setup_ids = _build_runtime_setup_ids(df_e, symbol)
        df_e["setup_id"] = setup_ids
        for _, identity_row in df_e.iterrows():
            print(
                f"[IDENTITY] canonical={identity_row.get('canonical_setup_key', '')} "
                f"setup_id={identity_row.get('setup_id', '')}"
            )

        df_e = _add_range_trigger_refresh_diagnostics(
            df=df_e,
            candles_df=candles_df,
            latest_ts=latest_ts,
        )

        stable_visible = []

        cache_updated = False
        for sid in setup_ids:
            if sid not in VISIBLE_TS_CACHE:
                VISIBLE_TS_CACHE[sid] = latest_ts
                cache_updated = True
            stable_visible.append(VISIBLE_TS_CACHE[sid])

        df_e["visible_ts"] = stable_visible
        df_e["pipeline_visible_ts"] = stable_visible

        if cache_updated:
            _save_visible_ts_cache(VISIBLE_TS_CACHE)

    visible_ts = pd.NaT
    pipeline_visible_ts = pd.NaT
    entry_anchor_ts = pd.NaT
    # ==============================
    # DISCOVERY GATE (NEW SIGNALS ONLY)
    # ==============================
    if df_e is not None and not df_e.empty:
        df_e = df_e.copy()

        try:
            df_e["timestamp"] = pd.to_datetime(df_e["timestamp"], utc=True, errors="coerce")
        except Exception:
            pass

        # --- ENTRY ANCHOR NORMALIZATION (MOVE BEFORE GATE) ---
        if "visible_ts" in df_e.columns:
            df_e["visible_ts"] = pd.to_datetime(df_e["visible_ts"], utc=True, errors="coerce")
        else:
            df_e["visible_ts"] = pd.NaT

        if "pipeline_visible_ts" in df_e.columns:
            df_e["pipeline_visible_ts"] = pd.to_datetime(df_e["pipeline_visible_ts"], utc=True, errors="coerce")
        else:
            df_e["pipeline_visible_ts"] = pd.NaT

        # fallback chain:
        # visible_ts -> pipeline_visible_ts -> timestamp
        anchor = df_e["visible_ts"].copy()

        m = anchor.isna()
        anchor.loc[m] = df_e.loc[m, "pipeline_visible_ts"]

        m = anchor.isna()
        anchor.loc[m] = df_e.loc[m, "timestamp"]

        df_e["entry_anchor_ts"] = pd.to_datetime(anchor, utc=True, errors="coerce")

        current_ts = pd.to_datetime(latest_ts, utc=True, errors="coerce")
        entry_window = pd.Timedelta(minutes=30)
        min_ts = current_ts - entry_window

        before = len(df_e)

        print(
            f"[DISCOVERY_GATE_DEBUG][{symbol}] "
            f"timestamp={df_e['timestamp'].astype(str).tolist()} "
            f"visible_ts={df_e['visible_ts'].astype(str).tolist()} "
            f"anchor={df_e['entry_anchor_ts'].astype(str).tolist()}"
        )

        df_e = df_e[
            (df_e["entry_anchor_ts"] > min_ts) &
            (df_e["entry_anchor_ts"] <= current_ts)
            ].copy()

        after = len(df_e)

        if after == 0:
            print(f"[POST_DROP][{symbol}] stage=DISCOVERY_GATE latest_ts={latest_ts}")

        if debug:
            print(
                f"[DISCOVERY_GATE][{symbol}] "
                f"window=({min_ts}, {current_ts}] before={before} after={after}"
            )

    if df_e is not None and not df_e.empty:
        df_e = df_e.copy()
        df_e["timestamp"] = pd.to_datetime(df_e["timestamp"], utc=True, errors="coerce")

        if "visible_ts" not in df_e.columns:
            df_e["visible_ts"] = pd.NaT
        if "pipeline_visible_ts" not in df_e.columns:
            df_e["pipeline_visible_ts"] = pd.NaT

        df_e["visible_ts"] = pd.to_datetime(df_e["visible_ts"], utc=True, errors="coerce")
        df_e["pipeline_visible_ts"] = pd.to_datetime(df_e["pipeline_visible_ts"], utc=True, errors="coerce")

        first_row = df_e.iloc[0]
        setup_created_ts = pd.to_datetime(first_row.get("timestamp"), utc=True, errors="coerce")
        visible_ts = pd.to_datetime(first_row.get("visible_ts"), utc=True, errors="coerce")
        pipeline_visible_ts = pd.to_datetime(first_row.get("pipeline_visible_ts"), utc=True, errors="coerce")

        entry_anchor_ts = visible_ts
        if pd.isna(entry_anchor_ts):
            entry_anchor_ts = pipeline_visible_ts
        if pd.isna(entry_anchor_ts):
            entry_anchor_ts = setup_created_ts

        df_e = _ensure_canonical_setup_key(df_e, symbol=symbol)
        setup_ids = _build_runtime_setup_ids(df_e, symbol)
        df_e["setup_id"] = setup_ids
        flow_row["setup_id"] = str(setup_ids.iloc[0])
        flow_row["canonical_setup_key"] = str(first_row.get("canonical_setup_key", ""))

        print(
            f"[VISIBLE_DEBUG] "
            f"timestamp={first_row['timestamp']} "
            f"visible_ts={first_row['visible_ts']} "
            f"pipeline_visible_ts={first_row['pipeline_visible_ts']}"
        )

        flow_row["model"] = str(first_row.get("model", ""))
        flow_row["side"] = str(first_row.get("side", "")).upper()
        flow_row["setup_created_ts"] = setup_created_ts
        flow_row["visible_ts"] = visible_ts
        flow_row["entry_anchor_ts"] = entry_anchor_ts
        flow_row["first_seen_ts"] = visible_ts
        flow_row["age_candles_at_first_seen"] = 0
        flow_row["age_minutes_at_first_seen"] = 0.0
        flow_row["trigger_refresh_candidate"] = bool(first_row.get("trigger_refresh_candidate", False))
        flow_row["trigger_refresh_reason"] = str(first_row.get("trigger_refresh_reason", ""))
        flow_row["old_canonical_setup_key"] = str(first_row.get("old_canonical_setup_key", ""))
        flow_row["original_setup_created_ts"] = pd.to_datetime(first_row.get("original_setup_created_ts"), utc=True, errors="coerce")
        flow_row["candidate_latest_ts"] = pd.to_datetime(first_row.get("candidate_latest_ts"), utc=True, errors="coerce")
        flow_row["range_retest_score"] = float(first_row.get("range_retest_score", 0.0) or 0.0)
        flow_row["trigger_refresh_applied"] = bool(first_row.get("trigger_refresh_applied", False))

    flow_row["phase"] = _derive_phase(df_e, ctx)
    flow_row["raw_entries_count"] = int(0 if df_e is None else len(df_e))
    flow_row["model_summary_raw"] = _series_summary(df_e, "model")
    flow_row["sub_label_summary_raw"] = _series_summary(df_e, "ctx_sub_label")
    raw_candidate_diag_rows = _make_raw_candidate_diag_rows(
        candidates_df=df_e,
        cycle_ts=cycle_ts,
        symbol=symbol,
        latest_ts=latest_ts,
        candles_df=candles_df,
    )
    if debug:
        _time_alignment_audit(
            symbol=symbol,
            candles_df=window,
            latest_ts=latest_ts,
            raw_df=df_e,
            visible_ts=visible_ts,
            pipeline_visible_ts=pipeline_visible_ts,
        )
    if df_e is None or df_e.empty:
        _write_state(state_path, latest_ts)
        print(f"[OBSERVE][{symbol}] pipeline rows=0 latest_ts={latest_ts}")
        flow_row["pipeline_zero_rows"] = True
        flow_row["notes"] = "pipeline_rows_0"
        flow_row["death_stage"] = "no_raw"
        flow_row["death_reason"] = "pipeline_rows_0"
        _append_flow_row(flow_log_csv, flow_row)
        return 0

    df_wait = df_e.copy()

    # preserve original structure origin
    df_wait["setup_created_ts"] = pd.to_datetime(
        df_wait["timestamp"], utc=True, errors="coerce"
    )

    # Wait confirmation is an EARLY STRUCTURAL VALIDATION.
    # It must evaluate setup_created_ts + one candle, not the late live
    # visible/entry-anchor/cycle timestamp.
    df_wait["cycle_ts"] = pd.to_datetime(cycle_ts, utc=True, errors="coerce")
    df_wait["pre_wait_live_execution_ts"] = pd.to_datetime(
        df_wait["entry_anchor_ts"], utc=True, errors="coerce"
    )
    df_wait["timestamp"] = pd.to_datetime(
        df_wait["setup_created_ts"], utc=True, errors="coerce"
    )

    entries = df_wait.to_dict("records")

    if bool(use_wait_confirmation):
        before_wait = len(entries)
        if bool(tdp_wait_off_only):
            range_entries = [
                e for e in entries
                if str(e.get("model", "") or "") != "TDP_REENTRY"
            ]
            tdp_entries = [
                dict(e) for e in entries
                if str(e.get("model", "") or "") == "TDP_REENTRY"
            ]

            range_after_wait = apply_wait_confirmation(range_entries, window) if range_entries else []
            tdp_shadow_after_wait = apply_wait_confirmation(tdp_entries, window) if tdp_entries else []
            tdp_shadow_keys = {
                str(e.get("canonical_setup_key") or e.get("setup_id") or "")
                for e in tdp_shadow_after_wait
            }

            for e in tdp_entries:
                k = str(e.get("canonical_setup_key") or e.get("setup_id") or "")
                e["wait_bypassed"] = True
                e["wait_bypass_scope"] = "TDP_REENTRY_ONLY"
                e["would_have_failed_wait"] = bool(k and k not in tdp_shadow_keys)
                e["wait_shadow_decision"] = "pass" if (k and k in tdp_shadow_keys) else "reject"
                if not e.get("wait_confirm_ts"):
                    e["wait_confirm_ts"] = e.get("timestamp", pd.NaT)
                if not e.get("wait_context_source"):
                    e["wait_context_source"] = "tdp_wait_off_only_bypass"

            entries_after_wait = list(range_after_wait) + list(tdp_entries)
        else:
            entries_after_wait = apply_wait_confirmation(entries, window)
        after_wait = len(entries_after_wait)

        if bool(tdp_wait_off_only):
            try:
                bypass_count = sum(1 for e in entries_after_wait if bool(e.get("wait_bypassed", False)))
                flow_row["notes"] = (str(flow_row.get("notes", "") or "") + f";tdp_wait_off_only_bypass={bypass_count}").strip(";")
            except Exception:
                pass

        if entries_after_wait:
            wait_checked = pd.DataFrame(entries_after_wait)
            if "wait_confirm_ts" not in wait_checked.columns:
                wait_checked["wait_confirm_ts"] = wait_checked.get("timestamp", pd.NaT)
            if "wait_context_source" not in wait_checked.columns:
                wait_checked["wait_context_source"] = "setup_created_ts_plus_1_candle"

            wait_checked = _maybe_refresh_range_trigger_instance(
                wait_checked=wait_checked,
                candles_df=candles_df,
                latest_ts=latest_ts,
            )

            wait_checked = _apply_execution_window_guard(
                wait_checked=wait_checked,
                latest_ts=latest_ts,
                symbol=symbol,
                flow_log_csv=flow_log_csv,
                flow_row=flow_row,
            )

            if wait_checked.empty:
                entries_after_wait = []
                after_wait = 0
                _write_state(state_path, latest_ts)
                print(f"[POST_DROP][{symbol}] stage=STALE_EXECUTION_WINDOW latest_ts={latest_ts}")
                flow_row["after_wait_count"] = 0
                flow_row["after_freshness_count"] = 0
                flow_row["after_idempotency_count"] = 0
                flow_row["after_stale_count"] = 0
                flow_row["after_per_cycle_guard_count"] = 0
                flow_row["emitted_count"] = 0
                flow_row["notes"] = "entry_window_expired"
                flow_row["death_stage"] = "stale"
                flow_row["death_reason"] = "stale_execution_window"
                flow_row["execution_ts_source"] = "wait_confirm_ts"
                flow_row["entry_timing_valid"] = False
                _append_flow_row(flow_log_csv, flow_row)
                _append_cycle_candidate_diags(
                    _mark_raw_candidate_diag_rows(
                        raw_candidate_diag_rows,
                        wait_df=pd.DataFrame(),
                        death_stage="stale",
                        death_reason="stale_execution_window",
                    ),
                )
                _rebuild_pressure_window_summary(raw_candidate_diag_csv, pressure_window_summary_csv, candles_df, symbol)
                return 0

            after_wait = len(wait_checked)

            diag = wait_checked.iloc[0]
            flow_row["wait_confirm_ts"] = pd.to_datetime(diag.get("wait_confirm_ts"), utc=True, errors="coerce")
            flow_row["wait_context_source"] = str(diag.get("wait_context_source", "setup_created_ts_plus_1_candle"))
            flow_row["intended_entry_ts"] = pd.to_datetime(diag.get("intended_entry_ts"), utc=True, errors="coerce")
            flow_row["entry_window_expires_ts"] = pd.to_datetime(diag.get("entry_window_expires_ts"), utc=True, errors="coerce")
            flow_row["entry_delay_minutes"] = float(diag.get("entry_delay_minutes", 0.0) or 0.0)
            flow_row["entry_timing_valid"] = bool(diag.get("entry_timing_valid", False))
            flow_row["execution_ts_source"] = str(diag.get("execution_ts_source", "wait_confirm_ts"))
            print(
                f"[WAIT_CONTEXT_ACCEPTED][{symbol}] "
                f"canonical={diag.get('canonical_setup_key', '')} "
                f"setup_created_ts={diag.get('setup_created_ts', '')} "
                f"wait_confirm_ts={diag.get('wait_confirm_ts', '')} "
                f"visible_ts={diag.get('visible_ts', '')} "
                f"pipeline_visible_ts={diag.get('pipeline_visible_ts', '')} "
                f"signal_ts={diag.get('signal_ts', '')} "
                f"cycle_ts={cycle_ts} "
                f"wait_context_source={diag.get('wait_context_source', 'setup_created_ts_plus_1_candle')}"
            )
            entries_after_wait = wait_checked.to_dict("records")

        if after_wait < before_wait:
            print(f"[DROP][{symbol}] stage=WAIT_CONFIRM before={before_wait} after={after_wait}")

        # strict gate semantics: no fallback to pre-wait entries
        entries = entries_after_wait

        if after_wait == 0:
            _write_state(state_path, latest_ts)
            print(f"[POST_DROP][{symbol}] stage=WAIT_CONFIRMATION latest_ts={latest_ts}")
            print(f"[OBSERVE][{symbol}] post_pipeline rows=0 after wait confirmation latest_ts={latest_ts}")
            flow_row["after_wait_count"] = 0
            flow_row["model_summary_after_wait"] = ""
            flow_row["after_freshness_count"] = 0
            flow_row["after_idempotency_count"] = 0
            flow_row["after_stale_count"] = 0
            flow_row["after_per_cycle_guard_count"] = 0
            flow_row["notes"] = "wait_confirmation_rejected_non_terminal"
            flow_row["wait_context_source"] = "setup_created_ts_plus_1_candle"
            flow_row["lifecycle_state"] = LIFECYCLE_WAIT_REJECTED
            flow_row["death_stage"] = "wait_confirmation"
            flow_row["death_reason"] = "apply_wait_confirmation"
            print(f"[TERMINAL][SKIP_NON_TERMINAL] stage=wait_confirmation reason=not_terminal")
            _append_flow_row(flow_log_csv, flow_row)
            _append_cycle_candidate_diags(
                _mark_raw_candidate_diag_rows(
                    raw_candidate_diag_rows,
                    wait_df=pd.DataFrame(entries),
                    death_stage="wait_confirmation",
                    death_reason="apply_wait_confirmation",
                ),
            )
            _rebuild_pressure_window_summary(raw_candidate_diag_csv, pressure_window_summary_csv, candles_df, symbol)
            return 0


    wait_df = pd.DataFrame(entries)
    flow_row["after_wait_count"] = int(len(wait_df))
    flow_row["model_summary_after_wait"] = _series_summary(wait_df, "model")
    diag_wait_df = wait_df.copy()

    out_df = pd.DataFrame(entries)
    out_df = model_freshness_filter(out_df, latest_ts)

    flow_row["after_freshness_count"] = int(len(out_df))

    if out_df.empty:
        _write_state(state_path, latest_ts)
        print(f"[POST_DROP][{symbol}] stage=FRESHNESS latest_ts={latest_ts}")
        print(f"[OBSERVE][{symbol}] post_pipeline rows=0 after freshness latest_ts={latest_ts}")
        flow_row["notes"] = "died_in_freshness"
        flow_row["death_stage"] = "freshness"
        flow_row["death_reason"] = "model_freshness_filter"
        _append_flow_row(flow_log_csv, flow_row)
        _append_cycle_candidate_diags(
            _mark_raw_candidate_diag_rows(
                raw_candidate_diag_rows,
                wait_df=diag_wait_df,
                death_stage="freshness",
                death_reason="model_freshness_filter",
            ),
        )
        _rebuild_pressure_window_summary(raw_candidate_diag_csv, pressure_window_summary_csv, candles_df, symbol)
        return 0


    out_df = _ensure_canonical_setup_key(out_df, symbol=symbol)
    out_df["setup_id"] = _build_runtime_setup_ids(out_df, symbol)

    fired_ids = _load_canonical_keys_from_csv(fired_setups_csv)
    terminal_ids = _load_terminal_canonical_keys(terminal_lifecycle_registry_csv)
    blocked_ids = fired_ids | terminal_ids
    blocked_mask = out_df["canonical_setup_key"].astype(str).isin(blocked_ids)
    for blocked_key in out_df.loc[blocked_mask, "canonical_setup_key"].astype(str).tolist():
        print(f"[DEDUPE] blocked canonical_key={blocked_key} reason=terminal_or_fired")
        print(f"[TERMINAL_BLOCK] canonical={blocked_key}")
    out_df = out_df.loc[~blocked_mask].copy()
    flow_row["after_idempotency_count"] = int(len(out_df))
    flow_row["model_summary_after_idempotency"] = _series_summary(out_df, "model")

    if out_df.empty:
        _write_state(state_path, latest_ts)
        print(f"[POST_DROP][{symbol}] stage=IDEMPOTENCY latest_ts={latest_ts}")
        print(f"[OBSERVE][{symbol}] post_pipeline rows=0 after idempotency latest_ts={latest_ts}")
        flow_row["after_stale_count"] = flow_row["after_wait_count"]
        flow_row["model_summary_after_stale"] = flow_row["model_summary_after_wait"]
        flow_row["notes"] = "died_in_idempotency"
        flow_row["death_stage"] = "idempotency"
        flow_row["death_reason"] = "already_fired"
        _append_flow_row(flow_log_csv, flow_row)
        _append_cycle_candidate_diags(
            _mark_raw_candidate_diag_rows(
                raw_candidate_diag_rows,
                wait_df=diag_wait_df,
                idempotency_df=out_df,
                death_stage="idempotency",
                death_reason="already_fired",
            ),
        )
        _rebuild_pressure_window_summary(raw_candidate_diag_csv, pressure_window_summary_csv, candles_df, symbol)
        return 0

    out_df["observed_ts"] = latest_ts

    before_stale = len(out_df)
    before_stale_df = out_df.copy()
    out_df = filter_live_emit_candidates(out_df, candles_df, latest_ts)
    after_stale = len(out_df)
    flow_row["after_stale_count"] = int(len(out_df))

    if after_stale < before_stale:
        print(f"[DROP][{symbol}] stage=STALE before={before_stale} after={after_stale}")
    flow_row["model_summary_after_stale"] = _series_summary(out_df, "model")

    if out_df.empty:
        _write_state(state_path, latest_ts)
        print(f"[POST_DROP][{symbol}] stage=STALE latest_ts={latest_ts}")
        print(f"[OBSERVE][{symbol}] post_pipeline rows=0 after stale-hit filter latest_ts={latest_ts}")
        for _, stale_row in before_stale_df.iterrows():
            _append_terminal_lifecycle_row(
                terminal_lifecycle_registry_csv,
                row=stale_row,
                terminal_stage=LIFECYCLE_STALE,
                terminal_ts=latest_ts,
                reason="filter_live_emit_candidates",
            )
        flow_row["notes"] = "died_in_stale"
        flow_row["lifecycle_state"] = LIFECYCLE_STALE
        flow_row["death_stage"] = "stale"
        flow_row["death_reason"] = "filter_live_emit_candidates"
        _append_flow_row(flow_log_csv, flow_row)
        _append_cycle_candidate_diags(
            _mark_raw_candidate_diag_rows(
                raw_candidate_diag_rows,
                wait_df=diag_wait_df,
                idempotency_df=before_stale_df,
                stale_df=out_df,
                death_stage="stale",
                death_reason="filter_live_emit_candidates",
            ),
        )
        _rebuild_pressure_window_summary(raw_candidate_diag_csv, pressure_window_summary_csv, candles_df, symbol)
        return 0

    out_df, parity_filter_stats = _apply_parity_research_filter(
        out_df=out_df,
        candles_df=candles_df,
        latest_ts=latest_ts,
        cycle_ts=cycle_ts,
        filter_mode=parity_filter_mode,
        filter_diag_csv=parity_filter_diagnostics_csv,
        range_width_min=float(parity_range_width_pct_min),
        distance_min=float(parity_distance_to_entry_R_min),
    )
    flow_row.update(parity_filter_stats)
    if out_df.empty and str(parity_filter_stats.get("parity_filter_mode", "NONE")) != "NONE":
        _write_state(state_path, latest_ts)
        flow_row["notes"] = "died_in_parity_filter"
        flow_row["death_stage"] = "parity_filter"
        flow_row["death_reason"] = str(parity_filter_stats.get("parity_filter_mode", ""))
        flow_row["emitted_count"] = 0
        _append_flow_row(flow_log_csv, flow_row)
        marked = _mark_raw_candidate_diag_rows(
            raw_candidate_diag_rows,
            wait_df=diag_wait_df,
            idempotency_df=before_stale_df,
            stale_df=before_stale_df,
            death_stage="parity_filter",
            death_reason=str(parity_filter_stats.get("parity_filter_mode", "")),
        )
        for r in marked:
            r["parity_filter_applied"] = True
            r["parity_filter_passed"] = False
            r["parity_filter_name"] = str(parity_filter_stats.get("parity_filter_mode", ""))
            r["parity_filter_reason"] = "rejected_or_not_at_hook"
            r["parity_filter_stage"] = "pre_final_selection_pre_emission"
            r["filter_threshold_range_width_pct"] = float(parity_range_width_pct_min)
            r["filter_threshold_distance_to_entry_R"] = float(parity_distance_to_entry_R_min)
        _append_cycle_candidate_diags(marked)
        _rebuild_pressure_window_summary(raw_candidate_diag_csv, pressure_window_summary_csv, candles_df, symbol)
        return 0

    opportunity_executable_df = out_df.copy()
    opportunity_executable_df["cycle_ts"] = pd.to_datetime(cycle_ts, utc=True, errors="coerce")
    opportunity_executable_df["latest_ts"] = pd.to_datetime(latest_ts, utc=True, errors="coerce")
    opportunity_executable_df["raw_candidate_count"] = int(flow_row.get("raw_entries_count", len(opportunity_executable_df)) or 0)
    if "cluster_group_count" not in opportunity_executable_df.columns:
        if "pressure_group_count" in opportunity_executable_df.columns:
            opportunity_executable_df["cluster_group_count"] = opportunity_executable_df["pressure_group_count"]
        else:
            opportunity_executable_df["cluster_group_count"] = ""
    if "cluster_score_mode" not in opportunity_executable_df.columns:
        opportunity_executable_df["cluster_score_mode"] = str(cluster_score_mode or "")

    out_df = select_newest_live_candidate(out_df)
    flow_row["after_per_cycle_guard_count"] = int(len(out_df))
    flow_row["model_summary_after_per_cycle_guard"] = _series_summary(out_df, "model")
    _append_opportunity_manager_snapshot(
        executable_candidates_df=opportunity_executable_df,
        selected_df=out_df,
        output_csv_path=opportunity_manager_snapshot_csv,
    )
    if out_df.empty:
        _write_state(state_path, latest_ts)
        print(f"[POST_DROP][{symbol}] stage=FINAL_SELECTION latest_ts={latest_ts}")
        flow_row["notes"] = "died_in_final_selection"
        flow_row["death_stage"] = "final_selection"
        flow_row["death_reason"] = "select_newest_live_candidate"
        _append_flow_row(flow_log_csv, flow_row)
        _append_cycle_candidate_diags(
            _mark_raw_candidate_diag_rows(
                raw_candidate_diag_rows,
                wait_df=diag_wait_df,
                idempotency_df=before_stale_df,
                stale_df=out_df,
                position_df=out_df,
                death_stage="final_selection",
                death_reason="select_newest_live_candidate",
            ),
        )
        _rebuild_pressure_window_summary(raw_candidate_diag_csv, pressure_window_summary_csv, candles_df, symbol)
        return 0

    out_df, position_gate_skipped = _filter_position_overlap_candidates(
        out_df=out_df,
        symbol=symbol,
        position_state_csv=position_state_csv,
        flow_log_csv=flow_log_csv,
        flow_row=flow_row,
    )
    flow_row["after_per_cycle_guard_count"] = int(len(out_df))
    flow_row["model_summary_after_per_cycle_guard"] = _series_summary(out_df, "model")
    if out_df.empty:
        _write_state(state_path, latest_ts)
        print(f"[POST_DROP][{symbol}] stage=POSITION_GATE latest_ts={latest_ts}")
        flow_row["skipped_open_position"] = True
        flow_row["notes"] = "blocked_by_open_position_at_intended_open_ts"
        flow_row["death_stage"] = "position_gate"
        flow_row["death_reason"] = "open_position_exists_at_intended_open_ts"
        flow_row["emitted_count"] = 0
        _append_flow_row(flow_log_csv, flow_row)
        _append_cycle_candidate_diags(
            _mark_raw_candidate_diag_rows(
                raw_candidate_diag_rows,
                wait_df=diag_wait_df,
                idempotency_df=before_stale_df,
                stale_df=out_df,
                position_df=out_df,
                death_stage="position_gate",
                death_reason="open_position_exists_at_intended_open_ts",
            ),
        )
        _rebuild_pressure_window_summary(raw_candidate_diag_csv, pressure_window_summary_csv, candles_df, symbol)
        return 0

    if position_gate_skipped > 0:
        flow_row["notes"] = f"position_gate_skipped={int(position_gate_skipped)}"

    # Keep flow_log aligned with the exact final emitted row.
    if out_df is not None and not out_df.empty:
        final_row = out_df.iloc[0]
        if "setup_id" in out_df.columns:
            flow_row["setup_id"] = str(final_row.get("setup_id", ""))
        flow_row["canonical_setup_key"] = str(final_row.get("canonical_setup_key", ""))
        flow_row["model"] = str(final_row.get("model", ""))
        flow_row["side"] = str(final_row.get("side", "")).upper()
        flow_row["setup_created_ts"] = pd.to_datetime(final_row.get("setup_created_ts", final_row.get("timestamp")), utc=True, errors="coerce")
        flow_row["visible_ts"] = pd.to_datetime(final_row.get("visible_ts"), utc=True, errors="coerce")
        flow_row["entry_anchor_ts"] = pd.to_datetime(final_row.get("entry_anchor_ts"), utc=True, errors="coerce")
        flow_row["first_seen_ts"] = flow_row["visible_ts"]
        flow_row["trigger_refresh_candidate"] = bool(final_row.get("trigger_refresh_candidate", False))
        flow_row["trigger_refresh_reason"] = str(final_row.get("trigger_refresh_reason", ""))
        flow_row["old_canonical_setup_key"] = str(final_row.get("old_canonical_setup_key", ""))
        flow_row["original_setup_created_ts"] = pd.to_datetime(final_row.get("original_setup_created_ts"), utc=True, errors="coerce")
        flow_row["candidate_latest_ts"] = pd.to_datetime(final_row.get("candidate_latest_ts"), utc=True, errors="coerce")
        flow_row["range_retest_score"] = float(final_row.get("range_retest_score", 0.0) or 0.0)
        flow_row["trigger_refresh_applied"] = bool(final_row.get("trigger_refresh_applied", False))

    written = _emit_observation_rows(
        out_csv=out_csv,
        symbol=symbol,
        latest_ts=latest_ts,
        df_e=out_df,
    )
    flow_row["emitted_count"] = int(written)
    if written > 0:
        out_df["lifecycle_state"] = LIFECYCLE_FIRED
    _append_fired_setup_ids(fired_setups_csv, out_df)

    if written > 0:
        first = out_df.iloc[0]
        first_setup_id = str(first.get("setup_id", ""))
        first_canonical_key = str(first.get("canonical_setup_key", ""))
        _append_terminal_lifecycle_row(
            terminal_lifecycle_registry_csv,
            row=first,
            terminal_stage=LIFECYCLE_FIRED,
            terminal_ts=latest_ts,
            reason="emitted_fired",
        )

        entry_ts = pd.to_datetime(out_df["timestamp"].iloc[0], utc=True, errors="coerce")

        if pd.isna(entry_ts):
            print(f"[OPEN_TS_ERROR][{symbol}] missing timestamp setup_id={first_setup_id}")
            return written

        print(
            f"[EXEC_CLOCK] canonical={first_canonical_key} "
            f"setup_created={first.get('setup_created_ts', pd.NaT)} "
            f"signal={entry_ts} trade_open={entry_ts} source=timestamp"
        )

        _mark_position_open(
            symbol,
            first_setup_id,
            entry_ts,
            position_state_csv,
            canonical_setup_key=first_canonical_key,
            setup_created_ts=first.get("setup_created_ts", pd.NaT),
            signal_ts=first.get("signal_ts", pd.NaT),
            wait_confirm_ts=first.get("wait_confirm_ts", pd.NaT),
            wait_context_source=first.get("wait_context_source", ""),
        )
        _append_terminal_lifecycle_row(
            terminal_lifecycle_registry_csv,
            row=first,
            terminal_stage=LIFECYCLE_OPENED,
            terminal_ts=entry_ts,
            reason="position_opened",
        )

    _write_state(state_path, latest_ts)

    if written > 0:
        flow_row["notes"] = "emitted_successfully"
        flow_row["lifecycle_state"] = LIFECYCLE_OPENED
        flow_row["death_stage"] = "emitted"
        flow_row["death_reason"] = "passed_all_filters"
    else:
        flow_row["notes"] = "post_guard_no_emit"
        flow_row["death_stage"] = "stale"
        flow_row["death_reason"] = "post_guard_no_emit"

    _append_flow_row(flow_log_csv, flow_row)
    _append_cycle_candidate_diags(
        _mark_raw_candidate_diag_rows(
            raw_candidate_diag_rows,
            wait_df=diag_wait_df,
            idempotency_df=before_stale_df,
            stale_df=out_df,
            position_df=out_df,
            emitted_df=out_df if written > 0 else pd.DataFrame(),
            death_stage="emitted" if written > 0 else "post_guard",
            death_reason="passed_all_filters" if written > 0 else "post_guard_no_emit",
        ),
    )
    _rebuild_pressure_window_summary(raw_candidate_diag_csv, pressure_window_summary_csv, candles_df, symbol)

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
    ap.add_argument("--terminal_lifecycle_registry_csv", default="backtest/journal/terminal_lifecycle_registry.csv")
    ap.add_argument("--flow_log_csv", default="backtest/journal/exports_live/flow_log.csv")
    ap.add_argument("--poll_seconds", type=int, default=30, help="Loop sleep when not using --once")
    ap.add_argument("--max_global_no_candles_cycles", type=int, default=20)
    ap.add_argument("--max_global_network_error_cycles", type=int, default=10)
    ap.add_argument("--once", action="store_true", help="Run one cycle only")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--debug_force_entries", action="store_true")
    ap.add_argument("--use_wait_confirmation", action="store_true")
    ap.add_argument("--tdp_wait_off_only", action="store_true")
    ap.add_argument("--candidate_pressure_csv", default="backtest/journal/exports_live/candidate_pressure.csv")
    ap.add_argument("--raw_candidate_diag_csv", default="backtest/journal/exports_live/raw_candidate_lifecycle_diag.csv")
    ap.add_argument("--pressure_window_summary_csv", default="backtest/journal/exports_live/pressure_window_summary.csv")
    ap.add_argument("--sniper_candidate_diag_csv", default="backtest/journal/exports_live/sniper_candidate_diag.csv")
    ap.add_argument("--sniper_candidate_summary_csv", default="backtest/journal/exports_live/sniper_candidate_summary.csv")
    ap.add_argument("--tdp_stale_shadow_csv", default=None)
    ap.add_argument("--structural_ts_shadow_csv", default=None)
    ap.add_argument("--pre_visible_entry_exposure_csv", default=None)
    ap.add_argument("--entry_model_pre_admission_csv", default=None)
    ap.add_argument("--tdp_visible_assignment_trace_csv", default=None)
    ap.add_argument("--tdp_identity_resurfacing_trace_csv", default=None)
    ap.add_argument("--tdp_disappearance_trace_csv", default=None)
    ap.add_argument("--tdp_true_birth_trace_csv", default=None)
    ap.add_argument("--parity_filter_mode", choices=sorted(PARITY_FILTER_MODES), default="NONE")
    ap.add_argument("--parity_filter_diagnostics_csv", default=None)
    ap.add_argument("--opportunity_manager_snapshot_csv", default=None)
    ap.add_argument("--authority_waterfall_csv", default=None)
    ap.add_argument("--parity_range_width_pct_min", type=float, default=0.0)
    ap.add_argument("--parity_distance_to_entry_R_min", type=float, default=0.5)

    ap.add_argument("--cluster_score_mode", choices=("LEGACY", "SIGNAL_SCORE", "SHADOW_SCORE_V2", "SHADOW_SCORE_V3_TDP_ONLY", "SHADOW_SCORE_V4A_RANGE_WIDE", "SHADOW_SCORE_V4B_RANGE_REALISTIC", "SHADOW_SCORE_V4C_RANGE_HIGH_RR_PENALTY"), default=None)
    ap.add_argument("--cluster_max_per_group", type=int, choices=(1, 2, 3), default=None)
    ap.add_argument("--cluster_rank_signal_score", action="store_true")
    ap.add_argument("--cluster_score_shadow_v2", action="store_true")
    ap.add_argument("--cluster_score_shadow_v2_csv", default=None)
    ap.add_argument(
        "--live_rotation_plan_csv",
        default="backtest/journal/live_rotation_plan.csv",
        help="Observe-only live rotation planning CSV path",
    )

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
    terminal_lifecycle_registry_csv = Path(args.terminal_lifecycle_registry_csv)
    flow_log_csv = Path(args.flow_log_csv)
    raw_candidate_diag_csv = Path(args.raw_candidate_diag_csv)
    pressure_window_summary_csv = Path(args.pressure_window_summary_csv)
    sniper_candidate_diag_csv = Path(args.sniper_candidate_diag_csv)
    sniper_candidate_summary_csv = Path(args.sniper_candidate_summary_csv)
    tdp_stale_shadow_csv = "" if args.tdp_stale_shadow_csv is None else str(args.tdp_stale_shadow_csv)
    structural_ts_shadow_csv = "" if args.structural_ts_shadow_csv is None else str(args.structural_ts_shadow_csv)
    pre_visible_entry_exposure_csv = "" if args.pre_visible_entry_exposure_csv is None else str(args.pre_visible_entry_exposure_csv)
    entry_model_pre_admission_csv = "" if args.entry_model_pre_admission_csv is None else str(args.entry_model_pre_admission_csv)
    tdp_visible_assignment_trace_csv = "" if args.tdp_visible_assignment_trace_csv is None else str(args.tdp_visible_assignment_trace_csv)
    tdp_identity_resurfacing_trace_csv = "" if args.tdp_identity_resurfacing_trace_csv is None else str(args.tdp_identity_resurfacing_trace_csv)
    tdp_disappearance_trace_csv = "" if args.tdp_disappearance_trace_csv is None else str(args.tdp_disappearance_trace_csv)
    tdp_true_birth_trace_csv = "" if args.tdp_true_birth_trace_csv is None else str(args.tdp_true_birth_trace_csv)
    parity_filter_diagnostics_csv = "" if args.parity_filter_diagnostics_csv is None else str(args.parity_filter_diagnostics_csv)
    opportunity_manager_snapshot_csv = "" if args.opportunity_manager_snapshot_csv is None else str(args.opportunity_manager_snapshot_csv)
    authority_waterfall_csv = "" if args.authority_waterfall_csv is None else str(args.authority_waterfall_csv)

    _ensure_output_csv(out_csv)
    _ensure_parent(flow_log_csv)

    live_rotation_manager = _initialize_live_rotation_manager()
    live_rotation_controller = _initialize_live_rotation_controller(live_rotation_manager)
    live_rotation_integration_boundary = _initialize_live_rotation_integration_boundary()

    total_written = 0
    consecutive_global_no_candles_cycles = 0
    consecutive_global_network_error_cycles = 0
    last_successful_data_ts: Optional[pd.Timestamp] = None

    cluster_score_shadow_v2_csv = "" if args.cluster_score_shadow_v2_csv is None else str(args.cluster_score_shadow_v2_csv)
    live_rotation_observed_csv_paths = [
        path
        for path in [
            out_csv,
            position_state_csv,
            fired_setups_csv,
            terminal_lifecycle_registry_csv,
            flow_log_csv,
            Path(str(args.candidate_pressure_csv)),
            raw_candidate_diag_csv,
            pressure_window_summary_csv,
            sniper_candidate_diag_csv,
            sniper_candidate_summary_csv,
            tdp_stale_shadow_csv,
            structural_ts_shadow_csv,
            pre_visible_entry_exposure_csv,
            entry_model_pre_admission_csv,
            tdp_visible_assignment_trace_csv,
            tdp_identity_resurfacing_trace_csv,
            tdp_disappearance_trace_csv,
            tdp_true_birth_trace_csv,
            parity_filter_diagnostics_csv,
            opportunity_manager_snapshot_csv,
            authority_waterfall_csv,
            cluster_score_shadow_v2_csv,
        ]
        if str(path).strip()
    ]
    while True:
        cycle_written = 0
        cycle_ts = pd.Timestamp.now("UTC")
        cycle_symbol_had_data = []
        cycle_symbol_network_error = []

        for i, symbol in enumerate(symbols):
            try:
                written_for_symbol = run_symbol_once(
                    cycle_ts=cycle_ts,
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
                    terminal_lifecycle_registry_csv=terminal_lifecycle_registry_csv,
                    flow_log_csv=flow_log_csv,
                    debug=bool(args.debug),
                    debug_force_entries=bool(args.debug_force_entries),
                    use_wait_confirmation=bool(args.use_wait_confirmation),
                    tdp_wait_off_only=bool(args.tdp_wait_off_only),
                    candidate_pressure_csv=str(args.candidate_pressure_csv),
                    cluster_score_mode=args.cluster_score_mode,
                    cluster_max_per_group=args.cluster_max_per_group,
                    cluster_rank_signal_score=bool(args.cluster_rank_signal_score),
                    cluster_score_shadow_v2=bool(args.cluster_score_shadow_v2),
                    cluster_score_shadow_v2_csv=cluster_score_shadow_v2_csv,
                    rr=float(args.rr),
                    sl_atr_buffer=float(args.sl_atr_buffer),
                    require_impulse_before_tdp=bool(args.require_impulse_before_tdp),
                    impulse_lookback=int(args.impulse_lookback),
                    impulse_size_atr=float(args.impulse_size_atr),
                    tdp_dev_lookback=int(args.tdp_dev_lookback),
                    tts_retest_lookback=int(args.tts_retest_lookback),
                    raw_candidate_diag_csv=str(raw_candidate_diag_csv),
                    pressure_window_summary_csv=str(pressure_window_summary_csv),
                    sniper_candidate_diag_csv=str(sniper_candidate_diag_csv),
                    sniper_candidate_summary_csv=str(sniper_candidate_summary_csv),
                    tdp_stale_shadow_csv=tdp_stale_shadow_csv,
                    structural_ts_shadow_csv=structural_ts_shadow_csv,
                    pre_visible_entry_exposure_csv=pre_visible_entry_exposure_csv,
                    entry_model_pre_admission_csv=entry_model_pre_admission_csv,
                    tdp_visible_assignment_trace_csv=tdp_visible_assignment_trace_csv,
                    tdp_identity_resurfacing_trace_csv=tdp_identity_resurfacing_trace_csv,
                    tdp_disappearance_trace_csv=tdp_disappearance_trace_csv,
                    tdp_true_birth_trace_csv=tdp_true_birth_trace_csv,
                    parity_filter_mode=str(args.parity_filter_mode or "NONE"),
                    parity_filter_diagnostics_csv=parity_filter_diagnostics_csv,
                    opportunity_manager_snapshot_csv=opportunity_manager_snapshot_csv,
                    authority_waterfall_csv=authority_waterfall_csv,
                    parity_range_width_pct_min=float(args.parity_range_width_pct_min),
                    parity_distance_to_entry_R_min=float(args.parity_distance_to_entry_R_min),
                )
                cycle_written += written_for_symbol

                fetch_status = LAST_BYBIT_FETCH_STATUS.get(str(symbol).upper(), FETCH_STATUS_EMPTY)
                had_data = fetch_status == FETCH_STATUS_OK
                is_network_error = _is_transport_fetch_status(fetch_status)

                cycle_symbol_had_data.append(had_data)
                cycle_symbol_network_error.append(is_network_error)

                if had_data:
                    last_successful_data_ts = cycle_ts

            except Exception as e:
                print(f"[OBSERVE][{symbol}] error={type(e).__name__}: {e}")
                err_row = _make_flow_row(cycle_ts=cycle_ts, symbol=symbol, latest_ts=None)
                err_row["notes"] = f"symbol_exception:{type(e).__name__}:{e}"
                err_row["pipeline_zero_rows"] = True
                err_row["death_stage"] = "no_raw"
                err_row["death_reason"] = f"symbol_exception:{type(e).__name__}"
                _append_flow_row(flow_log_csv, err_row)
                cycle_symbol_had_data.append(False)
                cycle_symbol_network_error.append(True)

            if i < len(symbols) - 1:
                time.sleep(0.3)

        total_written += cycle_written

        if cycle_symbol_had_data and all(not x for x in cycle_symbol_had_data):
            consecutive_global_no_candles_cycles += 1
            print(f"[HEALTH] no_candles_all_symbols consecutive={consecutive_global_no_candles_cycles}")
        else:
            consecutive_global_no_candles_cycles = 0

        if cycle_symbol_network_error and all(cycle_symbol_network_error):
            consecutive_global_network_error_cycles += 1
            print(f"[HEALTH] network_error_all_symbols consecutive={consecutive_global_network_error_cycles}")
        else:
            consecutive_global_network_error_cycles = 0

        print(
            json.dumps(
                {
                    "event": "cycle_done",
                    "cycle_ts": str(cycle_ts),
                    "written_this_cycle": int(cycle_written),
                    "written_total": int(total_written),
                    "symbols": symbols,
                    "consecutive_global_no_candles_cycles": int(consecutive_global_no_candles_cycles),
                    "consecutive_global_network_error_cycles": int(consecutive_global_network_error_cycles),
                    "last_successful_data_ts": str(last_successful_data_ts) if last_successful_data_ts is not None else None,
                },
                ensure_ascii=False,
            )
        )
        _observe_live_rotation_decisions(
            live_rotation_controller,
            cycle_ts=cycle_ts,
            csv_paths=live_rotation_observed_csv_paths,
            debug=bool(args.debug),
        )
        _write_live_rotation_plan(
            live_rotation_controller,
            live_rotation_manager,
            cycle_ts=cycle_ts,
            csv_paths=live_rotation_observed_csv_paths,
            plan_csv=Path(args.live_rotation_plan_csv),
        )

        if consecutive_global_no_candles_cycles >= int(args.max_global_no_candles_cycles):
            print(
                f"[HEALTH_EXIT] reason=global_no_candles threshold_hit "
                f"consecutive={consecutive_global_no_candles_cycles}"
            )
            raise SystemExit(2)

        if consecutive_global_network_error_cycles >= int(args.max_global_network_error_cycles):
            print(
                f"[HEALTH_EXIT] reason=global_network_error threshold_hit "
                f"consecutive={consecutive_global_network_error_cycles}"
            )
            raise SystemExit(3)

        if args.once:
            break

        time.sleep(max(1, int(args.poll_seconds)))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
