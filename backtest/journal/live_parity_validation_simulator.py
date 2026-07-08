from __future__ import annotations

"""
Exact live-observation-shell parity validation simulator.

Purpose:
    Replay historical candles by reusing the existing live_observation_shell
    behavior as directly as possible, without modifying live files.

Design:
    - dynamically load the real live shell module from a user-supplied .py path
    - verify the expected shell entrypoint signature before replay
    - monkeypatch ONLY the shell candle loader used by run_symbol_once(...)
    - call shell.run_symbol_once(...) directly for every replay cycle
    - let the live shell own the stage order and logic:
          1) close_symbol_if_hit
          2) position gate
          3) run_pipeline_once
          4) discovery gate
          5) wait confirmation
          6) freshness
          7) idempotency
          8) emit guard / stale
          9) final selection
         10) emit
         11) position open

Critical constraints preserved:
    - no entry_model changes
    - no pipeline changes
    - no SL/TP/RR changes
    - no duplicated discovery/freshness/idempotency/position-gate logic
    - no live behavior changes

Primary outputs (live-structure parity):
    - live_observation_entries.csv
    - fired_setups.csv
    - position_state.csv
    - flow_log.csv

Analytical-only output:
    - live_parity_lifecycle.csv
      Includes execution_ts and trade lifecycle fields for comparison only.
      execution_ts is NEVER used in simulator logic.
"""

import argparse
import importlib.util
import inspect
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


PRIMARY_OUTPUT_SPECS: Dict[str, List[str]] = {
    "live_observation_entries.csv": [],  # initialized by live shell
    "fired_setups.csv": ["canonical_setup_key", "setup_id", "symbol","timestamp", "setup_created_ts", "wait_confirm_ts","wait_context_source", "model", "side", "signal_ts", "observed_ts", "trade_open_ts", "opened_ts", "execution_ts_source", "lifecycle_state"],
    "position_state.csv": ["symbol", "canonical_setup_key", "setup_id", "setup_created_ts", "wait_confirm_ts", "wait_context_source", "signal_ts", "opened_ts", "status", "closed_ts", "close_reason", "lifecycle_state"],
    "terminal_lifecycle_registry.csv": ["canonical_setup_key", "setup_id", "symbol", "model", "side", "setup_created_ts", "terminal_stage", "lifecycle_state", "terminal_ts", "reason"],
    "flow_log.csv": [],  # initialized by live shell
    "live_parity_lifecycle.csv": [
        "canonical_setup_key",
        "setup_id",
        "symbol",
        "model",
        "side",
        "timestamp",
        "visible_ts",
        "pipeline_visible_ts",
        "signal_ts",
        "confirm_ts",
        "execution_ts",  # ANALYTICAL ONLY
        "execution_ts_source",  # ANALYTICAL ONLY
        "observed_ts",
        "emit_ts",
        "trade_open_ts",
        "opened_ts",
        "position_gate_reason",
        "skipped_open_position",
        "lifecycle_state",
        "trade_close_ts",
        "entry",
        "sl",
        "tp",
        "rr",
        "outcome",
        "exit_reason",
        "notes",
        "lifecycle_quality",
    ],
    "live_parity_summary.csv": ["metric", "value"],
    "raw_candidate_lifecycle_diag.csv": [],
    "pressure_window_summary.csv": [],
    "sniper_candidate_diag.csv": [],
    "sniper_candidate_summary.csv": ["symbol", "model", "death_stage", "death_reason", "candidate_expansion_state", "candidate_count", "emitted_count"],
    "tdp_stale_shadow_oos.csv": [],
    "structural_ts_shadow_oos.csv": [],
    "pre_visible_entry_exposure.csv": [],
    "entry_model_pre_admission.csv": [],
    "tdp_visible_assignment_trace.csv": [],
    "tdp_identity_resurfacing_trace.csv": [],
    "tdp_disappearance_trace.csv": [],
    "tdp_true_birth_trace.csv": [],
    "parity_filter_diagnostics.csv": [],
    "authority_waterfall.csv": [
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
    ],
}

FLOW_PARITY_COLUMNS = [
    "raw_entries_count",
    "after_wait_count",
    "after_freshness_count",
    "after_idempotency_count",
    "after_stale_count",
    "after_per_cycle_guard_count",
    "emitted_count",
    "death_stage",
]


class ParityValidationError(RuntimeError):
    pass


def _parse_dt(s: str) -> pd.Timestamp:
    ts = pd.to_datetime(s, utc=True, errors="coerce")
    if pd.isna(ts):
        raise ValueError(f"Bad datetime: {s}")
    return ts


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _load_shell_module(live_shell_py: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location("live_observation_shell_runtime", str(live_shell_py))
    if spec is None or spec.loader is None:
        raise ParityValidationError(f"Could not load live shell module from {live_shell_py}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _load_symbol_candles(candles_dir: Path, symbol: str) -> pd.DataFrame:
    candidates = [
        candles_dir / f"{symbol}.csv",
        candles_dir / f"{symbol}_15m.csv",
        candles_dir / f"{symbol}_15.csv",
    ]
    p = None
    for c in candidates:
        if c.exists():
            p = c
            break
    if p is None:
        raise FileNotFoundError(f"No candles CSV for {symbol} in {candles_dir}")

    df = pd.read_csv(p)
    if "timestamp" not in df.columns:
        for alt in ("time", "date", "ts"):
            if alt in df.columns:
                df = df.rename(columns={alt: "timestamp"})
                break

    required = {"timestamp", "open", "high", "low", "close"}
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{p} missing columns: {missing}")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

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


def _read_csv_safe(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()



def _build_authority_waterfall(
    *,
    out_dir: Path,
    authority_waterfall_csv: Path,
) -> None:
    """Build base authority-waterfall identity telemetry post-replay only.

    Patch 3 creates one row per canonical_setup_key found in existing replay
    telemetry. It performs conservative lifecycle labeling only and is never
    consumed by replay or trading logic.
    """
    columns = PRIMARY_OUTPUT_SPECS["authority_waterfall.csv"]
    input_files = {
        "entry_model_pre_admission": out_dir / "entry_model_pre_admission.csv",
        "raw_candidate_lifecycle_diag": out_dir / "raw_candidate_lifecycle_diag.csv",
        "flow_log": out_dir / "flow_log.csv",
        "opportunity_manager_snapshot": out_dir / "opportunity_manager_snapshot.csv",
        "position_state": out_dir / "position_state.csv",
        "fired_setups": out_dir / "fired_setups.csv",
        "terminal_lifecycle_registry": out_dir / "terminal_lifecycle_registry.csv",
    }
    inputs = {name: _read_csv_safe(path) for name, path in input_files.items()}

    def _blank(value: object) -> bool:
        try:
            if pd.isna(value):
                return True
        except Exception:
            pass
        return str(value or "").strip() == "" or str(value).strip().lower() == "nan"

    def _first_present(row: pd.Series, names: List[str]):
        for name in names:
            if name in row.index and not _blank(row.get(name, "")):
                return row.get(name, "")
        return ""

    def _truthy(value: object) -> bool:
        return str(value or "").strip().lower() in {"true", "1", "yes", "y"}

    def _set_if_blank(target: Dict[str, object], field: str, value: object) -> None:
        if field not in target or _blank(target.get(field, "")):
            if not _blank(value):
                target[field] = value

    def _final_r_from_row(row: pd.Series):
        for name in (
            "final_R_if_known",
            "R",
            "final_R",
            "realized_R",
            "realized_r",
            "pnl_R",
            "pnl_r",
            "outcome_R",
            "outcome_r",
            "result_R",
            "result_r",
            "total_R",
            "total_r",
        ):
            if name in row.index and not _blank(row.get(name, "")):
                return row.get(name, "")
        return ""

    def _flow_terminal_evidence(row: pd.Series):
        death_stage = str(row.get("death_stage", "") or "").strip().lower()
        death_reason = str(row.get("death_reason", "") or "").strip().lower()

        if death_stage == "emitted":
            return (5, "opened", "opened")
        if death_stage == "position_gate" or death_reason in {
            "open_position_exists",
            "open_position_exists_at_intended_open_ts",
        }:
            return (4, "execution", "position_gate")
        if death_stage == "idempotency" or death_reason == "already_fired":
            return (3, "executable", "idempotency")
        if death_stage == "stale" or death_reason == "stale_execution_window":
            return (2, "freshness", "stale")
        if death_stage == "wait_confirmation" or death_reason == "apply_wait_confirmation":
            return (1, "wait", "wait_failed")
        return None

    def _terminal_registry_evidence(row: pd.Series):
        fields = [
            str(row.get("terminal_stage", "") or ""),
            str(row.get("terminal_status", "") or ""),
            str(row.get("lifecycle_state", "") or ""),
            str(row.get("reason", "") or ""),
        ]
        text = " ".join(fields).strip().lower()
        if not text:
            return None

        if "opened" in text:
            return (5, "opened", "opened")
        if "position_gate" in text or "open_position_exists" in text:
            return (4, "execution", "position_gate")
        if "idempotency" in text or "already_fired" in text or "duplicate" in text:
            return (3, "executable", "idempotency")
        if "stale" in text or "freshness" in text or "expired" in text:
            return (2, "freshness", "stale")
        if "wait_failed" in text or "apply_wait_confirmation" in text or ("wait" in text and "failed" in text):
            return (1, "wait", "wait_failed")
        return None

    def _raw_lifecycle_evidence(row: pd.Series):
        death_stage = str(row.get("death_stage", "") or "").strip().lower()
        death_reason = str(row.get("death_reason", "") or "").strip().lower()

        if death_reason in {"stale", "stale_execution_window"}:
            return (2, "freshness", "stale")
        if death_reason in {"wait_failed", "apply_wait_confirmation"}:
            return (1, "wait", "wait_failed")
        if death_reason == "already_fired":
            return (3, "executable", "idempotency")
        if death_reason in {"open_position_exists", "open_position_exists_at_intended_open_ts"}:
            return (4, "execution", "position_gate")

        # Conservative exact-stage fallbacks only. Do not map generic filtered,
        # no_raw, pipeline_rows_0, or blank reasons in this patch.
        if death_stage == "stale":
            return (2, "freshness", "stale")
        if death_stage == "wait_confirmation" and death_reason:
            return (1, "wait", "wait_failed")
        if death_stage == "idempotency" and death_reason:
            return (3, "executable", "idempotency")
        if death_stage == "position_gate" and death_reason:
            return (4, "execution", "position_gate")
        return None

    def _apply_authority_evidence(row: Dict[str, object], terminal_stage: str, death_reason: str) -> None:
        row["terminal_stage"] = terminal_stage
        row["death_reason"] = death_reason

        if death_reason == "wait_failed":
            row["terminal_stage"] = "wait"
        elif death_reason == "stale":
            row["terminal_stage"] = "freshness"
            row["reached_freshness"] = "False"
        elif death_reason == "idempotency":
            row["terminal_stage"] = "executable"
            row["reached_executable"] = "True"
        elif death_reason == "position_gate":
            row["terminal_stage"] = "execution"
            row["reached_executable"] = "True"
            row["reached_opportunity_manager"] = "True"
        elif death_reason == "opened":
            row["terminal_stage"] = "opened"
            row["reached_execution"] = "True"
            row["reached_opened"] = "True"
            row["opened"] = "True"

    def _flow_sort_ts(row: pd.Series):
        for name in ("cycle_ts", "latest_ts", "timestamp"):
            if name in row.index:
                ts = pd.to_datetime(row.get(name, pd.NaT), utc=True, errors="coerce")
                if pd.notna(ts):
                    return ts
        return pd.Timestamp.max.tz_localize("UTC")

    identities: Dict[str, Dict[str, object]] = {}
    source_membership: Dict[str, set] = {}
    for source_name, df in inputs.items():
        if df.empty or "canonical_setup_key" not in df.columns:
            continue
        for _, src_row in df.iterrows():
            key = str(src_row.get("canonical_setup_key", "") or "").strip()
            if not key:
                continue
            row = identities.setdefault(key, {col: "" for col in columns})
            source_membership.setdefault(key, set()).add(source_name)
            row["canonical_setup_key"] = key

            for field in (
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
                "planned_rr",
                "risk_pct",
                "reward_pct",
                "distance_to_entry_R",
                "distance_to_entry_pct",
                "range_width_pct",
                "setup_age_bars",
                "candidate_persistence_bars",
                "close_reason",
                "selected_for_execution",
                "rejected_reason",
                "execution_rank",
            ):
                if field in src_row.index:
                    _set_if_blank(row, field, src_row.get(field, ""))

            _set_if_blank(row, "candidate_ts", _first_present(src_row, ["candidate_ts", "timestamp"]))
            _set_if_blank(row, "setup_created_ts", _first_present(src_row, ["setup_created_ts", "candidate_ts", "timestamp"]))
            _set_if_blank(row, "visible_ts", _first_present(src_row, ["visible_ts", "pipeline_visible_ts"]))
            _set_if_blank(row, "wait_confirm_ts", _first_present(src_row, ["wait_confirm_ts"]))
            _set_if_blank(row, "signal_ts", _first_present(src_row, ["signal_ts", "trade_open_ts", "opened_ts", "timestamp"]))

            if source_name == "position_state":
                _set_if_blank(row, "opened_ts", _first_present(src_row, ["opened_ts", "trade_open_ts"]))
                _set_if_blank(row, "closed_ts", _first_present(src_row, ["closed_ts", "trade_close_ts"]))
                _set_if_blank(row, "close_reason", _first_present(src_row, ["close_reason", "outcome", "exit_reason"]))
                _set_if_blank(row, "final_R_if_known", _final_r_from_row(src_row))
            elif source_name == "opportunity_manager_snapshot":
                _set_if_blank(row, "final_R_if_known", _first_present(src_row, ["final_R_if_known"]))

    flow_terminal_by_key: Dict[str, tuple] = {}
    flow_log = inputs.get("flow_log", pd.DataFrame())
    if not flow_log.empty and "canonical_setup_key" in flow_log.columns:
        flow_log = flow_log.copy()
        flow_log["_sort_ts"] = flow_log.apply(_flow_sort_ts, axis=1)
        flow_log = flow_log.sort_values("_sort_ts", na_position="last")
        for _, flow_row in flow_log.iterrows():
            key = str(flow_row.get("canonical_setup_key", "") or "").strip()
            if not key:
                continue
            evidence = _flow_terminal_evidence(flow_row)
            if evidence is None:
                continue
            prev = flow_terminal_by_key.get(key)
            if prev is None or int(evidence[0]) > int(prev[0]):
                flow_terminal_by_key[key] = evidence

    terminal_registry_by_key: Dict[str, tuple] = {}
    terminal_registry = inputs.get("terminal_lifecycle_registry", pd.DataFrame())
    if not terminal_registry.empty and "canonical_setup_key" in terminal_registry.columns:
        for _, terminal_row in terminal_registry.iterrows():
            key = str(terminal_row.get("canonical_setup_key", "") or "").strip()
            if not key:
                continue
            evidence = _terminal_registry_evidence(terminal_row)
            if evidence is None:
                continue
            prev = terminal_registry_by_key.get(key)
            if prev is None or int(evidence[0]) > int(prev[0]):
                terminal_registry_by_key[key] = evidence

    raw_lifecycle_by_key: Dict[str, tuple] = {}
    raw_lifecycle = inputs.get("raw_candidate_lifecycle_diag", pd.DataFrame())
    if not raw_lifecycle.empty and "canonical_setup_key" in raw_lifecycle.columns:
        for _, raw_row in raw_lifecycle.iterrows():
            key = str(raw_row.get("canonical_setup_key", "") or "").strip()
            if not key:
                continue
            evidence = _raw_lifecycle_evidence(raw_row)
            if evidence is None:
                continue
            prev = raw_lifecycle_by_key.get(key)
            if prev is None or int(evidence[0]) > int(prev[0]):
                raw_lifecycle_by_key[key] = evidence

    out_rows: List[Dict[str, object]] = []
    for key in sorted(identities):
        row = identities[key]
        sources = source_membership.get(key, set())
        reached_opportunity = "opportunity_manager_snapshot" in sources
        reached_execution = _truthy(row.get("selected_for_execution", ""))
        reached_opened = "position_state" in sources

        row["reached_created"] = "True"
        row["reached_visible"] = "True" if not _blank(row.get("visible_ts", "")) else "False"
        row["reached_wait"] = "True" if not _blank(row.get("wait_confirm_ts", "")) else "False"
        row["reached_freshness"] = ""
        row["reached_executable"] = "True" if reached_opportunity else "False"
        row["reached_opportunity_manager"] = "True" if reached_opportunity else "False"
        row["reached_execution"] = "True" if reached_execution else "False"
        row["reached_opened"] = "True" if reached_opened else "False"
        row["opened"] = "True" if reached_opened else "False"

        if reached_opened:
            row["terminal_stage"] = "opened"
            row["death_reason"] = "opened"
        elif reached_execution:
            row["terminal_stage"] = "opportunity_manager"
            row["death_reason"] = "selected_not_opened"
        elif reached_opportunity:
            row["terminal_stage"] = "opportunity_manager"
            row["death_reason"] = row.get("rejected_reason", "") if not _blank(row.get("rejected_reason", "")) else "not_selected"
        else:
            row["terminal_stage"] = "created"
            row["death_reason"] = "unresolved_pre_opportunity"

        if not reached_opened and key in flow_terminal_by_key:
            _, flow_terminal_stage, flow_death_reason = flow_terminal_by_key[key]
            _apply_authority_evidence(row, flow_terminal_stage, flow_death_reason)
            if flow_death_reason == "wait_failed" and _blank(row.get("wait_confirm_ts", "")):
                row["reached_wait"] = "False"
            if flow_death_reason == "position_gate":
                row["reached_execution"] = "False"
        elif not reached_opened and key in terminal_registry_by_key:
            _, terminal_stage, terminal_reason = terminal_registry_by_key[key]
            _apply_authority_evidence(row, terminal_stage, terminal_reason)
        elif not reached_opened and key in raw_lifecycle_by_key:
            _, raw_stage, raw_reason = raw_lifecycle_by_key[key]
            _apply_authority_evidence(row, raw_stage, raw_reason)

        out_rows.append({col: row.get(col, "") for col in columns})

    _ensure_parent(authority_waterfall_csv)
    pd.DataFrame(out_rows, columns=columns).to_csv(authority_waterfall_csv, index=False)

def _ensure_output_file(path: Path, expected_columns: List[str]) -> Path:
    """
    Safety rules:
      - create parent dirs
      - if file doesn't exist, initialize only if schema is known
      - if file exists and schema is known, validate exact ordered columns
      - on mismatch, fail clearly (no silent append corruption)
    """
    _ensure_parent(path)
    if not path.exists() or path.stat().st_size == 0:
        if expected_columns:
            pd.DataFrame(columns=expected_columns).to_csv(path, index=False)
        return path

    if not expected_columns:
        return path

    try:
        existing = pd.read_csv(path, nrows=0)
    except Exception as e:
        raise ParityValidationError(f"Could not read header from existing output file {path}: {e}")

    existing_cols = list(existing.columns)
    if existing_cols != expected_columns:
        # Backward compatibility for pre-parity-metadata CSVs: if the file only
        # lacks newly introduced columns, migrate it in place. Unknown extra
        # columns still fail fast to avoid silent append corruption.
        extras = [c for c in existing_cols if c not in expected_columns]
        if extras:
            raise ParityValidationError(
                "Output schema mismatch for "
                f"{path}. Existing columns={existing_cols}, expected={expected_columns}. "
                "Refusing to append incompatible rows."
            )
        full = pd.read_csv(path)
        for col in expected_columns:
            if col not in full.columns:
                full[col] = ""
        full = full[expected_columns]
        full.to_csv(path, index=False)
    return path


def _validate_position_closer_compatibility(shell: ModuleType, position_state_csv: Path) -> Tuple[bool, str]:
    expected_cols = PRIMARY_OUTPUT_SPECS["position_state.csv"]
    observed = []
    try:
        if hasattr(shell, "_load_open_positions"):
            sig = inspect.signature(shell._load_open_positions)
            observed.append(f"shell._load_open_positions{sig}")
        if hasattr(shell, "_position_is_open"):
            sig = inspect.signature(shell._position_is_open)
            observed.append(f"shell._position_is_open{sig}")
    except Exception:
        pass

    if position_state_csv.exists() and position_state_csv.stat().st_size > 0:
        try:
            cols = list(pd.read_csv(position_state_csv, nrows=0).columns)
            if cols != expected_cols:
                return False, f"position_state.csv columns={cols}, expected={expected_cols}"
        except Exception as e:
            return False, f"position_state.csv unreadable: {e}"

    # Proven compatibility target from provided live shell + closer code.
    return True, f"expected columns={expected_cols}; observed helpers={'; '.join(observed) or 'not introspected'}"


def _verify_run_symbol_once_signature(shell: ModuleType) -> List[str]:
    if not hasattr(shell, "run_symbol_once"):
        raise ParityValidationError("LIVE SHELL SIGNATURE VERIFIED: NO -- module missing run_symbol_once")

    sig = inspect.signature(shell.run_symbol_once)
    params = sig.parameters
    required = [
        "cycle_ts",
        "symbol",
        "category",
        "interval",
        "candles_n",
        "window_n",
        "portfolio_state_path",
        "out_csv",
        "state_dir",
        "position_state_csv",
        "fired_setups_csv",
        "terminal_lifecycle_registry_csv",
        "flow_log_csv",
        "debug",
        "debug_force_entries",
        "use_wait_confirmation",
        "tdp_wait_off_only",
        "candidate_pressure_csv",
        "cluster_score_mode",
        "cluster_max_per_group",
        "cluster_rank_signal_score",
        "cluster_score_shadow_v2",
        "cluster_score_shadow_v2_csv",
        "rr",
        "sl_atr_buffer",
        "require_impulse_before_tdp",
        "impulse_lookback",
        "impulse_size_atr",
        "tdp_dev_lookback",
        "tts_retest_lookback",
        "raw_candidate_diag_csv",
        "pressure_window_summary_csv",
        "sniper_candidate_diag_csv",
        "sniper_candidate_summary_csv",
        "tdp_stale_shadow_csv",
        "structural_ts_shadow_csv",
        "pre_visible_entry_exposure_csv",
        "entry_model_pre_admission_csv",
        "tdp_visible_assignment_trace_csv",
        "tdp_identity_resurfacing_trace_csv",
        "tdp_disappearance_trace_csv",
        "tdp_true_birth_trace_csv",
        "parity_filter_mode",
        "parity_filter_diagnostics_csv",
        "opportunity_manager_snapshot_csv",
        "authority_waterfall_csv",
        "parity_range_width_pct_min",
        "parity_distance_to_entry_R_min",
    ]
    missing = [p for p in required if p not in params]
    if missing:
        raise ParityValidationError(
            "LIVE SHELL SIGNATURE VERIFIED: NO -- missing run_symbol_once params: "
            + ", ".join(missing)
        )
    print(f"[LIVE_PARITY] LIVE SHELL SIGNATURE VERIFIED {sig}")
    return required


def _verify_loader_patch_target(shell: ModuleType) -> str:
    if not hasattr(shell, "load_bybit_latest"):
        raise ParityValidationError("Live shell does not expose load_bybit_latest; cannot patch historical loader safely")
    src_names = set(shell.run_symbol_once.__code__.co_names)
    if "load_bybit_latest" not in src_names:
        raise ParityValidationError(
            "run_symbol_once does not reference load_bybit_latest directly; loader patch target is uncertain"
        )
    print("[LIVE_PARITY] loader patch target verified: load_bybit_latest")
    return "load_bybit_latest"


def _derive_execution_ts_source(row: pd.Series) -> Tuple[pd.Timestamp, str]:
    """
    ANALYTICAL ONLY.
    Never used for filtering, idempotency, setup identity, emit, or position logic.
    """
    for col in ("confirm_ts", "signal_ts", "visible_ts", "pipeline_visible_ts", "timestamp"):
        if col in row.index:
            ts = pd.to_datetime(row.get(col), utc=True, errors="coerce")
            if pd.notna(ts):
                return ts, col
    return pd.NaT, "missing"


def _build_lifecycle_snapshot(
    *,
    emits_csv: Path,
    position_state_csv: Path,
    lifecycle_csv: Path,
    position_compatibility_ok: bool,
    position_compatibility_message: str,
) -> None:
    emits = _read_csv_safe(emits_csv)
    state = _read_csv_safe(position_state_csv)

    if emits.empty:
        pd.DataFrame(columns=PRIMARY_OUTPUT_SPECS["live_parity_lifecycle.csv"]).to_csv(lifecycle_csv, index=False)
        return

    for col in [
        "timestamp",
        "visible_ts",
        "pipeline_visible_ts",
        "signal_ts",
        "confirm_ts",
        "observed_ts",
    ]:
        if col in emits.columns:
            emits[col] = pd.to_datetime(emits[col], utc=True, errors="coerce")
        else:
            emits[col] = pd.NaT

    emits["emit_ts"] = emits["observed_ts"]

    exec_pairs = emits.apply(_derive_execution_ts_source, axis=1)
    emits["execution_ts"] = [pair[0] for pair in exec_pairs]
    emits["execution_ts_source"] = [pair[1] for pair in exec_pairs]
    emits["trade_open_ts"] = pd.NaT
    emits["opened_ts"] = pd.NaT
    emits["position_gate_reason"] = ""
    emits["skipped_open_position"] = False
    emits["lifecycle_state"] = ""
    emits["trade_close_ts"] = pd.NaT
    emits["outcome"] = ""
    emits["exit_reason"] = ""
    if "notes" not in emits.columns:
        emits["notes"] = ""

    lifecycle_quality = "best-effort" if not position_compatibility_ok else "position-state-compatible"

    if not state.empty and "setup_id" in state.columns and position_compatibility_ok:
        state = state.copy()
        if "opened_ts" in state.columns:
            state["opened_ts"] = pd.to_datetime(state["opened_ts"], utc=True, errors="coerce")
        else:
            state["opened_ts"] = pd.NaT
        if "closed_ts" in state.columns:
            state["closed_ts"] = pd.to_datetime(state["closed_ts"], utc=True, errors="coerce")
        else:
            state["closed_ts"] = pd.NaT
        if "close_reason" not in state.columns:
            state["close_reason"] = ""

        sort_cols = [c for c in ("closed_ts", "opened_ts") if c in state.columns]
        latest_state = state.sort_values(sort_cols, na_position="last") if sort_cols else state.copy()
        merge_key = "canonical_setup_key" if "canonical_setup_key" in emits.columns and "canonical_setup_key" in latest_state.columns else "setup_id"
        latest_state = latest_state.drop_duplicates(subset=[merge_key], keep="last")
        merge_cols = [c for c in [merge_key, "opened_ts", "closed_ts", "close_reason"] if c in latest_state.columns]
        emits = emits.merge(latest_state[merge_cols], on=merge_key, how="left")
        emits["trade_open_ts"] = emits.get("opened_ts")
        emits["trade_close_ts"] = emits.get("closed_ts")
        emits["outcome"] = emits.get("close_reason", "").fillna("")
        emits["lifecycle_state"] = emits["trade_close_ts"].apply(lambda x: "CLOSED" if pd.notna(x) else "OPENED")
        emits["exit_reason"] = emits.get("close_reason", "").fillna("")
        emits = emits.drop(columns=[c for c in ("opened_ts", "closed_ts", "close_reason") if c in emits.columns])

    emits["lifecycle_quality"] = lifecycle_quality
    if position_compatibility_message:
        emits["notes"] = emits["notes"].astype(str).where(
            emits["notes"].astype(str).str.len() > 0,
            f"{lifecycle_quality}:{position_compatibility_message}",
        )

    for c in PRIMARY_OUTPUT_SPECS["live_parity_lifecycle.csv"]:
        if c not in emits.columns:
            emits[c] = pd.NA

    for _, _row in emits.iterrows():
        if pd.notna(_row.get("trade_open_ts", pd.NaT)):
            print(
                f"[PARITY_OPEN_TS] runner=NA sim={_row.get('trade_open_ts', '')} "
                f"canonical={_row.get('canonical_setup_key', '')}"
            )

    emits[PRIMARY_OUTPUT_SPECS["live_parity_lifecycle.csv"]].to_csv(lifecycle_csv, index=False)


def _snapshot_blank(value: object) -> bool:
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    return str(value or "").strip() == "" or str(value).strip().lower() == "nan"


def _snapshot_csv_value(value: object) -> str:
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value or "")


def _position_state_final_r(row: pd.Series):
    for col in (
        "R",
        "final_R",
        "realized_R",
        "realized_r",
        "pnl_R",
        "pnl_r",
        "outcome_R",
        "outcome_r",
        "result_R",
        "result_r",
        "total_R",
        "total_r",
    ):
        if col in row.index and not _snapshot_blank(row.get(col, "")):
            return row.get(col, "")
    return ""


def _enrich_opportunity_manager_snapshot_with_position_state(
    *,
    snapshot_csv: Path,
    position_state_csv: Path,
) -> None:
    """Telemetry-only post-replay enrichment for opportunity manager snapshots.

    Reads each input once, builds an in-memory position_state lookup keyed by
    canonical_setup_key, and rewrites the snapshot once. The enriched snapshot is
    not consumed by replay logic and cannot affect trading behavior.
    """
    if snapshot_csv is None or not Path(snapshot_csv).exists() or Path(snapshot_csv).stat().st_size == 0:
        return
    if position_state_csv is None or not Path(position_state_csv).exists() or Path(position_state_csv).stat().st_size == 0:
        return

    snapshot = _read_csv_safe(Path(snapshot_csv))
    position_state = _read_csv_safe(Path(position_state_csv))
    if snapshot.empty or "canonical_setup_key" not in snapshot.columns:
        return

    enrich_cols = [
        "opened_in_position_state",
        "opened_ts",
        "closed_ts",
        "close_reason",
        "final_R_if_known",
    ]
    for col in enrich_cols:
        if col not in snapshot.columns:
            snapshot[col] = ""

    if position_state.empty or "canonical_setup_key" not in position_state.columns:
        snapshot["opened_in_position_state"] = "False"
        snapshot.to_csv(snapshot_csv, index=False)
        return

    state = position_state.copy()
    for col in ("opened_ts", "closed_ts"):
        if col in state.columns:
            state[col] = pd.to_datetime(state[col], utc=True, errors="coerce")
        else:
            state[col] = pd.NaT
    if "close_reason" not in state.columns:
        state["close_reason"] = ""

    state["_final_R_if_known"] = state.apply(_position_state_final_r, axis=1)
    sort_cols = [c for c in ("closed_ts", "opened_ts") if c in state.columns]
    if sort_cols:
        state = state.sort_values(sort_cols, na_position="last")
    state = state.drop_duplicates(subset=["canonical_setup_key"], keep="last")
    position_lookup = {
        str(row.get("canonical_setup_key", "") or ""): row
        for _, row in state.iterrows()
        if str(row.get("canonical_setup_key", "") or "")
    }

    for idx, row in snapshot.iterrows():
        key = str(row.get("canonical_setup_key", "") or "")
        match = position_lookup.get(key)
        if match is None:
            snapshot.at[idx, "opened_in_position_state"] = "False"
            continue

        snapshot.at[idx, "opened_in_position_state"] = "True"
        snapshot.at[idx, "opened_ts"] = _snapshot_csv_value(match.get("opened_ts", ""))
        snapshot.at[idx, "closed_ts"] = _snapshot_csv_value(match.get("closed_ts", ""))
        close_reason = _snapshot_csv_value(match.get("close_reason", "")).upper()
        snapshot.at[idx, "close_reason"] = close_reason

        if close_reason == "TP":
            snapshot.at[idx, "final_R_if_known"] = _snapshot_csv_value(row.get("planned_rr", ""))
        elif close_reason == "SL":
            snapshot.at[idx, "final_R_if_known"] = "-1"
        else:
            snapshot.at[idx, "final_R_if_known"] = ""

    snapshot.to_csv(snapshot_csv, index=False)


def _print_flow_parity_snapshot(flow_log_csv: Path, symbol: str, replay_ts: pd.Timestamp) -> None:
    flow = _read_csv_safe(flow_log_csv)
    if flow.empty:
        print(f"[PARITY_CHECK][{symbol}] replay_ts={replay_ts} flow_log unavailable")
        return

    available = [c for c in FLOW_PARITY_COLUMNS if c in flow.columns]
    missing = [c for c in FLOW_PARITY_COLUMNS if c not in flow.columns]
    if missing:
        print(f"[PARITY_CHECK][{symbol}] flow columns unavailable: {missing}")

    if not available:
        print(f"[PARITY_CHECK][{symbol}] replay_ts={replay_ts} no parity flow columns available")
        return

    last = flow.iloc[-1].to_dict()
    if str(last.get("symbol", "")).upper() != symbol.upper():
        print(f"[PARITY_CHECK][{symbol}] replay_ts={replay_ts} latest flow row belongs to another symbol")
        return

    parts = [f"{c}={last.get(c, '')}" for c in available]
    print(f"[PARITY_CHECK][{symbol}] replay_ts={replay_ts} " + " ".join(parts))


def _print_parity_limitations(position_compatibility_ok: bool, position_compatibility_message: str) -> None:
    print("PARITY LIMITATIONS:")
    print("- guaranteed: stage order is reused from the live shell via direct run_symbol_once(...) calls")
    print("- guaranteed: setup_id semantics used in replay logic are reused from the live shell")
    print("- guaranteed: discovery, wait, freshness, idempotency, stale, and final selection stages are not re-implemented here")
    if position_compatibility_ok:
        print("- guaranteed: position_state.csv schema matches the closer/open-position expectation found in provided live code")
    else:
        print(f"- not guaranteed: perfect closer parity; compatibility warning: {position_compatibility_message}")
    print("- analytical only: execution_ts and execution_ts_source in lifecycle CSV never influence replay logic")
    print("- depends on live shell internals: if the external live shell changes its internal loader or output schemas, this wrapper may fail fast")


def run_live_parity_replay(
    *,
    live_shell_py: Path,
    symbols: List[str],
    dt_from: pd.Timestamp,
    dt_to: pd.Timestamp,
    candles_dir: Path,
    portfolio_state_path: Path,
    out_dir: Path,
    candles_n: int,
    window_n: int,
    bybit_interval: int,
    debug: bool,
    debug_force_entries: bool,
    use_wait_confirmation: bool,
    tdp_wait_off_only: bool,
    candidate_pressure_csv: str,
    cluster_score_mode: Optional[str],
    cluster_max_per_group: Optional[int],
    cluster_rank_signal_score: bool,
    cluster_score_shadow_v2: bool,
    cluster_score_shadow_v2_csv: str,
    rr: float,
    sl_atr_buffer: float,
    require_impulse_before_tdp: bool,
    impulse_lookback: int,
    impulse_size_atr: float,
    tdp_dev_lookback: int,
    tts_retest_lookback: int,
    raw_candidate_diag_csv: Optional[Path],
    pressure_window_summary_csv: Optional[Path],
    sniper_candidate_diag_csv: Optional[Path],
    sniper_candidate_summary_csv: Optional[Path],
    tdp_stale_shadow_csv: str,
    structural_ts_shadow_csv: str,
    pre_visible_entry_exposure_csv: str,
    entry_model_pre_admission_csv: str,
    tdp_visible_assignment_trace_csv: str,
    tdp_identity_resurfacing_trace_csv: str,
    tdp_disappearance_trace_csv: str,
    tdp_true_birth_trace_csv: str,
    parity_filter_mode: str,
    parity_filter_diagnostics_csv: str,
    opportunity_manager_snapshot_csv: str,
    authority_waterfall_csv: str,
    parity_range_width_pct_min: float,
    parity_distance_to_entry_R_min: float,
    smoke_test: bool,
) -> None:
    shell = _load_shell_module(live_shell_py)
    required_params = _verify_run_symbol_once_signature(shell)
    loader_name = _verify_loader_patch_target(shell)

    candles_by_symbol: Dict[str, pd.DataFrame] = {s: _load_symbol_candles(candles_dir, s) for s in symbols}

    timelines = []
    for _, df in candles_by_symbol.items():
        sub = df[(df["timestamp"] >= dt_from) & (df["timestamp"] <= dt_to)].copy()
        timelines.append(sub["timestamp"])

    if not timelines or all(t.empty for t in timelines):
        print("[LIVE_PARITY] no candles in requested range")
        return

    out_csv = _ensure_output_file(out_dir / "live_observation_entries.csv", PRIMARY_OUTPUT_SPECS["live_observation_entries.csv"])
    fired_csv = _ensure_output_file(out_dir / "fired_setups.csv", PRIMARY_OUTPUT_SPECS["fired_setups.csv"])
    position_state_csv = _ensure_output_file(out_dir / "position_state.csv", PRIMARY_OUTPUT_SPECS["position_state.csv"])
    flow_log_csv = _ensure_output_file(
        out_dir / "flow_log.csv",
        PRIMARY_OUTPUT_SPECS["flow_log.csv"],
    )

    terminal_registry_csv = _ensure_output_file(
        out_dir / "terminal_lifecycle_registry.csv",
        PRIMARY_OUTPUT_SPECS["terminal_lifecycle_registry.csv"],
    )
    lifecycle_csv = _ensure_output_file(out_dir / "live_parity_lifecycle.csv", PRIMARY_OUTPUT_SPECS["live_parity_lifecycle.csv"])
    summary_csv = _ensure_output_file(out_dir / "live_parity_summary.csv", PRIMARY_OUTPUT_SPECS["live_parity_summary.csv"])
    # Telemetry-only diagnostics. Omitted CLI args default into out_dir. These
    # files are passed to the shell only as logging sinks and are never read by
    # replay decisions.
    raw_candidate_diag_csv = _ensure_output_file(
        raw_candidate_diag_csv or (out_dir / "raw_candidate_lifecycle_diag.csv"),
        PRIMARY_OUTPUT_SPECS["raw_candidate_lifecycle_diag.csv"],
    )
    pressure_window_summary_csv = _ensure_output_file(
        pressure_window_summary_csv or (out_dir / "pressure_window_summary.csv"),
        PRIMARY_OUTPUT_SPECS["pressure_window_summary.csv"],
    )
    sniper_candidate_diag_csv = _ensure_output_file(
        sniper_candidate_diag_csv or (out_dir / "sniper_candidate_diag.csv"),
        PRIMARY_OUTPUT_SPECS["sniper_candidate_diag.csv"],
    )
    sniper_candidate_summary_csv = _ensure_output_file(
        sniper_candidate_summary_csv or (out_dir / "sniper_candidate_summary.csv"),
        PRIMARY_OUTPUT_SPECS["sniper_candidate_summary.csv"],
    )
    tdp_stale_shadow_csv_path = _ensure_output_file(
        Path(tdp_stale_shadow_csv) if tdp_stale_shadow_csv else (out_dir / "tdp_stale_shadow_oos.csv"),
        PRIMARY_OUTPUT_SPECS["tdp_stale_shadow_oos.csv"],
    )
    structural_ts_shadow_csv_path = _ensure_output_file(
        Path(structural_ts_shadow_csv) if structural_ts_shadow_csv else (out_dir / "structural_ts_shadow_oos.csv"),
        PRIMARY_OUTPUT_SPECS["structural_ts_shadow_oos.csv"],
    )
    pre_visible_entry_exposure_csv_path = _ensure_output_file(
        Path(pre_visible_entry_exposure_csv) if pre_visible_entry_exposure_csv else (out_dir / "pre_visible_entry_exposure.csv"),
        PRIMARY_OUTPUT_SPECS["pre_visible_entry_exposure.csv"],
    )
    entry_model_pre_admission_csv_path = _ensure_output_file(
        Path(entry_model_pre_admission_csv) if entry_model_pre_admission_csv else (out_dir / "entry_model_pre_admission.csv"),
        PRIMARY_OUTPUT_SPECS["entry_model_pre_admission.csv"],
    )
    tdp_visible_assignment_trace_csv_path = _ensure_output_file(
        Path(tdp_visible_assignment_trace_csv) if tdp_visible_assignment_trace_csv else (out_dir / "tdp_visible_assignment_trace.csv"),
        PRIMARY_OUTPUT_SPECS["tdp_visible_assignment_trace.csv"],
    )
    tdp_identity_resurfacing_trace_csv_path = _ensure_output_file(
        Path(tdp_identity_resurfacing_trace_csv) if tdp_identity_resurfacing_trace_csv else (out_dir / "tdp_identity_resurfacing_trace.csv"),
        PRIMARY_OUTPUT_SPECS["tdp_identity_resurfacing_trace.csv"],
    )
    tdp_disappearance_trace_csv_path = _ensure_output_file(
        Path(tdp_disappearance_trace_csv) if tdp_disappearance_trace_csv else (out_dir / "tdp_disappearance_trace.csv"),
        PRIMARY_OUTPUT_SPECS["tdp_disappearance_trace.csv"],
    )
    parity_filter_diagnostics_csv_path = _ensure_output_file(
        Path(parity_filter_diagnostics_csv) if parity_filter_diagnostics_csv else (out_dir / "parity_filter_diagnostics.csv"),
        PRIMARY_OUTPUT_SPECS["parity_filter_diagnostics.csv"],
    )
    opportunity_manager_snapshot_csv_path = (
        Path(opportunity_manager_snapshot_csv)
        if opportunity_manager_snapshot_csv
        else (out_dir / "opportunity_manager_snapshot.csv")
    )
    authority_waterfall_csv_path = _ensure_output_file(
        Path(authority_waterfall_csv)
        if authority_waterfall_csv
        else (out_dir / "authority_waterfall.csv"),
        PRIMARY_OUTPUT_SPECS["authority_waterfall.csv"],
    )
    state_dir = out_dir / "live_observation_state"
    _ensure_parent(state_dir / "dummy.txt")
    tdp_true_birth_trace_csv_path = _ensure_output_file(
        Path(tdp_true_birth_trace_csv) if tdp_true_birth_trace_csv else (out_dir / "tdp_true_birth_trace.csv"),
        PRIMARY_OUTPUT_SPECS["tdp_true_birth_trace.csv"],
    )


    position_compatibility_ok, position_compatibility_message = _validate_position_closer_compatibility(shell, position_state_csv)
    if position_compatibility_ok:
        print(f"[LIVE_PARITY] position/closer compatibility verified: {position_compatibility_message}")
    else:
        print(f"[LIVE_PARITY][WARN] position/closer compatibility best-effort: {position_compatibility_message}")

    metrics = {
        "cycles_total": 0,
        "shell_calls": 0,
        "emits_total": 0,
        "first_replay_ts": "",
        "last_replay_ts": "",
        "symbols_processed": ",".join(symbols),
        "fired_setups_count": 0,
        "flow_rows_count": 0,
        "position_rows_count": 0,
    }

    replay_ts_list = sorted(set().union(*[set(t.tolist()) for t in timelines if not t.empty]))
    if smoke_test:
        replay_ts_list = replay_ts_list[: min(len(replay_ts_list), 12)]

    original_loader = getattr(shell, loader_name)
    print(f"[LIVE_PARITY] patched loader name: {loader_name}")

    try:
        for replay_ts in replay_ts_list:
            replay_ts = pd.to_datetime(replay_ts, utc=True, errors="coerce")
            if pd.isna(replay_ts):
                continue

            if not metrics["first_replay_ts"]:
                metrics["first_replay_ts"] = replay_ts.isoformat()
            metrics["last_replay_ts"] = replay_ts.isoformat()
            metrics["cycles_total"] += 1

            def _historical_loader(category: str, symbol: str, interval: str, candles: int) -> pd.DataFrame:
                full = candles_by_symbol[str(symbol).upper()].copy()
                hist = full[full["timestamp"] <= replay_ts].copy().sort_values("timestamp").reset_index(drop=True)
                if hist.empty:
                    return hist
                if len(hist) > int(candles):
                    hist = hist.iloc[-int(candles):].reset_index(drop=True)
                return hist

            setattr(shell, loader_name, _historical_loader)

            for symbol in symbols:
                before_emits = len(_read_csv_safe(out_csv))
                before_flow = len(_read_csv_safe(flow_log_csv))

                kwargs = {
                    "cycle_ts": pd.Timestamp(replay_ts),
                    "symbol": symbol,
                    "category": "historical",
                    "interval": str(bybit_interval),
                    "candles_n": int(candles_n),
                    "window_n": int(window_n),
                    "portfolio_state_path": portfolio_state_path,
                    "out_csv": out_csv,
                    "state_dir": state_dir,
                    "position_state_csv": position_state_csv,
                    "fired_setups_csv": fired_csv,
                    "terminal_lifecycle_registry_csv": terminal_registry_csv,
                    "flow_log_csv": flow_log_csv,
                    "debug": bool(debug),
                    "debug_force_entries": bool(debug_force_entries),
                    "use_wait_confirmation": bool(use_wait_confirmation),
                    "tdp_wait_off_only": bool(tdp_wait_off_only),
                    "candidate_pressure_csv": candidate_pressure_csv,
                    "cluster_score_mode": cluster_score_mode,
                    "cluster_max_per_group": cluster_max_per_group,
                    "cluster_rank_signal_score": bool(cluster_rank_signal_score),
                    "cluster_score_shadow_v2": bool(cluster_score_shadow_v2),
                    "cluster_score_shadow_v2_csv": str(cluster_score_shadow_v2_csv or (out_dir / "cluster_score_shadow_v2.csv")),
                    "rr": float(rr),
                    "sl_atr_buffer": float(sl_atr_buffer),
                    "require_impulse_before_tdp": bool(require_impulse_before_tdp),
                    "impulse_lookback": int(impulse_lookback),
                    "impulse_size_atr": float(impulse_size_atr),
                    "tdp_dev_lookback": int(tdp_dev_lookback),
                    "tts_retest_lookback": int(tts_retest_lookback),
                    "raw_candidate_diag_csv": str(raw_candidate_diag_csv),
                    "pressure_window_summary_csv": str(pressure_window_summary_csv),
                    "sniper_candidate_diag_csv": str(sniper_candidate_diag_csv),
                    "sniper_candidate_summary_csv": str(sniper_candidate_summary_csv),
                    "tdp_stale_shadow_csv": str(tdp_stale_shadow_csv_path),
                    "structural_ts_shadow_csv": str(structural_ts_shadow_csv_path),
                    "pre_visible_entry_exposure_csv": str(pre_visible_entry_exposure_csv_path),
                    "entry_model_pre_admission_csv": str(entry_model_pre_admission_csv_path),
                    "tdp_visible_assignment_trace_csv": str(tdp_visible_assignment_trace_csv_path),
                    "tdp_identity_resurfacing_trace_csv": str(tdp_identity_resurfacing_trace_csv_path),
                    "tdp_disappearance_trace_csv": str(tdp_disappearance_trace_csv_path),
                    "tdp_true_birth_trace_csv": str(tdp_true_birth_trace_csv_path),
                    "parity_filter_mode": str(parity_filter_mode or "NONE"),
                    "parity_filter_diagnostics_csv": str(parity_filter_diagnostics_csv_path),
                    "opportunity_manager_snapshot_csv": str(opportunity_manager_snapshot_csv_path),
                    "authority_waterfall_csv": str(authority_waterfall_csv_path),
                    "parity_range_width_pct_min": float(parity_range_width_pct_min),
                    "parity_distance_to_entry_R_min": float(parity_distance_to_entry_R_min),
                }
                call_kwargs = {k: kwargs[k] for k in required_params}
                written = shell.run_symbol_once(**call_kwargs)
                metrics["shell_calls"] += 1
                metrics["emits_total"] += int(written)

                if debug:
                    after_emits = len(_read_csv_safe(out_csv))
                    after_flow = len(_read_csv_safe(flow_log_csv))
                    _print_flow_parity_snapshot(flow_log_csv, symbol, replay_ts)
                    print(
                        f"[PARITY_CHECK][{symbol}] replay_ts={replay_ts} "
                        f"shell_written={written} emits_delta={after_emits - before_emits} "
                        f"flow_delta={after_flow - before_flow}"
                    )

        _build_lifecycle_snapshot(
            emits_csv=out_csv,
            position_state_csv=position_state_csv,
            lifecycle_csv=lifecycle_csv,
            position_compatibility_ok=position_compatibility_ok,
            position_compatibility_message=position_compatibility_message,
        )

        _enrich_opportunity_manager_snapshot_with_position_state(
            snapshot_csv=opportunity_manager_snapshot_csv_path,
            position_state_csv=position_state_csv,
        )

        _build_authority_waterfall(
            out_dir=out_dir,
            authority_waterfall_csv=authority_waterfall_csv_path,
        )

        metrics["fired_setups_count"] = len(_read_csv_safe(fired_csv))
        metrics["flow_rows_count"] = len(_read_csv_safe(flow_log_csv))
        metrics["position_rows_count"] = len(_read_csv_safe(position_state_csv))

        pd.DataFrame(
            [{"metric": k, "value": v} for k, v in metrics.items()],
            columns=PRIMARY_OUTPUT_SPECS["live_parity_summary.csv"],
        ).to_csv(summary_csv, index=False)

        print(f"[LIVE_PARITY] emits     -> {out_csv}")
        print(f"[LIVE_PARITY] fired     -> {fired_csv}")
        print(f"[LIVE_PARITY] positions -> {position_state_csv}")
        print(f"[LIVE_PARITY] flow_log  -> {flow_log_csv}")
        print(f"[LIVE_PARITY] terminal  -> {terminal_registry_csv}")
        print(f"[LIVE_PARITY] lifecycle -> {lifecycle_csv}")
        print(f"[LIVE_PARITY] summary   -> {summary_csv}")
        print(f"[LIVE_PARITY] raw_diag  -> {raw_candidate_diag_csv}")
        print(f"[LIVE_PARITY] pressure  -> {pressure_window_summary_csv}")
        print(f"[LIVE_PARITY] sniper_diag -> {sniper_candidate_diag_csv}")
        print(f"[LIVE_PARITY] sniper_summary -> {sniper_candidate_summary_csv}")
        print(f"[LIVE_PARITY] tdp_stale_shadow -> {tdp_stale_shadow_csv_path}")
        print(f"[LIVE_PARITY] structural_ts_shadow -> {structural_ts_shadow_csv_path}")
        print(f"[LIVE_PARITY] pre_visible_entry_exposure -> {pre_visible_entry_exposure_csv_path}")
        print(f"[LIVE_PARITY] entry_model_pre_admission -> {entry_model_pre_admission_csv_path}")
        print(f"[LIVE_PARITY] parity_filter_diag -> {parity_filter_diagnostics_csv_path}")
        print(f"[LIVE_PARITY] parity_filter_mode -> {parity_filter_mode}")

        if smoke_test:
            print("[SMOKE_TEST] first replay timestamp:", metrics["first_replay_ts"])
            print("[SMOKE_TEST] last replay timestamp:", metrics["last_replay_ts"])
            print("[SMOKE_TEST] symbols processed:", metrics["symbols_processed"])
            print("[SMOKE_TEST] emits count:", metrics["emits_total"])
            print("[SMOKE_TEST] fired setups count:", metrics["fired_setups_count"])
            print("[SMOKE_TEST] flow rows count:", metrics["flow_rows_count"])
            print("[SMOKE_TEST] position rows count:", metrics["position_rows_count"])
    finally:
        setattr(shell, loader_name, original_loader)
        restored = getattr(shell, loader_name) is original_loader
        print(f"[LIVE_PARITY] restored loader confirmation: {restored}")
        _print_parity_limitations(position_compatibility_ok, position_compatibility_message)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Exact live_observation_shell parity replay on historical candles")
    ap.add_argument("--live_shell_py", required=True, help="Path to the real live_observation_shell .py file")
    ap.add_argument("--symbols", default="BTCUSDT", help="Comma-separated symbols")
    ap.add_argument("--from", dest="dt_from", required=False)
    ap.add_argument("--start", dest="dt_from", required=False)
    ap.add_argument("--to", dest="dt_to", required=False)
    ap.add_argument("--end", dest="dt_to", required=False)
    ap.add_argument("--candles_dir", default="backtest/data")
    ap.add_argument("--portfolio_state", default="backtest/journal/exports_live/portfolio_state.json")
    ap.add_argument("--out_dir", default="backtest/journal/live_parity_validation")
    ap.add_argument("--bybit_candles", type=int, default=260)
    ap.add_argument("--window", type=int, default=200)
    ap.add_argument("--bybit_interval", type=int, default=15)
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--debug_force_entries", action="store_true")
    ap.add_argument("--use_wait_confirmation", action="store_true")
    ap.add_argument("--tdp_wait_off_only", action="store_true")
    ap.add_argument("--candidate_pressure_csv", default="backtest/journal/exports_live/candidate_pressure.csv")
    ap.add_argument("--raw_candidate_diag_csv", default=None)
    ap.add_argument("--pressure_window_summary_csv", default=None)
    ap.add_argument("--sniper_candidate_diag_csv", default=None)
    ap.add_argument("--sniper_candidate_summary_csv", default=None)
    ap.add_argument("--tdp_stale_shadow_csv", default="")
    ap.add_argument("--structural_ts_shadow_csv", default="")
    ap.add_argument("--pre_visible_entry_exposure_csv", default="")
    ap.add_argument("--entry_model_pre_admission_csv", default="")
    ap.add_argument("--tdp_visible_assignment_trace_csv", default="")
    ap.add_argument("--tdp_identity_resurfacing_trace_csv", default="")
    ap.add_argument("--tdp_disappearance_trace_csv", default="")
    ap.add_argument("--tdp_true_birth_trace_csv", default="")
    ap.add_argument("--parity_filter_mode", choices=("NONE", "RANGE_AGE_3_5", "COMBINED_FINGERPRINT", "REMOVE_RANGE_AGE_1_2", "RANGE_GEOMETRY_P50"), default="NONE")
    ap.add_argument("--parity_filter_diagnostics_csv", default="")
    ap.add_argument("--opportunity_manager_snapshot_csv", default=None)
    ap.add_argument("--authority_waterfall_csv", default=None)
    ap.add_argument("--parity_range_width_pct_min", type=float, default=0.0)
    ap.add_argument("--parity_distance_to_entry_R_min", type=float, default=0.5)
    ap.add_argument("--cluster_score_mode", choices=("LEGACY", "SIGNAL_SCORE", "SHADOW_SCORE_V2", "SHADOW_SCORE_V3_TDP_ONLY", "SHADOW_SCORE_V4A_RANGE_WIDE", "SHADOW_SCORE_V4B_RANGE_REALISTIC", "SHADOW_SCORE_V4C_RANGE_HIGH_RR_PENALTY"), default=None)
    ap.add_argument("--cluster_max_per_group", type=int, choices=(1, 2, 3), default=None)
    ap.add_argument("--cluster_rank_signal_score", action="store_true")
    ap.add_argument("--cluster_score_shadow_v2", action="store_true")
    ap.add_argument("--cluster_score_shadow_v2_csv", default="")
    ap.add_argument("--rr", type=float, default=2.0)
    ap.add_argument("--sl_atr_buffer", type=float, default=0.15)
    ap.add_argument("--require_impulse_before_tdp", action="store_true")
    ap.add_argument("--impulse_lookback", type=int, default=10)
    ap.add_argument("--impulse_size_atr", type=float, default=1.0)
    ap.add_argument("--tdp_dev_lookback", type=int, default=8)
    ap.add_argument("--tts_retest_lookback", type=int, default=24)
    ap.add_argument("--smoke_test", action="store_true", help="Run a very small replay subset for wiring validation")
    args = ap.parse_args(argv)

    if not args.dt_from or not args.dt_to:
        ap.error("Missing date range: provide --from/--to or --start/--end")

    dt_from = _parse_dt(args.dt_from)
    dt_to = _parse_dt(args.dt_to)
    if dt_to <= dt_from:
        raise SystemExit("--to/--end must be > --from/--start")

    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
    if not symbols:
        raise SystemExit("--symbols empty")

    cluster_score_mode = args.cluster_score_mode
    if args.cluster_rank_signal_score:
        if cluster_score_mode is not None and cluster_score_mode != "SIGNAL_SCORE":
            ap.error("--cluster_rank_signal_score conflicts with --cluster_score_mode LEGACY")
        cluster_score_mode = "SIGNAL_SCORE"

    run_live_parity_replay(
        live_shell_py=Path(args.live_shell_py),
        symbols=symbols,
        dt_from=dt_from,
        dt_to=dt_to,
        candles_dir=Path(args.candles_dir),
        portfolio_state_path=Path(args.portfolio_state),
        out_dir=Path(args.out_dir),
        candles_n=int(args.bybit_candles),
        window_n=int(args.window),
        bybit_interval=int(args.bybit_interval),
        debug=bool(args.debug),
        debug_force_entries=bool(args.debug_force_entries),
        use_wait_confirmation=bool(args.use_wait_confirmation),
        tdp_wait_off_only=bool(args.tdp_wait_off_only),
        candidate_pressure_csv=str(args.candidate_pressure_csv),
        cluster_score_mode=cluster_score_mode,
        cluster_max_per_group=args.cluster_max_per_group,
        cluster_rank_signal_score=bool(args.cluster_rank_signal_score),
        cluster_score_shadow_v2=bool(args.cluster_score_shadow_v2),
        cluster_score_shadow_v2_csv=str(args.cluster_score_shadow_v2_csv or ""),
        rr=float(args.rr),
        sl_atr_buffer=float(args.sl_atr_buffer),
        require_impulse_before_tdp=bool(args.require_impulse_before_tdp),
        impulse_lookback=int(args.impulse_lookback),
        impulse_size_atr=float(args.impulse_size_atr),
        tdp_dev_lookback=int(args.tdp_dev_lookback),
        tts_retest_lookback=int(args.tts_retest_lookback),
        raw_candidate_diag_csv=Path(args.raw_candidate_diag_csv) if args.raw_candidate_diag_csv else None,
        pressure_window_summary_csv=Path(args.pressure_window_summary_csv) if args.pressure_window_summary_csv else None,
        sniper_candidate_diag_csv=Path(args.sniper_candidate_diag_csv) if args.sniper_candidate_diag_csv else None,
        sniper_candidate_summary_csv=Path(args.sniper_candidate_summary_csv) if args.sniper_candidate_summary_csv else None,
        tdp_stale_shadow_csv=str(args.tdp_stale_shadow_csv or ""),
        structural_ts_shadow_csv=str(args.structural_ts_shadow_csv or ""),
        pre_visible_entry_exposure_csv=str(args.pre_visible_entry_exposure_csv or ""),
        entry_model_pre_admission_csv=str(args.entry_model_pre_admission_csv or ""),
        tdp_visible_assignment_trace_csv=str(args.tdp_visible_assignment_trace_csv or ""),
        tdp_identity_resurfacing_trace_csv=str(args.tdp_identity_resurfacing_trace_csv or ""),
        tdp_disappearance_trace_csv=str(args.tdp_disappearance_trace_csv or ""),
        tdp_true_birth_trace_csv=str(args.tdp_true_birth_trace_csv or ""),
        parity_filter_mode=str(args.parity_filter_mode or "NONE"),
        parity_filter_diagnostics_csv=str(args.parity_filter_diagnostics_csv or ""),
        opportunity_manager_snapshot_csv="" if args.opportunity_manager_snapshot_csv is None else str(args.opportunity_manager_snapshot_csv),
        authority_waterfall_csv="" if args.authority_waterfall_csv is None else str(args.authority_waterfall_csv),
        parity_range_width_pct_min=float(args.parity_range_width_pct_min),
        parity_distance_to_entry_R_min=float(args.parity_distance_to_entry_R_min),
        smoke_test=bool(args.smoke_test),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
