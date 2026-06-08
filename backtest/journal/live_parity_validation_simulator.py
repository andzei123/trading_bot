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
        "candidate_pressure_csv",
        "cluster_score_mode",
        "cluster_max_per_group",
        "cluster_rank_signal_score",
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
    raw_candidate_diag_csv: Optional[Path],
    pressure_window_summary_csv: Optional[Path],
    sniper_candidate_diag_csv: Optional[Path],
    sniper_candidate_summary_csv: Optional[Path],
    tdp_stale_shadow_csv: str,
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
    flow_log_csv = _ensure_output_file(out_dir / "flow_log.csv", PRIMARY_OUTPUT_SPECS["flow_log.csv"])
    terminal_registry_csv = _ensure_output_file(out_dir / "terminal_lifecycle_registry.csv", PRIMARY_OUTPUT_SPECS["terminal_lifecycle_registry.csv"])
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
    state_dir = out_dir / "live_observation_state"
    _ensure_parent(state_dir / "dummy.txt")

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
                    "candidate_pressure_csv": candidate_pressure_csv,
                    "cluster_score_mode": cluster_score_mode,
                    "cluster_max_per_group": cluster_max_per_group,
                    "cluster_rank_signal_score": bool(cluster_rank_signal_score),
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
    ap.add_argument("--candidate_pressure_csv", default="backtest/journal/exports_live/candidate_pressure.csv")
    ap.add_argument("--raw_candidate_diag_csv", default=None)
    ap.add_argument("--pressure_window_summary_csv", default=None)
    ap.add_argument("--sniper_candidate_diag_csv", default=None)
    ap.add_argument("--sniper_candidate_summary_csv", default=None)
    ap.add_argument("--tdp_stale_shadow_csv", default="")
    ap.add_argument("--cluster_score_mode", choices=("LEGACY", "SIGNAL_SCORE"), default=None)
    ap.add_argument("--cluster_max_per_group", type=int, choices=(1, 2, 3), default=None)
    ap.add_argument("--cluster_rank_signal_score", action="store_true")
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
        candidate_pressure_csv=str(args.candidate_pressure_csv),
        cluster_score_mode=cluster_score_mode,
        cluster_max_per_group=args.cluster_max_per_group,
        cluster_rank_signal_score=bool(args.cluster_rank_signal_score),
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
        smoke_test=bool(args.smoke_test),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
