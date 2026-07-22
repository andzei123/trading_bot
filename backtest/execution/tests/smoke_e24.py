from __future__ import annotations

import builtins
import socket
import sys
import tempfile
import types
import urllib.request
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import pandas as pd

GATEWAY_IMPORT_PREFIXES = (
    "backtest.execution.testnet_command_gateway",
)
TESTNET_IMPORT_PREFIXES = (
    "backtest.execution.testnet_submit_adapter",
    "backtest.execution.testnet_real_submit",
    "backtest.execution.end_to_end_testnet_drill",
)
# The repository has no separate LIVE submit module. MODE_LIVE is rejected at
# the concrete command-gateway and real-submit surfaces, so those are the
# repository-defined LIVE reachability sentinels.
LIVE_IMPORT_PREFIXES = (
    "backtest.execution.testnet_command_gateway",
    "backtest.execution.testnet_real_submit",
)
EXCHANGE_IMPORT_PREFIXES = (
    "backtest.execution.bybit_read_only_adapter",
    "backtest.execution.exchange_read_adapter",
    "backtest.execution.exchange_reconciler",
)


def _seed_active(ledger, key: str) -> None:
    from backtest.execution.execution_event_ledger import (
        EXCHANGE_READY,
        INTENT_ACCEPTED,
        MECHANICAL_SAFETY_PASSED,
        SIMULATED_FILLED,
    )

    for event_type in (
        INTENT_ACCEPTED,
        MECHANICAL_SAFETY_PASSED,
        EXCHANGE_READY,
        SIMULATED_FILLED,
    ):
        ledger.append_event(canonical_setup_key=key, event_type=event_type)


def _snapshot_guard(executor, key: str):
    return executor.ledger.events, executor.lifecycle_snapshot(key)


def _assert_unchanged(executor, key: str, before) -> None:
    events, snapshot = before
    assert executor.ledger.events == events
    assert executor.lifecycle_snapshot(key) == snapshot


def _stub_shell_dependencies() -> None:
    modules = {
        "backtest.live_pipeline.pipeline_core": {"run_pipeline_once": lambda *a, **k: None},
        "backtest.portfolio.portfolio_exposure": {"load_portfolio_exposure": lambda *a, **k: {}},
        "backtest.utils.wait_confirmation": {"apply_wait_confirmation": lambda value, *a, **k: value},
        "backtest.journal.live_emit_guard": {
            "filter_live_emit_candidates": lambda value, *a, **k: value,
            "select_newest_live_candidate": lambda value, *a, **k: value,
        },
        "backtest.journal.identity": {
            "LIFECYCLE_CLOSED": "CLOSED",
            "LIFECYCLE_FIRED": "FIRED",
            "LIFECYCLE_OPENED": "OPENED",
            "LIFECYCLE_STALE": "STALE",
            "LIFECYCLE_WAIT_REJECTED": "WAIT_REJECTED",
            "_append_terminal_lifecycle_row": lambda *a, **k: None,
            "_ensure_canonical_setup_key": lambda value: value,
            "_load_canonical_keys_from_csv": lambda *a, **k: set(),
            "_load_terminal_canonical_keys": lambda *a, **k: set(),
        },
    }
    for name, attrs in modules.items():
        module = types.ModuleType(name)
        for attr, value in attrs.items():
            setattr(module, attr, value)
        sys.modules.setdefault(name, module)


def _position_rows(target_key: str) -> list[dict[str, object]]:
    return [
        {
            "symbol": "BTCUSDT",
            "canonical_setup_key": target_key,
            "setup_id": "target",
            "setup_created_ts": "2026-07-21T18:00:00+00:00",
            "signal_ts": "2026-07-21T18:15:00+00:00",
            "opened_ts": "2026-07-21T19:00:00+00:00",
            "status": "OPEN",
            "closed_ts": "",
            "close_reason": "",
            "lifecycle_state": "OPENED",
        },
        {
            "symbol": "BTCUSDT",
            "canonical_setup_key": "BTCUSDT|UNCHANGED",
            "setup_id": "unchanged",
            "setup_created_ts": "2026-07-21T18:00:00+00:00",
            "signal_ts": "2026-07-21T18:15:00+00:00",
            "opened_ts": "2026-07-21T19:00:00+00:00",
            "status": "OPEN",
            "closed_ts": "",
            "close_reason": "",
            "lifecycle_state": "OPENED",
        },
        {
            "symbol": "BTCUSDT",
            "canonical_setup_key": "BTCUSDT|PREVIOUSLY_CLOSED",
            "setup_id": "old",
            "setup_created_ts": "2026-07-21T17:00:00+00:00",
            "signal_ts": "2026-07-21T17:15:00+00:00",
            "opened_ts": "2026-07-21T18:00:00+00:00",
            "status": "CLOSED",
            "closed_ts": "2026-07-21T19:30:00+00:00",
            "close_reason": "SL",
            "lifecycle_state": "CLOSED",
        },
        {
            "symbol": "ETHUSDT",
            "canonical_setup_key": "ETHUSDT|OTHER",
            "setup_id": "other",
            "setup_created_ts": "2026-07-21T18:00:00+00:00",
            "signal_ts": "2026-07-21T18:15:00+00:00",
            "opened_ts": "2026-07-21T19:00:00+00:00",
            "status": "OPEN",
            "closed_ts": "",
            "close_reason": "",
            "lifecycle_state": "OPENED",
        },
    ]


def _shell_integration_probe(boundary) -> dict[str, int]:
    _stub_shell_dependencies()
    from backtest.journal import live_observation_shell as shell
    from backtest.execution.execution_event_ledger import ExecutionEventLedger
    from backtest.execution.paper_executor import PaperExecutor

    key = "BTCUSDT|RANGE_TOP_SHORT_V2|SHORT|E24.2"
    ledger = ExecutionEventLedger()
    _seed_active(ledger, key)
    executor = PaperExecutor(ledger=ledger)
    boundary._SESSION_PAPER_EXECUTOR = executor

    markers = {
        "closer_called": 0,
        "ATS_close_persisted": 0,
        "executor_observer_called_after_persist": 0,
        "shell_continued": 0,
        "paper_mode_false_closer_calls": 0,
        "paper_mode_false_preparation_reads": 0,
        "paper_mode_false_post_reads": 0,
        "paper_mode_false_diff_calls": 0,
        "paper_mode_false_observer_calls": 0,
        "paper_mode_false_executor_diagnostics": 0,
        "paper_mode_false_closer_result_preserved": 0,
        "ats_close_filesystem_mutation": 0,
        "executor_position_state_mutation": 0,
        "executor_journal_mutation": 0,
        "executor_filesystem_mutation": 0,
        "shell_continuation_after_pre_read_failure": 0,
        "ats_closer_runs_after_pre_read_failure": 0,
        "pre_read_failure_closer_result_preserved": 0,
        "pre_read_failure_observer_calls": 0,
        "shell_continuation_after_post_read_failure": 0,
        "post_read_failure_closer_result_preserved": 0,
        "post_read_failure_observer_calls": 0,
        "shell_continuation_after_transition_derivation_failure": 0,
        "transition_failure_closer_result_preserved": 0,
        "transition_failure_partial_executor_events": 0,
        "shell_continuation_after_observer_failure": 0,
        "shell_continuation_after_diagnostic_failure": 0,
        "ats_closer_exception_preserved": 0,
        "ats_closer_failure_observer_calls": 0,
    }

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        state_path = root / "position_state.csv"
        out_path = root / "entries.csv"
        pd.DataFrame(_position_rows(key)).to_csv(state_path, index=False)
        out_path.write_text("placeholder\n", encoding="utf-8")
        initial_state_bytes = state_path.read_bytes()
        calls: list[str] = []
        received: list[tuple[str, str, str, str]] = []
        persisted_before_observer: list[bytes] = []

        def closer(**kwargs):
            calls.append("closer_called")
            markers["closer_called"] = 1
            frame = pd.read_csv(kwargs["position_state_csv"])
            mask = frame["canonical_setup_key"].astype(str) == key
            assert frame.loc[mask, "status"].iloc[0] == "OPEN"
            frame.loc[mask, "status"] = "CLOSED"
            frame.loc[mask, "closed_ts"] = "2026-07-21T20:00:00+00:00"
            frame.loc[mask, "close_reason"] = "TP"
            frame.loc[mask, "lifecycle_state"] = "CLOSED"
            frame.to_csv(kwargs["position_state_csv"], index=False)
            calls.append("ATS_close_persisted")
            markers["ATS_close_persisted"] = 1
            return "ATS_CLOSER_RESULT"

        real_observer = boundary.observe_ats_position_closed
        original_open = builtins.open
        original_path_open = Path.open
        write_attempts: list[str] = []

        def _write_mode(mode: object) -> bool:
            text = str(mode or "r")
            return any(flag in text for flag in ("w", "a", "x", "+"))

        def guarded_open(file, mode="r", *args, **kwargs):
            if _write_mode(mode):
                write_attempts.append(f"open:{file}:{mode}")
                raise AssertionError(f"Executor filesystem write attempted: {file} mode={mode}")
            return original_open(file, mode, *args, **kwargs)

        def guarded_path_open(self, mode="r", *args, **kwargs):
            if _write_mode(mode):
                write_attempts.append(f"Path.open:{self}:{mode}")
                raise AssertionError(f"Executor filesystem write attempted: {self} mode={mode}")
            return original_path_open(self, mode, *args, **kwargs)

        def forbidden_path_write(*args, **kwargs):
            write_attempts.append("Path.write")
            raise AssertionError("Executor Path write attempted")

        def observer(**kwargs):
            persisted = pd.read_csv(state_path)
            row = persisted.loc[persisted["canonical_setup_key"].astype(str) == key].iloc[0]
            assert row["status"] == "CLOSED"
            assert row["closed_ts"] == "2026-07-21T20:00:00+00:00"
            assert row["close_reason"] == "TP"
            calls.append("executor_observer_called_after_persist")
            markers["executor_observer_called_after_persist"] += 1
            received.append((kwargs["canonical_setup_key"], kwargs["status"], kwargs["closed_at_utc"], kwargs["close_reason"]))
            before_bytes = state_path.read_bytes()
            persisted_before_observer.append(before_bytes)
            tree_before = {item.relative_to(root): item.read_bytes() for item in root.rglob("*") if item.is_file()}
            with ExitStack() as stack:
                stack.enter_context(patch.object(builtins, "open", guarded_open))
                stack.enter_context(patch.object(Path, "open", guarded_path_open))
                stack.enter_context(patch.object(Path, "write_text", forbidden_path_write))
                stack.enter_context(patch.object(Path, "write_bytes", forbidden_path_write))
                stack.enter_context(patch.object(Path, "mkdir", forbidden_path_write))
                result = real_observer(**kwargs)
            after_bytes = state_path.read_bytes()
            tree_after = {item.relative_to(root): item.read_bytes() for item in root.rglob("*") if item.is_file()}
            assert after_bytes == before_bytes
            assert tree_after == tree_before
            assert write_attempts == []
            markers["executor_position_state_mutation"] = 0
            markers["executor_journal_mutation"] = 0
            markers["executor_filesystem_mutation"] = 0
            return result

        with patch.object(shell, "close_symbol_if_hit", closer), patch.object(
            boundary, "observe_ats_position_closed", observer
        ):
            closer_result = shell._run_authoritative_closer_then_executor_observation(
                symbol="BTCUSDT",
                candles_df=pd.DataFrame(),
                position_state_csv=state_path,
                out_csv=out_path,
                executor_paper_mode=True,
            )

        assert closer_result == "ATS_CLOSER_RESULT"
        assert calls == ["closer_called", "ATS_close_persisted", "executor_observer_called_after_persist"]
        assert received == [(key, "CLOSED", "2026-07-21T20:00:00+00:00", "TP")]
        assert persisted_before_observer
        assert initial_state_bytes != persisted_before_observer[0]
        markers["ats_close_filesystem_mutation"] = 1
        markers["shell_continued"] = 1

        persisted = pd.read_csv(state_path)
        assert persisted.loc[persisted["canonical_setup_key"] == key, "status"].iloc[0] == "CLOSED"
        assert persisted.loc[persisted["canonical_setup_key"] == "BTCUSDT|UNCHANGED", "status"].iloc[0] == "OPEN"
        assert persisted.loc[persisted["canonical_setup_key"] == "BTCUSDT|PREVIOUSLY_CLOSED", "closed_ts"].iloc[0] == "2026-07-21T19:30:00+00:00"
        assert persisted.loc[persisted["canonical_setup_key"] == "ETHUSDT|OTHER", "status"].iloc[0] == "OPEN"

        snapshot = executor.lifecycle_snapshot(key)
        assert snapshot.transition_history == (
            "INTENT_ACCEPTED", "MECHANICAL_ALLOWED", "OPEN_REQUESTED",
            "OPEN_CONFIRMED", "FULLY_FILLED", "POSITION_ACTIVE",
            "CLOSE_REQUESTED", "CLOSE_CONFIRMED", "EXECUTION_COMPLETED",
        )
        assert snapshot.current_state == "EXECUTION_COMPLETED"
        assert snapshot.completed is True
        assert snapshot.position_active is False
        assert snapshot.event_count == 7
        assert tuple(event.sequence for event in ledger.events) == tuple(range(1, 8))
        assert executor.lifecycle_snapshot(key) == snapshot

        # Executor boundary failure cannot roll back the ATS-persisted close.
        pd.DataFrame(_position_rows(key)).to_csv(state_path, index=False)
        with patch.object(shell, "close_symbol_if_hit", closer), patch.object(
            boundary, "observe_ats_position_closed", side_effect=RuntimeError("observer sentinel")
        ):
            assert shell._run_authoritative_closer_then_executor_observation(
                symbol="BTCUSDT", candles_df=pd.DataFrame(), position_state_csv=state_path,
                out_csv=out_path, executor_paper_mode=True,
            ) == "ATS_CLOSER_RESULT"
        failed_bytes = state_path.read_bytes()
        failed_row = pd.read_csv(state_path).loc[lambda df: df["canonical_setup_key"] == key].iloc[0]
        assert failed_row["status"] == "CLOSED"
        assert failed_row["closed_ts"] == "2026-07-21T20:00:00+00:00"
        assert failed_row["close_reason"] == "TP"

        # Diagnostic formatting failure is isolated after ATS persistence.
        pd.DataFrame(_position_rows(key)).to_csv(state_path, index=False)
        with patch.object(shell, "close_symbol_if_hit", closer), patch.object(
            shell, "_format_executor_close_diagnostic", side_effect=RuntimeError("format sentinel")
        ):
            assert shell._run_authoritative_closer_then_executor_observation(
                symbol="BTCUSDT", candles_df=pd.DataFrame(), position_state_csv=state_path,
                out_csv=out_path, executor_paper_mode=True,
            ) == "ATS_CLOSER_RESULT"
        assert state_path.read_bytes() != initial_state_bytes

        # Non-paper path: no E24 reads, diff, observer, or diagnostics.
        pd.DataFrame(_position_rows(key)).to_csv(state_path, index=False)
        nonpaper_counts = {"reads": 0, "diff": 0, "observer": 0, "diagnostic": 0, "closer": 0}

        def nonpaper_closer(**kwargs):
            nonpaper_counts["closer"] += 1
            frame = pd.read_csv(kwargs["position_state_csv"])
            mask = frame["canonical_setup_key"].astype(str) == key
            frame.loc[mask, "status"] = "CLOSED"
            frame.loc[mask, "closed_ts"] = "2026-07-21T21:00:00+00:00"
            frame.loc[mask, "close_reason"] = "SL"
            frame.to_csv(kwargs["position_state_csv"], index=False)
            return "NON_PAPER_CLOSER_RESULT"

        def counted_read(*args, **kwargs):
            nonpaper_counts["reads"] += 1
            raise AssertionError("E24 Position State read reached in non-paper mode")

        def counted_diff(*args, **kwargs):
            nonpaper_counts["diff"] += 1
            raise AssertionError("E24 diff reached in non-paper mode")

        def counted_observer(*args, **kwargs):
            nonpaper_counts["observer"] += 1
            raise AssertionError("Executor observer reached in non-paper mode")

        def counted_diagnostic(*args, **kwargs):
            nonpaper_counts["diagnostic"] += 1
            raise AssertionError("Executor diagnostics reached in non-paper mode")

        with patch.object(shell, "close_symbol_if_hit", nonpaper_closer), patch.object(
            shell, "_load_executor_close_rows", counted_read
        ), patch.object(shell, "_derive_executor_close_transitions", counted_diff), patch.object(
            boundary, "observe_ats_position_closed", counted_observer
        ), patch.object(shell, "_format_executor_close_diagnostic", counted_diagnostic):
            result = shell._run_authoritative_closer_then_executor_observation(
                symbol="BTCUSDT", candles_df=pd.DataFrame(), position_state_csv=state_path,
                out_csv=out_path, executor_paper_mode=False,
            )

        assert result == "NON_PAPER_CLOSER_RESULT"
        assert nonpaper_counts == {"reads": 0, "diff": 0, "observer": 0, "diagnostic": 0, "closer": 1}
        markers["paper_mode_false_closer_calls"] = nonpaper_counts["closer"]
        markers["paper_mode_false_preparation_reads"] = nonpaper_counts["reads"]
        markers["paper_mode_false_post_reads"] = nonpaper_counts["reads"]
        markers["paper_mode_false_diff_calls"] = nonpaper_counts["diff"]
        markers["paper_mode_false_observer_calls"] = nonpaper_counts["observer"]
        markers["paper_mode_false_executor_diagnostics"] = nonpaper_counts["diagnostic"]
        markers["paper_mode_false_closer_result_preserved"] = int(result == "NON_PAPER_CLOSER_RESULT")

        # Ensure the expected ATS mutation remains, while Executor never rewrites it.
        nonpaper_row = pd.read_csv(state_path).loc[lambda df: df["canonical_setup_key"] == key].iloc[0]
        assert nonpaper_row["status"] == "CLOSED"
        assert nonpaper_row["closed_ts"] == "2026-07-21T21:00:00+00:00"
        assert nonpaper_row["close_reason"] == "SL"
        assert failed_bytes != initial_state_bytes

        # Complete E24 preparation/read/diff failure isolation.
        def authoritative_result_closer(**kwargs):
            frame = pd.read_csv(kwargs["position_state_csv"])
            mask = frame["canonical_setup_key"].astype(str) == key
            frame.loc[mask, "status"] = "CLOSED"
            frame.loc[mask, "closed_ts"] = "2026-07-21T23:00:00+00:00"
            frame.loc[mask, "close_reason"] = "TP"
            frame.to_csv(kwargs["position_state_csv"], index=False)
            return "FAILURE_ISOLATION_RESULT"

        # Pre-close read failure cannot prevent ATS closer.
        pd.DataFrame(_position_rows(key)).to_csv(state_path, index=False)
        pre_counts = {"closer": 0, "observer": 0}
        def pre_closer(**kwargs):
            pre_counts["closer"] += 1
            return authoritative_result_closer(**kwargs)
        def pre_observer(**kwargs):
            pre_counts["observer"] += 1
            raise AssertionError("observer must not run after pre-read failure")
        with patch.object(shell, "_load_executor_close_rows", side_effect=RuntimeError("pre-read sentinel")), \
             patch.object(shell, "close_symbol_if_hit", pre_closer), \
             patch.object(boundary, "observe_ats_position_closed", pre_observer):
            pre_result = shell._run_authoritative_closer_then_executor_observation(
                symbol="BTCUSDT", candles_df=pd.DataFrame(), position_state_csv=state_path,
                out_csv=out_path, executor_paper_mode=True,
            )
        assert pre_counts == {"closer": 1, "observer": 0}
        assert pre_result == "FAILURE_ISOLATION_RESULT"
        assert pd.read_csv(state_path).loc[lambda df: df["canonical_setup_key"] == key, "status"].iloc[0] == "CLOSED"
        markers["shell_continuation_after_pre_read_failure"] = 1
        markers["ats_closer_runs_after_pre_read_failure"] = pre_counts["closer"]
        markers["pre_read_failure_closer_result_preserved"] = int(pre_result == "FAILURE_ISOLATION_RESULT")
        markers["pre_read_failure_observer_calls"] = pre_counts["observer"]

        # Post-close read failure leaves ATS close persisted and skips observer.
        pd.DataFrame(_position_rows(key)).to_csv(state_path, index=False)
        before_map = shell._load_executor_close_rows(state_path, "BTCUSDT")
        post_counts = {"loads": 0, "observer": 0}
        def post_loader(*args, **kwargs):
            post_counts["loads"] += 1
            if post_counts["loads"] == 1:
                return before_map
            raise RuntimeError("post-read sentinel")
        def post_observer(**kwargs):
            post_counts["observer"] += 1
            raise AssertionError("observer must not run after post-read failure")
        with patch.object(shell, "_load_executor_close_rows", post_loader), \
             patch.object(shell, "close_symbol_if_hit", authoritative_result_closer), \
             patch.object(boundary, "observe_ats_position_closed", post_observer):
            post_result = shell._run_authoritative_closer_then_executor_observation(
                symbol="BTCUSDT", candles_df=pd.DataFrame(), position_state_csv=state_path,
                out_csv=out_path, executor_paper_mode=True,
            )
        assert post_counts == {"loads": 2, "observer": 0}
        assert post_result == "FAILURE_ISOLATION_RESULT"
        markers["shell_continuation_after_post_read_failure"] = 1
        markers["post_read_failure_closer_result_preserved"] = int(post_result == "FAILURE_ISOLATION_RESULT")
        markers["post_read_failure_observer_calls"] = post_counts["observer"]

        # Transition derivation failure leaves Executor ledger unchanged.
        transition_key = "BTCUSDT|TRANSITION_FAILURE"
        transition_ledger = ExecutionEventLedger()
        _seed_active(transition_ledger, transition_key)
        transition_executor = PaperExecutor(ledger=transition_ledger)
        boundary._SESSION_PAPER_EXECUTOR = transition_executor
        transition_before = transition_ledger.events
        transition_rows = _position_rows(transition_key)
        pd.DataFrame(transition_rows).to_csv(state_path, index=False)
        with patch.object(shell, "close_symbol_if_hit", authoritative_result_closer), \
             patch.object(shell, "_derive_executor_close_transitions", side_effect=RuntimeError("diff sentinel")):
            transition_result = shell._run_authoritative_closer_then_executor_observation(
                symbol="BTCUSDT", candles_df=pd.DataFrame(), position_state_csv=state_path,
                out_csv=out_path, executor_paper_mode=True,
            )
        assert transition_result == "FAILURE_ISOLATION_RESULT"
        assert transition_ledger.events == transition_before
        markers["shell_continuation_after_transition_derivation_failure"] = 1
        markers["transition_failure_closer_result_preserved"] = 1
        markers["transition_failure_partial_executor_events"] = len(transition_ledger.events) - len(transition_before)

        # Existing observer and diagnostic failures remain shell-safe.
        markers["shell_continuation_after_observer_failure"] = 1
        markers["shell_continuation_after_diagnostic_failure"] = 1

        # Authoritative closer exception must escape unchanged; no observer runs.
        closer_failure_observer_calls = 0
        closer_exc = RuntimeError("authoritative closer sentinel")
        def closer_failure(**kwargs):
            raise closer_exc
        def forbidden_observer(**kwargs):
            nonlocal closer_failure_observer_calls
            closer_failure_observer_calls += 1
        pd.DataFrame(_position_rows(key)).to_csv(state_path, index=False)
        try:
            with patch.object(shell, "close_symbol_if_hit", closer_failure), \
                 patch.object(boundary, "observe_ats_position_closed", forbidden_observer):
                shell._run_authoritative_closer_then_executor_observation(
                    symbol="BTCUSDT", candles_df=pd.DataFrame(), position_state_csv=state_path,
                    out_csv=out_path, executor_paper_mode=True,
                )
        except RuntimeError as exc:
            assert exc is closer_exc
            markers["ats_closer_exception_preserved"] = 1
        else:
            raise AssertionError("authoritative closer exception was swallowed")
        markers["ats_closer_failure_observer_calls"] = closer_failure_observer_calls

    return markers


def main() -> int:
    from backtest.execution import shadow_integration_boundary as boundary
    from backtest.execution.execution_event_ledger import ExecutionEventLedger
    from backtest.execution.paper_executor import PaperExecutor

    key = "BTCUSDT|RANGE_TOP_SHORT_V2|SHORT|E24-PAYLOAD"
    ledger = ExecutionEventLedger()
    _seed_active(ledger, key)
    executor = PaperExecutor(ledger=ledger)
    boundary._SESSION_PAPER_EXECUTOR = executor

    malformed = (
        ({"canonical_setup_key": "", "status": "CLOSED", "closed_at_utc": "2026-07-21T20:00:00+00:00", "close_reason": "TP"}, "MISSING_CANONICAL_KEY"),
        ({"canonical_setup_key": key, "status": "OPEN", "closed_at_utc": "2026-07-21T20:00:00+00:00", "close_reason": "TP"}, "STATUS_NOT_CLOSED"),
        ({"canonical_setup_key": key, "status": "", "closed_at_utc": "2026-07-21T20:00:00+00:00", "close_reason": "TP"}, "STATUS_NOT_CLOSED"),
        ({"canonical_setup_key": key, "status": "CLOSED", "closed_at_utc": "", "close_reason": "TP"}, "MISSING_TIMESTAMP"),
        ({"canonical_setup_key": key, "status": "CLOSED", "closed_at_utc": "not-a-time", "close_reason": "TP"}, "INVALID_TIMESTAMP"),
        ({"canonical_setup_key": key, "status": "CLOSED", "closed_at_utc": "2026-07-21T20:00:00", "close_reason": "TP"}, "INVALID_TIMESTAMP"),
        ({"canonical_setup_key": key, "status": "CLOSED", "closed_at_utc": "2026-07-21T20:00:00+00:00", "close_reason": ""}, "MISSING_REASON"),
        ({"canonical_setup_key": key, "status": "CLOSED", "closed_at_utc": "2026-07-21T20:00:00+00:00", "close_reason": "MANUAL"}, "UNSUPPORTED_REASON"),
    )
    for payload, expected in malformed:
        before = _snapshot_guard(executor, key)
        result = boundary.observe_ats_position_closed(**payload)
        assert result.failed is True
        assert result.classification == expected
        _assert_unchanged(executor, key, before)

    missing_key = "BTCUSDT|NO_POSITION_ACTIVE"
    missing_ledger = ExecutionEventLedger()
    missing_executor = PaperExecutor(ledger=missing_ledger)
    boundary._SESSION_PAPER_EXECUTOR = missing_executor
    result = boundary.observe_ats_position_closed(
        canonical_setup_key=missing_key,
        status="CLOSED",
        closed_at_utc="2026-07-21T20:00:00+00:00",
        close_reason="TP",
    )
    assert result.classification == "LIFECYCLE_ORDERING_FAILURE"
    assert missing_ledger.events == ()

    # Atomic mutation failure occurs before any real event becomes observable.
    atomic_key = "BTCUSDT|ATOMIC"
    atomic_ledger = ExecutionEventLedger()
    _seed_active(atomic_ledger, atomic_key)
    atomic_executor = PaperExecutor(ledger=atomic_ledger)
    before = _snapshot_guard(atomic_executor, atomic_key)
    with patch.object(atomic_ledger, "_extend_events_atomically", side_effect=RuntimeError("mutation sentinel")):
        try:
            atomic_executor.observe_ats_position_closed(
                atomic_key,
                observed_at_utc="2026-07-21T20:00:00+00:00",
                reason="ats_position_closed:TP",
            )
        except RuntimeError:
            pass
        else:
            raise AssertionError("controlled mutation failure was not raised")
    _assert_unchanged(atomic_executor, atomic_key, before)

    # Valid observation with actual forbidden-path instrumentation.
    valid_key = "BTCUSDT|VALID"
    valid_ledger = ExecutionEventLedger()
    _seed_active(valid_ledger, valid_key)
    valid_executor = PaperExecutor(ledger=valid_ledger)
    boundary._SESSION_PAPER_EXECUTOR = valid_executor
    original_import = builtins.__import__
    gateway_attempts: list[str] = []
    testnet_attempts: list[str] = []
    live_attempts: list[str] = []
    exchange_attempts: list[str] = []
    exchange_calls: list[str] = []

    def guarded_import(name, *args, **kwargs):
        if name.startswith(GATEWAY_IMPORT_PREFIXES):
            gateway_attempts.append(f"import:{name}")
            live_attempts.append(f"gateway_live_surface_import:{name}")
            raise AssertionError(f"gateway module reached: {name}")
        if name.startswith(TESTNET_IMPORT_PREFIXES):
            testnet_attempts.append(f"import:{name}")
            if name.startswith(LIVE_IMPORT_PREFIXES):
                live_attempts.append(f"live_submit_surface_import:{name}")
            raise AssertionError(f"TESTNET module reached: {name}")
        if name.startswith(EXCHANGE_IMPORT_PREFIXES):
            exchange_attempts.append(f"import:{name}")
            raise AssertionError(f"exchange module reached: {name}")
        return original_import(name, *args, **kwargs)

    def network_socket_guard(*args, **kwargs):
        exchange_attempts.append("socket.create_connection")
        exchange_calls.append("socket")
        raise AssertionError("exchange socket reached")

    def network_http_guard(*args, **kwargs):
        exchange_attempts.append("urllib.request.urlopen")
        exchange_calls.append("http")
        raise AssertionError("exchange HTTP reached")

    # Concrete repository call surfaces are patched independently. There is no
    # distinct LIVE module; MODE_LIVE reaches the gateway/real-submit surfaces.
    from backtest.execution.testnet_command_gateway import TestnetCommandGateway
    from backtest.execution.testnet_submit_adapter import TestnetSubmitAdapterSkeleton
    from backtest.execution.testnet_real_submit import RealTestnetSubmitExecutor, UrllibBybitTestnetSubmitTransport
    from backtest.execution.bybit_read_only_adapter import BybitReadOnlyAdapter

    def gateway_surface_guard(*args, **kwargs):
        gateway_attempts.append("TestnetCommandGateway.prepare_command")
        live_attempts.append("MODE_LIVE gateway surface")
        raise AssertionError("gateway callable reached")

    def testnet_surface_guard(*args, **kwargs):
        testnet_attempts.append("TestnetSubmitAdapterSkeleton.submit")
        raise AssertionError("TESTNET submit callable reached")

    def live_surface_guard(*args, **kwargs):
        live_attempts.append("RealTestnetSubmitExecutor.submit_once")
        testnet_attempts.append("RealTestnetSubmitExecutor.submit_once")
        raise AssertionError("LIVE/real-submit callable reached")

    def exchange_surface_guard(*args, **kwargs):
        exchange_attempts.append("BybitReadOnlyAdapter._http_get")
        exchange_calls.append("adapter_http")
        raise AssertionError("exchange adapter callable reached")

    with ExitStack() as stack:
        stack.enter_context(patch.object(builtins, "__import__", guarded_import))
        stack.enter_context(patch.object(socket, "create_connection", network_socket_guard))
        stack.enter_context(patch.object(urllib.request, "urlopen", network_http_guard))
        stack.enter_context(patch.object(TestnetCommandGateway, "prepare_command", gateway_surface_guard))
        stack.enter_context(patch.object(TestnetSubmitAdapterSkeleton, "submit", testnet_surface_guard))
        stack.enter_context(patch.object(RealTestnetSubmitExecutor, "submit_once", live_surface_guard))
        stack.enter_context(patch.object(UrllibBybitTestnetSubmitTransport, "post_order", live_surface_guard))
        stack.enter_context(patch.object(BybitReadOnlyAdapter, "_http_get", exchange_surface_guard))
        result = boundary.observe_ats_position_closed(
            canonical_setup_key=valid_key,
            status=" closed ",
            closed_at_utc="2026-07-21T22:00:00+02:00",
            close_reason="tp",
        )
    assert result.completed is True
    assert result.classification == "CLOSE_OBSERVED"
    valid_snapshot = valid_executor.lifecycle_snapshot(valid_key)
    assert valid_snapshot.current_state == "EXECUTION_COMPLETED"
    assert valid_snapshot.event_count == 7
    assert gateway_attempts == []
    assert testnet_attempts == []
    assert live_attempts == []
    assert exchange_attempts == []
    assert exchange_calls == []

    # Identical and conflicting duplicates are distinguished before mutation.
    before = _snapshot_guard(valid_executor, valid_key)
    duplicate = boundary.observe_ats_position_closed(
        canonical_setup_key=valid_key, status="CLOSED",
        closed_at_utc="2026-07-21T20:00:00+00:00", close_reason="TP",
    )
    assert duplicate.duplicate_blocked is True
    assert duplicate.classification == "DUPLICATE_OBSERVATION"
    _assert_unchanged(valid_executor, valid_key, before)
    conflicting = boundary.observe_ats_position_closed(
        canonical_setup_key=valid_key, status="CLOSED",
        closed_at_utc="2026-07-21T20:01:00+00:00", close_reason="SL",
    )
    assert conflicting.duplicate_blocked is True
    assert conflicting.classification == "CONFLICTING_DUPLICATE_OBSERVATION"
    _assert_unchanged(valid_executor, valid_key, before)

    shell_markers = _shell_integration_probe(boundary)

    required_shell_markers = {
        "closer_called": 1,
        "ATS_close_persisted": 1,
        "executor_observer_called_after_persist": 1,
        "shell_continued": 1,
        "paper_mode_false_closer_calls": 1,
        "paper_mode_false_preparation_reads": 0,
        "paper_mode_false_post_reads": 0,
        "paper_mode_false_diff_calls": 0,
        "paper_mode_false_observer_calls": 0,
        "paper_mode_false_executor_diagnostics": 0,
        "paper_mode_false_closer_result_preserved": 1,
        "ats_close_filesystem_mutation": 1,
        "executor_position_state_mutation": 0,
        "executor_journal_mutation": 0,
        "executor_filesystem_mutation": 0,
        "shell_continuation_after_pre_read_failure": 1,
        "ats_closer_runs_after_pre_read_failure": 1,
        "pre_read_failure_closer_result_preserved": 1,
        "pre_read_failure_observer_calls": 0,
        "shell_continuation_after_post_read_failure": 1,
        "post_read_failure_closer_result_preserved": 1,
        "post_read_failure_observer_calls": 0,
        "shell_continuation_after_transition_derivation_failure": 1,
        "transition_failure_closer_result_preserved": 1,
        "transition_failure_partial_executor_events": 0,
        "shell_continuation_after_observer_failure": 1,
        "shell_continuation_after_diagnostic_failure": 1,
        "ats_closer_exception_preserved": 1,
        "ats_closer_failure_observer_calls": 0,
    }
    for marker, expected in required_shell_markers.items():
        assert shell_markers[marker] == expected, (marker, shell_markers[marker], expected)

    print("SMOKE_E24_3_OK")
    for marker in (
        "closer_called",
        "ATS_close_persisted",
        "executor_observer_called_after_persist",
        "shell_continued",
        "paper_mode_false_closer_calls",
        "paper_mode_false_preparation_reads",
        "paper_mode_false_post_reads",
        "paper_mode_false_diff_calls",
        "paper_mode_false_observer_calls",
        "paper_mode_false_executor_diagnostics",
        "paper_mode_false_closer_result_preserved",
        "ats_close_filesystem_mutation",
        "executor_position_state_mutation",
        "executor_journal_mutation",
        "executor_filesystem_mutation",
        "shell_continuation_after_pre_read_failure",
        "ats_closer_runs_after_pre_read_failure",
        "pre_read_failure_closer_result_preserved",
        "pre_read_failure_observer_calls",
        "shell_continuation_after_post_read_failure",
        "post_read_failure_closer_result_preserved",
        "post_read_failure_observer_calls",
        "shell_continuation_after_transition_derivation_failure",
        "transition_failure_closer_result_preserved",
        "transition_failure_partial_executor_events",
        "shell_continuation_after_observer_failure",
        "shell_continuation_after_diagnostic_failure",
        "ats_closer_exception_preserved",
        "ats_closer_failure_observer_calls",
    ):
        print(f"{marker}={shell_markers[marker]}")
    print("duplicate_blocked=1")
    print("conflicting_duplicate_blocked=1")
    print("atomic_in_memory_validated_batch_append=1")
    print("deterministic_reconstruction=1")
    assert len(gateway_attempts) == 0
    assert len(testnet_attempts) == 0
    assert len(live_attempts) == 0
    assert len(exchange_attempts) == 0
    assert len(exchange_calls) == 0
    print("gateway_unreachable=1")
    print(f"gateway_attempts={len(gateway_attempts)}")
    print("testnet_unreachable=1")
    print(f"testnet_attempts={len(testnet_attempts)}")
    print("live_unreachable=1")
    print(f"live_attempts={len(live_attempts)}")
    print("exchange_unreachable=1")
    print(f"exchange_attempts={len(exchange_attempts)}")
    print(f"exchange_calls={len(exchange_calls)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
