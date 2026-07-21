from __future__ import annotations

import builtins
import importlib.util
import os
import socket
import subprocess
import sys
import urllib.request
from contextlib import ExitStack
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch


FORBIDDEN_IMPORT_PREFIXES = (
    "backtest.execution.testnet_command_gateway",
    "backtest.execution.testnet_submit_adapter",
    "backtest.execution.testnet_real_submit",
    "backtest.execution.end_to_end_testnet_drill",
    "backtest.execution.bybit_read_only_adapter",
    "backtest.execution.exchange_read_adapter",
    "backtest.execution.exchange_reconciler",
)


def _row() -> dict[str, object]:
    return {
        "schema_version": "ATS_EXECUTION_INTENT_V1",
        "cycle_ts": "2026-07-21T18:00:00+00:00",
        "symbol": "BTCUSDT",
        "model": "RANGE_TOP_SHORT_V2",
        "side": "SHORT",
        "canonical_setup_key": "BTCUSDT|RANGE_TOP_SHORT_V2|SHORT|E23.1",
        "setup_id": "setup-e23-1",
        "setup_created_ts": "2026-07-21T17:30:00+00:00",
        "signal_ts": "2026-07-21T17:45:00+00:00",
        "visible_ts": "2026-07-21T17:45:00+00:00",
        "wait_confirm_ts": "2026-07-21T17:45:00+00:00",
        "intended_entry_ts": "2026-07-21T18:00:00+00:00",
        "entry_window_expires_ts": "2026-07-21T18:15:00+00:00",
        "selected_for_execution": "true",
        "execution_rank": "1",
        "selection_reason": "signal_score_rank_1",
        "entry": "100",
        "sl": "101",
        "tp": "97",
        "planned_rr": "3",
        "risk_pct": "0.01",
        "reward_pct": "0.03",
        "risk_distance": "1",
        "reward_distance": "3",
        "authorized_qty": "0.5",
        "authorized_notional": "50",
        "authorized_risk_usd": "0.5",
        "risk_snapshot_id": "risk-1",
        "position_snapshot_id": "position-1",
        "authority_waterfall_id": "authority-1",
    }


def _guard_import(original_import):
    def guarded(name, globals=None, locals=None, fromlist=(), level=0):
        absolute_name = name
        if level > 0 and globals is not None and globals.get("__package__"):
            absolute_name = importlib.util.resolve_name("." * level + name, str(globals["__package__"]))
        if absolute_name.startswith(FORBIDDEN_IMPORT_PREFIXES):
            raise AssertionError(f"forbidden execution path imported: {absolute_name}")
        return original_import(name, globals, locals, fromlist, level)

    return guarded


def _mutation_guards():
    original_path_open = Path.open
    original_open = builtins.open

    def reject_mode(mode):
        if any(flag in str(mode) for flag in ("w", "a", "x", "+")):
            raise AssertionError(f"filesystem mutation reached: mode={mode}")

    def guarded_path_open(self, mode="r", *args, **kwargs):
        reject_mode(mode)
        return original_path_open(self, mode, *args, **kwargs)

    def guarded_open(file, mode="r", *args, **kwargs):
        reject_mode(mode)
        return original_open(file, mode, *args, **kwargs)

    return (
        patch.object(Path, "open", guarded_path_open),
        patch.object(Path, "write_text", side_effect=AssertionError("Path.write_text mutation reached")),
        patch.object(Path, "write_bytes", side_effect=AssertionError("Path.write_bytes mutation reached")),
        patch.object(Path, "mkdir", side_effect=AssertionError("Path.mkdir mutation reached")),
        patch.object(builtins, "open", guarded_open),
        patch.object(socket, "create_connection", side_effect=AssertionError("exchange socket reached")),
        patch.object(urllib.request, "urlopen", side_effect=AssertionError("exchange HTTP reached")),
    )


def _clean_probe_main() -> int:
    sys.dont_write_bytecode = True
    original_import = builtins.__import__
    with ExitStack() as stack:
        stack.enter_context(patch.object(builtins, "__import__", _guard_import(original_import)))
        from backtest.execution.decision_consumer import consume_decision_row
        from backtest.execution.execution_event_ledger import ExecutionEventLedger
        from backtest.execution.execution_identity_registry import ExecutionIdentityRegistry
        from backtest.execution.paper_executor import PaperExecutionRequest, PaperExecutor

        for guard in _mutation_guards():
            stack.enter_context(guard)
        intent = consume_decision_row(_row())
        executor = PaperExecutor(ledger=ExecutionEventLedger(), identity_registry=ExecutionIdentityRegistry())
        executor.run(
            intent,
            PaperExecutionRequest(
                checked_at_utc="2026-07-21T18:00:00+00:00",
                simulated_at_utc="2026-07-21T18:00:01+00:00",
            ),
        )
        executor.observe_close_requested(intent.canonical_setup_key)
        executor.observe_close_confirmed(intent.canonical_setup_key)
        executor.observe_execution_completed(intent.canonical_setup_key)
    print("CLEAN_E23_IMPORT_MUTATION_PROBE_OK")
    return 0


def _clean_import_probe() -> None:
    env = dict(os.environ)
    env["ATS_E23_CLEAN_PROBE"] = "1"
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    completed = subprocess.run(
        [sys.executable, "-m", "backtest.execution.tests.smoke_e23"],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    if completed.returncode != 0:
        raise AssertionError(
            "clean E23 import/mutation probe failed\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    assert "CLEAN_E23_IMPORT_MUTATION_PROBE_OK" in completed.stdout


def _append_raw(ledger, key: str, event_types: tuple[str, ...]) -> None:
    for event_type in event_types:
        ledger.append_event(canonical_setup_key=key, event_type=event_type)


def _assert_rebuild_blocked(event_types: tuple[str, ...]) -> None:
    from backtest.execution.execution_event_ledger import ExecutionEventLedger, ExecutionLifecycleTransitionError

    ledger = ExecutionEventLedger()
    key = "MALFORMED|" + "|".join(event_types)
    _append_raw(ledger, key, event_types)
    try:
        ledger.rebuild_lifecycle_snapshot(key)
    except ExecutionLifecycleTransitionError:
        return
    raise AssertionError(f"malformed lifecycle stream returned a snapshot: {event_types}")


def _assert_duplicate_blocked(executor, key: str, operation) -> None:
    from backtest.execution.paper_executor import PaperExecutorError

    before_events = executor.ledger.events
    before_snapshot = executor.lifecycle_snapshot(key)
    try:
        operation()
    except PaperExecutorError:
        pass
    else:
        raise AssertionError("duplicate lifecycle transition was not blocked")
    assert executor.ledger.events == before_events
    assert len(executor.ledger.events) == len(before_events)
    assert executor.lifecycle_snapshot(key) == before_snapshot


def main() -> int:
    _clean_import_probe()

    from backtest.execution.decision_consumer import consume_decision_row
    from backtest.execution.execution_event_ledger import (
        CLOSE_CONFIRMED,
        CLOSE_REQUESTED,
        EXECUTION_COMPLETED,
        EXCHANGE_READY,
        INTENT_ACCEPTED,
        MECHANICAL_SAFETY_PASSED,
        SIMULATED_ACKED,
        SIMULATED_FILLED,
        ExecutionEventLedger,
    )
    from backtest.execution.execution_identity_registry import ExecutionIdentityRegistry
    from backtest.execution.execution_simulator import PARTIALLY_FILLED
    from backtest.execution.paper_executor import PaperExecutionRequest, PaperExecutor

    row = _row()
    before_row = deepcopy(row)
    intent = consume_decision_row(row)
    executor = PaperExecutor(ledger=ExecutionEventLedger(), identity_registry=ExecutionIdentityRegistry())

    with ExitStack() as stack:
        for guard in _mutation_guards():
            stack.enter_context(guard)
        open_snapshot = executor.run(
            intent,
            PaperExecutionRequest(
                checked_at_utc="2026-07-21T18:00:00+00:00",
                simulated_at_utc="2026-07-21T18:00:01+00:00",
            ),
        )
        active = executor.lifecycle_snapshot(intent.canonical_setup_key)
        active_repeat = executor.lifecycle_snapshot(intent.canonical_setup_key)
        requested = executor.observe_close_requested(intent.canonical_setup_key)
        _assert_duplicate_blocked(
            executor,
            intent.canonical_setup_key,
            lambda: executor.observe_close_requested(intent.canonical_setup_key),
        )
        confirmed = executor.observe_close_confirmed(intent.canonical_setup_key)
        _assert_duplicate_blocked(
            executor,
            intent.canonical_setup_key,
            lambda: executor.observe_close_confirmed(intent.canonical_setup_key),
        )
        completed = executor.observe_execution_completed(intent.canonical_setup_key)
        _assert_duplicate_blocked(
            executor,
            intent.canonical_setup_key,
            lambda: executor.observe_execution_completed(intent.canonical_setup_key),
        )

    expected_open = (
        "INTENT_ACCEPTED",
        "MECHANICAL_ALLOWED",
        "OPEN_REQUESTED",
        "OPEN_CONFIRMED",
        "FULLY_FILLED",
        "POSITION_ACTIVE",
    )
    expected_complete = expected_open + (
        "CLOSE_REQUESTED",
        "CLOSE_CONFIRMED",
        "EXECUTION_COMPLETED",
    )
    assert row == before_row
    assert open_snapshot.current_state == "FILLED_CONFIRMED"
    assert active == active_repeat
    assert active.transition_history == expected_open
    assert active.current_state == "POSITION_ACTIVE" and active.position_active and not active.completed
    assert requested.current_state == "CLOSE_REQUESTED"
    assert confirmed.current_state == "CLOSE_CONFIRMED"
    assert completed.current_state == "EXECUTION_COMPLETED"
    assert completed.transition_history == expected_complete
    assert completed.completed and not completed.position_active
    assert executor.lifecycle_snapshot(intent.canonical_setup_key) == completed
    assert [event.sequence for event in executor.ledger.events] == list(range(1, len(executor.ledger.events) + 1))

    staged = ExecutionEventLedger()
    staged_key = "VALID|STAGED|E23.1"
    staged.append_event(canonical_setup_key=staged_key, event_type=INTENT_ACCEPTED)
    assert staged.rebuild_lifecycle_snapshot(staged_key).current_state == "INTENT_ACCEPTED"
    staged.append_event(canonical_setup_key=staged_key, event_type=MECHANICAL_SAFETY_PASSED)
    assert staged.rebuild_lifecycle_snapshot(staged_key).current_state == "MECHANICAL_ALLOWED"
    staged.append_event(canonical_setup_key=staged_key, event_type=EXCHANGE_READY)
    assert staged.rebuild_lifecycle_snapshot(staged_key).current_state == "OPEN_REQUESTED"
    staged.append_event(canonical_setup_key=staged_key, event_type=SIMULATED_ACKED)
    assert staged.rebuild_lifecycle_snapshot(staged_key).current_state == "OPEN_CONFIRMED"
    staged.append_event(canonical_setup_key=staged_key, event_type=SIMULATED_FILLED)
    assert staged.rebuild_lifecycle_snapshot(staged_key).current_state == "POSITION_ACTIVE"
    staged.append_lifecycle_event(canonical_setup_key=staged_key, event_type=CLOSE_REQUESTED)
    assert staged.rebuild_lifecycle_snapshot(staged_key).current_state == "CLOSE_REQUESTED"
    staged.append_lifecycle_event(canonical_setup_key=staged_key, event_type=CLOSE_CONFIRMED)
    assert staged.rebuild_lifecycle_snapshot(staged_key).current_state == "CLOSE_CONFIRMED"
    staged.append_lifecycle_event(canonical_setup_key=staged_key, event_type=EXECUTION_COMPLETED)
    assert staged.rebuild_lifecycle_snapshot(staged_key).current_state == "EXECUTION_COMPLETED"

    _assert_rebuild_blocked((CLOSE_CONFIRMED,))
    _assert_rebuild_blocked((EXECUTION_COMPLETED,))
    _assert_rebuild_blocked((CLOSE_REQUESTED,))
    _assert_rebuild_blocked((MECHANICAL_SAFETY_PASSED,))
    _assert_rebuild_blocked((SIMULATED_ACKED,))
    _assert_rebuild_blocked((INTENT_ACCEPTED, EXCHANGE_READY))
    _assert_rebuild_blocked((INTENT_ACCEPTED, MECHANICAL_SAFETY_PASSED, SIMULATED_FILLED))
    _assert_rebuild_blocked((INTENT_ACCEPTED, MECHANICAL_SAFETY_PASSED, EXCHANGE_READY, CLOSE_CONFIRMED))
    _assert_rebuild_blocked((
        INTENT_ACCEPTED,
        MECHANICAL_SAFETY_PASSED,
        EXCHANGE_READY,
        SIMULATED_FILLED,
        CLOSE_REQUESTED,
        CLOSE_CONFIRMED,
        EXECUTION_COMPLETED,
        CLOSE_REQUESTED,
    ))

    partial_row = _row()
    partial_row["canonical_setup_key"] = "BTCUSDT|RANGE_TOP_SHORT_V2|SHORT|E23.1|PARTIAL"
    partial_executor = PaperExecutor(ledger=ExecutionEventLedger(), identity_registry=ExecutionIdentityRegistry())
    partial_executor.run(
        consume_decision_row(partial_row),
        PaperExecutionRequest(
            simulation_outcome=PARTIALLY_FILLED,
            checked_at_utc="2026-07-21T18:00:00+00:00",
            simulated_at_utc="2026-07-21T18:00:01+00:00",
        ),
    )
    partial = partial_executor.lifecycle_snapshot(str(partial_row["canonical_setup_key"]))
    assert partial.current_state == "PARTIALLY_FILLED"
    assert partial.transition_history[-2:] == ("OPEN_CONFIRMED", "PARTIALLY_FILLED")
    assert not partial.position_active and not partial.completed

    print("SMOKE_E23_1_OK")
    print("valid_transition_history=1")
    print("deterministic_reconstruction=1")
    print("ledger_sequence_continuity=1")
    print("immutable_input=1")
    print("partial_fill_preserved=1")
    print("duplicate_close_requested_blocked=1")
    print("duplicate_close_confirmed_blocked=1")
    print("duplicate_execution_completed_blocked=1")
    print("duplicate_event_count_unchanged=1")
    print("duplicate_sequence_unchanged=1")
    print("duplicate_snapshot_unchanged=1")
    print("malformed_close_confirmed_blocked=1")
    print("malformed_execution_completed_blocked=1")
    print("malformed_close_requested_blocked=1")
    print("malformed_open_order_blocked=1")
    print("post_completion_transition_blocked=1")
    print("lifecycle_rebuild_fail_closed=1")
    print("journal_mutation=0")
    print("filesystem_mutation=0")
    print("gateway_unreachable=1")
    print("testnet_unreachable=1")
    print("live_unreachable=1")
    print("exchange_unreachable=1")
    print("exchange_calls=0")
    return 0


if os.environ.get("ATS_E23_CLEAN_PROBE") == "1":
    raise SystemExit(_clean_probe_main())

if __name__ == "__main__":
    raise SystemExit(main())
