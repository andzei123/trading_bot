from __future__ import annotations

import ast
import builtins
import importlib.util
import json
import socket
import subprocess
import sys
import urllib.request
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

import backtest.execution.execution_event_ledger as ledger_module
import backtest.execution.shadow_integration_boundary as boundary
from backtest.execution.execution_event_ledger import (
    CLOSE_CONFIRMED,
    CLOSE_REQUESTED,
    EXECUTION_COMPLETED,
    EXCHANGE_READY,
    IDEMPOTENCY_ALLOWED,
    INTENT_ACCEPTED,
    MECHANICAL_SAFETY_PASSED,
    PERSISTENCE_STATE_UNKNOWN,
    SIMULATED_FILLED,
    ExecutionEventLedger,
    ExecutionEventLedgerError,
    LedgerWriterOwnershipError,
    PersistenceStateUnknownError,
    load_persisted_execution_events,
    rebuild_execution_state_snapshot,
)
from backtest.execution.execution_identity_registry import ExecutionIdentityRegistry

TS = "2026-07-22T12:00:00+00:00"
KEY = "BTCUSDT|RANGE_TOP_SHORT_V2|SHORT|E25"

GATEWAY_IMPORT_PREFIXES = ("backtest.execution.testnet_command_gateway",)
TESTNET_IMPORT_PREFIXES = (
    "backtest.execution.testnet_submit_adapter",
    "backtest.execution.testnet_real_submit",
    "backtest.execution.end_to_end_testnet_drill",
)
LIVE_IMPORT_PREFIXES = (
    "backtest.execution.testnet_command_gateway",
    "backtest.execution.testnet_real_submit",
)
EXCHANGE_IMPORT_PREFIXES = (
    "backtest.execution.bybit_read_only_adapter",
    "backtest.execution.exchange_read_adapter",
    "backtest.execution.exchange_reconciler",
)


def _append_active(ledger: ExecutionEventLedger, key: str = KEY) -> None:
    for event_type in (
        INTENT_ACCEPTED,
        MECHANICAL_SAFETY_PASSED,
        IDEMPOTENCY_ALLOWED,
        EXCHANGE_READY,
        SIMULATED_FILLED,
    ):
        ledger.append_event(
            canonical_setup_key=key,
            event_type=event_type,
            recorded_at_utc=TS,
        )


def _row(key: str = KEY) -> dict[str, object]:
    return {
        "symbol": "BTCUSDT",
        "model": "RANGE_TOP_SHORT_V2",
        "side": "SHORT",
        "canonical_setup_key": key,
        "setup_id": "setup-e25-1",
        "setup_created_ts": "2026-07-21T10:00:00+00:00",
        "signal_ts": "2026-07-21T10:15:00+00:00",
        "visible_ts": "2026-07-21T10:15:00+00:00",
        "wait_confirm_ts": "2026-07-21T10:15:00+00:00",
        "intended_entry_ts": "2026-07-21T10:30:00+00:00",
        "entry_window_expires_ts": "2026-07-21T11:00:00+00:00",
        "selected_for_execution": True,
        "execution_rank": 1,
        "selection_reason": "signal_score_rank_1",
        "entry": Decimal("100"),
        "sl": Decimal("101"),
        "tp": Decimal("97"),
        "planned_rr": Decimal("3"),
        "risk_pct": Decimal("0.01"),
        "reward_pct": Decimal("0.03"),
        "risk_distance": Decimal("1"),
        "reward_distance": Decimal("3"),
        "authorized_qty": Decimal("0.5"),
        "authorized_notional": Decimal("50"),
        "authorized_risk_usd": Decimal("0.5"),
        "risk_snapshot_id": "risk-1",
        "position_snapshot_id": "position-1",
        "authority_waterfall_id": "authority-1",
    }


def _load_shell_wrapper():
    shell_path = Path("backtest/journal/live_observation_shell.py")
    source = shell_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(shell_path))
    wanted = {"_run_executor_shadow_diagnostic", "_emit_with_optional_executor_shadow"}
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in wanted]
    emitted: list[dict] = []

    def fake_emit_observation_rows(**kwargs):
        emitted.append(kwargs)
        return 1

    namespace = {"pd": pd, "Path": Path, "_emit_observation_rows": fake_emit_observation_rows}
    module = ast.Module(body=nodes, type_ignores=[])
    ast.fix_missing_locations(module)
    exec(compile(module, str(shell_path), "exec"), namespace)
    return namespace["_emit_with_optional_executor_shadow"], emitted


def _write_records(path: Path, records: list[dict], *, trailing_newline: bool = True) -> None:
    payload = "\n".join(json.dumps(record, sort_keys=True) for record in records)
    if trailing_newline:
        payload += "\n"
    path.write_text(payload, encoding="utf-8", newline="")


def _expect_recovery_failure(path: Path) -> None:
    try:
        ExecutionEventLedger(ledger_path=path)
    except ExecutionEventLedgerError:
        return
    raise AssertionError(f"expected fail-closed recovery for {path}")


class _FakeFile:
    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.write_calls = 0
        self.flush_calls = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def write(self, payload: str) -> int:
        self.write_calls += 1
        if self.mode == "write_exception":
            raise OSError("sentinel write failure")
        if self.mode == "short_write":
            return max(0, len(payload) - 1)
        return len(payload)

    def flush(self) -> None:
        self.flush_calls += 1
        if self.mode == "flush_exception":
            raise OSError("sentinel flush failure")


def _exercise_write_failure(path: Path, mode: str) -> tuple[ExecutionEventLedger, _FakeFile]:
    ledger = ExecutionEventLedger(ledger_path=path)
    fake = _FakeFile(mode)
    with patch.object(Path, "open", return_value=fake):
        try:
            ledger.append_event(
                canonical_setup_key=KEY,
                event_type=INTENT_ACCEPTED,
                recorded_at_utc=TS,
            )
        except PersistenceStateUnknownError:
            pass
        else:
            raise AssertionError(f"expected persistence failure: {mode}")
    assert ledger.persistence_health == PERSISTENCE_STATE_UNKNOWN
    assert ledger.events == ()
    assert ledger.unknown_persistence_sequence == 1
    assert fake.write_calls == 1
    before_calls = fake.write_calls
    try:
        ledger.append_event(
            canonical_setup_key=KEY,
            event_type=INTENT_ACCEPTED,
            recorded_at_utc=TS,
        )
    except PersistenceStateUnknownError:
        pass
    else:
        raise AssertionError("subsequent append was not blocked")
    assert fake.write_calls == before_calls
    return ledger, fake


def main() -> int:
    markers: dict[str, int] = {}
    with TemporaryDirectory() as tmp:
        root = Path(tmp)
        ledger_path = root / "executor" / "execution_event_ledger.jsonl"

        # Active process-restart recovery and direct ExecutionStateSnapshot proof.
        original = ExecutionEventLedger(ledger_path=ledger_path)
        _append_active(original)
        before_lifecycle = original.rebuild_lifecycle_snapshot(KEY)
        before_state = rebuild_execution_state_snapshot(original.events, KEY)
        original_events = original.events
        original.close()

        recovered = ExecutionEventLedger(ledger_path=ledger_path)
        after_lifecycle = recovered.rebuild_lifecycle_snapshot(KEY)
        after_state = rebuild_execution_state_snapshot(recovered.events, KEY)
        assert recovered.events == original_events
        assert after_lifecycle == before_lifecycle
        assert after_state == before_state
        markers["persisted_events_loaded"] = 1
        markers["lifecycle_snapshot_restored"] = 1
        markers["execution_state_snapshot_restored"] = 1
        markers["deterministic_reconstruction"] = 1

        registry = ExecutionIdentityRegistry()
        registry.restore_validated_identities(
            event.canonical_setup_key
            for event in recovered.events
            if event.event_type == INTENT_ACCEPTED
        )
        assert registry.check_and_record(KEY, checked_at_utc=TS).duplicate_detected
        markers["duplicate_protection_restored"] = 1
        recovered.close()

        boundary._release_session_paper_executor()
        result = boundary.observe_ats_position_closed(
            canonical_setup_key=KEY,
            status="CLOSED",
            closed_at_utc="2026-07-22T13:00:00+00:00",
            close_reason="TP",
            executor_ledger_path=ledger_path,
        )
        assert result.completed and not result.failed
        boundary._release_session_paper_executor()
        completed = ExecutionEventLedger(ledger_path=ledger_path)
        completed_before = rebuild_execution_state_snapshot(completed.events, KEY)
        assert completed_before.current_state == "EXECUTION_COMPLETED"
        assert completed_before.terminal is True
        assert completed_before.block_new_orders is False
        assert completed_before.requires_manual_review is False
        completed_events = completed.events
        completed.close()
        completed_again = ExecutionEventLedger(ledger_path=ledger_path)
        completed_after = rebuild_execution_state_snapshot(completed_again.events, KEY)
        assert completed_after == completed_before
        assert completed_again.events == completed_events
        completed_again.close()
        markers["restart_close_observation_resumed"] = 1
        markers["completed_execution_state_snapshot_restored"] = 1

        boundary._release_session_paper_executor()
        duplicate = boundary.observe_ats_position_closed(
            canonical_setup_key=KEY,
            status="CLOSED",
            closed_at_utc="2026-07-22T13:00:00+00:00",
            close_reason="TP",
            executor_ledger_path=ledger_path,
        )
        assert duplicate.classification == "DUPLICATE_OBSERVATION"
        boundary._release_session_paper_executor()
        conflict = boundary.observe_ats_position_closed(
            canonical_setup_key=KEY,
            status="CLOSED",
            closed_at_utc="2026-07-22T13:01:00+00:00",
            close_reason="SL",
            executor_ledger_path=ledger_path,
        )
        assert conflict.classification == "CONFLICTING_DUPLICATE_OBSERVATION"
        boundary._release_session_paper_executor()
        markers["duplicate_close_protection_restored"] = 1
        markers["conflicting_duplicate_protection_restored"] = 1

        # Write outcome latch: short, write exception, flush exception.
        short_ledger, short_fake = _exercise_write_failure(root / "short.jsonl", "short_write")
        markers["persistent_short_write_latched"] = 1
        markers["failed_write_memory_unchanged"] = 1
        markers["subsequent_append_blocked"] = 1
        markers["unknown_commit_not_retried"] = 1
        markers["sequence_not_reused_after_unknown_write"] = 1
        short_ledger.close()

        write_ledger, _ = _exercise_write_failure(root / "write.jsonl", "write_exception")
        markers["persistent_write_exception_latched"] = 1
        write_ledger.close()

        flush_ledger, flush_fake = _exercise_write_failure(root / "flush.jsonl", "flush_exception")
        assert flush_fake.flush_calls == 1
        markers["persistent_flush_exception_latched"] = 1
        flush_ledger.close()

        batch_path = root / "batch.jsonl"
        batch = ExecutionEventLedger(ledger_path=batch_path)
        _append_active(batch)
        before_batch = batch.events
        fake_batch = _FakeFile("write_exception")
        with patch.object(Path, "open", return_value=fake_batch):
            try:
                batch.append_lifecycle_events(
                    canonical_setup_key=KEY,
                    event_types=(CLOSE_REQUESTED, CLOSE_CONFIRMED, EXECUTION_COMPLETED),
                    recorded_at_utc=TS,
                )
            except PersistenceStateUnknownError:
                pass
            else:
                raise AssertionError("expected batch persistence failure")
        assert batch.events == before_batch
        assert batch.persistence_health == PERSISTENCE_STATE_UNKNOWN
        for operation in (
            lambda: batch.append_event(canonical_setup_key=KEY, event_type=CLOSE_REQUESTED, recorded_at_utc=TS),
            lambda: batch.append_lifecycle_events(canonical_setup_key=KEY, event_types=(CLOSE_REQUESTED,), recorded_at_utc=TS),
        ):
            try:
                operation()
            except PersistenceStateUnknownError:
                pass
            else:
                raise AssertionError("unknown ledger mutation was not blocked")
        markers["persistent_batch_failure_latched"] = 1
        markers["failed_batch_memory_unchanged"] = 1
        markers["subsequent_batch_blocked"] = 1
        batch.close()

        # Mechanical single-writer ownership and explicit transfer.
        owner_path = root / "owner.jsonl"
        first = ExecutionEventLedger(ledger_path=owner_path)
        first.append_event(canonical_setup_key=KEY, event_type=INTENT_ACCEPTED, recorded_at_utc=TS)
        owner_bytes = owner_path.read_bytes()
        try:
            ExecutionEventLedger(ledger_path=owner_path)
        except LedgerWriterOwnershipError:
            pass
        else:
            raise AssertionError("second writer was accepted")
        child_code = """from backtest.execution.execution_event_ledger import ExecutionEventLedger, LedgerWriterOwnershipError
import sys
try:
    ExecutionEventLedger(ledger_path=sys.argv[1])
except LedgerWriterOwnershipError:
    print("SECOND_WRITER_BLOCKED")
else:
    raise SystemExit("second writer unexpectedly acquired ownership")
"""
        child = subprocess.run(
            [sys.executable, "-c", child_code, str(owner_path)],
            cwd=str(Path.cwd()),
            capture_output=True,
            text=True,
            check=False,
        )
        assert child.returncode == 0, child.stderr
        assert "SECOND_WRITER_BLOCKED" in child.stdout
        assert owner_path.read_bytes() == owner_bytes
        different = ExecutionEventLedger(ledger_path=root / "different.jsonl")
        different.close()
        readonly = load_persisted_execution_events(owner_path)
        assert readonly == first.events
        first_events = first.events
        first.close()
        transferred = ExecutionEventLedger(ledger_path=owner_path)
        assert transferred.events == first_events
        transferred.close()
        markers["single_writer_enforced"] = 1
        markers["second_writer_fail_closed"] = 1
        markers["second_writer_ledger_unchanged"] = 1
        markers["different_ledger_path_allowed"] = 1
        markers["writer_release_allows_restart"] = 1
        markers["ownership_transfer_preserves_history"] = 1
        markers["readonly_recovery_validation_allowed"] = 1

        # Bounded ownership lifecycle: close before TemporaryDirectory cleanup.
        bounded_tmp = TemporaryDirectory()
        bounded_root = Path(bounded_tmp.name)
        bounded_path = bounded_root / "execution_event_ledger.jsonl"
        bounded = ExecutionEventLedger(ledger_path=bounded_path)
        bounded.append_event(
            canonical_setup_key=KEY,
            event_type=INTENT_ACCEPTED,
            recorded_at_utc=TS,
        )
        lock_path = Path(f"{bounded_path.resolve()}.lock")
        assert lock_path.exists()
        try:
            ExecutionEventLedger(ledger_path=bounded_path)
        except LedgerWriterOwnershipError:
            pass
        else:
            raise AssertionError("second writer acquired bounded ledger while active")
        markers["active_writer_lock_preserved"] = 1
        markers["second_writer_still_blocked"] = 1
        bounded_events = bounded.events
        bounded.close()
        restarted = ExecutionEventLedger(ledger_path=bounded_path)
        assert restarted.events == bounded_events
        restarted.close()
        markers["ownership_transfer_after_close"] = 1
        bounded_tmp.cleanup()
        assert not bounded_root.exists()
        markers["windows_lock_released_before_temp_cleanup"] = 1
        markers["bounded_ledger_close_deterministic"] = 1

        # Shell containment for open-path persistence failure.
        shell_path = root / "shell-open.jsonl"
        shell_key = "BTCUSDT|RANGE_TOP_SHORT_V2|SHORT|SHELL"
        shell_cycle = "2026-07-21T10:30:00+00:00"
        boundary._release_session_paper_executor()
        with patch.object(ledger_module, "_append_persisted_event_lines", side_effect=OSError("open persistence failure")):
            admission = boundary.evaluate_ats_shadow_admission(
                _row(shell_key),
                cycle_ts=shell_cycle,
                checked_at_utc=shell_cycle,
                paper_mode=True,
                executor_ledger_path=shell_path,
            )
        assert admission.paper_requested and admission.paper_started and admission.paper_failed
        boundary._release_session_paper_executor()

        wrapper, emitted = _load_shell_wrapper()
        with patch.object(ledger_module, "_append_persisted_event_lines", side_effect=OSError("open persistence failure")):
            result_count = wrapper(
                out_csv=str(root / "ats.csv"),
                symbol="BTCUSDT",
                latest_ts=pd.Timestamp(shell_cycle),
                out_df=pd.DataFrame([_row(shell_key)]),
                cycle_ts=pd.Timestamp(shell_cycle),
                paper_mode=True,
                executor_ledger_path=shell_path,
            )
        boundary._release_session_paper_executor()
        assert result_count == 1 and len(emitted) == 1
        markers["ats_emission_continues_after_persistence_failure"] = 1

        # Close observation persistence failure is contained and cannot undo ATS fact.
        close_path = root / "shell-close.jsonl"
        seed = ExecutionEventLedger(ledger_path=close_path)
        _append_active(seed, "BTCUSDT|RANGE_TOP_SHORT_V2|SHORT|CLOSEFAIL")
        seed.close()
        boundary._release_session_paper_executor()
        with patch.object(ledger_module, "_append_persisted_event_lines", side_effect=OSError("close persistence failure")):
            close_result = boundary.observe_ats_position_closed(
                canonical_setup_key="BTCUSDT|RANGE_TOP_SHORT_V2|SHORT|CLOSEFAIL",
                status="CLOSED",
                closed_at_utc="2026-07-22T14:00:00+00:00",
                close_reason="TP",
                executor_ledger_path=close_path,
            )
        assert close_result.observed and close_result.failed
        boundary._release_session_paper_executor()
        markers["ats_close_preserved_after_persistence_failure"] = 1

        # Non-paper mode never creates, loads or locks persistent ledger.
        boundary._release_session_paper_executor()
        non_paper_path = root / "must-not-exist.jsonl"
        non_paper = boundary.evaluate_ats_shadow_admission(
            _row("BTCUSDT|RANGE_TOP_SHORT_V2|SHORT|NONPAPER"),
            cycle_ts=TS,
            checked_at_utc=TS,
            paper_mode=False,
            executor_ledger_path=non_paper_path,
        )
        assert non_paper.accepted_by_ingress and not non_paper.paper_requested
        assert boundary._SESSION_PAPER_EXECUTOR is None
        assert not non_paper_path.exists()
        assert not Path(f"{non_paper_path.resolve()}.lock").exists()
        markers["non_paper_persistence_unreachable"] = 1

        # Preserve malformed/truncated/gap/schema/order/identity fail-closed coverage.
        source = ExecutionEventLedger(ledger_path=root / "source.jsonl")
        _append_active(source)
        records = [asdict(event) for event in source.events]
        source.close()
        bad_cases: dict[str, Path] = {}
        malformed = root / "malformed.jsonl"; malformed.write_bytes(b"{bad}\n"); bad_cases["malformed_records_fail_closed"] = malformed
        truncated = root / "truncated.jsonl"; _write_records(truncated, records, trailing_newline=False); bad_cases["truncated_write_fail_closed"] = truncated
        gap = root / "gap.jsonl"; r=[dict(x) for x in records]; r[2]["sequence"]=4; _write_records(gap,r); bad_cases["sequence_gap_fail_closed"] = gap
        dup = root / "dup.jsonl"; r=[dict(x) for x in records]; r[1]["sequence"]=1; _write_records(dup,r); bad_cases["duplicate_event_fail_closed"] = dup
        conflict_seq = root / "conflict-seq.jsonl"; r=[dict(x) for x in records]; r[1]["sequence"]=1; r[1]["event_type"]=EXCHANGE_READY; _write_records(conflict_seq,r); bad_cases["conflicting_event_fail_closed"] = conflict_seq
        unknown = root / "unknown.jsonl"; r=[dict(x) for x in records]; r[2]["event_type"]="UNKNOWN_EVENT"; _write_records(unknown,r); bad_cases["unknown_event_type_fail_closed"] = unknown
        ordering = root / "ordering.jsonl"; r=[dict(records[0]),dict(records[3])]; r[1]["sequence"]=2; _write_records(ordering,r); bad_cases["invalid_lifecycle_ordering_fail_closed"] = ordering
        mismatch = root / "mismatch.jsonl"; r=[dict(x) for x in records]; r[0]["canonical_setup_key"]=" "+KEY; _write_records(mismatch,r); bad_cases["mismatched_identity_fail_closed"] = mismatch
        schema = root / "schema.jsonl"; r=[dict(x) for x in records]; del r[0]["reason"]; _write_records(schema,r); bad_cases["schema_mismatch_fail_closed"] = schema
        for marker, path in bad_cases.items():
            _expect_recovery_failure(path)
            markers[marker] = 1

        incomplete_path = root / "incomplete.jsonl"
        incomplete = ExecutionEventLedger(ledger_path=incomplete_path)
        incomplete.append_event(canonical_setup_key="ETHUSDT|TDP_REENTRY|LONG|E25", event_type=INTENT_ACCEPTED, recorded_at_utc=TS)
        incomplete.close()
        incomplete_recovered = ExecutionEventLedger(ledger_path=incomplete_path)
        snap = incomplete_recovered.rebuild_lifecycle_snapshot("ETHUSDT|TDP_REENTRY|LONG|E25")
        assert not snap.position_active and not snap.completed
        incomplete_recovered.close()
        markers["incomplete_history_not_synthesized"] = 1

        # Independently instrument forbidden categories during recovery.
        gateway_attempts: list[str] = []
        testnet_attempts: list[str] = []
        live_attempts: list[str] = []
        exchange_attempts: list[str] = []
        original_import = builtins.__import__

        def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
            absolute = name
            if level and globals and globals.get("__package__"):
                absolute = importlib.util.resolve_name("." * level + name, globals["__package__"])
            if absolute.startswith(GATEWAY_IMPORT_PREFIXES): gateway_attempts.append(absolute); raise AssertionError(absolute)
            if absolute.startswith(TESTNET_IMPORT_PREFIXES): testnet_attempts.append(absolute); raise AssertionError(absolute)
            if absolute.startswith(LIVE_IMPORT_PREFIXES): live_attempts.append(absolute); raise AssertionError(absolute)
            if absolute.startswith(EXCHANGE_IMPORT_PREFIXES): exchange_attempts.append(absolute); raise AssertionError(absolute)
            return original_import(name, globals, locals, fromlist, level)

        def blocked_socket(*args, **kwargs):
            exchange_attempts.append("socket.create_connection")
            raise AssertionError("network reached")

        def blocked_http(*args, **kwargs):
            exchange_attempts.append("urllib.request.urlopen")
            raise AssertionError("HTTP reached")

        with patch.object(builtins, "__import__", guarded_import), patch.object(socket, "create_connection", blocked_socket), patch.object(urllib.request, "urlopen", blocked_http):
            recovered_events = load_persisted_execution_events(ledger_path)
        assert recovered_events == completed_events
        assert gateway_attempts == []
        assert testnet_attempts == []
        assert live_attempts == []
        assert exchange_attempts == []
        markers["gateway_unreachable"] = 1; markers["gateway_attempts"] = 0
        markers["testnet_unreachable"] = 1; markers["testnet_attempts"] = 0
        markers["live_unreachable"] = 1; markers["live_attempts"] = 0
        markers["exchange_unreachable"] = 1; markers["exchange_attempts"] = 0; markers["exchange_calls"] = 0

    required_one = [name for name, value in markers.items() if not name.endswith("attempts") and name != "exchange_calls"]
    assert required_one and all(markers[name] == 1 for name in required_one)
    assert markers["gateway_attempts"] == markers["testnet_attempts"] == markers["live_attempts"] == 0
    assert markers["exchange_attempts"] == markers["exchange_calls"] == 0
    print("SMOKE_E25_2_OK")
    for name, value in markers.items():
        print(f"{name}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
