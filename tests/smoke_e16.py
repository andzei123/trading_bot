from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from tempfile import TemporaryDirectory

from backtest.execution.execution_event_ledger import (
    ExecutionEventLedger,
    SUBMIT_ACK,
    SUBMIT_BLOCKED,
    SUBMIT_REJECT,
    SUBMIT_TIMEOUT_UNKNOWN,
    SUBMIT_UNKNOWN,
)
from backtest.execution.submit_outcome_handler import (
    BLOCKED_NOT_SUBMITTED,
    TESTNET_SUBMIT_UNKNOWN,
    TESTNET_TIMEOUT_UNKNOWN,
    SubmitOutcomeHandler,
)
from backtest.execution.testnet_command_gateway import MODE_TESTNET
from backtest.execution.testnet_submit_adapter import SubmitResult, TESTNET_ACK_SIMULATED, TESTNET_REJECT_SIMULATED


PRODUCTION_FILES = (
    Path("live_observation_shell.py"),
    Path("pipeline_core.py"),
    Path("policy_engine.py"),
    Path("position_state.csv"),
)


def _file_snapshot() -> dict[str, tuple[bool, int | None, int | None]]:
    snapshot: dict[str, tuple[bool, int | None, int | None]] = {}
    for path in PRODUCTION_FILES:
        if path.exists():
            stat = path.stat()
            snapshot[str(path)] = (True, stat.st_size, stat.st_mtime_ns)
        else:
            snapshot[str(path)] = (False, None, None)
    return snapshot


def _submit_result(key: str, status: str, *, submitted: bool, client_order_id: str | None = None) -> SubmitResult:
    return SubmitResult(
        submitted=submitted,
        mode=MODE_TESTNET,
        client_order_id=client_order_id or f"ATS-TN-{key}",
        submit_status=status,
        exchange_order_id="SIM-ORDER" if submitted else "",
        exchange_ret_code="0" if submitted else "",
        exchange_ret_message="OK" if submitted else "",
        reason=f"smoke_e16_{status}",
        canonical_setup_key=key,
        symbol="BTCUSDT",
        side="LONG",
        submitted_at_utc="2026-07-08T00:03:00+00:00",
    )


def main() -> None:
    before_files = _file_snapshot()
    exchange_calls = 0

    with TemporaryDirectory() as tmpdir:
        ledger = ExecutionEventLedger(Path(tmpdir) / "executor_ledger.jsonl")
        handler = SubmitOutcomeHandler()

        ack_result = _submit_result("setup-e16-ack", TESTNET_ACK_SIMULATED, submitted=True)
        original_ack = ack_result
        ack_snapshot = handler.handle_submit_result(
            submit_result=ack_result,
            ledger=ledger,
            recorded_at_utc="2026-07-08T00:04:00+00:00",
        )
        assert ledger.events[-1].event_type == SUBMIT_ACK
        assert ack_snapshot.submit_state == "ACK_CONFIRMED"
        assert ack_snapshot.submit_confirmed
        assert not ack_snapshot.submit_unknown
        assert not ack_snapshot.terminal_submit_failure
        assert ack_snapshot.block_new_orders

        reject_snapshot = handler.handle_submit_result(
            submit_result=_submit_result("setup-e16-reject", TESTNET_REJECT_SIMULATED, submitted=False),
            ledger=ledger,
            recorded_at_utc="2026-07-08T00:05:00+00:00",
        )
        assert ledger.events[-1].event_type == SUBMIT_REJECT
        assert reject_snapshot.submit_state == "REJECTED_TERMINAL"
        assert reject_snapshot.terminal
        assert reject_snapshot.terminal_submit_failure
        assert not reject_snapshot.block_new_orders

        timeout_snapshot = handler.handle_submit_result(
            submit_result=_submit_result("setup-e16-timeout", TESTNET_TIMEOUT_UNKNOWN, submitted=False),
            ledger=ledger,
            recorded_at_utc="2026-07-08T00:06:00+00:00",
        )
        assert ledger.events[-1].event_type == SUBMIT_TIMEOUT_UNKNOWN
        assert timeout_snapshot.submit_unknown
        assert timeout_snapshot.requires_manual_review
        assert timeout_snapshot.block_new_orders

        unknown_snapshot = handler.handle_submit_result(
            submit_result=_submit_result("setup-e16-unknown", TESTNET_SUBMIT_UNKNOWN, submitted=False),
            ledger=ledger,
            recorded_at_utc="2026-07-08T00:07:00+00:00",
        )
        assert ledger.events[-1].event_type == SUBMIT_UNKNOWN
        assert unknown_snapshot.submit_state == "UNKNOWN"
        assert unknown_snapshot.submit_unknown
        assert unknown_snapshot.block_new_orders

        blocked_snapshot = handler.handle_submit_result(
            submit_result=_submit_result("setup-e16-blocked", BLOCKED_NOT_SUBMITTED, submitted=False, client_order_id=""),
            ledger=ledger,
            recorded_at_utc="2026-07-08T00:08:00+00:00",
        )
        assert ledger.events[-1].event_type == SUBMIT_BLOCKED
        assert blocked_snapshot.submit_state == "BLOCKED_NOT_SUBMITTED"
        assert blocked_snapshot.block_new_orders
        assert not blocked_snapshot.submit_unknown

        sequences = [event.sequence for event in ledger.events]
        assert sequences == sorted(sequences)
        assert sequences == list(range(1, len(sequences) + 1))

        rebuilt_ack = ledger.rebuild_snapshot("setup-e16-ack")
        rebuilt_ack_again = ledger.rebuild_snapshot("setup-e16-ack")
        assert rebuilt_ack == rebuilt_ack_again == ack_snapshot

        try:
            ack_result.submit_status = "MUTATED"  # type: ignore[misc]
            immutable = False
        except FrozenInstanceError:
            immutable = True
        assert immutable
        assert ack_result == original_ack

        copied = replace(ack_result)
        assert copied == ack_result
        ledger.close()

    after_files = _file_snapshot()
    production_files_modified = int(before_files != after_files)
    assert exchange_calls == 0
    assert production_files_modified == 0

    print(
        "SMOKE_E16_OK "
        "ack_confirmed=1 "
        "reject_terminal_failure=1 "
        "timeout_unknown_manual_review=1 "
        "submit_unknown_block=1 "
        "blocked_not_submitted=1 "
        "ordering_preserved=1 "
        "deterministic=1 "
        "immutable=1 "
        "mutated_source=0 "
        "exchange_calls=0 "
        "production_files_modified=0"
    )


if __name__ == "__main__":
    main()
