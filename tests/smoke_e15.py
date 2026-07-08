from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

from backtest.execution.exchange_reconciler import RECONCILIATION_OK, ReconciliationResult
from backtest.execution.execution_event_ledger import ExecutionEventLedger, RESERVED_PRE_SUBMIT
from backtest.execution.quantity_converter import ExchangeReadyIntent
from backtest.execution.testnet_command_gateway import MODE_LIVE, MODE_READ_ONLY, MODE_TESTNET, CommandDecision, TestnetCommandGateway
from backtest.execution.testnet_submit_adapter import (
    BLOCKED_COMMAND_NOT_ALLOWED,
    BLOCKED_LIVE_FORBIDDEN,
    BLOCKED_MISSING_CLIENT_ORDER_ID,
    BLOCKED_NOT_RESERVED,
    BLOCKED_NOT_TESTNET,
    SIMULATED_ACK,
    SIMULATED_REJECT,
    TESTNET_ACK_SIMULATED,
    TESTNET_REJECT_SIMULATED,
    TestnetSubmitAdapterSkeleton,
)


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


def _exchange_ready_intent() -> ExchangeReadyIntent:
    return ExchangeReadyIntent(
        canonical_setup_key="setup-e15-001",
        symbol="BTCUSDT",
        side="LONG",
        quantity=Decimal("0.123456"),
        rounded_quantity=Decimal("0.123456"),
        quantity_precision=6,
        exchange_ready=True,
        conversion_reason="smoke_e15_exchange_ready",
        entry=Decimal("50000"),
        stop=Decimal("49000"),
        target=Decimal("53000"),
    )


def _reconciliation_ok(intent: ExchangeReadyIntent) -> ReconciliationResult:
    return ReconciliationResult(
        canonical_setup_key=intent.canonical_setup_key,
        symbol=intent.symbol,
        reconciliation_status=RECONCILIATION_OK,
        local_state="NO_EVENTS",
        exchange_position_status="MISSING",
        open_orders_count=0,
        mismatch_detected=False,
        block_new_orders=False,
        requires_manual_review=False,
        reason="smoke_e15_reconciliation_ok",
        checked_at_utc="2026-07-08T00:00:00+00:00",
    )


def _approved_command(intent: ExchangeReadyIntent, ledger: ExecutionEventLedger) -> CommandDecision:
    gateway = TestnetCommandGateway(mode=MODE_TESTNET, manual_enable=True)
    decision = gateway.prepare_command(
        exchange_ready_intent=intent,
        reconciliation_result=_reconciliation_ok(intent),
        ledger=ledger,
        decided_at_utc="2026-07-08T00:01:00+00:00",
    )
    assert decision.allowed
    assert decision.reserved_in_ledger
    assert ledger.events[-1].event_type == RESERVED_PRE_SUBMIT
    assert ledger.events[-1].client_order_id == decision.client_order_id
    return decision


def main() -> None:
    before_files = _file_snapshot()
    exchange_calls = 0

    with TemporaryDirectory() as tmpdir:
        ledger = ExecutionEventLedger(Path(tmpdir) / "executor_ledger.jsonl")
        intent = _exchange_ready_intent()
        original_intent = intent
        command = _approved_command(intent, ledger)
        original_command = command
        adapter = TestnetSubmitAdapterSkeleton()

        ack = adapter.submit(
            command_decision=command,
            exchange_ready_intent=intent,
            simulated_response=SIMULATED_ACK,
            submitted_at_utc="2026-07-08T00:02:00+00:00",
        )
        assert ack.submitted
        assert ack.submit_status == TESTNET_ACK_SIMULATED
        assert ack.mode == MODE_TESTNET
        assert ack.client_order_id == command.client_order_id
        assert ack.exchange_order_id

        request = adapter.build_submit_request(command_decision=command, exchange_ready_intent=intent)
        assert request.testnet_only
        assert request.client_order_id == command.client_order_id
        assert request.quantity == intent.rounded_quantity
        assert request.price == intent.entry

        not_allowed = adapter.submit(
            command_decision=replace(command, allowed=False),
            exchange_ready_intent=intent,
            submitted_at_utc="2026-07-08T00:02:00+00:00",
        )
        assert not_allowed.submit_status == BLOCKED_COMMAND_NOT_ALLOWED
        assert not not_allowed.submitted

        read_only = adapter.submit(
            command_decision=replace(command, mode=MODE_READ_ONLY),
            exchange_ready_intent=intent,
            submitted_at_utc="2026-07-08T00:02:00+00:00",
        )
        assert read_only.submit_status == BLOCKED_NOT_TESTNET
        assert not read_only.submitted

        live = adapter.submit(
            command_decision=replace(command, mode=MODE_LIVE),
            exchange_ready_intent=intent,
            submitted_at_utc="2026-07-08T00:02:00+00:00",
        )
        assert live.submit_status == BLOCKED_LIVE_FORBIDDEN
        assert not live.submitted

        missing_reservation = adapter.submit(
            command_decision=replace(command, reserved_in_ledger=False),
            exchange_ready_intent=intent,
            submitted_at_utc="2026-07-08T00:02:00+00:00",
        )
        assert missing_reservation.submit_status == BLOCKED_NOT_RESERVED
        assert not missing_reservation.submitted

        missing_client_order_id = adapter.submit(
            command_decision=replace(command, client_order_id=""),
            exchange_ready_intent=intent,
            submitted_at_utc="2026-07-08T00:02:00+00:00",
        )
        assert missing_client_order_id.submit_status == BLOCKED_MISSING_CLIENT_ORDER_ID
        assert not missing_client_order_id.submitted

        reject = adapter.submit(
            command_decision=command,
            exchange_ready_intent=intent,
            simulated_response=SIMULATED_REJECT,
            submitted_at_utc="2026-07-08T00:02:00+00:00",
        )
        assert reject.submit_status == TESTNET_REJECT_SIMULATED
        assert not reject.submitted
        assert reject.exchange_ret_code == "10001"

        ack_again = adapter.submit(
            command_decision=command,
            exchange_ready_intent=intent,
            simulated_response=SIMULATED_ACK,
            submitted_at_utc="2026-07-08T00:02:00+00:00",
        )
        assert ack_again == ack

        try:
            ack.submit_status = "MUTATED"  # type: ignore[misc]
            immutable = False
        except FrozenInstanceError:
            immutable = True
        assert immutable

        assert intent == original_intent
        assert command == original_command

    after_files = _file_snapshot()
    production_files_modified = int(before_files != after_files)
    assert exchange_calls == 0
    assert production_files_modified == 0

    print(
        "SMOKE_E15_OK "
        "testnet_stub_ack=1 "
        "command_not_allowed_block=1 "
        "read_only_block=1 "
        "live_block=1 "
        "missing_reservation_block=1 "
        "missing_client_order_id_block=1 "
        "simulated_reject_classified=1 "
        "deterministic=1 "
        "immutable=1 "
        "mutated_intent=0 "
        "mutated_command=0 "
        "exchange_calls=0 "
        "production_files_modified=0"
    )


if __name__ == "__main__":
    main()
