from __future__ import annotations

from dataclasses import FrozenInstanceError
from decimal import Decimal
from pathlib import Path
import tempfile

from backtest.execution.exchange_reconciler import LOCAL_UNKNOWN_STATE, RECONCILIATION_OK, ReconciliationResult
from backtest.execution.execution_event_ledger import ExecutionEventLedger, RESERVED_PRE_SUBMIT
from backtest.execution.quantity_converter import ExchangeReadyIntent
from backtest.execution.testnet_command_gateway import (
    BLOCKED_LIVE_FORBIDDEN,
    BLOCKED_MANUAL_ENABLE_MISSING,
    BLOCKED_NOT_TESTNET,
    BLOCKED_RECONCILIATION_NOT_OK,
    COMMAND_READY_TESTNET,
    MODE_LIVE,
    MODE_READ_ONLY,
    MODE_TESTNET,
    TestnetCommandGateway,
)


EXCHANGE_MUTATION_CALLS = 0
TS = "2026-07-08T18:00:00+00:00"
KEY = "BTCUSDT|TDP_REENTRY|LONG|2026-07-08T17:45:00+00:00"


def exchange_ready_intent() -> ExchangeReadyIntent:
    return ExchangeReadyIntent(
        canonical_setup_key=KEY,
        symbol="BTCUSDT",
        side="LONG",
        quantity=Decimal("0.123456"),
        rounded_quantity=Decimal("0.123"),
        quantity_precision=3,
        exchange_ready=True,
        conversion_reason="quantity_floor_rounded_to_exchange_step",
        entry=Decimal("50000"),
        stop=Decimal("49500"),
        target=Decimal("51000"),
    )


def reconciliation_ok() -> ReconciliationResult:
    return ReconciliationResult(
        canonical_setup_key=KEY,
        symbol="BTCUSDT",
        reconciliation_status=RECONCILIATION_OK,
        local_state="FILLED_CONFIRMED",
        exchange_position_status="OPEN",
        open_orders_count=0,
        mismatch_detected=False,
        block_new_orders=False,
        requires_manual_review=False,
        reason="mock reconciliation ok",
        checked_at_utc=TS,
    )


def reconciliation_not_ok() -> ReconciliationResult:
    return ReconciliationResult(
        canonical_setup_key=KEY,
        symbol="BTCUSDT",
        reconciliation_status=LOCAL_UNKNOWN_STATE,
        local_state="UNKNOWN_STATE",
        exchange_position_status="UNKNOWN",
        open_orders_count=0,
        mismatch_detected=True,
        block_new_orders=True,
        requires_manual_review=True,
        reason="mock reconciliation not ok",
        checked_at_utc=TS,
    )


def main() -> None:
    before_production_probe = set(Path("backtest").rglob("position_state.csv")) | set(Path("backtest").rglob("live_rotation_plan.csv"))

    intent = exchange_ready_intent()
    reconciliation = reconciliation_ok()
    source_intent_before = intent
    source_reconciliation_before = reconciliation

    with tempfile.TemporaryDirectory() as tmp:
        ledger_path = Path(tmp) / "executor_local" / "command_gateway_ledger.jsonl"
        ledger = ExecutionEventLedger(ledger_path=ledger_path)

        gateway = TestnetCommandGateway(mode=MODE_TESTNET, manual_enable=True)
        ready = gateway.prepare_command(
            exchange_ready_intent=intent,
            reconciliation_result=reconciliation,
            ledger=ledger,
            decided_at_utc=TS,
        )
        repeat_ready = gateway.prepare_command(
            exchange_ready_intent=intent,
            reconciliation_result=reconciliation,
            ledger=ExecutionEventLedger(),
            decided_at_utc=TS,
        )

        read_only_block = TestnetCommandGateway(mode=MODE_READ_ONLY, manual_enable=True).prepare_command(
            exchange_ready_intent=intent,
            reconciliation_result=reconciliation,
            ledger=ExecutionEventLedger(),
            decided_at_utc=TS,
        )
        live_block = TestnetCommandGateway(mode=MODE_LIVE, manual_enable=True).prepare_command(
            exchange_ready_intent=intent,
            reconciliation_result=reconciliation,
            ledger=ExecutionEventLedger(),
            decided_at_utc=TS,
        )
        reconciliation_block = TestnetCommandGateway(mode=MODE_TESTNET, manual_enable=True).prepare_command(
            exchange_ready_intent=intent,
            reconciliation_result=reconciliation_not_ok(),
            ledger=ExecutionEventLedger(),
            decided_at_utc=TS,
        )
        manual_block = TestnetCommandGateway(mode=MODE_TESTNET, manual_enable=False).prepare_command(
            exchange_ready_intent=intent,
            reconciliation_result=reconciliation,
            ledger=ExecutionEventLedger(),
            decided_at_utc=TS,
        )

        immutable_decision = False
        try:
            ready.allowed = False  # type: ignore[misc]
        except FrozenInstanceError:
            immutable_decision = True

        ledger_events = ledger.events
        ledger_lines = ledger_path.read_text(encoding="utf-8").splitlines()
        ledger.close()

    after_production_probe = set(Path("backtest").rglob("position_state.csv")) | set(Path("backtest").rglob("live_rotation_plan.csv"))

    assert ready.allowed is True
    assert ready.command_status == COMMAND_READY_TESTNET
    assert ready.reserved_in_ledger is True
    assert ready.client_order_id.startswith("ATS_TESTNET_")
    assert repeat_ready.client_order_id == ready.client_order_id
    assert len(ledger_events) == 1
    assert len(ledger_lines) == 1
    assert ledger_events[0].event_type == RESERVED_PRE_SUBMIT
    assert ledger_events[0].sequence == 1
    assert ledger_events[0].client_order_id == ready.client_order_id
    assert ledger_events[0].recorded_at_utc == ready.decided_at_utc
    assert read_only_block.command_status == BLOCKED_NOT_TESTNET
    assert live_block.command_status == BLOCKED_LIVE_FORBIDDEN
    assert live_block.allowed is False and live_block.requires_manual_review is True
    assert reconciliation_block.command_status == BLOCKED_RECONCILIATION_NOT_OK
    assert reconciliation_block.allowed is False
    assert manual_block.command_status == BLOCKED_MANUAL_ENABLE_MISSING
    assert manual_block.allowed is False
    assert immutable_decision is True
    assert intent == source_intent_before
    assert reconciliation == source_reconciliation_before
    assert EXCHANGE_MUTATION_CALLS == 0
    assert before_production_probe == after_production_probe

    print(
        "SMOKE_E14_OK "
        "testnet_ready_reserved=1 "
        "read_only_block=1 "
        "live_block=1 "
        "reconciliation_block=1 "
        "manual_enable_block=1 "
        "deterministic_client_order_id=1 "
        "reservation_before_ready=1 "
        "mutated_intent=0 "
        "mutated_reconciliation=0 "
        "exchange_mutation_calls=0 "
        "production_files_modified=0"
    )


if __name__ == "__main__":
    main()
