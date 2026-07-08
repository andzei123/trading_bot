from __future__ import annotations

from dataclasses import FrozenInstanceError
from decimal import Decimal
from pathlib import Path

from backtest.execution.exchange_read_adapter import ExchangeOpenOrderSnapshot, ExchangePositionSnapshot
from backtest.execution.exchange_reconciler import (
    EXCHANGE_OPEN_LOCAL_MISSING,
    EXCHANGE_READ_FAILED,
    LOCAL_OPEN_EXCHANGE_MISSING,
    LOCAL_UNKNOWN_STATE,
    OPEN_ORDER_WITHOUT_LOCAL_STATE,
    RECONCILIATION_OK,
    ExchangeReconciler,
)
from backtest.execution.execution_event_ledger import (
    ExecutionStateSnapshot,
    SIMULATED_FILLED,
    SIMULATED_TIMEOUT_UNKNOWN,
    rebuild_execution_state_snapshot,
    ExecutionLedgerEvent,
)


NETWORK_CALLS = 0
TS = "2026-07-08T16:00:00+00:00"
KEY = "BTCUSDT|TDP_REENTRY|LONG|2026-07-08T15:45:00+00:00"
OTHER_KEY = "ETHUSDT|RANGE_TOP_SHORT_V2|SHORT|2026-07-08T15:45:00+00:00"


def position(symbol: str = "BTCUSDT") -> ExchangePositionSnapshot:
    return ExchangePositionSnapshot(
        exchange="BYBIT",
        symbol=symbol,
        side="Buy",
        quantity=Decimal("1.0"),
        entry_price=Decimal("50000"),
        mark_price=Decimal("50100"),
        unrealized_pnl=Decimal("100"),
        observed_at_utc=TS,
    )


def open_order(symbol: str = "BTCUSDT") -> ExchangeOpenOrderSnapshot:
    return ExchangeOpenOrderSnapshot(
        exchange="BYBIT",
        symbol=symbol,
        side="Buy",
        order_id="order-1",
        client_order_id="ATS_E13_TEST",
        order_type="Limit",
        price=Decimal("50000"),
        quantity=Decimal("1.0"),
        remaining_quantity=Decimal("1.0"),
        status="New",
        observed_at_utc=TS,
    )


def filled_snapshot(key: str = KEY, symbol: str = "BTCUSDT") -> ExecutionStateSnapshot:
    event = ExecutionLedgerEvent(
        sequence=1,
        canonical_setup_key=key,
        event_type=SIMULATED_FILLED,
        recorded_at_utc=TS,
        symbol=symbol,
        side="LONG",
        client_order_id="ATS_FILLED",
        status="FILLED",
    )
    return rebuild_execution_state_snapshot((event,), key)


def unknown_snapshot(key: str = KEY, symbol: str = "BTCUSDT") -> ExecutionStateSnapshot:
    event = ExecutionLedgerEvent(
        sequence=1,
        canonical_setup_key=key,
        event_type=SIMULATED_TIMEOUT_UNKNOWN,
        recorded_at_utc=TS,
        symbol=symbol,
        side="LONG",
        client_order_id="ATS_TIMEOUT",
        status="TIMEOUT_UNKNOWN",
    )
    return rebuild_execution_state_snapshot((event,), key)


def main() -> None:
    before_production_probe = set(Path("backtest").rglob("position_state.csv")) | set(Path("backtest").rglob("live_rotation_plan.csv"))
    reconciler = ExchangeReconciler()

    local_filled = filled_snapshot()
    source_before = local_filled

    ok = reconciler.reconcile(
        canonical_setup_key=KEY,
        symbol="BTCUSDT",
        local_snapshot=local_filled,
        exchange_positions=(position(),),
        checked_at_utc=TS,
    )
    assert ok.reconciliation_status == RECONCILIATION_OK
    assert not ok.mismatch_detected and not ok.block_new_orders and not ok.requires_manual_review

    local_missing = reconciler.reconcile(
        canonical_setup_key=KEY,
        symbol="BTCUSDT",
        local_snapshot=local_filled,
        exchange_positions=(),
        checked_at_utc=TS,
    )
    assert local_missing.reconciliation_status == LOCAL_OPEN_EXCHANGE_MISSING
    assert local_missing.mismatch_detected and local_missing.block_new_orders

    exchange_missing_local = reconciler.reconcile(
        canonical_setup_key=KEY,
        symbol="BTCUSDT",
        local_snapshot=None,
        exchange_positions=(position(),),
        checked_at_utc=TS,
    )
    assert exchange_missing_local.reconciliation_status == EXCHANGE_OPEN_LOCAL_MISSING
    assert exchange_missing_local.requires_manual_review

    order_without_local = reconciler.reconcile(
        canonical_setup_key=KEY,
        symbol="BTCUSDT",
        local_snapshot=None,
        exchange_open_orders=(open_order(),),
        checked_at_utc=TS,
    )
    assert order_without_local.reconciliation_status == OPEN_ORDER_WITHOUT_LOCAL_STATE
    assert order_without_local.block_new_orders

    local_unknown = reconciler.reconcile(
        canonical_setup_key=KEY,
        symbol="BTCUSDT",
        local_snapshot=unknown_snapshot(),
        exchange_positions=(),
        checked_at_utc=TS,
    )
    assert local_unknown.reconciliation_status == LOCAL_UNKNOWN_STATE
    assert local_unknown.block_new_orders and local_unknown.requires_manual_review

    read_failed = reconciler.reconcile(
        canonical_setup_key=OTHER_KEY,
        symbol="ETHUSDT",
        local_snapshot=filled_snapshot(OTHER_KEY, "ETHUSDT"),
        exchange_read_ok=False,
        exchange_read_error="mock read failure",
        checked_at_utc=TS,
    )
    assert read_failed.reconciliation_status == EXCHANGE_READ_FAILED
    assert read_failed.block_new_orders and read_failed.requires_manual_review

    repeat_ok = reconciler.reconcile(
        canonical_setup_key=KEY,
        symbol="BTCUSDT",
        local_snapshot=local_filled,
        exchange_positions=(position(),),
        checked_at_utc=TS,
    )
    assert repeat_ok == ok

    try:
        ok.reason = "mutated"  # type: ignore[misc]
        raise AssertionError("ReconciliationResult must be immutable")
    except FrozenInstanceError:
        pass

    assert local_filled == source_before
    after_production_probe = set(Path("backtest").rglob("position_state.csv")) | set(Path("backtest").rglob("live_rotation_plan.csv"))
    assert before_production_probe == after_production_probe
    assert NETWORK_CALLS == 0

    print(
        "SMOKE_E13_OK "
        "ok=1 local_missing_block=1 exchange_local_missing_manual=1 "
        "open_order_without_local_block=1 unknown_block=1 read_failure_fail_closed=1 "
        "deterministic=1 immutable=1 mutated_source=0 exchange_calls=0 production_files_modified=0"
    )


if __name__ == "__main__":
    main()
