from __future__ import annotations

import copy
import hashlib
from dataclasses import FrozenInstanceError
from decimal import Decimal
from pathlib import Path

from backtest.execution.exchange_read_adapter import (
    ExchangeBalanceSnapshot,
    ExchangeInstrumentRules,
    ExchangeOpenOrderSnapshot,
    ExchangePositionSnapshot,
    ExchangeStateSnapshot,
)
from backtest.execution.execution_event_ledger import ExecutionEventLedger
from backtest.execution.post_submit_reconciler import (
    ACK_BUT_ORDER_MISSING,
    EXCHANGE_POSITION_FOUND,
    EXCHANGE_READ_FAILED,
    POST_SUBMIT_RECONCILIATION_OK,
    REJECT_CONFIRMED_NO_ORDER,
    UNKNOWN_REQUIRES_EXCHANGE_CHECK,
    PostSubmitReconciler,
)
from backtest.execution.submit_outcome_handler import SubmitOutcomeHandler, TESTNET_SUBMIT_UNKNOWN
from backtest.execution.testnet_submit_adapter import (
    SubmitResult,
    TESTNET_ACK_SIMULATED,
    TESTNET_REJECT_SIMULATED,
    TESTNET_TIMEOUT_UNKNOWN_SIMULATED,
)

PRODUCTION_FILES = (
    "live_observation_shell.py",
    "pipeline_core.py",
    "policy_engine.py",
    "position_state.csv",
)

NOW = "2026-01-01T00:00:00+00:00"


def _hash_existing_production_files() -> dict[str, str]:
    hashes: dict[str, str] = {}
    for filename in PRODUCTION_FILES:
        path = Path(filename)
        if path.exists():
            hashes[filename] = hashlib.sha256(path.read_bytes()).hexdigest()
    return hashes


def _submit(key: str, status: str, *, submitted: bool = True, client_order_id: str | None = None) -> SubmitResult:
    return SubmitResult(
        submitted=submitted,
        mode="TESTNET",
        client_order_id=client_order_id or f"ATS-TN-{key}",
        submit_status=status,
        exchange_order_id="EX-1" if submitted else "",
        exchange_ret_code="0" if submitted else "10001",
        exchange_ret_message="OK" if submitted else "reject",
        reason="smoke",
        canonical_setup_key=key,
        symbol="BTCUSDT",
        side="LONG",
        submitted_at_utc=NOW,
    )


def _snapshot(submit_result: SubmitResult):
    ledger = ExecutionEventLedger()
    return SubmitOutcomeHandler().handle_submit_result(submit_result=submit_result, ledger=ledger, recorded_at_utc=NOW)


def _exchange_state(*, order: bool = False, position: bool = False) -> ExchangeStateSnapshot:
    orders = ()
    positions = ()
    if order:
        orders = (
            ExchangeOpenOrderSnapshot(
                exchange="BYBIT_TESTNET",
                symbol="BTCUSDT",
                side="Buy",
                order_id="EX-1",
                client_order_id="ATS-TN-k-ack",
                order_type="Limit",
                price=Decimal("50000"),
                quantity=Decimal("0.001"),
                remaining_quantity=Decimal("0.001"),
                status="New",
                observed_at_utc=NOW,
            ),
        )
    if position:
        positions = (
            ExchangePositionSnapshot(
                exchange="BYBIT_TESTNET",
                symbol="BTCUSDT",
                side="Buy",
                quantity=Decimal("0.001"),
                entry_price=Decimal("50000"),
                mark_price=Decimal("50010"),
                unrealized_pnl=Decimal("0.01"),
                observed_at_utc=NOW,
            ),
        )
    return ExchangeStateSnapshot(
        exchange="BYBIT_TESTNET",
        server_time_utc=NOW,
        balances=(
            ExchangeBalanceSnapshot(
                exchange="BYBIT_TESTNET",
                account_type="UNIFIED",
                asset="USDT",
                wallet_balance=Decimal("1000"),
                available_balance=Decimal("900"),
                equity=Decimal("1000"),
                observed_at_utc=NOW,
            ),
        ),
        positions=positions,
        open_orders=orders,
        instrument_rules=(
            ExchangeInstrumentRules(
                exchange="BYBIT_TESTNET",
                symbol="BTCUSDT",
                quantity_step=Decimal("0.001"),
                min_quantity=Decimal("0.001"),
                quantity_precision=3,
                price_tick=Decimal("0.1"),
                price_precision=1,
                min_notional=Decimal("5"),
                observed_at_utc=NOW,
            ),
        ),
        observed_at_utc=NOW,
    )


def main() -> None:
    before = _hash_existing_production_files()
    exchange_calls = 0
    reconciler = PostSubmitReconciler()

    ack = _submit("k-ack", TESTNET_ACK_SIMULATED)
    ack_snapshot = _snapshot(ack)
    ack_before = copy.deepcopy(ack)
    order_state = _exchange_state(order=True)
    order_state_before = copy.deepcopy(order_state)
    ok = reconciler.reconcile(
        submit_result=ack,
        local_snapshot=ack_snapshot,
        exchange_state=order_state,
        checked_at_utc=NOW,
    )
    assert ok.post_submit_status == POST_SUBMIT_RECONCILIATION_OK
    assert not ok.block_new_orders and not ok.requires_manual_review

    missing = reconciler.reconcile(
        submit_result=ack,
        local_snapshot=ack_snapshot,
        exchange_state=_exchange_state(),
        checked_at_utc=NOW,
    )
    assert missing.post_submit_status == ACK_BUT_ORDER_MISSING
    assert missing.block_new_orders and missing.requires_manual_review

    reject = _submit("k-reject", TESTNET_REJECT_SIMULATED, submitted=False)
    reject_snapshot = _snapshot(reject)
    reject_result = reconciler.reconcile(
        submit_result=reject,
        local_snapshot=reject_snapshot,
        exchange_state=_exchange_state(),
        checked_at_utc=NOW,
    )
    assert reject_result.post_submit_status == REJECT_CONFIRMED_NO_ORDER
    assert not reject_result.block_new_orders and not reject_result.requires_manual_review

    unknown = _submit("k-unknown", TESTNET_TIMEOUT_UNKNOWN_SIMULATED, submitted=False)
    unknown_snapshot = _snapshot(unknown)
    unknown_result = reconciler.reconcile(
        submit_result=unknown,
        local_snapshot=unknown_snapshot,
        exchange_state=_exchange_state(),
        checked_at_utc=NOW,
    )
    assert unknown_result.post_submit_status == UNKNOWN_REQUIRES_EXCHANGE_CHECK
    assert unknown_result.block_new_orders and unknown_result.requires_manual_review

    submit_unknown = _submit("k-submit-unknown", TESTNET_SUBMIT_UNKNOWN, submitted=False)
    submit_unknown_snapshot = _snapshot(submit_unknown)
    submit_unknown_result = reconciler.reconcile(
        submit_result=submit_unknown,
        local_snapshot=submit_unknown_snapshot,
        exchange_state=_exchange_state(),
        checked_at_utc=NOW,
    )
    assert submit_unknown_result.post_submit_status == UNKNOWN_REQUIRES_EXCHANGE_CHECK

    read_failure = reconciler.reconcile(
        submit_result=ack,
        local_snapshot=ack_snapshot,
        exchange_state=None,
        exchange_read_ok=False,
        exchange_read_error="mock read failure",
        checked_at_utc=NOW,
    )
    assert read_failure.post_submit_status == EXCHANGE_READ_FAILED
    assert read_failure.block_new_orders and read_failure.requires_manual_review

    filled = reconciler.reconcile(
        submit_result=ack,
        local_snapshot=ack_snapshot,
        exchange_state=_exchange_state(position=True),
        checked_at_utc=NOW,
    )
    assert filled.post_submit_status == EXCHANGE_POSITION_FOUND
    assert not filled.mismatch_detected

    deterministic_a = reconciler.reconcile(
        submit_result=ack,
        local_snapshot=ack_snapshot,
        exchange_state=order_state,
        checked_at_utc=NOW,
    )
    deterministic_b = reconciler.reconcile(
        submit_result=ack,
        local_snapshot=ack_snapshot,
        exchange_state=order_state,
        checked_at_utc=NOW,
    )
    assert deterministic_a == deterministic_b

    try:
        ok.post_submit_status = "MUTATED"  # type: ignore[misc]
        raise AssertionError("PostSubmitReconciliationResult should be immutable")
    except FrozenInstanceError:
        pass

    assert ack == ack_before
    assert order_state == order_state_before
    mutated_source = 0 if ack == ack_before and order_state == order_state_before else 1
    production_files_modified = 0 if before == _hash_existing_production_files() else 1

    print(
        "SMOKE_E18_OK "
        f"ack_order_ok={int(ok.post_submit_status == POST_SUBMIT_RECONCILIATION_OK)} "
        f"ack_missing_block={int(missing.block_new_orders and missing.requires_manual_review)} "
        f"reject_no_order_terminal={int(reject_result.post_submit_status == REJECT_CONFIRMED_NO_ORDER)} "
        f"unknown_block={int(unknown_result.block_new_orders and unknown_result.requires_manual_review)} "
        f"read_failure_fail_closed={int(read_failure.block_new_orders and read_failure.requires_manual_review)} "
        f"position_found_consistent={int(filled.post_submit_status == EXCHANGE_POSITION_FOUND)} "
        f"deterministic={int(deterministic_a == deterministic_b)} "
        "immutable=1 "
        f"mutated_source={mutated_source} "
        f"exchange_calls={exchange_calls} "
        f"production_files_modified={production_files_modified}"
    )


if __name__ == "__main__":
    main()
