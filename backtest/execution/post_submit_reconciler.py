from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Sequence

from .exchange_read_adapter import ExchangeOpenOrderSnapshot, ExchangePositionSnapshot, ExchangeStateSnapshot
from .execution_event_ledger import ExecutionStateSnapshot
from .submit_outcome_handler import TESTNET_SUBMIT_UNKNOWN, TESTNET_TIMEOUT_UNKNOWN
from .testnet_submit_adapter import (
    SubmitResult,
    TESTNET_ACK_SIMULATED,
    TESTNET_REJECT_SIMULATED,
    TESTNET_TIMEOUT_UNKNOWN_SIMULATED,
)


POST_SUBMIT_RECONCILIATION_OK = "POST_SUBMIT_RECONCILIATION_OK"
ACK_BUT_ORDER_MISSING = "ACK_BUT_ORDER_MISSING"
REJECT_CONFIRMED_NO_ORDER = "REJECT_CONFIRMED_NO_ORDER"
UNKNOWN_REQUIRES_EXCHANGE_CHECK = "UNKNOWN_REQUIRES_EXCHANGE_CHECK"
EXCHANGE_ORDER_FOUND = "EXCHANGE_ORDER_FOUND"
EXCHANGE_POSITION_FOUND = "EXCHANGE_POSITION_FOUND"
EXCHANGE_READ_FAILED = "EXCHANGE_READ_FAILED"
MANUAL_REVIEW_REQUIRED = "MANUAL_REVIEW_REQUIRED"


@dataclass(frozen=True)
class PostSubmitReconciliationResult:
    """Immutable E18 post-submit reconciliation classification.

    E18 compares a local SubmitResult and ExecutionStateSnapshot with already
    supplied read-only exchange snapshots. It does not call exchanges, retry,
    cancel, amend, place TP/SL, close positions, write ATS production files, or
    perform automatic recovery.
    """

    canonical_setup_key: str
    symbol: str
    client_order_id: str
    post_submit_status: str
    submit_status: str
    local_state: str
    exchange_order_status: str
    exchange_position_status: str
    mismatch_detected: bool
    block_new_orders: bool
    requires_manual_review: bool
    reason: str
    checked_at_utc: str


class PostSubmitReconciler:
    """Deterministic post-submit consistency classifier.

    The reconciler is passive and read-only. It receives normalized exchange
    state from E7/E12 and local executor state from E10/E16, then returns a
    fail-closed classification for the caller to audit.
    """

    def reconcile(
        self,
        *,
        submit_result: SubmitResult,
        local_snapshot: ExecutionStateSnapshot,
        exchange_state: ExchangeStateSnapshot | None,
        exchange_read_ok: bool = True,
        exchange_read_error: str = "",
        checked_at_utc: str | None = None,
    ) -> PostSubmitReconciliationResult:
        if not isinstance(submit_result, SubmitResult):
            raise TypeError("post-submit reconciliation requires SubmitResult")
        if not isinstance(local_snapshot, ExecutionStateSnapshot):
            raise TypeError("post-submit reconciliation requires ExecutionStateSnapshot")

        checked_ts = checked_at_utc or _utc_now()
        key = submit_result.canonical_setup_key
        symbol = submit_result.symbol.upper()
        client_order_id = submit_result.client_order_id
        submit_status = str(submit_result.submit_status or "").strip().upper()
        local_state = local_snapshot.current_state

        if not exchange_read_ok or exchange_state is None:
            return _result(
                key=key,
                symbol=symbol,
                client_order_id=client_order_id,
                status=EXCHANGE_READ_FAILED,
                submit_status=submit_status,
                local_state=local_state,
                order_status="UNKNOWN",
                position_status="UNKNOWN",
                mismatch=True,
                block=True,
                manual=True,
                reason=exchange_read_error or "read-only exchange state unavailable after submit; fail closed",
                checked_at_utc=checked_ts,
            )

        matching_orders = _matching_open_orders(exchange_state.open_orders, symbol, client_order_id)
        matching_positions = _matching_positions(exchange_state.positions, symbol)
        has_order = bool(matching_orders)
        has_position = bool(matching_positions)
        order_status = "OPEN" if has_order else "MISSING"
        position_status = "OPEN" if has_position else "MISSING"

        if submit_status in {TESTNET_TIMEOUT_UNKNOWN_SIMULATED, TESTNET_TIMEOUT_UNKNOWN, TESTNET_SUBMIT_UNKNOWN} or local_snapshot.submit_unknown:
            return _result(
                key=key,
                symbol=symbol,
                client_order_id=client_order_id,
                status=UNKNOWN_REQUIRES_EXCHANGE_CHECK,
                submit_status=submit_status,
                local_state=local_state,
                order_status=order_status,
                position_status=position_status,
                mismatch=True,
                block=True,
                manual=True,
                reason="submit outcome is unknown; block and require read-only exchange verification/manual review",
                checked_at_utc=checked_ts,
            )

        if has_position and submit_status in {TESTNET_ACK_SIMULATED}:
            return _result(
                key=key,
                symbol=symbol,
                client_order_id=client_order_id,
                status=EXCHANGE_POSITION_FOUND,
                submit_status=submit_status,
                local_state=local_state,
                order_status=order_status,
                position_status=position_status,
                mismatch=False,
                block=False,
                manual=False,
                reason="submit ack has corresponding exchange position in supplied read-only snapshot",
                checked_at_utc=checked_ts,
            )

        if submit_status in {TESTNET_ACK_SIMULATED}:
            if has_order:
                return _result(
                    key=key,
                    symbol=symbol,
                    client_order_id=client_order_id,
                    status=POST_SUBMIT_RECONCILIATION_OK,
                    submit_status=submit_status,
                    local_state=local_state,
                    order_status=order_status,
                    position_status=position_status,
                    mismatch=False,
                    block=False,
                    manual=False,
                    reason="submit ack has matching open order in supplied read-only exchange snapshot",
                    checked_at_utc=checked_ts,
                )
            return _result(
                key=key,
                symbol=symbol,
                client_order_id=client_order_id,
                status=ACK_BUT_ORDER_MISSING,
                submit_status=submit_status,
                local_state=local_state,
                order_status=order_status,
                position_status=position_status,
                mismatch=True,
                block=True,
                manual=True,
                reason="submit ack was recorded but no matching order or position was found",
                checked_at_utc=checked_ts,
            )

        if submit_status in {TESTNET_REJECT_SIMULATED}:
            if has_order or has_position:
                return _result(
                    key=key,
                    symbol=symbol,
                    client_order_id=client_order_id,
                    status=MANUAL_REVIEW_REQUIRED,
                    submit_status=submit_status,
                    local_state=local_state,
                    order_status=order_status,
                    position_status=position_status,
                    mismatch=True,
                    block=True,
                    manual=True,
                    reason="submit reject conflicts with exchange exposure; manual review required",
                    checked_at_utc=checked_ts,
                )
            return _result(
                key=key,
                symbol=symbol,
                client_order_id=client_order_id,
                status=REJECT_CONFIRMED_NO_ORDER,
                submit_status=submit_status,
                local_state=local_state,
                order_status=order_status,
                position_status=position_status,
                mismatch=False,
                block=False,
                manual=False,
                reason="submit reject has no matching exchange order or position; no exposure detected",
                checked_at_utc=checked_ts,
            )

        if has_order:
            return _result(
                key=key,
                symbol=symbol,
                client_order_id=client_order_id,
                status=EXCHANGE_ORDER_FOUND,
                submit_status=submit_status,
                local_state=local_state,
                order_status=order_status,
                position_status=position_status,
                mismatch=False,
                block=False,
                manual=False,
                reason="matching exchange order found in supplied read-only snapshot",
                checked_at_utc=checked_ts,
            )

        return _result(
            key=key,
            symbol=symbol,
            client_order_id=client_order_id,
            status=MANUAL_REVIEW_REQUIRED,
            submit_status=submit_status,
            local_state=local_state,
            order_status=order_status,
            position_status=position_status,
            mismatch=True,
            block=True,
            manual=True,
            reason="unrecognized post-submit state; fail closed for manual review",
            checked_at_utc=checked_ts,
        )


def _matching_open_orders(
    orders: Sequence[ExchangeOpenOrderSnapshot], symbol: str, client_order_id: str
) -> tuple[ExchangeOpenOrderSnapshot, ...]:
    return tuple(
        order
        for order in orders
        if order.symbol.upper() == symbol
        and (not client_order_id or order.client_order_id == client_order_id)
        and order.remaining_quantity > 0
    )


def _matching_positions(positions: Sequence[ExchangePositionSnapshot], symbol: str) -> tuple[ExchangePositionSnapshot, ...]:
    return tuple(position for position in positions if position.symbol.upper() == symbol and position.quantity != 0)


def _result(
    *,
    key: str,
    symbol: str,
    client_order_id: str,
    status: str,
    submit_status: str,
    local_state: str,
    order_status: str,
    position_status: str,
    mismatch: bool,
    block: bool,
    manual: bool,
    reason: str,
    checked_at_utc: str,
) -> PostSubmitReconciliationResult:
    return PostSubmitReconciliationResult(
        canonical_setup_key=key,
        symbol=symbol,
        client_order_id=client_order_id,
        post_submit_status=status,
        submit_status=submit_status,
        local_state=local_state,
        exchange_order_status=order_status,
        exchange_position_status=position_status,
        mismatch_detected=mismatch,
        block_new_orders=block,
        requires_manual_review=manual,
        reason=reason,
        checked_at_utc=checked_at_utc,
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
