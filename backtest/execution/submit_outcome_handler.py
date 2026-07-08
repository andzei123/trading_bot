from __future__ import annotations

from dataclasses import dataclass

from .execution_event_ledger import (
    ExecutionEventLedger,
    ExecutionStateSnapshot,
    SUBMIT_ACK,
    SUBMIT_BLOCKED,
    SUBMIT_REJECT,
    SUBMIT_TIMEOUT_UNKNOWN,
    SUBMIT_UNKNOWN,
)
from .testnet_submit_adapter import (
    BLOCKED_COMMAND_NOT_ALLOWED,
    BLOCKED_INVALID_INTENT,
    BLOCKED_LIVE_FORBIDDEN,
    BLOCKED_MISSING_CLIENT_ORDER_ID,
    BLOCKED_NOT_RESERVED,
    BLOCKED_NOT_TESTNET,
    SubmitResult,
    TESTNET_ACK_SIMULATED,
    TESTNET_REJECT_SIMULATED,
    TESTNET_TIMEOUT_UNKNOWN_SIMULATED,
)


TESTNET_TIMEOUT_UNKNOWN = "TESTNET_TIMEOUT_UNKNOWN"
TESTNET_SUBMIT_UNKNOWN = "TESTNET_SUBMIT_UNKNOWN"
BLOCKED_NOT_SUBMITTED = "BLOCKED_NOT_SUBMITTED"

TESTNET_ACK_CONFIRMED = "TESTNET_ACK_CONFIRMED"
TESTNET_REJECT_CONFIRMED = "TESTNET_REJECT_CONFIRMED"


@dataclass(frozen=True)
class SubmitOutcomeLedgerEvent:
    """Immutable description of the E16 ledger event produced from SubmitResult.

    This is executor-local outcome handling only. It does not retry, poll,
    reconcile exchange state, submit/cancel/amend orders, create TP/SL, alter
    ATS quantity/risk decisions, or write ATS production telemetry.
    """

    canonical_setup_key: str
    symbol: str
    side: str
    client_order_id: str
    submit_status: str
    ledger_event_type: str
    block_new_orders: bool
    requires_manual_review: bool
    reason: str


class SubmitOutcomeHandler:
    """Deterministic SubmitResult -> ledger event -> snapshot handler.

    The E10 Execution Event Ledger remains the only executor state source. This
    handler appends exactly one local submit outcome event per call and rebuilds
    the state snapshot through the existing ledger reconstruction path.
    """

    def handle_submit_result(
        self,
        *,
        submit_result: SubmitResult,
        ledger: ExecutionEventLedger,
        recorded_at_utc: str | None = None,
    ) -> ExecutionStateSnapshot:
        if not isinstance(submit_result, SubmitResult):
            raise TypeError("handle_submit_result requires SubmitResult")
        if not isinstance(ledger, ExecutionEventLedger):
            raise TypeError("handle_submit_result requires ExecutionEventLedger")

        outcome = classify_submit_outcome(submit_result)
        ledger.append_event(
            canonical_setup_key=outcome.canonical_setup_key,
            event_type=outcome.ledger_event_type,
            recorded_at_utc=recorded_at_utc or submit_result.submitted_at_utc,
            symbol=outcome.symbol,
            side=outcome.side,
            client_order_id=outcome.client_order_id,
            status=outcome.submit_status,
            block_new_orders=outcome.block_new_orders,
            requires_manual_review=outcome.requires_manual_review,
            reason=outcome.reason,
        )
        return ledger.rebuild_snapshot(outcome.canonical_setup_key)


def classify_submit_outcome(submit_result: SubmitResult) -> SubmitOutcomeLedgerEvent:
    """Classify one immutable E15 SubmitResult without side effects."""

    if not isinstance(submit_result, SubmitResult):
        raise TypeError("classify_submit_outcome requires SubmitResult")

    status = str(submit_result.submit_status or "").strip().upper()
    key = submit_result.canonical_setup_key
    symbol = submit_result.symbol
    side = submit_result.side
    client_order_id = submit_result.client_order_id

    if status in {TESTNET_ACK_SIMULATED, TESTNET_ACK_CONFIRMED}:
        return SubmitOutcomeLedgerEvent(
            canonical_setup_key=key,
            symbol=symbol,
            side=side,
            client_order_id=client_order_id,
            submit_status=status,
            ledger_event_type=SUBMIT_ACK,
            block_new_orders=True,
            requires_manual_review=False,
            reason="TESTNET submit ack recorded; waiting for later fill/reconciliation layers",
        )

    if status in {TESTNET_REJECT_SIMULATED, TESTNET_REJECT_CONFIRMED}:
        return SubmitOutcomeLedgerEvent(
            canonical_setup_key=key,
            symbol=symbol,
            side=side,
            client_order_id=client_order_id,
            submit_status=status,
            ledger_event_type=SUBMIT_REJECT,
            block_new_orders=False,
            requires_manual_review=False,
            reason="TESTNET submit reject recorded as terminal submit failure",
        )

    if status in {TESTNET_TIMEOUT_UNKNOWN_SIMULATED, TESTNET_TIMEOUT_UNKNOWN}:
        return SubmitOutcomeLedgerEvent(
            canonical_setup_key=key,
            symbol=symbol,
            side=side,
            client_order_id=client_order_id,
            submit_status=status,
            ledger_event_type=SUBMIT_TIMEOUT_UNKNOWN,
            block_new_orders=True,
            requires_manual_review=True,
            reason="TESTNET submit timeout outcome unknown; no retry performed",
        )

    if status == TESTNET_SUBMIT_UNKNOWN:
        return SubmitOutcomeLedgerEvent(
            canonical_setup_key=key,
            symbol=symbol,
            side=side,
            client_order_id=client_order_id,
            submit_status=status,
            ledger_event_type=SUBMIT_UNKNOWN,
            block_new_orders=True,
            requires_manual_review=True,
            reason="TESTNET submit outcome unknown; fail closed without retry",
        )

    if status in _BLOCKED_STATUSES or not submit_result.submitted:
        return SubmitOutcomeLedgerEvent(
            canonical_setup_key=key,
            symbol=symbol,
            side=side,
            client_order_id=client_order_id,
            submit_status=status or BLOCKED_NOT_SUBMITTED,
            ledger_event_type=SUBMIT_BLOCKED,
            block_new_orders=True,
            requires_manual_review=False,
            reason="submit was blocked or not submitted; no exchange mutation was performed",
        )

    return SubmitOutcomeLedgerEvent(
        canonical_setup_key=key,
        symbol=symbol,
        side=side,
        client_order_id=client_order_id,
        submit_status=status or TESTNET_SUBMIT_UNKNOWN,
        ledger_event_type=SUBMIT_UNKNOWN,
        block_new_orders=True,
        requires_manual_review=True,
        reason="unrecognized submit status; fail closed without retry",
    )


_BLOCKED_STATUSES = frozenset(
    {
        BLOCKED_COMMAND_NOT_ALLOWED,
        BLOCKED_NOT_TESTNET,
        BLOCKED_LIVE_FORBIDDEN,
        BLOCKED_NOT_RESERVED,
        BLOCKED_MISSING_CLIENT_ORDER_ID,
        BLOCKED_INVALID_INTENT,
        BLOCKED_NOT_SUBMITTED,
    }
)
