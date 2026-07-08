from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from .recovery_simulator import RecoveryDecision


class ExecutionEventLedgerError(ValueError):
    """Raised when an executor-local event ledger operation is invalid."""


INTENT_ACCEPTED = "INTENT_ACCEPTED"
MECHANICAL_SAFETY_PASSED = "MECHANICAL_SAFETY_PASSED"
IDEMPOTENCY_ALLOWED = "IDEMPOTENCY_ALLOWED"
EXCHANGE_READY = "EXCHANGE_READY"
SIMULATED_ACKED = "SIMULATED_ACKED"
SIMULATED_REJECTED = "SIMULATED_REJECTED"
SIMULATED_PARTIALLY_FILLED = "SIMULATED_PARTIALLY_FILLED"
SIMULATED_FILLED = "SIMULATED_FILLED"
SIMULATED_TIMEOUT_UNKNOWN = "SIMULATED_TIMEOUT_UNKNOWN"
SIMULATED_EXCHANGE_UNAVAILABLE = "SIMULATED_EXCHANGE_UNAVAILABLE"
RECOVERY_DECISION = "RECOVERY_DECISION"
RESERVED_PRE_SUBMIT = "RESERVED_PRE_SUBMIT"
SUBMIT_ACK = "SUBMIT_ACK"
SUBMIT_REJECT = "SUBMIT_REJECT"
SUBMIT_TIMEOUT_UNKNOWN = "SUBMIT_TIMEOUT_UNKNOWN"
SUBMIT_UNKNOWN = "SUBMIT_UNKNOWN"
SUBMIT_BLOCKED = "SUBMIT_BLOCKED"

SUPPORTED_EXECUTION_LEDGER_EVENTS = frozenset(
    {
        INTENT_ACCEPTED,
        MECHANICAL_SAFETY_PASSED,
        IDEMPOTENCY_ALLOWED,
        EXCHANGE_READY,
        SIMULATED_ACKED,
        SIMULATED_REJECTED,
        SIMULATED_PARTIALLY_FILLED,
        SIMULATED_FILLED,
        SIMULATED_TIMEOUT_UNKNOWN,
        SIMULATED_EXCHANGE_UNAVAILABLE,
        RECOVERY_DECISION,
        RESERVED_PRE_SUBMIT,
        SUBMIT_ACK,
        SUBMIT_REJECT,
        SUBMIT_TIMEOUT_UNKNOWN,
        SUBMIT_UNKNOWN,
        SUBMIT_BLOCKED,
    }
)

_TERMINAL_EVENTS = frozenset({SIMULATED_FILLED, SIMULATED_REJECTED})
_UNKNOWN_EVENTS = frozenset({SIMULATED_TIMEOUT_UNKNOWN, SIMULATED_EXCHANGE_UNAVAILABLE, SUBMIT_TIMEOUT_UNKNOWN, SUBMIT_UNKNOWN})


@dataclass(frozen=True)
class ExecutionLedgerEvent:
    """Immutable executor-local append-only event.

    E10 records local executor events only. It does not call exchanges, read
    exchange state, submit/cancel/amend orders, write ATS production files,
    scan restart persistence, reconcile exchange state, or mutate ATS telemetry.
    """

    sequence: int
    canonical_setup_key: str
    event_type: str
    recorded_at_utc: str
    symbol: str = ""
    side: str = ""
    client_order_id: str = ""
    status: str = ""
    block_new_orders: bool = False
    requires_manual_review: bool = False
    reason: str = ""


@dataclass(frozen=True)
class ExecutionStateSnapshot:
    """Immutable deterministic state reconstructed from local ledger events."""

    canonical_setup_key: str
    current_state: str
    last_event_type: str
    event_count: int
    has_unknown_state: bool
    has_partial_fill: bool
    terminal: bool
    block_new_orders: bool
    requires_manual_review: bool
    submit_state: str = ""
    submit_confirmed: bool = False
    submit_unknown: bool = False
    terminal_submit_failure: bool = False


class ExecutionEventLedger:
    """Executor-local append-only ledger with deterministic reconstruction.

    Disk writes are optional and only occur when an explicit executor-local path
    is supplied by the caller. There is no default production journal path.
    """

    def __init__(self, ledger_path: str | Path | None = None) -> None:
        self._events: list[ExecutionLedgerEvent] = []
        self._ledger_path = Path(ledger_path) if ledger_path is not None else None
        if self._ledger_path is not None:
            self._ledger_path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def events(self) -> tuple[ExecutionLedgerEvent, ...]:
        return tuple(self._events)

    def append_event(
        self,
        *,
        canonical_setup_key: str,
        event_type: str,
        recorded_at_utc: str | None = None,
        symbol: str = "",
        side: str = "",
        client_order_id: str = "",
        status: str = "",
        block_new_orders: bool = False,
        requires_manual_review: bool = False,
        reason: str = "",
    ) -> ExecutionLedgerEvent:
        normalized_key = canonical_setup_key.strip()
        normalized_event_type = event_type.strip().upper()
        if not normalized_key:
            raise ExecutionEventLedgerError("canonical_setup_key is required")
        if normalized_event_type not in SUPPORTED_EXECUTION_LEDGER_EVENTS:
            raise ExecutionEventLedgerError(f"unsupported execution ledger event type: {event_type}")

        event = ExecutionLedgerEvent(
            sequence=len(self._events) + 1,
            canonical_setup_key=normalized_key,
            event_type=normalized_event_type,
            recorded_at_utc=recorded_at_utc or _utc_now(),
            symbol=symbol,
            side=side,
            client_order_id=client_order_id,
            status=status,
            block_new_orders=block_new_orders,
            requires_manual_review=requires_manual_review,
            reason=reason,
        )
        self._events.append(event)
        if self._ledger_path is not None:
            with self._ledger_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(asdict(event), sort_keys=True) + "\n")
        return event

    def append_recovery_decision(
        self,
        decision: RecoveryDecision,
        *,
        recorded_at_utc: str | None = None,
    ) -> ExecutionLedgerEvent:
        if not isinstance(decision, RecoveryDecision):
            raise ExecutionEventLedgerError("append_recovery_decision requires RecoveryDecision")
        return self.append_event(
            canonical_setup_key=decision.canonical_setup_key,
            event_type=RECOVERY_DECISION,
            recorded_at_utc=recorded_at_utc or decision.decided_at_utc,
            symbol=decision.symbol,
            client_order_id=decision.client_order_id,
            status=decision.recovery_action,
            block_new_orders=decision.block_new_orders,
            requires_manual_review=decision.requires_manual_review,
            reason=decision.reason,
        )

    def rebuild_snapshot(self, canonical_setup_key: str) -> ExecutionStateSnapshot:
        return rebuild_execution_state_snapshot(self._events, canonical_setup_key)


def rebuild_execution_state_snapshot(
    events: Iterable[ExecutionLedgerEvent],
    canonical_setup_key: str,
) -> ExecutionStateSnapshot:
    """Rebuild one identity state deterministically from ordered local events."""

    key = canonical_setup_key.strip()
    if not key:
        raise ExecutionEventLedgerError("canonical_setup_key is required")

    identity_events = [event for event in events if event.canonical_setup_key == key]
    if not identity_events:
        return ExecutionStateSnapshot(
            canonical_setup_key=key,
            current_state="NO_EVENTS",
            last_event_type="",
            event_count=0,
            has_unknown_state=False,
            has_partial_fill=False,
            terminal=False,
            block_new_orders=False,
            requires_manual_review=False,
            submit_state="NO_SUBMIT",
            submit_confirmed=False,
            submit_unknown=False,
            terminal_submit_failure=False,
        )

    last_event_type = ""
    current_state = "UNKNOWN"
    has_unknown_state = False
    has_partial_fill = False
    terminal = False
    block_new_orders = False
    requires_manual_review = False
    submit_state = "NO_SUBMIT"
    submit_confirmed = False
    submit_unknown = False
    terminal_submit_failure = False

    for event in identity_events:
        last_event_type = event.event_type
        if event.event_type == INTENT_ACCEPTED:
            current_state = "INTENT_ACCEPTED"
        elif event.event_type == MECHANICAL_SAFETY_PASSED:
            current_state = "MECHANICAL_SAFETY_PASSED"
        elif event.event_type == IDEMPOTENCY_ALLOWED:
            current_state = "IDEMPOTENCY_ALLOWED"
        elif event.event_type == EXCHANGE_READY:
            current_state = "EXCHANGE_READY"
        elif event.event_type == SIMULATED_ACKED:
            current_state = "ACKED_PENDING_FILL"
            block_new_orders = True
        elif event.event_type == SIMULATED_REJECTED:
            current_state = "REJECTED_TERMINAL"
            terminal = True
        elif event.event_type == SIMULATED_PARTIALLY_FILLED:
            current_state = "PARTIAL_FILL_PENDING"
            has_partial_fill = True
            block_new_orders = True
            requires_manual_review = True
        elif event.event_type == SIMULATED_FILLED:
            current_state = "FILLED_CONFIRMED"
            terminal = True
            block_new_orders = False
            requires_manual_review = False
        elif event.event_type == SIMULATED_TIMEOUT_UNKNOWN:
            current_state = "UNKNOWN_STATE"
            has_unknown_state = True
            block_new_orders = True
            requires_manual_review = True
        elif event.event_type == SIMULATED_EXCHANGE_UNAVAILABLE:
            current_state = "EXCHANGE_UNAVAILABLE"
            has_unknown_state = True
            block_new_orders = True
            requires_manual_review = True
        elif event.event_type == RECOVERY_DECISION:
            current_state = event.status or "RECOVERY_DECISION"
            block_new_orders = event.block_new_orders
            requires_manual_review = event.requires_manual_review
        elif event.event_type == RESERVED_PRE_SUBMIT:
            current_state = "PRE_SUBMIT_RESERVED"
            block_new_orders = True
            requires_manual_review = event.requires_manual_review
        elif event.event_type == SUBMIT_ACK:
            current_state = "SUBMIT_ACK_CONFIRMED"
            submit_state = "ACK_CONFIRMED"
            submit_confirmed = True
            submit_unknown = False
            terminal_submit_failure = False
            block_new_orders = True
            requires_manual_review = event.requires_manual_review
        elif event.event_type == SUBMIT_REJECT:
            current_state = "SUBMIT_REJECT_TERMINAL"
            submit_state = "REJECTED_TERMINAL"
            submit_confirmed = False
            submit_unknown = False
            terminal_submit_failure = True
            terminal = True
            block_new_orders = event.block_new_orders
            requires_manual_review = event.requires_manual_review
        elif event.event_type == SUBMIT_TIMEOUT_UNKNOWN:
            current_state = "SUBMIT_TIMEOUT_UNKNOWN"
            submit_state = "TIMEOUT_UNKNOWN"
            submit_confirmed = False
            submit_unknown = True
            terminal_submit_failure = False
            has_unknown_state = True
            block_new_orders = True
            requires_manual_review = True
        elif event.event_type == SUBMIT_UNKNOWN:
            current_state = "SUBMIT_UNKNOWN"
            submit_state = "UNKNOWN"
            submit_confirmed = False
            submit_unknown = True
            terminal_submit_failure = False
            has_unknown_state = True
            block_new_orders = True
            requires_manual_review = True
        elif event.event_type == SUBMIT_BLOCKED:
            current_state = "SUBMIT_BLOCKED"
            submit_state = "BLOCKED_NOT_SUBMITTED"
            submit_confirmed = False
            submit_unknown = False
            terminal_submit_failure = False
            block_new_orders = event.block_new_orders
            requires_manual_review = event.requires_manual_review
        else:  # pragma: no cover - append_event validates supported event types
            raise ExecutionEventLedgerError(f"unsupported execution ledger event type: {event.event_type}")

        if event.event_type in _UNKNOWN_EVENTS:
            has_unknown_state = True
        if event.event_type == SIMULATED_PARTIALLY_FILLED:
            has_partial_fill = True
        if event.event_type in _TERMINAL_EVENTS:
            terminal = True
        if event.event_type == SUBMIT_REJECT:
            terminal = True

    return ExecutionStateSnapshot(
        canonical_setup_key=key,
        current_state=current_state,
        last_event_type=last_event_type,
        event_count=len(identity_events),
        has_unknown_state=has_unknown_state,
        has_partial_fill=has_partial_fill,
        terminal=terminal,
        block_new_orders=block_new_orders,
        requires_manual_review=requires_manual_review,
        submit_state=submit_state,
        submit_confirmed=submit_confirmed,
        submit_unknown=submit_unknown,
        terminal_submit_failure=terminal_submit_failure,
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
