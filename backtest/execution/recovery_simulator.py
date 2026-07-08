from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from .execution_simulator import (
    ACKED,
    EXCHANGE_UNAVAILABLE,
    FILLED,
    PARTIALLY_FILLED,
    REJECTED,
    TIMEOUT_UNKNOWN,
    SimulatedExecutionEvent,
)


class RecoverySimulationError(ValueError):
    """Raised when a local recovery simulation input is invalid."""


NO_RECOVERY_REQUIRED = "NO_RECOVERY_REQUIRED"
BLOCK_AND_RECONCILE = "BLOCK_AND_RECONCILE"
WAIT_FOR_RECHECK = "WAIT_FOR_RECHECK"
MARK_REJECTED_TERMINAL = "MARK_REJECTED_TERMINAL"
MARK_FILLED_CONFIRMED = "MARK_FILLED_CONFIRMED"
MARK_PARTIAL_PENDING = "MARK_PARTIAL_PENDING"
EXCHANGE_UNAVAILABLE_FAIL_CLOSED = "EXCHANGE_UNAVAILABLE_FAIL_CLOSED"


@dataclass(frozen=True)
class RecoveryDecision:
    """Immutable executor-local recovery decision for simulated execution states.

    E9 models deterministic restart/unknown-state behavior only. It does not
    call exchanges, read exchange state, submit/cancel/amend orders, write ATS
    production files, scan disk recovery state, or perform reconciliation.
    """

    canonical_setup_key: str
    symbol: str
    client_order_id: str
    source_event_type: str
    recovery_action: str
    block_new_orders: bool
    requires_manual_review: bool
    reason: str
    decided_at_utc: str


@dataclass(frozen=True)
class RecoverySimulationRequest:
    """Optional deterministic controls for local recovery decision simulation."""

    decided_at_utc: str | None = None


class RecoverySimulator:
    """Deterministic local recovery decision simulator.

    This simulator is fail-closed for uncertain states. It intentionally does
    not perform real restart persistence, disk scans, exchange reconciliation,
    exchange polling, ATS telemetry writes, or production state mutation.
    """

    def decide(
        self,
        event: SimulatedExecutionEvent,
        request: RecoverySimulationRequest | None = None,
    ) -> RecoveryDecision:
        if not isinstance(event, SimulatedExecutionEvent):
            raise RecoverySimulationError("recovery simulator requires SimulatedExecutionEvent")

        decided_at = (request.decided_at_utc if request else None) or _utc_now()
        status = event.status.strip().upper()

        if status == TIMEOUT_UNKNOWN:
            return self._decision(
                event,
                BLOCK_AND_RECONCILE,
                block_new_orders=True,
                requires_manual_review=True,
                reason="timeout_unknown_requires_reconciliation_before_new_orders",
                decided_at_utc=decided_at,
            )
        if status == PARTIALLY_FILLED:
            return self._decision(
                event,
                MARK_PARTIAL_PENDING,
                block_new_orders=True,
                requires_manual_review=True,
                reason="partial_fill_pending_requires_safe_recheck",
                decided_at_utc=decided_at,
            )
        if status == EXCHANGE_UNAVAILABLE:
            return self._decision(
                event,
                EXCHANGE_UNAVAILABLE_FAIL_CLOSED,
                block_new_orders=True,
                requires_manual_review=True,
                reason="exchange_unavailable_fail_closed",
                decided_at_utc=decided_at,
            )
        if status == ACKED:
            return self._decision(
                event,
                WAIT_FOR_RECHECK,
                block_new_orders=True,
                requires_manual_review=False,
                reason="acked_without_fill_wait_for_read_only_recheck",
                decided_at_utc=decided_at,
            )
        if status == REJECTED:
            return self._decision(
                event,
                MARK_REJECTED_TERMINAL,
                block_new_orders=False,
                requires_manual_review=False,
                reason="rejected_terminal_no_new_exposure",
                decided_at_utc=decided_at,
            )
        if status == FILLED:
            return self._decision(
                event,
                MARK_FILLED_CONFIRMED,
                block_new_orders=False,
                requires_manual_review=False,
                reason="filled_confirmed_terminal",
                decided_at_utc=decided_at,
            )

        raise RecoverySimulationError(f"unsupported simulated execution status: {event.status}")

    @staticmethod
    def _decision(
        event: SimulatedExecutionEvent,
        recovery_action: str,
        *,
        block_new_orders: bool,
        requires_manual_review: bool,
        reason: str,
        decided_at_utc: str,
    ) -> RecoveryDecision:
        return RecoveryDecision(
            canonical_setup_key=event.canonical_setup_key,
            symbol=event.symbol,
            client_order_id=event.client_order_id,
            source_event_type=event.event_type,
            recovery_action=recovery_action,
            block_new_orders=block_new_orders,
            requires_manual_review=requires_manual_review,
            reason=reason,
            decided_at_utc=decided_at_utc,
        )


def simulate_recovery_decision(
    event: SimulatedExecutionEvent,
    request: RecoverySimulationRequest | None = None,
) -> RecoveryDecision:
    """Convenience wrapper for one deterministic local recovery decision."""

    return RecoverySimulator().decide(event, request)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
