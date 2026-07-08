from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from .decision_consumer import ValidatedExecutionIntent
from .execution_event_ledger import (
    EXCHANGE_READY,
    IDEMPOTENCY_ALLOWED,
    INTENT_ACCEPTED,
    MECHANICAL_SAFETY_PASSED,
    RECOVERY_DECISION,
    SIMULATED_ACKED,
    SIMULATED_EXCHANGE_UNAVAILABLE,
    SIMULATED_FILLED,
    SIMULATED_PARTIALLY_FILLED,
    SIMULATED_REJECTED,
    SIMULATED_TIMEOUT_UNKNOWN,
    ExecutionEventLedger,
    ExecutionStateSnapshot,
)
from .execution_identity_registry import ExecutionIdentityRegistry
from .execution_simulator import (
    ACKED,
    EXCHANGE_UNAVAILABLE,
    FILLED,
    PARTIALLY_FILLED,
    REJECTED,
    TIMEOUT_UNKNOWN,
    ExecutionSimulationRequest,
    ExecutionSimulator,
    SimulatedExecutionEvent,
)
from .intent_journal import IntentJournal
from .mechanical_safety_bridge import MechanicalSafetyResult, check_mechanical_safety
from .quantity_converter import ExchangeQuantityRules, convert_validated_intent_quantity
from .recovery_simulator import RecoveryDecision, RecoverySimulator


class PaperExecutorError(ValueError):
    """Raised when the local paper lifecycle receives invalid E11 inputs."""


_SIMULATED_EVENT_TO_LEDGER_EVENT = {
    ACKED: SIMULATED_ACKED,
    REJECTED: SIMULATED_REJECTED,
    PARTIALLY_FILLED: SIMULATED_PARTIALLY_FILLED,
    FILLED: SIMULATED_FILLED,
    TIMEOUT_UNKNOWN: SIMULATED_TIMEOUT_UNKNOWN,
    EXCHANGE_UNAVAILABLE: SIMULATED_EXCHANGE_UNAVAILABLE,
}


@dataclass(frozen=True)
class PaperExecutionRequest:
    """Deterministic controls for one local paper lifecycle run.

    This request controls only executor-local simulation behavior. It does not
    enable exchange calls, paper trading integration, ATS strategy changes,
    retries, polling, or production writes.
    """

    simulation_outcome: str = FILLED
    checked_at_utc: str | None = None
    simulated_at_utc: str | None = None
    decided_at_utc: str | None = None
    filled_qty: Decimal | None = None
    fill_price: Decimal | None = None
    reject_reason: str = ""
    client_order_id: str | None = None


class PaperExecutor:
    """Compose validated executor layers into a local paper lifecycle.

    E11 is local composition only. It accepts an already validated ATS intent,
    runs executor-local safety/idempotency/quantity conversion/simulation/
    recovery, records executor events in the E10 ledger, and returns an E10
    snapshot. It does not call Bybit, submit/cancel/amend orders, read exchange
    state, write ATS production files, create paper_state.csv, or create a
    separate paper authority.
    """

    def __init__(
        self,
        *,
        ledger: ExecutionEventLedger,
        identity_registry: ExecutionIdentityRegistry | None = None,
        intent_journal_path: str | Path | None = None,
        quantity_rules: ExchangeQuantityRules | None = None,
        executor_mode: str = "DRY_RUN",
        kill_switch_path: str | Path | None = None,
    ) -> None:
        if not isinstance(ledger, ExecutionEventLedger):
            raise PaperExecutorError("PaperExecutor requires ExecutionEventLedger")
        self._ledger = ledger
        self._identity_registry = identity_registry or ExecutionIdentityRegistry()
        self._intent_journal = IntentJournal(intent_journal_path) if intent_journal_path is not None else None
        self._quantity_rules = quantity_rules or ExchangeQuantityRules()
        self._executor_mode = executor_mode
        self._kill_switch_path = kill_switch_path
        self._simulator = ExecutionSimulator()
        self._recovery_simulator = RecoverySimulator()

    @property
    def ledger(self) -> ExecutionEventLedger:
        return self._ledger

    @property
    def identity_registry(self) -> ExecutionIdentityRegistry:
        return self._identity_registry

    def run(
        self,
        validated_intent: ValidatedExecutionIntent,
        request: PaperExecutionRequest | None = None,
    ) -> ExecutionStateSnapshot:
        """Run one deterministic local lifecycle and return E10 snapshot."""

        if not isinstance(validated_intent, ValidatedExecutionIntent):
            raise PaperExecutorError("paper executor accepts only ValidatedExecutionIntent")

        active_request = request or PaperExecutionRequest()
        key = validated_intent.canonical_setup_key

        if self._intent_journal is not None:
            self._intent_journal.append(
                validated_intent,
                executor_accepted_ts=active_request.checked_at_utc,
            )

        safety = check_mechanical_safety(
            validated_intent,
            executor_mode=self._executor_mode,
            kill_switch_path=self._kill_switch_path,
            checked_at_utc=active_request.checked_at_utc,
        )
        if not safety.allowed:
            return self._ledger.rebuild_snapshot(key)

        idempotency = self._identity_registry.check_and_record(
            key,
            checked_at_utc=active_request.checked_at_utc,
        )
        if not idempotency.allowed:
            return self._ledger.rebuild_snapshot(key)

        self._ledger.append_event(
            canonical_setup_key=key,
            event_type=INTENT_ACCEPTED,
            recorded_at_utc=active_request.checked_at_utc,
            symbol=validated_intent.symbol,
            side=validated_intent.side,
            reason="paper_lifecycle_intent_accepted",
        )
        self._append_mechanical_safety_passed(safety)
        self._ledger.append_event(
            canonical_setup_key=key,
            event_type=IDEMPOTENCY_ALLOWED,
            recorded_at_utc=idempotency.checked_at_utc,
            symbol=validated_intent.symbol,
            side=validated_intent.side,
            reason=idempotency.reason,
        )

        exchange_ready_intent = convert_validated_intent_quantity(validated_intent, self._quantity_rules)
        self._ledger.append_event(
            canonical_setup_key=key,
            event_type=EXCHANGE_READY,
            recorded_at_utc=active_request.checked_at_utc,
            symbol=exchange_ready_intent.symbol,
            side=exchange_ready_intent.side,
            status=exchange_ready_intent.conversion_reason,
            reason="exchange_quantity_converter_ready",
        )

        simulated_event = self._simulator.simulate(
            exchange_ready_intent,
            ExecutionSimulationRequest(
                outcome=active_request.simulation_outcome,
                simulated_at_utc=active_request.simulated_at_utc,
                client_order_id=active_request.client_order_id,
                fill_price=active_request.fill_price,
                filled_qty=active_request.filled_qty,
                reject_reason=active_request.reject_reason,
            ),
        )
        self._append_simulated_event(simulated_event)

        if simulated_event.status in {ACKED, REJECTED, FILLED, PARTIALLY_FILLED, TIMEOUT_UNKNOWN, EXCHANGE_UNAVAILABLE}:
            recovery_decision = self._recovery_simulator.decide(
                simulated_event,
                request=_recovery_request(active_request.decided_at_utc),
            )
            self._append_recovery_if_required(recovery_decision, simulated_event.status)

        return self._ledger.rebuild_snapshot(key)

    def _append_mechanical_safety_passed(self, safety: MechanicalSafetyResult) -> None:
        self._ledger.append_event(
            canonical_setup_key=safety.canonical_setup_key,
            event_type=MECHANICAL_SAFETY_PASSED,
            recorded_at_utc=safety.checked_at_utc,
            symbol=safety.symbol,
            side=safety.side,
            status=safety.severity,
            reason=safety.reason,
        )

    def _append_simulated_event(self, event: SimulatedExecutionEvent) -> None:
        ledger_event_type = _SIMULATED_EVENT_TO_LEDGER_EVENT[event.status]
        self._ledger.append_event(
            canonical_setup_key=event.canonical_setup_key,
            event_type=ledger_event_type,
            recorded_at_utc=event.simulated_at_utc,
            symbol=event.symbol,
            side=event.side,
            client_order_id=event.client_order_id,
            status=event.status,
            block_new_orders=event.status in {ACKED, PARTIALLY_FILLED, TIMEOUT_UNKNOWN, EXCHANGE_UNAVAILABLE},
            requires_manual_review=event.status in {PARTIALLY_FILLED, TIMEOUT_UNKNOWN, EXCHANGE_UNAVAILABLE},
            reason=event.reject_reason,
        )

    def _append_recovery_if_required(self, decision: RecoveryDecision, status: str) -> None:
        if status == FILLED:
            return
        self._ledger.append_recovery_decision(decision)


def _recovery_request(decided_at_utc: str | None):
    from .recovery_simulator import RecoverySimulationRequest

    return RecoverySimulationRequest(decided_at_utc=decided_at_utc)
