from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from .decision_consumer import ValidatedExecutionIntent
from .exchange_read_adapter import ExchangeStateSnapshot
from .execution_event_ledger import (
    EXCHANGE_READY,
    IDEMPOTENCY_ALLOWED,
    INTENT_ACCEPTED,
    MECHANICAL_SAFETY_PASSED,
    ExecutionEventLedger,
    ExecutionStateSnapshot,
)
from .execution_identity_registry import ExecutionIdentityRegistry
from .intent_journal import IntentJournal
from .mechanical_safety_bridge import check_mechanical_safety
from .post_submit_reconciler import PostSubmitReconciliationResult, PostSubmitReconciler
from .quantity_converter import ExchangeQuantityRules, ExchangeReadyIntent, convert_validated_intent_quantity
from .submit_outcome_handler import SubmitOutcomeHandler
from .testnet_command_gateway import MODE_TESTNET, TestnetCommandGateway
from .testnet_submit_adapter import (
    SIMULATED_ACK,
    SIMULATED_REJECT,
    SIMULATED_TIMEOUT_UNKNOWN,
    SubmitResult,
    TestnetSubmitAdapterSkeleton,
)


@dataclass(frozen=True)
class EndToEndDrillResult:
    """Immutable E19 end-to-end local drill result.

    E19 adds no new execution authority. It composes previously validated
    executor-local layers using fake/stubbed submit outcomes and supplied
    read-only exchange snapshots. It never calls exchanges, retries, sets TP/SL,
    writes production ATS files, or integrates with the production shell.
    """

    canonical_setup_key: str
    allowed: bool
    scenario: str
    reason: str
    exchange_ready_intent: ExchangeReadyIntent | None
    submit_result: SubmitResult | None
    post_submit_reconciliation: PostSubmitReconciliationResult | None
    state_snapshot: ExecutionStateSnapshot
    ledger_event_count: int
    deterministic_rebuild: bool


class EndToEndTestnetDrill:
    """Deterministic E1-E18 composition drill for executor-local testing.

    The drill accepts only a ValidatedExecutionIntent. It uses the existing
    intent journal, mechanical safety bridge, in-memory identity registry,
    quantity converter, E14 command gateway, E15 submit adapter stub, E16 submit
    outcome handler, E18 post-submit reconciler, and E10 ledger reconstruction.
    """

    def __init__(
        self,
        *,
        ledger: ExecutionEventLedger,
        identity_registry: ExecutionIdentityRegistry,
        intent_journal_path: str | Path,
        manual_enable: bool = True,
        checked_at_utc: str | None = None,
        quantity_rules: ExchangeQuantityRules | None = None,
        max_authorized_notional: Decimal | None = None,
    ) -> None:
        self.ledger = ledger
        self.identity_registry = identity_registry
        self.intent_journal = IntentJournal(intent_journal_path)
        self.manual_enable = bool(manual_enable)
        self.checked_at_utc = checked_at_utc
        self.quantity_rules = quantity_rules
        self.max_authorized_notional = max_authorized_notional

    def run(
        self,
        *,
        validated_intent: ValidatedExecutionIntent,
        exchange_state: ExchangeStateSnapshot,
        simulated_submit_response: str = SIMULATED_ACK,
        reconciliation_ok: bool = True,
        scenario: str = "NORMAL_ACK",
    ) -> EndToEndDrillResult:
        if not isinstance(validated_intent, ValidatedExecutionIntent):
            raise TypeError("E19 drill accepts only ValidatedExecutionIntent")

        key = validated_intent.canonical_setup_key
        ts = self.checked_at_utc

        self.intent_journal.append(validated_intent, executor_accepted_ts=ts)
        self.ledger.append_event(
            canonical_setup_key=key,
            event_type=INTENT_ACCEPTED,
            recorded_at_utc=ts,
            symbol=validated_intent.symbol,
            side=validated_intent.side,
            reason="E19 drill accepted validated intent",
        )

        safety = check_mechanical_safety(
            validated_intent,
            executor_mode="DRY_RUN",
            checked_at_utc=ts,
            max_authorized_notional=self.max_authorized_notional,
        )
        if not safety.allowed:
            snapshot = self.ledger.rebuild_snapshot(key)
            return self._blocked_result(key, scenario, safety.reason, snapshot)

        self.ledger.append_event(
            canonical_setup_key=key,
            event_type=MECHANICAL_SAFETY_PASSED,
            recorded_at_utc=safety.checked_at_utc,
            symbol=validated_intent.symbol,
            side=validated_intent.side,
            reason=safety.reason,
        )

        idem = self.identity_registry.check_and_record(key, checked_at_utc=ts)
        if not idem.allowed:
            snapshot = self.ledger.rebuild_snapshot(key)
            return self._blocked_result(key, scenario, idem.reason, snapshot)

        self.ledger.append_event(
            canonical_setup_key=key,
            event_type=IDEMPOTENCY_ALLOWED,
            recorded_at_utc=idem.checked_at_utc,
            symbol=validated_intent.symbol,
            side=validated_intent.side,
            reason=idem.reason,
        )

        exchange_ready = convert_validated_intent_quantity(validated_intent, self.quantity_rules)
        self.ledger.append_event(
            canonical_setup_key=key,
            event_type=EXCHANGE_READY,
            recorded_at_utc=ts,
            symbol=exchange_ready.symbol,
            side=exchange_ready.side,
            reason=exchange_ready.conversion_reason,
        )

        local_before_command = self.ledger.rebuild_snapshot(key)
        from .exchange_reconciler import ExchangeReconciler

        reconciliation = ExchangeReconciler().reconcile(
            local_snapshot=local_before_command,
            exchange_positions=exchange_state.positions,
            exchange_open_orders=exchange_state.open_orders,
            exchange_balances=exchange_state.balances,
            instrument_rules=exchange_state.instrument_rules,
            canonical_setup_key=key,
            symbol=exchange_ready.symbol,
            exchange_read_ok=reconciliation_ok,
            checked_at_utc=ts,
        )

        command = TestnetCommandGateway(mode=MODE_TESTNET, manual_enable=self.manual_enable).prepare_command(
            exchange_ready_intent=exchange_ready,
            reconciliation_result=reconciliation,
            ledger=self.ledger,
            decided_at_utc=ts,
        )
        if not command.allowed:
            snapshot = self.ledger.rebuild_snapshot(key)
            return EndToEndDrillResult(
                canonical_setup_key=key,
                allowed=False,
                scenario=scenario,
                reason=command.reason,
                exchange_ready_intent=exchange_ready,
                submit_result=None,
                post_submit_reconciliation=None,
                state_snapshot=snapshot,
                ledger_event_count=snapshot.event_count,
                deterministic_rebuild=snapshot == self.ledger.rebuild_snapshot(key),
            )

        submit_result = TestnetSubmitAdapterSkeleton().submit(
            command_decision=command,
            exchange_ready_intent=exchange_ready,
            simulated_response=simulated_submit_response,
            submitted_at_utc=ts,
        )
        snapshot = SubmitOutcomeHandler().handle_submit_result(
            submit_result=submit_result,
            ledger=self.ledger,
            recorded_at_utc=ts,
        )
        post_submit = PostSubmitReconciler().reconcile(
            submit_result=submit_result,
            local_snapshot=snapshot,
            exchange_state=exchange_state,
            exchange_read_ok=reconciliation_ok,
            checked_at_utc=ts,
        )
        rebuilt = self.ledger.rebuild_snapshot(key)
        return EndToEndDrillResult(
            canonical_setup_key=key,
            allowed=not post_submit.block_new_orders and not post_submit.requires_manual_review,
            scenario=scenario,
            reason=post_submit.reason,
            exchange_ready_intent=exchange_ready,
            submit_result=submit_result,
            post_submit_reconciliation=post_submit,
            state_snapshot=rebuilt,
            ledger_event_count=rebuilt.event_count,
            deterministic_rebuild=rebuilt == self.ledger.rebuild_snapshot(key),
        )

    def _blocked_result(
        self,
        key: str,
        scenario: str,
        reason: str,
        snapshot: ExecutionStateSnapshot,
    ) -> EndToEndDrillResult:
        return EndToEndDrillResult(
            canonical_setup_key=key,
            allowed=False,
            scenario=scenario,
            reason=reason,
            exchange_ready_intent=None,
            submit_result=None,
            post_submit_reconciliation=None,
            state_snapshot=snapshot,
            ledger_event_count=snapshot.event_count,
            deterministic_rebuild=snapshot == self.ledger.rebuild_snapshot(key),
        )
