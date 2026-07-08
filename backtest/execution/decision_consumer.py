from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping

from .intent import ExecutionIntent, IntentValidationError


class DecisionConsumerError(ValueError):
    """Raised when an ATS execution decision cannot be consumed."""


@dataclass(frozen=True)
class ValidatedExecutionIntent:
    """Immutable execution intent accepted by the E2 decision consumer.

    The consumer is an ingress boundary only. It validates completeness and
    shape, then returns an immutable wrapper around the already-authorized ATS
    intent. It does not calculate risk, quantity, RR, or execution decisions.
    """

    intent: ExecutionIntent

    @property
    def schema_version(self) -> str:
        return self.intent.schema_version

    @property
    def cycle_ts(self) -> str:
        return self.intent.cycle_ts

    @property
    def symbol(self) -> str:
        return self.intent.symbol

    @property
    def model(self) -> str:
        return self.intent.model

    @property
    def side(self) -> str:
        return self.intent.side

    @property
    def canonical_setup_key(self) -> str:
        return self.intent.canonical_setup_key

    @property
    def setup_id(self) -> str:
        return self.intent.setup_id

    @property
    def setup_created_ts(self) -> str:
        return self.intent.setup_created_ts

    @property
    def signal_ts(self) -> str:
        return self.intent.signal_ts

    @property
    def visible_ts(self) -> str:
        return self.intent.visible_ts

    @property
    def wait_confirm_ts(self) -> str:
        return self.intent.wait_confirm_ts

    @property
    def intended_entry_ts(self) -> str:
        return self.intent.intended_entry_ts

    @property
    def entry_window_expires_ts(self) -> str:
        return self.intent.entry_window_expires_ts

    @property
    def selected_for_execution(self) -> bool:
        return self.intent.selected_for_execution

    @property
    def execution_rank(self) -> int:
        return self.intent.execution_rank

    @property
    def selection_reason(self) -> str:
        return self.intent.selection_reason

    @property
    def entry(self) -> Decimal:
        return self.intent.entry

    @property
    def sl(self) -> Decimal:
        return self.intent.sl

    @property
    def tp(self) -> Decimal:
        return self.intent.tp

    @property
    def planned_rr(self) -> Decimal:
        return self.intent.planned_rr

    @property
    def risk_pct(self) -> Decimal:
        return self.intent.risk_pct

    @property
    def reward_pct(self) -> Decimal:
        return self.intent.reward_pct

    @property
    def risk_distance(self) -> Decimal:
        return self.intent.risk_distance

    @property
    def reward_distance(self) -> Decimal:
        return self.intent.reward_distance

    @property
    def authorized_qty(self) -> Decimal:
        return self.intent.authorized_qty

    @property
    def authorized_notional(self) -> Decimal:
        return self.intent.authorized_notional

    @property
    def authorized_risk_usd(self) -> Decimal:
        return self.intent.authorized_risk_usd

    @property
    def risk_snapshot_id(self) -> str:
        return self.intent.risk_snapshot_id

    @property
    def position_snapshot_id(self) -> str:
        return self.intent.position_snapshot_id

    @property
    def authority_waterfall_id(self) -> str:
        return self.intent.authority_waterfall_id


def consume_decision(intent: ExecutionIntent) -> ValidatedExecutionIntent:
    """Accept an already-authorized ATS execution intent.

    This function intentionally performs no sizing, risk calculation, exchange
    communication, telemetry writes, polling, retries, or strategy evaluation.
    """

    if not isinstance(intent, ExecutionIntent):
        raise DecisionConsumerError("decision consumer requires ExecutionIntent")
    if not intent.selected_for_execution:
        raise DecisionConsumerError("intent is not selected_for_execution")
    return ValidatedExecutionIntent(intent=intent)


def consume_decision_row(row: Mapping[str, str]) -> ValidatedExecutionIntent:
    """Validate and consume an ATS intent row without mutating the source row."""

    try:
        intent = ExecutionIntent.from_row(dict(row))
    except IntentValidationError as exc:
        raise DecisionConsumerError(str(exc)) from exc
    return consume_decision(intent)
