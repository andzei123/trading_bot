from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from .decision_consumer import DecisionConsumerError, consume_decision
from .intent import ExecutionIntent, IntentValidationError
from .mechanical_safety_bridge import (
    MechanicalSafetyBridgeError,
    check_mechanical_safety,
)


@dataclass(frozen=True)
class ExecutorShadowAdmissionResult:
    """Immutable E21 shadow-only observation.

    This result is advisory telemetry printed by the ATS shell. It cannot
    authorize execution and the shell must not use it to change ATS behavior.
    """

    accepted_by_ingress: bool
    mechanical_allowed: bool
    reason: str
    severity: str
    canonical_setup_key: str
    symbol: str
    side: str


def _text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def evaluate_ats_shadow_admission(
    ats_row: Mapping[str, object],
    *,
    cycle_ts: object,
    checked_at_utc: str,
) -> ExecutorShadowAdmissionResult:
    """Run the E1 -> E2 -> E4 boundary without side effects.

    The caller supplies a copy of the final ATS-admitted row. E21 adds only the
    factual boundary metadata that the shell itself knows at this point. It
    does not calculate quantity, notional, risk, strategy fields, or authority
    snapshot identifiers. Missing authoritative fields fail closed locally.
    """

    source = dict(ats_row)
    intent_row = {key: _text(value) for key, value in source.items()}

    # Protocol metadata and the shell cycle timestamp are boundary facts. ATS
    # authority fields are never synthesized: selected_for_execution,
    # execution_rank, selection_reason, quantity, risk, and snapshot identity
    # must already be present in the final ATS-admitted row.
    intent_row["schema_version"] = "ATS_EXECUTION_INTENT_V1"
    intent_row["cycle_ts"] = _text(cycle_ts)

    canonical_key = _text(source.get("canonical_setup_key"))
    symbol = _text(source.get("symbol")).upper()
    side = _text(source.get("side")).upper()

    try:
        intent = ExecutionIntent.from_row(intent_row)
        validated = consume_decision(intent)
        mechanical = check_mechanical_safety(
            validated,
            executor_mode="DRY_RUN",
            checked_at_utc=checked_at_utc,
        )
    except (
        IntentValidationError,
        DecisionConsumerError,
        MechanicalSafetyBridgeError,
        TypeError,
        ValueError,
    ) as exc:
        return ExecutorShadowAdmissionResult(
            accepted_by_ingress=False,
            mechanical_allowed=False,
            reason=f"shadow_ingress_blocked:{exc}",
            severity="BLOCK",
            canonical_setup_key=canonical_key,
            symbol=symbol,
            side=side,
        )
    except Exception as exc:
        return ExecutorShadowAdmissionResult(
            accepted_by_ingress=False,
            mechanical_allowed=False,
            reason=(
                "EXECUTOR_SHADOW_INTERNAL_ERROR:"
                f"{type(exc).__name__}:{exc}"
            ),
            severity="CRITICAL",
            canonical_setup_key=canonical_key,
            symbol=symbol,
            side=side,
        )

    return ExecutorShadowAdmissionResult(
        accepted_by_ingress=True,
        mechanical_allowed=mechanical.allowed,
        reason=mechanical.reason,
        severity=mechanical.severity,
        canonical_setup_key=mechanical.canonical_setup_key,
        symbol=mechanical.symbol,
        side=mechanical.side,
    )
