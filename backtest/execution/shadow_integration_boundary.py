from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
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
    paper_requested: bool = False
    paper_started: bool = False
    paper_completed: bool = False
    paper_failed: bool = False
    paper_duplicate_blocked: bool = False
    paper_state: str = ""
    paper_event_count: int = 0


_SESSION_PAPER_EXECUTOR = None


def _session_paper_executor():
    """Return the E22.1 session-local paper executor without persistence."""

    global _SESSION_PAPER_EXECUTOR
    if _SESSION_PAPER_EXECUTOR is None:
        from .execution_event_ledger import ExecutionEventLedger
        from .execution_identity_registry import ExecutionIdentityRegistry
        from .paper_executor import PaperExecutor

        _SESSION_PAPER_EXECUTOR = PaperExecutor(
            ledger=ExecutionEventLedger(),
            identity_registry=ExecutionIdentityRegistry(),
            executor_mode="DRY_RUN",
        )
    return _SESSION_PAPER_EXECUTOR


def _text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def evaluate_ats_shadow_admission(
    ats_row: Mapping[str, object],
    *,
    cycle_ts: object,
    checked_at_utc: str,
    paper_mode: bool = False,
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

    if not mechanical.allowed or not paper_mode:
        return ExecutorShadowAdmissionResult(
            accepted_by_ingress=True,
            mechanical_allowed=mechanical.allowed,
            reason=mechanical.reason,
            severity=mechanical.severity,
            canonical_setup_key=mechanical.canonical_setup_key,
            symbol=mechanical.symbol,
            side=mechanical.side,
            paper_requested=bool(paper_mode),
        )

    paper_requested = True
    paper_started = False
    try:
        from .paper_executor import PaperExecutionRequest

        executor = _session_paper_executor()
        paper_started = True
        paper_duplicate_blocked = executor.identity_registry.contains(
            validated.canonical_setup_key
        )
        event_count_before = len(executor.ledger.events)
        snapshot = executor.run(
            validated,
            PaperExecutionRequest(
                checked_at_utc=checked_at_utc,
                simulated_at_utc=checked_at_utc,
            ),
        )
        paper_duplicate_blocked = bool(
            paper_duplicate_blocked
            and len(executor.ledger.events) == event_count_before
        )
    except Exception as exc:
        return ExecutorShadowAdmissionResult(
            accepted_by_ingress=True,
            mechanical_allowed=True,
            reason=f"EXECUTOR_PAPER_FAILED:{type(exc).__name__}:{exc}",
            severity="CRITICAL",
            canonical_setup_key=mechanical.canonical_setup_key,
            symbol=mechanical.symbol,
            side=mechanical.side,
            paper_requested=paper_requested,
            paper_started=paper_started,
            paper_failed=True,
        )

    return ExecutorShadowAdmissionResult(
        accepted_by_ingress=True,
        mechanical_allowed=True,
        reason=mechanical.reason,
        severity=mechanical.severity,
        canonical_setup_key=mechanical.canonical_setup_key,
        symbol=mechanical.symbol,
        side=mechanical.side,
        paper_requested=paper_requested,
        paper_started=True,
        paper_completed=True,
        paper_duplicate_blocked=paper_duplicate_blocked,
        paper_state=snapshot.current_state,
        paper_event_count=snapshot.event_count,
    )

_ATS_CLOSE_REASONS = frozenset({"TP", "SL"})


@dataclass(frozen=True)
class AtsCloseObservation:
    """Immutable structurally validated ATS Position State CLOSED fact."""

    canonical_setup_key: str
    status: str
    closed_at_utc: str
    close_reason: str


@dataclass(frozen=True)
class ExecutorCloseObservationResult:
    """Diagnostic-only result for one ATS-authoritative close observation."""

    observed: bool
    completed: bool
    failed: bool
    duplicate_blocked: bool
    classification: str
    reason: str
    canonical_setup_key: str
    lifecycle_state: str = ""
    lifecycle_event_count: int = 0


def _validate_ats_close_observation(
    *,
    canonical_setup_key: object,
    status: object,
    closed_at_utc: object,
    close_reason: object,
) -> tuple[AtsCloseObservation | None, str, str]:
    key = _text(canonical_setup_key)
    if not key:
        return None, "MISSING_CANONICAL_KEY", "canonical_setup_key is required"

    normalized_status = _text(status).upper()
    if normalized_status != "CLOSED":
        return None, "STATUS_NOT_CLOSED", "status must be CLOSED"

    timestamp_text = _text(closed_at_utc)
    if not timestamp_text:
        return None, "MISSING_TIMESTAMP", "closed_at_utc is required"
    try:
        parsed = datetime.fromisoformat(timestamp_text.replace("Z", "+00:00"))
    except ValueError:
        return None, "INVALID_TIMESTAMP", "closed_at_utc is not ISO-8601 parseable"
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None, "INVALID_TIMESTAMP", "closed_at_utc must be timezone-aware"
    normalized_timestamp = parsed.astimezone(timezone.utc).isoformat()

    normalized_reason = _text(close_reason).upper()
    if not normalized_reason:
        return None, "MISSING_REASON", "close_reason is required"
    if normalized_reason not in _ATS_CLOSE_REASONS:
        return None, "UNSUPPORTED_REASON", (
            "close_reason must match authoritative position_closer output: TP or SL"
        )

    return (
        AtsCloseObservation(
            canonical_setup_key=key,
            status="CLOSED",
            closed_at_utc=normalized_timestamp,
            close_reason=normalized_reason,
        ),
        "VALID",
        "ats close observation payload valid",
    )


def _completed_close_metadata(executor, key: str) -> tuple[str, str]:
    """Return recorded E24 close timestamp/reason without changing E23 snapshots."""

    from .execution_event_ledger import CLOSE_REQUESTED

    for event in reversed(executor.ledger.events):
        if event.canonical_setup_key != key or event.event_type != CLOSE_REQUESTED:
            continue
        prefix = "ats_position_closed:"
        reason = event.reason[len(prefix):] if event.reason.startswith(prefix) else event.reason
        return event.recorded_at_utc, reason.upper()
    return "", ""


def observe_ats_position_closed(
    *,
    canonical_setup_key: object,
    status: object,
    closed_at_utc: object,
    close_reason: object,
) -> ExecutorCloseObservationResult:
    """Validate and record a close already persisted by authoritative ATS."""

    payload, classification, detail = _validate_ats_close_observation(
        canonical_setup_key=canonical_setup_key,
        status=status,
        closed_at_utc=closed_at_utc,
        close_reason=close_reason,
    )
    if payload is None:
        return ExecutorCloseObservationResult(
            observed=False,
            completed=False,
            failed=True,
            duplicate_blocked=False,
            classification=classification,
            reason=f"EXECUTOR_CLOSE_OBSERVATION_BLOCKED:{classification}:{detail}",
            canonical_setup_key=_text(canonical_setup_key),
        )

    try:
        executor = _session_paper_executor()
        before = executor.lifecycle_snapshot(payload.canonical_setup_key)
        if before.completed:
            prior_timestamp, prior_reason = _completed_close_metadata(
                executor, payload.canonical_setup_key
            )
            identical = (
                prior_timestamp == payload.closed_at_utc
                and prior_reason == payload.close_reason
            )
            duplicate_classification = (
                "DUPLICATE_OBSERVATION"
                if identical
                else "CONFLICTING_DUPLICATE_OBSERVATION"
            )
            return ExecutorCloseObservationResult(
                observed=True,
                completed=False,
                failed=True,
                duplicate_blocked=True,
                classification=duplicate_classification,
                reason=f"EXECUTOR_CLOSE_OBSERVATION_BLOCKED:{duplicate_classification}",
                canonical_setup_key=payload.canonical_setup_key,
                lifecycle_state=before.current_state,
                lifecycle_event_count=before.event_count,
            )

        snapshot = executor.observe_ats_position_closed(
            payload.canonical_setup_key,
            observed_at_utc=payload.closed_at_utc,
            reason=f"ats_position_closed:{payload.close_reason}",
        )
    except Exception as exc:
        from .paper_executor import PaperExecutorError

        is_lifecycle = isinstance(exc, PaperExecutorError)
        failure_classification = (
            "LIFECYCLE_ORDERING_FAILURE" if is_lifecycle else "UNEXPECTED_INTERNAL_FAILURE"
        )
        return ExecutorCloseObservationResult(
            observed=True,
            completed=False,
            failed=True,
            duplicate_blocked=False,
            classification=failure_classification,
            reason=(
                f"EXECUTOR_CLOSE_OBSERVATION_FAILED:{failure_classification}:"
                f"{type(exc).__name__}:{exc}"
            ),
            canonical_setup_key=payload.canonical_setup_key,
        )

    return ExecutorCloseObservationResult(
        observed=True,
        completed=True,
        failed=False,
        duplicate_blocked=False,
        classification="CLOSE_OBSERVED",
        reason="ats_close_observed",
        canonical_setup_key=payload.canonical_setup_key,
        lifecycle_state=snapshot.current_state,
        lifecycle_event_count=snapshot.event_count,
    )
