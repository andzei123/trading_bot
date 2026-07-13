from __future__ import annotations

"""Diagnostic-only production rotation recovery-state classification for R11.

This module classifies hypothetical interruption states. It never authorizes
rotation, invokes an executor, modifies journals, or performs recovery.
"""

from dataclasses import dataclass
from typing import Any

NO_ROTATION_STARTED = "NO_ROTATION_STARTED"
WAITING_FOR_AUTHORITY = "WAITING_FOR_AUTHORITY"
WAITING_FOR_READINESS = "WAITING_FOR_READINESS"
READY_BUT_NOT_AUTHORIZED = "READY_BUT_NOT_AUTHORIZED"
ROTATION_NOT_STARTED = "ROTATION_NOT_STARTED"
CRASH_BEFORE_EXECUTOR = "CRASH_BEFORE_EXECUTOR"
CRASH_AFTER_AUTHORITY = "CRASH_AFTER_AUTHORITY"
UNKNOWN_STATE = "UNKNOWN_STATE"

DIAGNOSTIC_VERSION = "R11.1"

SUPPORTED_STATES = {
    NO_ROTATION_STARTED,
    WAITING_FOR_AUTHORITY,
    WAITING_FOR_READINESS,
    READY_BUT_NOT_AUTHORIZED,
    ROTATION_NOT_STARTED,
    CRASH_BEFORE_EXECUTOR,
    CRASH_AFTER_AUTHORITY,
    UNKNOWN_STATE,
}


@dataclass(frozen=True)
class LiveRotationRecoveryState:
    state: str
    recoverable: bool
    reason: str
    requires_operator: bool
    requires_executor: bool
    diagnostic_version: str = DIAGNOSTIC_VERSION

    def as_dict(self) -> dict[str, object]:
        return {
            field: getattr(self, field)
            for field in self.__dataclass_fields__
        }


def _result(
    state: str,
    *,
    recoverable: bool,
    reason: str,
    requires_operator: bool,
    requires_executor: bool,
) -> LiveRotationRecoveryState:
    if state not in SUPPORTED_STATES:
        state = UNKNOWN_STATE
        recoverable = False
        reason = "UNSUPPORTED_RECOVERY_STATE"
        requires_operator = True
        requires_executor = False
    return LiveRotationRecoveryState(
        state=state,
        recoverable=bool(recoverable),
        reason=str(reason or "UNKNOWN_RECOVERY_REASON"),
        requires_operator=bool(requires_operator),
        requires_executor=bool(requires_executor),
    )


def classify_live_rotation_recovery_state(
    *,
    integration_status: str,
    authority_result: Any,
    readiness_result: Any,
    rotation_started: bool = False,
    executor_reached: bool = False,
    crash_marker: str = "",
) -> LiveRotationRecoveryState:
    """Classify a hypothetical recovery state without triggering recovery."""
    status = str(integration_status or "").strip()
    crash = str(crash_marker or "").strip().upper()

    if crash not in {"", "BEFORE_EXECUTOR", "AFTER_AUTHORITY"}:
        return _result(
            UNKNOWN_STATE,
            recoverable=False,
            reason="MALFORMED_CRASH_MARKER",
            requires_operator=True,
            requires_executor=False,
        )

    if authority_result is None:
        return _result(
            WAITING_FOR_AUTHORITY,
            recoverable=False,
            reason="AUTHORITY_RESULT_UNAVAILABLE",
            requires_operator=True,
            requires_executor=False,
        )

    authority_decision = str(getattr(authority_result, "decision", "UNKNOWN") or "UNKNOWN")
    authority_authorized = bool(getattr(authority_result, "authorized", False))

    if readiness_result is None:
        return _result(
            WAITING_FOR_READINESS,
            recoverable=False,
            reason="READINESS_RESULT_UNAVAILABLE",
            requires_operator=True,
            requires_executor=False,
        )

    readiness_state = str(getattr(readiness_result, "overall_state", "UNKNOWN") or "UNKNOWN")
    rotation_allowed = bool(getattr(readiness_result, "rotation_allowed", False))

    if crash == "AFTER_AUTHORITY":
        return _result(
            CRASH_AFTER_AUTHORITY,
            recoverable=True,
            reason="INTERRUPTION_AFTER_AUTHORITY_BEFORE_EXECUTOR",
            requires_operator=True,
            requires_executor=False,
        )

    if crash == "BEFORE_EXECUTOR":
        return _result(
            CRASH_BEFORE_EXECUTOR,
            recoverable=True,
            reason="INTERRUPTION_BEFORE_EXECUTOR_ENTRY",
            requires_operator=True,
            requires_executor=False,
        )

    if executor_reached:
        return _result(
            UNKNOWN_STATE,
            recoverable=False,
            reason="EXECUTOR_REACHED_OUTSIDE_R11_CONTRACT",
            requires_operator=True,
            requires_executor=False,
        )

    if rotation_started:
        return _result(
            UNKNOWN_STATE,
            recoverable=False,
            reason="ROTATION_STARTED_WITHOUT_RECOVERY_IMPLEMENTATION",
            requires_operator=True,
            requires_executor=False,
        )

    if status in {"", "INTEGRATION_BOUNDARY_UNAVAILABLE", "INTEGRATION_BOUNDARY_ERROR"}:
        return _result(
            NO_ROTATION_STARTED,
            recoverable=True,
            reason="INTEGRATION_BOUNDARY_NOT_READY",
            requires_operator=False,
            requires_executor=False,
        )

    if authority_authorized or authority_decision != "DENY":
        return _result(
            UNKNOWN_STATE,
            recoverable=False,
            reason="AUTHORITY_RESULT_NOT_FAIL_CLOSED",
            requires_operator=True,
            requires_executor=False,
        )

    if readiness_state == "READY" and not rotation_allowed:
        return _result(
            READY_BUT_NOT_AUTHORIZED,
            recoverable=True,
            reason="READINESS_READY_AUTHORITY_DENIED",
            requires_operator=False,
            requires_executor=False,
        )

    if readiness_state == "NOT_READY" and not rotation_allowed:
        return _result(
            ROTATION_NOT_STARTED,
            recoverable=True,
            reason="AUTHORITY_DENIED_AND_READINESS_NOT_READY",
            requires_operator=False,
            requires_executor=False,
        )

    return _result(
        UNKNOWN_STATE,
        recoverable=False,
        reason="UNCLASSIFIED_RECOVERY_INPUT",
        requires_operator=True,
        requires_executor=False,
    )
