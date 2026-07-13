from __future__ import annotations

"""Mandatory fail-closed production rotation authority gate for R9."""

from dataclasses import dataclass
from typing import Callable

DECISION_DENY = "DENY"
REASON_INTEGRATION_DISABLED = "INTEGRATION_DISABLED"
REASON_PRODUCTION_ROTATION_DISABLED = "PRODUCTION_ROTATION_DISABLED"
REASON_NOT_AUTHORIZED = "NOT_AUTHORIZED"
REASON_AUTHORITY_EVALUATION_FAILED = "AUTHORITY_EVALUATION_FAILED"


@dataclass(frozen=True)
class RotationAuthorizationResult:
    decision: str
    reason: str
    authorized: bool

    def as_dict(self) -> dict[str, object]:
        return {"decision": self.decision, "reason": self.reason, "authorized": self.authorized}


class LiveRotationAuthorityGate:
    """R9 authority component. No input can produce authorization."""

    def evaluate(self, *, integration_status: str) -> RotationAuthorizationResult:
        status = str(integration_status or "").strip()
        if status == "INTEGRATION_DISABLED":
            reason = REASON_INTEGRATION_DISABLED
        elif status == "BOUNDARY_REACHED_EXECUTION_DISABLED":
            reason = REASON_PRODUCTION_ROTATION_DISABLED
        else:
            reason = REASON_NOT_AUTHORIZED
        return RotationAuthorizationResult(DECISION_DENY, reason, False)


def evaluate_rotation_authority(
    *, integration_status: str,
    gate_factory: Callable[[], LiveRotationAuthorityGate] = LiveRotationAuthorityGate,
) -> RotationAuthorizationResult:
    """Evaluate exactly one gate and normalize every failure or allow-like result to DENY."""
    try:
        result = gate_factory().evaluate(integration_status=integration_status)
    except Exception:
        return RotationAuthorizationResult(DECISION_DENY, REASON_AUTHORITY_EVALUATION_FAILED, False)
    if result.decision != DECISION_DENY or result.authorized:
        return RotationAuthorizationResult(DECISION_DENY, REASON_NOT_AUTHORIZED, False)
    return result
