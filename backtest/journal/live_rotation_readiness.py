from __future__ import annotations

"""Immutable production rotation readiness diagnostics for R10.

This module is observation-only. It cannot authorize rotation, execute an
executor, or mutate journal files.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

STATE_NOT_READY = "NOT_READY"
STATE_READY = "READY"


@dataclass(frozen=True)
class LiveRotationReadinessResult:
    integration_boundary_present: bool
    authority_gate_reachable: bool
    authority_decision: str
    authority_denial_reason: str
    executor_reachable: bool
    rotation_plan_available: bool
    rotation_policy_available: bool
    rotation_manager_initialized: bool
    rotation_controller_initialized: bool
    workspace_valid: bool
    integration_ready: bool
    authority_ready: bool
    executor_ready: bool
    rotation_allowed: bool
    overall_state: str
    reason: str

    def as_dict(self) -> dict[str, object]:
        return {
            field: getattr(self, field)
            for field in self.__dataclass_fields__
        }


def _safe_exists(path_like: Optional[Path]) -> bool:
    if path_like is None:
        return False
    try:
        return Path(path_like).exists()
    except Exception:
        return False


def _workspace_valid(path_like: Optional[Path]) -> bool:
    if path_like is None:
        return False
    try:
        path = Path(path_like)
        parent = path.parent
        return not path.is_absolute() and ".." not in path.parts and parent != Path("")
    except Exception:
        return False


def evaluate_live_rotation_readiness(
    *,
    integration_boundary: Any,
    authority_result: Any,
    executor_reachable: bool,
    rotation_plan_path: Optional[Path],
    rotation_policy_available: bool,
    rotation_manager_initialized: bool,
    rotation_controller_initialized: bool,
) -> LiveRotationReadinessResult:
    """Evaluate readiness without authorizing or executing rotation."""
    integration_present = integration_boundary is not None
    authority_reachable = authority_result is not None
    authority_decision = str(getattr(authority_result, "decision", "UNKNOWN") or "UNKNOWN")
    authority_reason = str(getattr(authority_result, "reason", "NOT_AUTHORIZED") or "NOT_AUTHORIZED")
    authority_authorized = bool(getattr(authority_result, "authorized", False))
    plan_available = _safe_exists(rotation_plan_path)
    workspace_valid = _workspace_valid(rotation_plan_path)

    integration_ready = integration_present
    authority_ready = authority_reachable and authority_decision == "DENY" and not authority_authorized
    executor_ready = bool(executor_reachable)

    # R10 is diagnostic-only. Rotation remains mechanically forbidden.
    rotation_allowed = False
    overall_state = STATE_NOT_READY

    if not integration_present:
        reason = "INTEGRATION_BOUNDARY_UNAVAILABLE"
    elif not authority_reachable:
        reason = "AUTHORITY_GATE_UNAVAILABLE"
    elif authority_authorized or authority_decision != "DENY":
        reason = "AUTHORITY_RESULT_INVALID"
    elif not rotation_manager_initialized:
        reason = "ROTATION_MANAGER_NOT_INITIALIZED"
    elif not rotation_controller_initialized:
        reason = "ROTATION_CONTROLLER_NOT_INITIALIZED"
    elif not rotation_policy_available:
        reason = "ROTATION_POLICY_UNAVAILABLE"
    elif not plan_available:
        reason = "ROTATION_PLAN_UNAVAILABLE"
    elif not workspace_valid:
        reason = "WORKSPACE_INVALID"
    elif not executor_ready:
        reason = "EXECUTOR_UNREACHABLE"
    else:
        reason = authority_reason or "PRODUCTION_ROTATION_DISABLED"

    return LiveRotationReadinessResult(
        integration_boundary_present=integration_present,
        authority_gate_reachable=authority_reachable,
        authority_decision=authority_decision,
        authority_denial_reason=authority_reason,
        executor_reachable=bool(executor_reachable),
        rotation_plan_available=plan_available,
        rotation_policy_available=bool(rotation_policy_available),
        rotation_manager_initialized=bool(rotation_manager_initialized),
        rotation_controller_initialized=bool(rotation_controller_initialized),
        workspace_valid=workspace_valid,
        integration_ready=integration_ready,
        authority_ready=authority_ready,
        executor_ready=executor_ready,
        rotation_allowed=rotation_allowed,
        overall_state=overall_state,
        reason=reason,
    )
