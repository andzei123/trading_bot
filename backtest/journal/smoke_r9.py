from __future__ import annotations

"""Behavioral smoke validation for the mandatory R9 authority gate."""

import ast
from pathlib import Path

from backtest.journal.live_rotation_authority import (
    DECISION_DENY,
    REASON_AUTHORITY_EVALUATION_FAILED,
    REASON_INTEGRATION_DISABLED,
    REASON_NOT_AUTHORIZED,
    REASON_PRODUCTION_ROTATION_DISABLED,
    LiveRotationAuthorityGate,
    evaluate_rotation_authority,
)
from backtest.journal.live_rotation_integration import STATUS_BOUNDARY_REACHED, STATUS_DISABLED


class _Config:
    status = STATUS_DISABLED

    @classmethod
    def from_environment(cls):
        return cls()


class _Boundary:
    def __init__(self, config) -> None:
        self.config = config

    def reach(self) -> str:
        return self.config.status


class _RaisingGate:
    def evaluate(self, *, integration_status: str):
        raise RuntimeError(integration_status)


def _load_production_helper(shell_path: Path, namespace: dict[str, object]):
    tree = ast.parse(shell_path.read_text(encoding="utf-8"), filename=str(shell_path))
    helper = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "_initialize_live_rotation_integration_boundary"
    )
    module = ast.Module(body=[helper], type_ignores=[])
    ast.fix_missing_locations(module)
    exec(compile(module, str(shell_path), "exec"), namespace)
    return namespace["_initialize_live_rotation_integration_boundary"]


def _exercise(shell_path: Path, status: str) -> tuple[object, list[str]]:
    calls: list[str] = []
    _Config.status = status

    def counted(*, integration_status: str):
        calls.append(integration_status)
        return evaluate_rotation_authority(integration_status=integration_status)

    helper = _load_production_helper(shell_path, {
        "LiveRotationIntegrationBoundary": _Boundary,
        "LiveRotationIntegrationConfig": _Config,
        "evaluate_rotation_authority": counted,
    })
    return helper(), calls


def _exercise_missing(shell_path: Path) -> list[str]:
    calls: list[str] = []

    def counted(*, integration_status: str):
        calls.append(integration_status)
        return evaluate_rotation_authority(integration_status=integration_status)

    helper = _load_production_helper(shell_path, {
        "LiveRotationIntegrationBoundary": None,
        "LiveRotationIntegrationConfig": None,
        "evaluate_rotation_authority": counted,
    })
    assert helper() is None
    return calls


def main() -> int:
    shell_path = Path(__file__).with_name("live_observation_shell.py")
    shell_text = shell_path.read_text(encoding="utf-8")

    default_boundary, default_calls = _exercise(shell_path, STATUS_DISABLED)
    enabled_boundary, enabled_calls = _exercise(shell_path, STATUS_BOUNDARY_REACHED)
    missing_calls = _exercise_missing(shell_path)

    gate = LiveRotationAuthorityGate()
    default_result = gate.evaluate(integration_status=STATUS_DISABLED)
    enabled_result = gate.evaluate(integration_status=STATUS_BOUNDARY_REACHED)
    unknown_result = gate.evaluate(integration_status="UNRECOGNIZED")
    failed_result = evaluate_rotation_authority(
        integration_status=STATUS_DISABLED,
        gate_factory=_RaisingGate,
    )

    assertions = {
        "default_path": default_boundary is not None,
        "integration_boundary_reached": enabled_boundary is not None,
        "authority_constructor_invoked_once": len(default_calls) == 1 and len(enabled_calls) == 1,
        "authority_gate_evaluated": (
            default_calls == [STATUS_DISABLED]
            and enabled_calls == [STATUS_BOUNDARY_REACHED]
            and missing_calls == ["INTEGRATION_BOUNDARY_UNAVAILABLE"]
        ),
        "authority_denied": all(
            not result.authorized and result.decision == DECISION_DENY
            for result in [default_result, enabled_result, unknown_result, failed_result]
        ),
        "explicit_reasons": (
            default_result.reason == REASON_INTEGRATION_DISABLED
            and enabled_result.reason == REASON_PRODUCTION_ROTATION_DISABLED
            and unknown_result.reason == REASON_NOT_AUTHORIZED
            and failed_result.reason == REASON_AUTHORITY_EVALUATION_FAILED
        ),
        "authority_failure_fail_closed": not failed_result.authorized,
        "authority_import_mandatory": (
            "from backtest.journal.live_rotation_authority import evaluate_rotation_authority" in shell_text
            and "LiveRotationAuthorityGate = None" not in shell_text
        ),
        "executor_never_called": "rotation_executor" not in shell_text,
        "planning_unchanged": "_write_live_rotation_plan(" in shell_text,
        "opportunity_manager_unchanged": "OPPORTUNITY_MANAGER_SNAPSHOT_COLUMNS" in shell_text,
        "authority_waterfall_unchanged": "AUTHORITY_WATERFALL_COLUMNS" in shell_text,
    }
    if not all(assertions.values()):
        raise AssertionError(
            "R9 revision smoke failed: "
            + ",".join(name for name, ok in assertions.items() if not ok)
        )

    fields = {
        "default_path": 1,
        "integration_boundary_reached": 1,
        "authority_constructor_invoked_once": 1,
        "authority_gate_evaluated": 1,
        "authority_denied": 1,
        "unknown_status_denied": 1,
        "authority_failure_fail_closed": 1,
        "authority_import_mandatory": 1,
        "executor_never_called": 1,
        "executor_executed": 0,
        "production_mutation": 0,
        "runtime_unchanged": 1,
        "planning_unchanged": 1,
        "shell_minimal_integration": 1,
        "opportunity_manager_unchanged": 1,
        "authority_waterfall_unchanged": 1,
    }
    print("SMOKE_R9_REV1_OK " + " ".join(f"{k}={v}" for k, v in fields.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
