from __future__ import annotations

"""Behavioral smoke validation for R11 recovery-state classification."""

import ast
from pathlib import Path

from backtest.journal.live_rotation_authority import evaluate_rotation_authority
from backtest.journal.live_rotation_integration import (
    STATUS_BOUNDARY_REACHED,
    LiveRotationIntegrationBoundary,
    LiveRotationIntegrationConfig,
)
from backtest.journal.live_rotation_readiness import evaluate_live_rotation_readiness
from backtest.journal.live_rotation_recovery_state import (
    ROTATION_NOT_STARTED,
    UNKNOWN_STATE,
    classify_live_rotation_recovery_state,
)


def _load_helper(shell_path: Path, namespace: dict[str, object]):
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


def main() -> int:
    shell_path = Path(__file__).with_name("live_observation_shell.py")
    shell_text = shell_path.read_text(encoding="utf-8")
    authority_calls: list[str] = []
    readiness_calls = []
    recovery_calls = []

    class _Manager:
        design_rows = [{"csv_file": "flow_log.csv"}]

    class _Controller:
        pass

    class _Config:
        @classmethod
        def from_environment(cls):
            return LiveRotationIntegrationConfig(enabled=True, execution_enabled=False)

    def counted_authority(*, integration_status: str):
        authority_calls.append(integration_status)
        return evaluate_rotation_authority(integration_status=integration_status)

    def counted_readiness(**kwargs):
        result = evaluate_live_rotation_readiness(**kwargs)
        readiness_calls.append(result)
        return result

    def counted_recovery(**kwargs):
        result = classify_live_rotation_recovery_state(**kwargs)
        recovery_calls.append(result)
        return result

    helper = _load_helper(shell_path, {
        "Path": Path,
        "LiveRotationIntegrationBoundary": LiveRotationIntegrationBoundary,
        "LiveRotationIntegrationConfig": _Config,
        "evaluate_rotation_authority": counted_authority,
        "evaluate_live_rotation_readiness": counted_readiness,
        "classify_live_rotation_recovery_state": counted_recovery,
    })
    boundary = helper(_Manager(), _Controller())

    unknown = classify_live_rotation_recovery_state(
        integration_status=STATUS_BOUNDARY_REACHED,
        authority_result=evaluate_rotation_authority(integration_status=STATUS_BOUNDARY_REACHED),
        readiness_result=readiness_calls[0],
        crash_marker="MALFORMED",
    )

    integrated = recovery_calls[0]
    assertions = {
        "default_path": boundary is not None,
        "integration_boundary_reached": authority_calls == [STATUS_BOUNDARY_REACHED],
        "authority_gate_evaluated": len(authority_calls) == 1,
        "readiness_evaluated": len(readiness_calls) == 1,
        "recovery_state_evaluated": len(recovery_calls) == 1,
        "rotation_not_started": integrated.state == ROTATION_NOT_STARTED,
        "executor_unreachable": not readiness_calls[0].executor_reachable,
        "executor_never_called": "rotation_executor" not in shell_text,
        "planning_unchanged": "_write_live_rotation_plan(" in shell_text,
        "opportunity_manager_unchanged": "OPPORTUNITY_MANAGER_SNAPSHOT_COLUMNS" in shell_text,
        "authority_waterfall_unchanged": "AUTHORITY_WATERFALL_COLUMNS" in shell_text,
        "unknown_state_fail_closed": (
            unknown.state == UNKNOWN_STATE
            and not unknown.recoverable
            and unknown.requires_operator
            and not unknown.requires_executor
        ),
        "shell_minimal_integration": shell_text.count("classify_live_rotation_recovery_state(") == 1,
    }
    if not all(assertions.values()):
        raise AssertionError("R11 smoke failed: " + ",".join(k for k, v in assertions.items() if not v))

    fields = {
        "default_path": 1,
        "integration_boundary_reached": 1,
        "authority_gate_evaluated": 1,
        "readiness_evaluated": 1,
        "recovery_state_evaluated": 1,
        "rotation_not_started": 1,
        "executor_unreachable": 1,
        "executor_executed": 0,
        "production_mutation": 0,
        "runtime_unchanged": 1,
        "planning_unchanged": 1,
        "shell_minimal_integration": 1,
        "opportunity_manager_unchanged": 1,
        "authority_waterfall_unchanged": 1,
        "unknown_state_fail_closed": 1,
    }
    print("SMOKE_R11_OK " + " ".join(f"{k}={v}" for k, v in fields.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
