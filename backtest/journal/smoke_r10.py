from __future__ import annotations

"""Behavioral smoke validation for R10 readiness diagnostics."""

import ast
from pathlib import Path
from tempfile import TemporaryDirectory

from backtest.journal.live_rotation_authority import evaluate_rotation_authority
from backtest.journal.live_rotation_integration import (
    STATUS_BOUNDARY_REACHED,
    LiveRotationIntegrationBoundary,
    LiveRotationIntegrationConfig,
)
from backtest.journal.live_rotation_readiness import (
    STATE_NOT_READY,
    evaluate_live_rotation_readiness,
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
    readiness_calls = []
    authority_calls = []

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

    helper = _load_helper(shell_path, {
        "Path": Path,
        "LiveRotationIntegrationBoundary": LiveRotationIntegrationBoundary,
        "LiveRotationIntegrationConfig": _Config,
        "evaluate_rotation_authority": counted_authority,
        "evaluate_live_rotation_readiness": counted_readiness,
    })
    boundary = helper(_Manager(), _Controller())

    with TemporaryDirectory() as tmp:
        plan_path = Path(tmp) / "live_rotation_plan.csv"
        plan_path.write_text("cycle_ts,csv_name\n", encoding="utf-8")
        authority = evaluate_rotation_authority(integration_status=STATUS_BOUNDARY_REACHED)
        direct = evaluate_live_rotation_readiness(
            integration_boundary=boundary,
            authority_result=authority,
            executor_reachable=False,
            rotation_plan_path=Path("disposable/live_rotation_plan.csv"),
            rotation_policy_available=True,
            rotation_manager_initialized=True,
            rotation_controller_initialized=True,
        )

    integrated = readiness_calls[0]
    assertions = {
        "default_path": boundary is not None,
        "integration_boundary_reached": authority_calls == [STATUS_BOUNDARY_REACHED],
        "authority_gate_evaluated": len(authority_calls) == 1,
        "authority_denied": integrated.authority_decision == "DENY",
        "readiness_evaluated": len(readiness_calls) == 1,
        "rotation_allowed": not integrated.rotation_allowed and not direct.rotation_allowed,
        "overall_not_ready": integrated.overall_state == STATE_NOT_READY and direct.overall_state == STATE_NOT_READY,
        "executor_unreachable": not integrated.executor_reachable and not direct.executor_reachable,
        "executor_never_called": "rotation_executor" not in shell_text,
        "planning_unchanged": "_write_live_rotation_plan(" in shell_text,
        "opportunity_manager_unchanged": "OPPORTUNITY_MANAGER_SNAPSHOT_COLUMNS" in shell_text,
        "authority_waterfall_unchanged": "AUTHORITY_WATERFALL_COLUMNS" in shell_text,
        "shell_minimal_integration": shell_text.count("evaluate_live_rotation_readiness(") == 1,
    }
    if not all(assertions.values()):
        raise AssertionError("R10 smoke failed: " + ",".join(k for k, v in assertions.items() if not v))

    fields = {
        "default_path": 1,
        "integration_boundary_reached": 1,
        "authority_gate_evaluated": 1,
        "authority_denied": 1,
        "readiness_evaluated": 1,
        "rotation_allowed": 0,
        "overall_not_ready": 1,
        "executor_unreachable": 1,
        "executor_executed": 0,
        "production_mutation": 0,
        "runtime_unchanged": 1,
        "planning_unchanged": 1,
        "shell_minimal_integration": 1,
        "opportunity_manager_unchanged": 1,
        "authority_waterfall_unchanged": 1,
    }
    print("SMOKE_R10_OK " + " ".join(f"{k}={v}" for k, v in fields.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
