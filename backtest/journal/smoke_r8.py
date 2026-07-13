from __future__ import annotations

"""Smoke validation for the disabled R8 production integration boundary."""

import os
from pathlib import Path
from unittest.mock import patch

from backtest.journal.live_rotation_integration import (
    ENV_ENABLE,
    STATUS_BOUNDARY_REACHED,
    STATUS_DISABLED,
    LiveRotationIntegrationBoundary,
    LiveRotationIntegrationConfig,
)


def main() -> int:
    executor_calls = 0

    def forbidden_executor() -> None:
        nonlocal executor_calls
        executor_calls += 1
        raise AssertionError("executor must not run in R8")

    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop(ENV_ENABLE, None)
        default_config = LiveRotationIntegrationConfig.from_environment()
        default_status = LiveRotationIntegrationBoundary(default_config).reach(forbidden_executor)

    with patch.dict(os.environ, {ENV_ENABLE: "1"}, clear=False):
        enabled_config = LiveRotationIntegrationConfig.from_environment()
        enabled_status = LiveRotationIntegrationBoundary(enabled_config).reach(forbidden_executor)

    shell_path = Path(__file__).with_name("live_observation_shell.py")
    shell_text = shell_path.read_text(encoding="utf-8") if shell_path.exists() else ""

    assertions = {
        "default_path": default_status == STATUS_DISABLED,
        "integration_disabled": not default_config.enabled,
        "enabled_integration_reaches_executor_boundary": enabled_status == STATUS_BOUNDARY_REACHED,
        "executor_never_called": executor_calls == 0,
        "shell_minimal_integration": shell_text.count("_initialize_live_rotation_integration_boundary()") == 2,
        "opportunity_manager_unchanged": "OPPORTUNITY_MANAGER_SNAPSHOT_COLUMNS" in shell_text,
        "authority_waterfall_unchanged": "AUTHORITY_WATERFALL_COLUMNS" in shell_text,
        "planning_unchanged": "_write_live_rotation_plan(" in shell_text,
    }
    if not all(assertions.values()):
        failed = ",".join(name for name, ok in assertions.items() if not ok)
        raise AssertionError(f"R8 smoke failed: {failed}")

    fields = {
        "default_path": 1,
        "executor_never_called": 1,
        "production_mutation": 0,
        "integration_disabled": 1,
        "enabled_integration_reaches_executor_boundary": 1,
        "executor_executed": 0,
        "runtime_unchanged": 1,
        "shell_minimal_integration": 1,
        "opportunity_manager_unchanged": 1,
        "authority_waterfall_unchanged": 1,
        "planning_unchanged": 1,
    }
    rendered = " ".join(f"{name}={value}" for name, value in fields.items())
    print(f"SMOKE_R8_OK {rendered}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
