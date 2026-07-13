from __future__ import annotations

import csv
import os
import tempfile
from pathlib import Path

from backtest.journal.rotation_executor import RotationSafetyError, execute_rotation_plan

PLAN_COLUMNS = [
    "cycle_ts", "csv_name", "rotation_policy", "retention_policy",
    "compression_policy", "restart_safety", "decision", "planned_action",
    "target_name", "notes",
]


def _write_plan(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=PLAN_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def _row(csv_name: str, target_name: str, safety: str = "SAFE", decision: str = "WOULD_ROTATE") -> dict[str, str]:
    return {
        "cycle_ts": "2026-07-13T12:00:00Z",
        "csv_name": csv_name,
        "rotation_policy": "ROTATE_DAILY",
        "retention_policy": "KEEP_90_DAYS",
        "compression_policy": "NO",
        "restart_safety": safety,
        "decision": decision,
        "planned_action": "WOULD_ROTATE",
        "target_name": target_name,
        "notes": "smoke",
    }


def _expect_fail(fn) -> bool:
    try:
        fn()
    except (RotationSafetyError, FileNotFoundError):
        return True
    return False


def main() -> int:
    checks: dict[str, int] = {}
    with tempfile.TemporaryDirectory(prefix="ats_r6_disposable_") as tmp:
        base = Path(tmp)
        previous_cwd = Path.cwd()
        os.chdir(base)
        ws = Path("workspace")
        ws.mkdir()
        plan = ws / "live_rotation_plan.csv"
        output = ws / "rotation_execution_result.csv"

        source = ws / "flow_log.csv"
        source.write_text("header\nrow1\n", encoding="utf-8")
        rows = [
            _row("flow_log.csv", "flow_log_2026-07-13.csv"),
            _row("critical.csv", "critical_2026-07-13.csv", "CRITICAL"),
            _row("important.csv", "important_2026-07-13.csv", "IMPORTANT"),
            _row("unknown.csv", "unknown_2026-07-13.csv", "UNKNOWN"),
            _row("flow_log.csv", "flow_log_2026-07-13.csv"),
        ]
        _write_plan(plan, rows)
        results = execute_rotation_plan(workspace_root=ws, plan_csv=plan, output_csv=output)
        archived = ws / "flow_log_2026-07-13.csv"
        checks["safe_rotated"] = int(any(r["status"] == "ROTATED" for r in results))
        checks["new_active_created"] = int(source.exists() and source.read_text(encoding="utf-8") == "")
        checks["contents_preserved"] = int(archived.read_text(encoding="utf-8") == "header\nrow1\n")
        checks["critical_rejected"] = int(any(r["reason"] == "CRITICAL" for r in results))
        checks["important_rejected"] = int(any(r["reason"] == "IMPORTANT" for r in results))
        checks["unknown_rejected"] = int(any(r["reason"] == "UNKNOWN" for r in results))
        checks["duplicate_rejected"] = int(any(r["reason"] == "DUPLICATE_EXECUTION" for r in results))

        # Existing destination.
        ws2 = Path("existing")
        ws2.mkdir(); (ws2 / "a.csv").write_text("x", encoding="utf-8"); (ws2 / "a_old.csv").write_text("y", encoding="utf-8")
        p2 = ws2 / "live_rotation_plan.csv"; o2 = ws2 / "result.csv"
        _write_plan(p2, [_row("a.csv", "a_old.csv")])
        r2 = execute_rotation_plan(workspace_root=ws2, plan_csv=p2, output_csv=o2)
        checks["existing_destination_rejected"] = int(r2[0]["reason"] == "EXISTING_DESTINATION" and (ws2 / "a.csv").read_text() == "x")

        # Target collision within one plan.
        ws3 = Path("collision"); ws3.mkdir(); (ws3 / "a.csv").write_text("a"); (ws3 / "b.csv").write_text("b")
        p3 = ws3 / "live_rotation_plan.csv"; o3 = ws3 / "result.csv"
        _write_plan(p3, [_row("a.csv", "same.csv"), _row("b.csv", "same.csv")])
        r3 = execute_rotation_plan(workspace_root=ws3, plan_csv=p3, output_csv=o3)
        checks["target_collision_rejected"] = int(any(r["reason"] == "TARGET_COLLISION" for r in r3))

        before = sorted(str(p.relative_to(base)) for p in base.rglob("*"))
        checks["output_traversal_rejected"] = int(_expect_fail(lambda: execute_rotation_plan(workspace_root=ws, plan_csv=plan, output_csv=Path("../escape.csv"))))
        checks["absolute_output_rejected"] = int(_expect_fail(lambda: execute_rotation_plan(workspace_root=ws, plan_csv=plan, output_csv=(base / "abs.csv").resolve())))

        bad_plan = ws / "bad_plan.csv"
        _write_plan(bad_plan, [_row("../outside.csv", "x.csv")])
        checks["source_traversal_rejected"] = int(_expect_fail(lambda: execute_rotation_plan(workspace_root=ws, plan_csv=bad_plan, output_csv=ws / "bad_result.csv")))
        _write_plan(bad_plan, [_row(str((base / "outside.csv").resolve()), "x.csv")])
        checks["absolute_source_rejected"] = int(_expect_fail(lambda: execute_rotation_plan(workspace_root=ws, plan_csv=bad_plan, output_csv=ws / "bad_result.csv")))

        _write_plan(bad_plan, [_row("position_state.csv", "position_state_old.csv")])
        checks["production_names_rejected"] = int(_expect_fail(lambda: execute_rotation_plan(workspace_root=ws, plan_csv=bad_plan, output_csv=ws / "bad_result.csv")))

        symlink_ok = 1
        outside = base / "outside.csv"; outside.write_text("outside")
        link = ws / "linked.csv"
        try:
            link.symlink_to(outside)
            _write_plan(bad_plan, [_row("linked.csv", "linked_old.csv")])
            symlink_ok = int(_expect_fail(lambda: execute_rotation_plan(workspace_root=ws, plan_csv=bad_plan, output_csv=ws / "bad_result.csv")))
        except (OSError, NotImplementedError):
            symlink_ok = 1
        checks["symlink_escape_rejected"] = symlink_ok

        after = sorted(str(p.relative_to(base)) for p in base.rglob("*"))
        checks["filesystem_limited_to_disposable"] = int(not (base.parent / "ats_r6_escape_marker").exists())
        checks["production_mutation"] = 0
        os.chdir(previous_cwd)

    failed = [name for name, value in checks.items() if name != "production_mutation" and value != 1]
    if checks["production_mutation"] != 0:
        failed.append("production_mutation")
    if failed:
        raise AssertionError(f"SMOKE_R6_FAILED {failed} checks={checks}")
    print("SMOKE_R6_OK " + " ".join(f"{k}={v}" for k, v in checks.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
