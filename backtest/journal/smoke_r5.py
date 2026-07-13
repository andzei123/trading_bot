from __future__ import annotations

import csv
import hashlib
import tempfile
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backtest.journal.rotation_execution_simulator import (
    OutputPathSafetyError,
    run_simulation,
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_plan(path: Path) -> None:
    columns = [
        "cycle_ts", "csv_name", "rotation_policy", "retention_policy",
        "compression_policy", "restart_safety", "decision", "planned_action",
        "target_name", "notes",
    ]
    rows = [
        ["2026-07-07T00:00:00+00:00", "flow_log.csv", "ROTATE_DAILY", "KEEP_90_DAYS", "ZIP_AFTER_ROTATION", "SAFE", "WOULD_ROTATE", "WOULD_ROTATE", "flow_log_2026-07-07.csv", "safe"],
        ["2026-07-07T00:00:00+00:00", "position_state.csv", "NEVER_ROTATE", "KEEP_FOREVER", "NO", "CRITICAL", "BLOCKED_CRITICAL", "BLOCKED", "position_state.csv", "critical"],
        ["2026-07-07T00:00:00+00:00", "live_observation_entries.csv", "ROTATE_AFTER_RUNTIME_VALIDATION", "KEEP_180_DAYS", "ZIP_AFTER_ROTATION", "IMPORTANT", "BLOCKED_IMPORTANT", "BLOCKED", "live_observation_entries.csv", "important"],
        ["2026-07-07T00:00:00+00:00", "unknown.csv", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN_POLICY", "UNKNOWN", "unknown.csv", "unknown"],
        ["2026-07-07T00:00:00+00:00", "flow_log.csv", "ROTATE_DAILY", "KEEP_90_DAYS", "ZIP_AFTER_ROTATION", "SAFE", "WOULD_ROTATE", "WOULD_ROTATE", "flow_log_2026-07-07.csv", "duplicate"],
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)


def _expect_output_rejection(plan_csv: Path, source_root: Path, output_csv: Path) -> None:
    before = set(plan_csv.parent.rglob("*"))
    try:
        run_simulation(plan_csv, source_root, output_csv)
    except OutputPathSafetyError:
        pass
    else:
        raise AssertionError(f"unsafe output path was not rejected: {output_csv}")
    after = set(plan_csv.parent.rglob("*"))
    assert before == after


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="ats_rotation_r5_") as tmp:
        root = Path(tmp)
        source_root = root / "copied_journals"
        source_root.mkdir()
        for name in ["flow_log.csv", "position_state.csv", "live_observation_entries.csv", "unknown.csv"]:
            (source_root / name).write_text(f"fixture={name}\n", encoding="utf-8")

        before = {path.name: _sha256(path) for path in source_root.iterdir()}
        plan_csv = root / "live_rotation_plan.csv"
        output_csv = Path("rotation_execution_simulation.csv")
        _write_plan(plan_csv)
        run_simulation(plan_csv, source_root, output_csv)
        resolved_output = root / output_csv
        after = {path.name: _sha256(path) for path in source_root.iterdir()}

        with resolved_output.open("r", newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        results = [row["simulation_result"] for row in rows]

        assert results == [
            "ACCEPTED",
            "REJECTED_CRITICAL",
            "REJECTED_IMPORTANT",
            "REJECTED_UNKNOWN",
            "REJECTED_DUPLICATE",
        ]
        assert rows[0]["target_name"] == "flow_log_2026-07-07.csv"
        assert rows[0]["simulated_action"] == "WOULD_ROTATE"
        assert before == after
        assert sorted(path.name for path in source_root.iterdir()) == sorted(before)

        outside_parent = root.parent / f"{root.name}_outside"
        outside_output = Path("..") / outside_parent.name / "rotation_execution_simulation.csv"
        _expect_output_rejection(plan_csv, source_root, outside_output)
        assert not outside_parent.exists()

        _expect_output_rejection(plan_csv, source_root, Path("live_rotation_plan.csv"))
        _expect_output_rejection(plan_csv, source_root, Path("copied_journals"))
        _expect_output_rejection(plan_csv, source_root, Path("flow_log.csv"))
        _expect_output_rejection(plan_csv, source_root, Path("nested") / ".." / "rotation_execution_simulation.csv")
        _expect_output_rejection(plan_csv, source_root, root / "absolute_output.csv")

        blocked_parent = root / "blocked_parent"
        _expect_output_rejection(
            plan_csv,
            source_root,
            Path("blocked_parent") / ".." / "rotation_execution_simulation.csv",
        )
        assert not blocked_parent.exists()

        sentinel = root / "sentinel.csv"
        sentinel.write_text("DO_NOT_TRUNCATE\n", encoding="utf-8")
        sentinel_hash = _sha256(sentinel)
        _expect_output_rejection(plan_csv, source_root, sentinel)
        assert _sha256(sentinel) == sentinel_hash

        outside_output_dir = root.parent / f"{root.name}_output_escape"
        outside_output_dir.mkdir()
        output_link = root / "output_link"
        try:
            output_link.symlink_to(outside_output_dir, target_is_directory=True)
        except (OSError, NotImplementedError):
            pass
        else:
            _expect_output_rejection(
                plan_csv,
                source_root,
                Path("output_link") / "rotation_execution_simulation.csv",
            )
            assert not (outside_output_dir / "rotation_execution_simulation.csv").exists()
        finally:
            outside_output_dir.rmdir()

        traversal_plan = root / "traversal_plan.csv"
        _write_plan(traversal_plan)
        rows = list(csv.DictReader(traversal_plan.open("r", newline="", encoding="utf-8")))
        rows[0]["csv_name"] = "../outside.csv"
        with traversal_plan.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        run_simulation(traversal_plan, source_root, Path("traversal_result.csv"))
        with (root / "traversal_result.csv").open("r", newline="", encoding="utf-8") as handle:
            traversal_rows = list(csv.DictReader(handle))
        assert traversal_rows[0]["simulation_result"] == "REJECTED_SOURCE_PATH"

        absolute_plan = root / "absolute_source_plan.csv"
        _write_plan(absolute_plan)
        rows = list(csv.DictReader(absolute_plan.open("r", newline="", encoding="utf-8")))
        rows[0]["csv_name"] = str((root / "outside.csv").resolve())
        with absolute_plan.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        run_simulation(absolute_plan, source_root, Path("absolute_source_result.csv"))
        with (root / "absolute_source_result.csv").open("r", newline="", encoding="utf-8") as handle:
            absolute_rows = list(csv.DictReader(handle))
        assert absolute_rows[0]["simulation_result"] == "REJECTED_SOURCE_PATH"

        if hasattr(Path, "symlink_to"):
            outside_file = root / "outside_target.csv"
            outside_file.write_text("outside\n", encoding="utf-8")
            link = source_root / "escape.csv"
            try:
                link.symlink_to(outside_file)
            except (OSError, NotImplementedError):
                pass
            else:
                link_plan = root / "symlink_source_plan.csv"
                _write_plan(link_plan)
                rows = list(csv.DictReader(link_plan.open("r", newline="", encoding="utf-8")))
                rows[0]["csv_name"] = "escape.csv"
                with link_plan.open("w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
                    writer.writeheader()
                    writer.writerows(rows)
                run_simulation(link_plan, source_root, Path("symlink_source_result.csv"))
                with (root / "symlink_source_result.csv").open("r", newline="", encoding="utf-8") as handle:
                    symlink_rows = list(csv.DictReader(handle))
                assert symlink_rows[0]["simulation_result"] == "REJECTED_SOURCE_PATH"

    print(
        "SMOKE_R5_REV2_OK safe_accepted=1 critical_rejected=1 important_rejected=1 "
        "unknown_rejected=1 duplicate_rejected=1 deterministic_target=1 "
        "output_outside_rejected=1 plan_overwrite_blocked=1 source_root_output_blocked=1 "
        "production_name_blocked=1 output_symlink_escape_blocked=1 "
        "output_traversal_blocked=1 absolute_output_blocked=1 pre_mkdir_blocked=1 "
        "pre_open_write_blocked=1 source_traversal_blocked=1 absolute_source_blocked=1 "
        "source_symlink_escape_blocked=1 filesystem_mutation=0"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
