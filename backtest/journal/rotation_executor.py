from __future__ import annotations

"""R6 disposable filesystem rotation executor.

This module performs real rename/create operations only inside an explicitly
approved disposable workspace. It has no production runtime integration.
"""

import argparse
import csv
import os
from dataclasses import dataclass
from pathlib import Path, PurePath
from typing import Dict, Iterable, List, Sequence

RESULT_COLUMNS = [
    "cycle_ts",
    "csv_name",
    "target_name",
    "decision",
    "restart_safety",
    "status",
    "reason",
]

PROTECTED_PRODUCTION_NAMES = {
    "position_state.csv",
    "fired_setups.csv",
    "visible_ts_cache.csv",
    "terminal_lifecycle_registry.csv",
    "live_rotation_plan.csv",
    "live_journal_rotation_design.csv",
}

ALLOWED_DECISION = "WOULD_ROTATE"
ALLOWED_RESTART_SAFETY = "SAFE"


class RotationSafetyError(RuntimeError):
    """Raised before filesystem mutation when a safety boundary is violated."""


@dataclass(frozen=True)
class PreparedRotation:
    row: Dict[str, str]
    source_path: Path
    target_path: Path


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _reject_absolute_or_traversal(raw_value: str, field_name: str) -> str:
    value = (raw_value or "").strip()
    if not value:
        raise RotationSafetyError(f"{field_name} is empty")
    candidate = Path(value)
    if candidate.is_absolute() or candidate.drive:
        raise RotationSafetyError(f"{field_name} must be repository-relative")
    if any(part == ".." for part in PurePath(value).parts):
        raise RotationSafetyError(f"{field_name} contains path traversal")
    return value


def _require_plain_csv_name(raw_value: str, field_name: str) -> str:
    value = _reject_absolute_or_traversal(raw_value, field_name)
    path = Path(value)
    if len(path.parts) != 1 or path.name != value:
        raise RotationSafetyError(f"{field_name} must be a plain filename")
    if path.suffix.lower() != ".csv":
        raise RotationSafetyError(f"{field_name} must end with .csv")
    return value


def _resolved_existing_root(workspace_root: Path) -> Path:
    raw = _reject_absolute_or_traversal(str(workspace_root), "workspace_root")
    root = Path(raw)
    if not root.exists() or not root.is_dir():
        raise RotationSafetyError("workspace_root must already exist")
    resolved = root.resolve(strict=True)
    if resolved.is_symlink():
        raise RotationSafetyError("workspace_root cannot be a symlink")
    return resolved


def _validate_control_path(path: Path, *, root: Path, field_name: str, must_exist: bool) -> Path:
    raw = _reject_absolute_or_traversal(str(path), field_name)
    candidate = Path(raw)
    parent = candidate.parent.resolve(strict=True)
    if not _is_relative_to(parent, root):
        raise RotationSafetyError(f"{field_name} escapes disposable workspace")
    resolved = parent / candidate.name
    if must_exist:
        resolved = candidate.resolve(strict=True)
        if not _is_relative_to(resolved, root):
            raise RotationSafetyError(f"{field_name} resolves outside disposable workspace")
        if candidate.is_symlink():
            raise RotationSafetyError(f"{field_name} cannot be a symlink")
    elif candidate.exists() and candidate.is_symlink():
        raise RotationSafetyError(f"{field_name} cannot be a symlink")
    return resolved


def _validate_global_paths(*, workspace_root: Path, plan_csv: Path, output_csv: Path) -> tuple[Path, Path, Path]:
    """Validate every control path before mkdir, open-for-write, or rename."""
    root = _resolved_existing_root(workspace_root)
    plan = _validate_control_path(plan_csv, root=root, field_name="plan_csv", must_exist=True)
    output = _validate_control_path(output_csv, root=root, field_name="output_csv", must_exist=False)

    if output == plan:
        raise RotationSafetyError("output_csv cannot equal plan_csv")
    if output == root:
        raise RotationSafetyError("output_csv cannot equal workspace_root")
    if output.name.lower() in PROTECTED_PRODUCTION_NAMES:
        raise RotationSafetyError("output_csv uses a protected production filename")
    return root, plan, output


def _read_plan(plan_csv: Path) -> List[Dict[str, str]]:
    with plan_csv.open("r", newline="", encoding="utf-8") as handle:
        return [{k: (v or "").strip() for k, v in row.items()} for row in csv.DictReader(handle)]


def _result(row: Dict[str, str], status: str, reason: str) -> Dict[str, str]:
    return {
        "cycle_ts": row.get("cycle_ts", ""),
        "csv_name": row.get("csv_name", ""),
        "target_name": row.get("target_name", ""),
        "decision": row.get("decision", ""),
        "restart_safety": row.get("restart_safety", ""),
        "status": status,
        "reason": reason,
    }


def _prepare_rows(rows: Sequence[Dict[str, str]], root: Path) -> tuple[List[PreparedRotation], List[Dict[str, str]]]:
    prepared: List[PreparedRotation] = []
    results: List[Dict[str, str]] = []
    seen_keys: set[tuple[str, str, str]] = set()
    reserved_sources: set[Path] = set()
    reserved_targets: set[Path] = set()

    for row in rows:
        safety = row.get("restart_safety", "").upper()
        decision = row.get("decision", "").upper()
        if safety == "CRITICAL":
            results.append(_result(row, "REJECTED", "CRITICAL"))
            continue
        if safety == "IMPORTANT":
            results.append(_result(row, "REJECTED", "IMPORTANT"))
            continue
        if safety != ALLOWED_RESTART_SAFETY:
            results.append(_result(row, "REJECTED", "UNKNOWN"))
            continue
        if decision != ALLOWED_DECISION:
            results.append(_result(row, "REJECTED", "DECISION_NOT_ALLOWED"))
            continue

        csv_name = _require_plain_csv_name(row.get("csv_name", ""), "csv_name")
        target_name = _require_plain_csv_name(row.get("target_name", ""), "target_name")
        if csv_name.lower() in PROTECTED_PRODUCTION_NAMES or target_name.lower() in PROTECTED_PRODUCTION_NAMES:
            raise RotationSafetyError("plan references a protected production filename")
        if csv_name == target_name:
            raise RotationSafetyError("target_name cannot equal csv_name")

        key = (row.get("cycle_ts", ""), csv_name, target_name)
        if key in seen_keys:
            results.append(_result(row, "REJECTED", "DUPLICATE_EXECUTION"))
            continue
        seen_keys.add(key)

        source = root / csv_name
        target = root / target_name
        source_resolved = source.resolve(strict=True)
        if not _is_relative_to(source_resolved, root):
            raise RotationSafetyError("source path resolves outside disposable workspace")
        if source.is_symlink() or not source.is_file():
            raise RotationSafetyError("source must be a regular file inside disposable workspace")

        target_parent = target.parent.resolve(strict=True)
        if not _is_relative_to(target_parent, root):
            raise RotationSafetyError("target path resolves outside disposable workspace")
        if target.exists():
            results.append(_result(row, "REJECTED", "EXISTING_DESTINATION"))
            continue
        if target.is_symlink():
            raise RotationSafetyError("target cannot be a symlink")
        if source in reserved_sources or target in reserved_targets or target in reserved_sources:
            results.append(_result(row, "REJECTED", "TARGET_COLLISION"))
            continue

        reserved_sources.add(source)
        reserved_targets.add(target)
        prepared.append(PreparedRotation(row=row, source_path=source, target_path=target))

    return prepared, results


def _write_results(output_csv: Path, rows: Iterable[Dict[str, str]]) -> None:
    # Parent existence was proven before any mutation; do not mkdir here.
    with output_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=RESULT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def execute_rotation_plan(*, workspace_root: Path, plan_csv: Path, output_csv: Path) -> List[Dict[str, str]]:
    root, plan, output = _validate_global_paths(
        workspace_root=workspace_root,
        plan_csv=plan_csv,
        output_csv=output_csv,
    )
    rows = _read_plan(plan)
    prepared, results = _prepare_rows(rows, root)

    # All path/policy/collision checks complete before the first mutation.
    for item in prepared:
        os.rename(item.source_path, item.target_path)
        with item.source_path.open("x", encoding="utf-8"):
            pass
        results.append(_result(item.row, "ROTATED", "SAFE_ROTATION_COMPLETE"))

    _write_results(output, results)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="R6 disposable filesystem rotation executor")
    parser.add_argument("--workspace-root", required=True)
    parser.add_argument("--plan-csv", required=True)
    parser.add_argument("--output-csv", required=True)
    args = parser.parse_args()
    try:
        results = execute_rotation_plan(
            workspace_root=Path(args.workspace_root),
            plan_csv=Path(args.plan_csv),
            output_csv=Path(args.output_csv),
        )
    except RotationSafetyError as exc:
        print(f"[R6_FAIL_CLOSED] {exc}")
        return 2
    print(f"[R6_OK] rows={len(results)} output={args.output_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
