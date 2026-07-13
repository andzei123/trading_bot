from __future__ import annotations

"""Offline, simulation-only Live Journal rotation executor.

The simulator reads a copied/disposable rotation plan and validates what an
executor would do. It never renames, moves, deletes, compresses, or modifies
journal source files. Its only write is the requested simulation CSV.
"""

import argparse
import csv
import tempfile
from datetime import datetime
from pathlib import Path, PureWindowsPath
from typing import Dict, Iterable, List, Set, Tuple

OUTPUT_COLUMNS = [
    "cycle_ts",
    "csv_name",
    "rotation_policy",
    "restart_safety",
    "decision",
    "target_name",
    "source_path",
    "source_exists",
    "simulation_result",
    "simulated_action",
    "reason",
]

ACCEPTED = "ACCEPTED"
REJECTED_CRITICAL = "REJECTED_CRITICAL"
REJECTED_IMPORTANT = "REJECTED_IMPORTANT"
REJECTED_UNKNOWN = "REJECTED_UNKNOWN"
REJECTED_DECISION = "REJECTED_DECISION"
REJECTED_SOURCE_MISSING = "REJECTED_SOURCE_MISSING"
REJECTED_SOURCE_PATH = "REJECTED_SOURCE_PATH"
REJECTED_TARGET_MISMATCH = "REJECTED_TARGET_MISMATCH"
REJECTED_DUPLICATE = "REJECTED_DUPLICATE"


class OutputPathSafetyError(ValueError):
    """Raised when --output-csv is outside the disposable simulation boundary."""


DISPOSABLE_PATH_MARKERS = {
    "tmp",
    "temp",
    "temporary",
    "disposable",
    "scratch",
    "sandbox",
    "simulation",
    "simulator",
    "validation",
    "test",
    "tests",
    "fixture",
    "fixtures",
}

PROTECTED_OUTPUT_NAMES = {
    "live_rotation_plan.csv",
    "position_state.csv",
    "fired_setups.csv",
    "visible_ts_cache.csv",
    "terminal_lifecycle_registry.csv",
    "live_observation_entries.csv",
    "flow_log.csv",
    "raw_candidate_lifecycle_diag.csv",
    "entry_model_pre_admission.csv",
    "pressure_window_summary.csv",
    "sniper_candidate_diag.csv",
    "sniper_candidate_summary.csv",
    "opportunity_manager_snapshot.csv",
    "authority_waterfall.csv",
    "tdp_stale_shadow_oos.csv",
    "structural_ts_shadow_oos.csv",
    "candidate_pressure.csv",
    "symbol_performance.csv",
    "equity_curve.csv",
    "parity_filter_diagnostics.csv",
}


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _is_disposable_workspace(path: Path) -> bool:
    temp_root = Path(tempfile.gettempdir()).resolve()
    if _is_relative_to(path, temp_root):
        return True
    return any(
        marker in part.lower()
        for part in path.parts
        for marker in DISPOSABLE_PATH_MARKERS
    )


def _has_parent_traversal(path: Path) -> bool:
    return any(part == ".." for part in path.parts)


def _looks_absolute_on_any_platform(value: str) -> bool:
    return Path(value).is_absolute() or PureWindowsPath(value).is_absolute()


def validate_output_csv_path(
    *,
    plan_csv: Path,
    source_root: Path,
    output_csv: Path,
) -> Path:
    """Return a safe output path or fail before any filesystem mutation.

    ``--output-csv`` is intentionally required to be a relative path beneath
    the disposable plan workspace. Existing symlinks/reparse points are
    resolved before containment is accepted.
    """
    raw_output = str(output_csv)
    if _looks_absolute_on_any_platform(raw_output):
        raise OutputPathSafetyError("absolute output paths are not allowed")
    if _has_parent_traversal(output_csv):
        raise OutputPathSafetyError("output path traversal is not allowed")

    plan_resolved = plan_csv.resolve(strict=False)
    workspace = plan_resolved.parent.resolve(strict=False)
    source_resolved = source_root.resolve(strict=False)
    output_resolved = (workspace / output_csv).resolve(strict=False)

    if not _is_disposable_workspace(workspace):
        raise OutputPathSafetyError(
            "plan workspace is not an approved temporary/disposable location"
        )
    if not _is_relative_to(source_resolved, workspace):
        raise OutputPathSafetyError(
            "source root must resolve inside the disposable plan workspace"
        )
    if source_resolved == workspace:
        raise OutputPathSafetyError(
            "source root must be a dedicated child of the disposable workspace"
        )
    if not _is_relative_to(output_resolved, workspace):
        raise OutputPathSafetyError(
            "output CSV must resolve inside the disposable plan workspace"
        )
    if output_resolved == workspace or output_resolved == source_resolved:
        raise OutputPathSafetyError("output path cannot equal a directory root")
    if _is_relative_to(output_resolved, source_resolved):
        raise OutputPathSafetyError(
            "output CSV must not resolve inside the copied journal source root"
        )
    if output_resolved == plan_resolved:
        raise OutputPathSafetyError("output CSV must not overwrite the plan CSV")
    if output_resolved.name.lower() in PROTECTED_OUTPUT_NAMES:
        raise OutputPathSafetyError(
            "output filename is reserved for a production journal or plan artifact"
        )
    if output_resolved.suffix.lower() != ".csv":
        raise OutputPathSafetyError("output path must use a .csv suffix")

    return output_resolved


def resolve_source_path(source_root: Path, csv_name: str) -> Path:
    """Resolve one plan source without permitting root escape or link escape."""
    raw_name = (csv_name or "").strip()
    candidate = Path(raw_name)
    windows_candidate = PureWindowsPath(raw_name)

    if not raw_name:
        raise ValueError("source CSV name is empty")
    if _looks_absolute_on_any_platform(raw_name):
        raise ValueError("absolute source paths are not allowed")
    if _has_parent_traversal(candidate) or ".." in windows_candidate.parts:
        raise ValueError("source path traversal is not allowed")
    if len(candidate.parts) != 1 or len(windows_candidate.parts) != 1:
        raise ValueError("source must be an exact CSV filename, not a path")
    if candidate.suffix.lower() != ".csv":
        raise ValueError("source must be a CSV filename")

    root_resolved = source_root.resolve(strict=False)
    source_resolved = (root_resolved / candidate.name).resolve(strict=False)
    if not _is_relative_to(source_resolved, root_resolved):
        raise ValueError("source path resolves outside the disposable source root")
    return source_resolved


def _parse_cycle_ts(value: str) -> datetime:
    cleaned = (value or "").strip().replace("Z", "+00:00")
    return datetime.fromisoformat(cleaned)


def deterministic_target_name(csv_name: str, cycle_ts: str, rotation_policy: str) -> str:
    """Compute the only valid target filename for a supported rotation policy."""
    source = Path((csv_name or "").strip())
    ts = _parse_cycle_ts(cycle_ts)
    policy = (rotation_policy or "").strip()

    if policy == "ROTATE_DAILY":
        suffix = ts.strftime("%Y-%m-%d")
    elif policy == "ROTATE_WEEKLY":
        iso_year, iso_week, _ = ts.isocalendar()
        suffix = f"{iso_year}-W{iso_week:02d}"
    elif policy == "ROTATE_MONTHLY":
        suffix = ts.strftime("%Y-%m")
    else:
        return ""

    return f"{source.stem}_{suffix}{source.suffix}"


def _load_plan(plan_csv: Path) -> List[Dict[str, str]]:
    with plan_csv.open("r", newline="", encoding="utf-8") as handle:
        return [
            {key: (value or "").strip() for key, value in row.items()}
            for row in csv.DictReader(handle)
        ]


def _duplicate_key(row: Dict[str, str]) -> Tuple[str, str, str]:
    return (
        row.get("cycle_ts", ""),
        row.get("csv_name", ""),
        row.get("target_name", ""),
    )


def simulate_plan_rows(
    plan_rows: Iterable[Dict[str, str]],
    source_root: Path,
) -> List[Dict[str, object]]:
    """Validate plan rows and return deterministic simulated outcomes only."""
    source_root = source_root.resolve()
    seen: Set[Tuple[str, str, str]] = set()
    results: List[Dict[str, object]] = []

    for raw in plan_rows:
        row = {key: str(value or "").strip() for key, value in raw.items()}
        csv_name = row.get("csv_name", "")
        source_path_error = ""
        try:
            source_path = resolve_source_path(source_root, csv_name)
            source_exists = source_path.is_file()
        except ValueError as exc:
            source_path = source_root
            source_exists = False
            source_path_error = str(exc)
        restart_safety = row.get("restart_safety", "UNKNOWN") or "UNKNOWN"
        decision = row.get("decision", "")
        expected_target = ""
        try:
            expected_target = deterministic_target_name(
                csv_name,
                row.get("cycle_ts", ""),
                row.get("rotation_policy", ""),
            )
        except (TypeError, ValueError):
            expected_target = ""

        key = _duplicate_key(row)
        result = REJECTED_UNKNOWN
        reason = "unknown or unsupported policy metadata"

        if source_path_error:
            result = REJECTED_SOURCE_PATH
            reason = source_path_error
        elif key in seen:
            result = REJECTED_DUPLICATE
            reason = "duplicate plan execution key"
        elif restart_safety == "CRITICAL":
            result = REJECTED_CRITICAL
            reason = "CRITICAL journals are never executable"
        elif restart_safety == "IMPORTANT":
            result = REJECTED_IMPORTANT
            reason = "IMPORTANT journals are not executable"
        elif restart_safety != "SAFE":
            result = REJECTED_UNKNOWN
            reason = "restart safety is not SAFE"
        elif decision != "WOULD_ROTATE":
            result = REJECTED_DECISION
            reason = "decision is not WOULD_ROTATE"
        elif not source_exists:
            result = REJECTED_SOURCE_MISSING
            reason = "source journal does not exist in disposable source root"
        elif not expected_target or row.get("target_name", "") != expected_target:
            result = REJECTED_TARGET_MISMATCH
            reason = f"target must equal deterministic value: {expected_target or 'UNKNOWN'}"
        else:
            result = ACCEPTED
            reason = "SAFE WOULD_ROTATE row validated for simulation"

        seen.add(key)
        results.append(
            {
                "cycle_ts": row.get("cycle_ts", ""),
                "csv_name": csv_name,
                "rotation_policy": row.get("rotation_policy", ""),
                "restart_safety": restart_safety,
                "decision": decision,
                "target_name": row.get("target_name", ""),
                "source_path": str(source_path),
                "source_exists": source_exists,
                "simulation_result": result,
                "simulated_action": "WOULD_ROTATE" if result == ACCEPTED else "NONE",
                "reason": reason,
            }
        )

    return results


def write_simulation_csv(output_csv: Path, rows: Iterable[Dict[str, object]]) -> None:
    """Write simulation evidence only; never touch journal source files."""
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def run_simulation(plan_csv: Path, source_root: Path, output_csv: Path) -> int:
    safe_output_csv = validate_output_csv_path(
        plan_csv=plan_csv,
        source_root=source_root,
        output_csv=output_csv,
    )
    rows = simulate_plan_rows(_load_plan(plan_csv), source_root)
    write_simulation_csv(safe_output_csv, rows)
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Offline Live Journal rotation execution simulator")
    parser.add_argument("--plan-csv", required=True, help="Copied/disposable live_rotation_plan.csv")
    parser.add_argument("--source-root", required=True, help="Copied/disposable journal directory")
    parser.add_argument("--output-csv", required=True, help="Simulation result CSV")
    args = parser.parse_args()

    try:
        count = run_simulation(Path(args.plan_csv), Path(args.source_root), Path(args.output_csv))
    except OutputPathSafetyError as exc:
        print(f"[ROTATION_R5_SIMULATOR] output_path_rejected: {exc}")
        return 2
    print(f"[ROTATION_R5_SIMULATOR] rows={count} output={args.output_csv} filesystem_actions=0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
