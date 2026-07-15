from __future__ import annotations

"""Append-only live journal rotation plan writer.

Architecture phase: RUNTIME_LEVEL_2.

Safety contract:
    - planning only
    - no rotation execution
    - no file moves or renames
    - no directory creation
    - no compression
    - no deletion
    - no CSV destination changes
"""

import csv
from datetime import datetime
from pathlib import Path
from typing import Iterable, Mapping


PLAN_COLUMNS = [
    "cycle_ts",
    "csv_name",
    "rotation_policy",
    "retention_policy",
    "compression_policy",
    "restart_safety",
    "decision",
    "planned_action",
    "target_name",
    "notes",
]


def _policy_lookup(rotation_manager, csv_name: str) -> Mapping[str, str]:
    if rotation_manager is None:
        return {}
    if hasattr(rotation_manager, "get_policy_for_csv"):
        try:
            return rotation_manager.get_policy_for_csv(csv_name)
        except Exception:
            return {}
    try:
        from backtest.journal.live_journal_rotation_manager import get_policy_for_csv

        return get_policy_for_csv(csv_name, list(getattr(rotation_manager, "design_rows", []) or []))
    except Exception:
        return {}


def _planned_action(decision: str) -> str:
    if decision == "WOULD_ROTATE":
        return "WOULD_ROTATE"
    if decision in {"BLOCKED_CRITICAL", "BLOCKED_IMPORTANT", "BLOCKED_RUNTIME_VALIDATION"}:
        return "BLOCKED"
    if decision == "UNKNOWN_POLICY":
        return "UNKNOWN"
    return "NONE"


def _target_name(csv_name: str, cycle_ts: datetime, rotation_policy: str, decision: str) -> str:
    if decision != "WOULD_ROTATE":
        return ""

    base = Path(csv_name)
    if rotation_policy == "ROTATE_DAILY":
        suffix = cycle_ts.strftime("%Y-%m-%d")
    elif rotation_policy == "ROTATE_WEEKLY":
        iso_year, iso_week, _ = cycle_ts.isocalendar()
        suffix = f"{iso_year}-W{iso_week:02d}"
    elif rotation_policy == "ROTATE_MONTHLY":
        suffix = cycle_ts.strftime("%Y-%m")
    else:
        return ""

    return f"{base.stem}_{suffix}{base.suffix}"


def _cycle_datetime(cycle_ts) -> datetime:
    if isinstance(cycle_ts, datetime):
        return cycle_ts
    if hasattr(cycle_ts, "to_pydatetime"):
        return cycle_ts.to_pydatetime()
    return datetime.fromisoformat(str(cycle_ts).replace("Z", "+00:00"))


def build_plan_row(*, rotation_controller, rotation_manager, csv_path: Path, cycle_ts) -> dict[str, str]:
    csv_name = Path(csv_path).name
    cycle_dt = _cycle_datetime(cycle_ts)
    policy = dict(_policy_lookup(rotation_manager, csv_name) or {})

    try:
        result = dict(rotation_controller.evaluate(Path(csv_path), cycle_ts))
    except Exception as exc:
        result = {
            "csv": csv_name,
            "policy": "UNKNOWN",
            "restart": "UNKNOWN",
            "decision": "UNKNOWN_POLICY",
            "action": "NONE",
            "error": f"{type(exc).__name__}: {exc}",
        }

    rotation_policy = str(result.get("policy") or policy.get("rotation_policy") or "UNKNOWN")
    restart_safety = str(result.get("restart") or policy.get("restart_safety") or "UNKNOWN")
    retention_policy = str(policy.get("retention_policy") or "UNKNOWN")
    compression_policy = str(policy.get("compression_policy") or "UNKNOWN")
    decision = str(result.get("decision") or "UNKNOWN_POLICY")
    planned_action = _planned_action(decision)
    notes = "planning_only_no_filesystem_action"
    if result.get("error"):
        notes = f"{notes}; error={result.get('error')}"

    return {
        "cycle_ts": str(cycle_ts),
        "csv_name": csv_name,
        "rotation_policy": rotation_policy,
        "retention_policy": retention_policy,
        "compression_policy": compression_policy,
        "restart_safety": restart_safety,
        "decision": decision,
        "planned_action": planned_action,
        "target_name": _target_name(csv_name, cycle_dt, rotation_policy, decision),
        "notes": notes,
    }


def append_live_rotation_plan_rows(
    plan_csv: Path,
    *,
    rotation_controller,
    rotation_manager,
    cycle_ts,
    csv_paths: Iterable[Path],
) -> int:
    """Append one planning row per configured CSV path.

    This function intentionally does not create directories, move files, rename
    files, compress files, delete files, or alter any runtime CSV destination.
    """
    if rotation_controller is None:
        return 0

    rows = [
        build_plan_row(
            rotation_controller=rotation_controller,
            rotation_manager=rotation_manager,
            csv_path=Path(csv_path),
            cycle_ts=cycle_ts,
        )
        for csv_path in csv_paths
        if str(csv_path).strip()
    ]
    if not rows:
        return 0

    plan_csv = Path(plan_csv)
    write_header = not plan_csv.exists() or plan_csv.stat().st_size == 0
    with plan_csv.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=PLAN_COLUMNS)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)
    return len(rows)
