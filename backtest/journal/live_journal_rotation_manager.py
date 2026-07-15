from __future__ import annotations

"""Standalone Live Journal Rotation Manager scaffold.

Architecture phase: RUNTIME_SCAFFOLD.

Safety contract:
    - no production imports
    - no journal path changes
    - no scheduler
    - no rotation execution
    - no file moves
    - no file writes during smoke
    - path computation only
"""

import argparse
import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List


ROTATION_POLICIES = {
    "NEVER_ROTATE",
    "ROTATE_DAILY",
    "ROTATE_WEEKLY",
    "ROTATE_MONTHLY",
    "ROTATE_AFTER_RUNTIME_VALIDATION",
    "UNKNOWN",
}

RETENTION_POLICIES = {
    "KEEP_FOREVER",
    "KEEP_30_DAYS",
    "KEEP_90_DAYS",
    "KEEP_180_DAYS",
    "ARCHIVE_ONLY",
    "UNKNOWN",
}

COMPRESSION_POLICIES = {
    "NO",
    "ZIP_AFTER_ROTATION",
    "UNKNOWN",
}

RESTART_SAFETY_LEVELS = {
    "CRITICAL",
    "IMPORTANT",
    "SAFE",
    "UNKNOWN",
}

UNKNOWN_POLICY: Dict[str, str] = {
    "csv_file": "",
    "lifecycle_group": "UNKNOWN",
    "rotation_policy": "UNKNOWN",
    "retention_policy": "UNKNOWN",
    "compression_policy": "UNKNOWN",
    "restart_safety": "UNKNOWN",
    "reason": "No exact csv_file match found in rotation design.",
    "dependency_summary": "UNKNOWN",
    "architect_notes": "UNKNOWN",
}

DEFAULT_DESIGN_CSV = Path("backtest/journal/live_journal_rotation_design.csv")


class LiveJournalRotationManager:
    """Observation-only manager wrapper for Runtime Level 0 integration.

    This class only loads the approved design CSV and reports load status.
    It does not evaluate rotation decisions, compute rotated paths, create
    directories, write files, move files, or mutate runtime state.
    """

    STATUS_NOT_LOADED = "NOT_LOADED"
    STATUS_LOADED = "LOADED"
    STATUS_ERROR = "ERROR"

    def __init__(self, design_csv: Path = DEFAULT_DESIGN_CSV) -> None:
        self.design_csv = Path(design_csv)
        self.design_rows: List[Dict[str, str]] = []
        self._status = self.STATUS_NOT_LOADED
        self.error: str = ""

    def load(self) -> int:
        """Load design rows for observability only and return row count."""
        try:
            if not self.design_csv.exists():
                self.design_rows = []
                self._status = self.STATUS_NOT_LOADED
                self.error = ""
                return 0
            self.design_rows = load_rotation_design(self.design_csv)
            self._status = self.STATUS_LOADED
            self.error = ""
            return len(self.design_rows)
        except Exception as exc:
            self.design_rows = []
            self._status = self.STATUS_ERROR
            self.error = f"{type(exc).__name__}: {exc}"
            return 0

    def status(self) -> str:
        """Return observation-only load status: NOT_LOADED, LOADED, or ERROR."""
        if self._status in {self.STATUS_NOT_LOADED, self.STATUS_LOADED, self.STATUS_ERROR}:
            return self._status
        return self.STATUS_ERROR

    def get_policy_for_csv(self, csv_file: str) -> Dict[str, str]:
        """Return a design policy by exact csv_file match only.

        Policy lookup only; this method performs no rotation decision, path
        resolution, directory creation, file move, or write.
        """
        return get_policy_for_csv(csv_file, self.design_rows)


def _clean_policy_value(value: str, allowed_values: set[str], default: str = "UNKNOWN") -> str:
    cleaned = (value or "").strip()
    if cleaned in allowed_values:
        return cleaned
    return default


def load_rotation_design(design_csv: Path) -> List[Dict[str, str]]:
    """Read rotation design rows from CSV with no side effects."""
    if not design_csv.exists():
        return []

    rows: List[Dict[str, str]] = []
    with design_csv.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for raw_row in reader:
            row = {key: (value or "").strip() for key, value in raw_row.items()}
            row["rotation_policy"] = _clean_policy_value(
                row.get("rotation_policy", ""), ROTATION_POLICIES
            )
            row["retention_policy"] = _clean_policy_value(
                row.get("retention_policy", ""), RETENTION_POLICIES
            )
            row["compression_policy"] = _clean_policy_value(
                row.get("compression_policy", ""), COMPRESSION_POLICIES
            )
            row["restart_safety"] = _clean_policy_value(
                row.get("restart_safety", ""), RESTART_SAFETY_LEVELS
            )
            rows.append(row)
    return rows


def get_policy_for_csv(csv_file: str, design_rows: List[Dict[str, str]]) -> Dict[str, str]:
    """Return a policy by exact csv_file match only."""
    target = (csv_file or "").strip()
    for row in design_rows:
        if (row.get("csv_file") or "").strip() == target:
            return dict(row)

    fallback = dict(UNKNOWN_POLICY)
    fallback["csv_file"] = target
    return fallback


def should_rotate(csv_file: str, policy: Dict[str, str]) -> bool:
    """Return the scaffold decision for whether a CSV is eligible to rotate now."""
    del csv_file  # Exact matching is performed before this function is called.

    restart_safety = _clean_policy_value(
        policy.get("restart_safety", ""), RESTART_SAFETY_LEVELS
    )
    rotation_policy = _clean_policy_value(
        policy.get("rotation_policy", ""), ROTATION_POLICIES
    )

    if restart_safety in {"CRITICAL", "IMPORTANT"}:
        return False
    if rotation_policy in {"NEVER_ROTATE", "UNKNOWN", "ROTATE_AFTER_RUNTIME_VALIDATION"}:
        return False
    if rotation_policy in {"ROTATE_DAILY", "ROTATE_WEEKLY", "ROTATE_MONTHLY"}:
        return restart_safety == "SAFE"
    return False


def resolve_rotated_path(base_csv_path: Path, cycle_ts: datetime, policy: Dict[str, str]) -> Path:
    """Compute the rotated output path only; do not create, move, or write anything."""
    rotation_policy = _clean_policy_value(
        policy.get("rotation_policy", ""), ROTATION_POLICIES
    )
    restart_safety = _clean_policy_value(
        policy.get("restart_safety", ""), RESTART_SAFETY_LEVELS
    )

    if restart_safety != "SAFE":
        return base_csv_path

    if rotation_policy == "ROTATE_DAILY":
        suffix = cycle_ts.strftime("%Y-%m-%d")
    elif rotation_policy == "ROTATE_WEEKLY":
        iso_year, iso_week, _ = cycle_ts.isocalendar()
        suffix = f"{iso_year}-W{iso_week:02d}"
    elif rotation_policy == "ROTATE_MONTHLY":
        suffix = cycle_ts.strftime("%Y-%m")
    else:
        return base_csv_path

    return base_csv_path.with_name(f"{base_csv_path.stem}_{suffix}{base_csv_path.suffix}")


def _smoke(design_csv: Path) -> int:
    rows = load_rotation_design(design_csv)
    print(f"[ROTATION_MANAGER_SMOKE] policies_loaded={len(rows)} design_csv={design_csv}")

    sample_csvs = [
        "position_state.csv",
        "fired_setups.csv",
        "visible_ts_cache.csv",
        "flow_log.csv",
        "live_observation_entries.csv",
        "raw_candidate_lifecycle_diag.csv",
        "pressure_window_summary.csv",
        "unknown_file.csv",
    ]
    cycle_ts = datetime(2026, 7, 7, 0, 0, 0)
    for csv_name in sample_csvs:
        policy = get_policy_for_csv(csv_name, rows)
        decision = should_rotate(csv_name, policy)
        resolved = resolve_rotated_path(Path("backtest/journal") / csv_name, cycle_ts, policy)
        print(
            "[ROTATION_MANAGER_SMOKE] "
            f"csv={csv_name} "
            f"rotation_policy={policy.get('rotation_policy', 'UNKNOWN')} "
            f"restart_safety={policy.get('restart_safety', 'UNKNOWN')} "
            f"should_rotate={decision} "
            f"resolved_path={resolved}"
        )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Standalone Live Journal Rotation Manager scaffold"
    )
    parser.add_argument(
        "--design-csv",
        default=str(DEFAULT_DESIGN_CSV),
        help="Path to live_journal_rotation_design.csv",
    )
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="Run a no-side-effect smoke check",
    )
    args = parser.parse_args()

    if args.smoke:
        return _smoke(Path(args.design_csv))

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
