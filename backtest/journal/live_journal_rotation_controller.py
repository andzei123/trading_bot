from __future__ import annotations

"""Observe-only Live Journal Rotation Controller.

Architecture phase: RUNTIME_LEVEL_1.

Safety contract:
    - no file moves
    - no file renames
    - no directory creation
    - no compression
    - no deletion
    - no CSV path mutation
    - no write/append behavior changes
    - decision reporting only
"""

from datetime import datetime
from pathlib import Path
from typing import Dict, Mapping


NO_ACTION = "NO_ACTION"
WOULD_ROTATE = "WOULD_ROTATE"
BLOCKED_CRITICAL = "BLOCKED_CRITICAL"
BLOCKED_IMPORTANT = "BLOCKED_IMPORTANT"
BLOCKED_RUNTIME_VALIDATION = "BLOCKED_RUNTIME_VALIDATION"
UNKNOWN_POLICY = "UNKNOWN_POLICY"


class LiveJournalRotationController:
    """Runtime Level 1 observer for live journal rotation decisions.

    The controller computes what would happen for a configured CSV at a cycle
    timestamp, but never performs the action. It deliberately does not call
    rotation execution, rotated path resolution, directory creation, compression,
    deletion, or any production write path.
    """

    def __init__(self, manager) -> None:
        self.manager = manager

    def evaluate(self, csv_path: Path, cycle_ts: datetime) -> Dict[str, str]:
        """Return an observe-only rotation decision for a configured CSV path."""
        del cycle_ts  # Reserved for future observation; no time-based action here.

        csv_name = Path(csv_path).name
        policy = self._lookup_policy(csv_name)
        rotation_policy = str(policy.get("rotation_policy", "UNKNOWN") or "UNKNOWN")
        restart_safety = str(policy.get("restart_safety", "UNKNOWN") or "UNKNOWN")
        decision = self._decision(rotation_policy=rotation_policy, restart_safety=restart_safety)
        return {
            "csv": csv_name,
            "policy": rotation_policy,
            "restart": restart_safety,
            "decision": decision,
            "action": "NONE",
        }

    def _lookup_policy(self, csv_name: str) -> Mapping[str, str]:
        if self.manager is None:
            return {}
        if hasattr(self.manager, "get_policy_for_csv"):
            return self.manager.get_policy_for_csv(csv_name)

        # Compatibility fallback for older manager scaffolds. This remains lookup
        # only and performs no runtime decision or path resolution.
        try:
            from backtest.journal.live_journal_rotation_manager import get_policy_for_csv

            return get_policy_for_csv(csv_name, list(getattr(self.manager, "design_rows", []) or []))
        except Exception:
            return {}

    @staticmethod
    def _decision(*, rotation_policy: str, restart_safety: str) -> str:
        if rotation_policy == "UNKNOWN" or restart_safety == "UNKNOWN":
            return UNKNOWN_POLICY
        if restart_safety == "CRITICAL":
            return BLOCKED_CRITICAL
        if restart_safety == "IMPORTANT":
            return BLOCKED_IMPORTANT
        if rotation_policy == "ROTATE_AFTER_RUNTIME_VALIDATION":
            return BLOCKED_RUNTIME_VALIDATION
        if restart_safety == "SAFE":
            return WOULD_ROTATE
        return UNKNOWN_POLICY
