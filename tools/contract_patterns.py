# tools/contract_patterns.py
"""
Single source of truth for contract log tags / patterns.

Goal:
- Keep "core governance" tags stable and non-negotiable.
- Allow adding "spec/phase" tags without mixing them into core.

DEV A will import REQUIRED_PATTERNS from here.
"""

from __future__ import annotations

from typing import Dict, List

# NOTE:
# - Patterns are regex fragments, used with re.search on full log output.
# - Keep them strict enough to prevent accidental format drift,
#   but not so strict that normal metadata changes break the contract.

REQUIRED_PATTERNS: Dict[str, Dict[str, List[str]]] = {
    # === CORE GOVERNANCE (must never be removed / renamed) ===
    "core": {
        # Core tags required by governance rules
        "WATCHDOG": [r"\[WATCHDOG\]"],
        "KILL_SWITCH": [r"\[KILL_SWITCH\]"],
        "BUDGET": [r"\[BUDGET\]"],
        "CORR_CAP": [r"\[CORR_CAP\]"],
        "EQUITY_GOVERNOR": [r"\[EQUITY_GOVERNOR\]"],
    },

    # === SPEC / PHASE CONTRACT (allowed to evolve, but must exist for that phase) ===
    "spec": {
        # Phase contract tags used by runner telemetry / validation
        "PYRAMID": [r"\[PYRAMID\]"],
        # Phase 2/3 requirement: portfolio exposure telemetry tag
        "PORTFOLIO_CAP": [r"\[PORTFOLIO_CAP\]"],
    },
}