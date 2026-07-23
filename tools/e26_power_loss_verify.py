from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from backtest.execution.execution_event_ledger import (
    INTENT_ACCEPTED,
    inspect_persisted_execution_integrity,
    load_persisted_execution_events,
)

PHASE_ALLOWED_OUTCOMES = {
    "BEFORE_TEMP_CREATE": {"OLD_VALID_COMMIT"},
    "TEMP_WRITING": {"OLD_VALID_COMMIT", "FAIL_CLOSED_INCOMPLETE"},
    "TEMP_FLUSHED": {"OLD_VALID_COMMIT", "FAIL_CLOSED_INCOMPLETE"},
    "BEFORE_REPLACE": {"OLD_VALID_COMMIT", "FAIL_CLOSED_INCOMPLETE"},
    "REPLACE_RETURNED": {
        "OLD_VALID_COMMIT",
        "NEW_VALID_COMMIT",
        "FAIL_CLOSED_INCOMPLETE",
        "FAIL_CLOSED_UNKNOWN",
    },
    "COMMITTED_FILE_FLUSHED": {
        "NEW_VALID_COMMIT",
        "FAIL_CLOSED_INCOMPLETE",
        "FAIL_CLOSED_UNKNOWN",
    },
    "COMMIT_ACKNOWLEDGED": {"NEW_VALID_COMMIT"},
}

INPUT_FIELDS = frozenset(
    {
        "schema_version",
        "run_id",
        "writer_pid",
        "target_phase",
        "iteration",
        "ledger_path",
        "result_path",
        "baseline_commit",
        "patch_sha256",
        "previous_acknowledged_commit_id",
        "in_flight_commit_id",
        "vm_identity",
        "snapshot_identity",
        "host_environment",
        "virtual_disk",
        "storage_controller",
        "write_cache_details",
        "physical_hard_power_off_executed",
    }
)


def sha256(path: Path) -> str | None:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None


def _required_text(payload: dict, field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value.strip()


def load_verifier_input(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        raise ValueError(f"invalid verifier input JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("verifier input must be one JSON object")
    unknown = set(payload) - INPUT_FIELDS
    missing = INPUT_FIELDS - set(payload)
    if unknown:
        raise ValueError(f"unknown verifier input fields: {sorted(unknown)}")
    if missing:
        raise ValueError(f"missing verifier input fields: {sorted(missing)}")
    if payload["schema_version"] != 1:
        raise ValueError("unsupported verifier input schema_version")
    for field in (
        "run_id",
        "target_phase",
        "ledger_path",
        "result_path",
        "baseline_commit",
        "patch_sha256",
        "vm_identity",
        "snapshot_identity",
    ):
        _required_text(payload, field)
    for field in (
        "writer_pid",
        "iteration",
        "previous_acknowledged_commit_id",
        "in_flight_commit_id",
    ):
        if type(payload[field]) is not int or payload[field] < 0:
            raise ValueError(f"{field} must be a non-negative integer")
    if payload["writer_pid"] < 1 or payload["iteration"] < 1:
        raise ValueError("writer_pid and iteration must be positive")
    if payload["in_flight_commit_id"] != payload["previous_acknowledged_commit_id"] + 1:
        raise ValueError("in-flight commit must immediately follow previous acknowledged commit")
    if payload["target_phase"] not in PHASE_ALLOWED_OUTCOMES:
        raise ValueError("unknown target_phase")
    if not re.fullmatch(r"[0-9a-fA-F]{40}", payload["baseline_commit"]):
        raise ValueError("baseline_commit must be a 40-character Git object id")
    if not re.fullmatch(r"[0-9a-fA-F]{64}", payload["patch_sha256"]):
        raise ValueError("patch_sha256 must be a SHA256 hex digest")
    for field in ("host_environment", "virtual_disk", "storage_controller"):
        if not isinstance(payload[field], (dict, list)) or not payload[field]:
            raise ValueError(f"{field} must contain structured evidence")
    if payload["write_cache_details"] is not None and not isinstance(
        payload["write_cache_details"], (dict, list)
    ):
        raise ValueError("write_cache_details must be structured evidence or null")
    if payload["physical_hard_power_off_executed"] is not True:
        raise ValueError("physical hard-power-off evidence must be explicitly asserted by host")
    return payload


def _event_identity(event) -> tuple[str | None, int | None]:
    match = re.fullmatch(r"E26_POWER\|([^|]+)\|(\d+)", event.canonical_setup_key)
    if not match:
        return None, None
    run_id, commit_text = match.groups()
    return run_id, int(commit_text)


def validate_recovered_history(events: Iterable, expected_run_id: str) -> list[str]:
    forbidden: list[str] = []
    seen_commit_ids: set[int] = set()
    for expected_commit_id, event in enumerate(tuple(events), start=1):
        run_id, commit_id = _event_identity(event)
        if run_id != expected_run_id:
            forbidden.append("WRONG_RUN_ID")
        if commit_id != expected_commit_id:
            forbidden.append("WRONG_COMMIT_ID")
        if commit_id in seen_commit_ids:
            forbidden.append("DUPLICATED_COMMIT_ID")
        if commit_id is not None:
            seen_commit_ids.add(commit_id)
        if event.sequence != expected_commit_id:
            forbidden.append("SEQUENCE_GAP_ACCEPTED")
        if event.event_type != INTENT_ACCEPTED:
            forbidden.append("WRONG_EVENT_TYPE")
        if event.reason != f"run_id={expected_run_id};commit_id={expected_commit_id}":
            forbidden.append("WRONG_EVENT_REASON")
        if event.canonical_setup_key != f"E26_POWER|{expected_run_id}|{expected_commit_id}":
            forbidden.append("MISMATCHED_CANONICAL_IDENTITY")
    return sorted(set(forbidden))


def classify_recovery(
    integrity_status: str,
    events: tuple,
    *,
    previous_acknowledged_commit_id: int,
    in_flight_commit_id: int,
    expected_run_id: str,
) -> tuple[str, list[str]]:
    forbidden: list[str] = []
    if integrity_status == "INCOMPLETE":
        return "FAIL_CLOSED_INCOMPLETE", forbidden
    if integrity_status == "CORRUPTED":
        return "FAIL_CLOSED_CORRUPTED", forbidden
    if integrity_status == "UNKNOWN":
        return "FAIL_CLOSED_UNKNOWN", forbidden
    if integrity_status != "VALID":
        return "VERIFICATION_ERROR", ["UNRECOGNIZED_INTEGRITY_STATUS"]
    forbidden.extend(validate_recovered_history(events, expected_run_id))
    recovered_last_commit = len(events)
    if recovered_last_commit < previous_acknowledged_commit_id:
        forbidden.append("ACKNOWLEDGED_COMMIT_LOST")
        return "ACKNOWLEDGED_COMMIT_LOST", sorted(set(forbidden))
    if recovered_last_commit == previous_acknowledged_commit_id:
        return "OLD_VALID_COMMIT", sorted(set(forbidden))
    if recovered_last_commit == in_flight_commit_id:
        return "NEW_VALID_COMMIT", sorted(set(forbidden))
    if recovered_last_commit > in_flight_commit_id:
        forbidden.append("RECOVERED_BEYOND_IN_FLIGHT_COMMIT")
    else:
        forbidden.append("VALID_BUT_NON_PREFIX_HISTORY")
    return "VERIFICATION_ERROR", sorted(set(forbidden))


def collect_guest_environment(ledger_path: Path) -> dict:
    result = {
        "windows_version": platform.platform(),
        "windows_build": platform.version(),
        "filesystem": None,
        "volume_identity": None,
    }
    if os.name != "nt":
        return result
    drive = ledger_path.resolve().drive or "C:"
    try:
        command = [
            "powershell",
            "-NoProfile",
            "-Command",
            f"Get-Volume -DriveLetter '{drive[0]}' | "
            "Select-Object FileSystem,UniqueId,DriveType | ConvertTo-Json -Compress",
        ]
        completed = subprocess.run(
            command, capture_output=True, text=True, timeout=15, check=True
        )
        volume = json.loads(completed.stdout)
        result["filesystem"] = volume.get("FileSystem")
        result["volume_identity"] = volume.get("UniqueId")
        result["volume_drive_type"] = volume.get("DriveType")
    except Exception as exc:
        result["environment_collection_error"] = repr(exc)
    return result


def verify(payload: dict) -> tuple[dict, int]:
    ledger_path = Path(payload["ledger_path"])
    temp_path = ledger_path.with_name(ledger_path.name + ".tmp")
    report = inspect_persisted_execution_integrity(ledger_path)
    events = ()
    verification_errors: list[str] = []
    try:
        if report.status == "VALID":
            events = load_persisted_execution_events(ledger_path)
        classification, forbidden = classify_recovery(
            report.status,
            events,
            previous_acknowledged_commit_id=payload["previous_acknowledged_commit_id"],
            in_flight_commit_id=payload["in_flight_commit_id"],
            expected_run_id=payload["run_id"],
        )
    except Exception as exc:
        classification = "VERIFICATION_ERROR"
        forbidden = []
        verification_errors.append(repr(exc))
    allowed = PHASE_ALLOWED_OUTCOMES[payload["target_phase"]]
    if classification not in allowed:
        forbidden.append("OUTCOME_NOT_ALLOWED_FOR_PHASE")
    exit_code = 1 if forbidden or verification_errors else 0
    output = {
        "schema_version": 1,
        "run_id": payload["run_id"],
        "writer_pid": payload["writer_pid"],
        "target_phase": payload["target_phase"],
        "iteration": payload["iteration"],
        "vm_identity": payload["vm_identity"],
        "snapshot_identity": payload["snapshot_identity"],
        "baseline_commit": payload["baseline_commit"],
        "patch_sha256": payload["patch_sha256"].lower(),
        "previous_acknowledged_commit_id": payload["previous_acknowledged_commit_id"],
        "in_flight_commit_id": payload["in_flight_commit_id"],
        "post_boot_classification": classification,
        "allowed_outcomes_for_phase": sorted(allowed),
        "main_sha256": sha256(ledger_path),
        "temp_sha256": sha256(temp_path),
        "integrity_status": report.status,
        "integrity_reason": report.reason,
        "recovered_event_count": len(events),
        "recovered_last_sequence": events[-1].sequence if events else 0,
        "recovered_last_commit_id": len(events),
        "forbidden_outcomes": sorted(set(forbidden)),
        "verification_errors": verification_errors,
        "verifier_exit_code": exit_code,
        "guest_environment": collect_guest_environment(ledger_path),
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }
    return output, exit_code


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    args = parser.parse_args()
    try:
        payload = load_verifier_input(Path(args.input))
        output, exit_code = verify(payload)
        result_path = Path(payload["result_path"])
        result_path.parent.mkdir(parents=True, exist_ok=True)
        result_path.write_text(
            json.dumps(output, sort_keys=True) + "\n", encoding="utf-8"
        )
        print(json.dumps(output, indent=2, sort_keys=True))
        return exit_code
    except Exception as exc:
        print(json.dumps({"verification_error": repr(exc)}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
