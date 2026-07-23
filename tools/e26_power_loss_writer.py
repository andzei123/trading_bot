from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from backtest.execution.execution_event_ledger import (
    ExecutionEventLedger,
    INTENT_ACCEPTED,
    write_durable_json_metadata,
)


def _load_input(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    required = {"schema_version", "ledger_path", "phase_file", "state_file", "run_id", "delay"}
    if not isinstance(payload, dict) or set(payload) != required:
        raise ValueError("writer input schema fields are not exact")
    if payload["schema_version"] != 1:
        raise ValueError("unsupported writer input schema_version")
    for field in ("ledger_path", "phase_file", "state_file", "run_id"):
        if not isinstance(payload[field], str) or not payload[field].strip():
            raise ValueError(f"{field} must be a non-empty string")
    if not isinstance(payload["delay"], (int, float)) or isinstance(payload["delay"], bool):
        raise ValueError("delay must be numeric")
    if payload["delay"] < 0:
        raise ValueError("delay must be non-negative")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    args = parser.parse_args()
    payload = _load_input(Path(args.input))

    run_id = payload["run_id"].strip()
    phase_path = Path(payload["phase_file"])
    state_path = Path(payload["state_file"])
    writer_pid = os.getpid()
    commit_id = 0
    previous_acknowledged_commit_id = 0

    write_durable_json_metadata(
        state_path,
        {
            "run_id": run_id,
            "writer_pid": writer_pid,
            "last_acknowledged_commit": 0,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        },
    )

    def observer(phase: str) -> None:
        write_durable_json_metadata(
            phase_path,
            {
                "run_id": run_id,
                "writer_pid": writer_pid,
                "phase": phase,
                "previous_acknowledged_commit_id": previous_acknowledged_commit_id,
                "in_flight_commit_id": commit_id,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            },
        )
        time.sleep(float(payload["delay"]))

    with ExecutionEventLedger(
        payload["ledger_path"], persistence_phase_observer=observer
    ) as ledger:
        while True:
            commit_id += 1
            ledger.append_event(
                canonical_setup_key=f"E26_POWER|{run_id}|{commit_id}",
                event_type=INTENT_ACCEPTED,
                recorded_at_utc=datetime.now(timezone.utc).isoformat(),
                reason=f"run_id={run_id};commit_id={commit_id}",
            )
            previous_acknowledged_commit_id = commit_id
            write_durable_json_metadata(
                state_path,
                {
                    "run_id": run_id,
                    "writer_pid": writer_pid,
                    "last_acknowledged_commit": commit_id,
                    "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                },
            )
            time.sleep(float(payload["delay"]))


if __name__ == "__main__":
    raise SystemExit(main())
