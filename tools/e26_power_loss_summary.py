from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

REQUIRED_PHASES = (
    "BEFORE_TEMP_CREATE",
    "TEMP_WRITING",
    "TEMP_FLUSHED",
    "BEFORE_REPLACE",
    "REPLACE_RETURNED",
    "COMMITTED_FILE_FLUSHED",
    "COMMIT_ACKNOWLEDGED",
)

REQUIRED_ARTIFACTS = (
    "host_observation.json",
    "verifier_result.json",
    "verifier_stdout.txt",
    "verifier_stderr.txt",
    "verifier_process.json",
    "environment.json",
    "artifact_hashes.json",
)


def load_host_results(path: Path) -> list[dict]:
    rows: list[dict] = []
    for number, line in enumerate(path.read_text(encoding="utf-8-sig").splitlines(), 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except Exception as exc:
            raise ValueError(f"malformed host JSONL line {number}: {exc}") from exc
        if not isinstance(row, dict):
            raise ValueError(f"host JSONL line {number} is not an object")
        rows.append(row)
    return rows


def assess(rows: list[dict], minimum_iterations: int) -> dict:
    phases = Counter(row.get("target_phase") for row in rows)
    outcomes = Counter(row.get("post_boot_classification") for row in rows)
    run_ids = [row.get("run_id") for row in rows]
    run_keys = [
        (row.get("run_id"), row.get("target_phase"), row.get("iteration"))
        for row in rows
    ]
    duplicate_runs = len(run_ids) - len(set(run_ids))
    duplicate_keys = len(run_keys) - len(set(run_keys))
    unexpected_phases = sum(phase not in REQUIRED_PHASES for phase in phases)
    forbidden = sum(len(row.get("forbidden_outcomes") or []) for row in rows)
    verification_errors = sum(len(row.get("verification_errors") or []) for row in rows)
    join_failures = sum(not row.get("join_validation", {}).get("passed") for row in rows)
    nonzero_exits = sum(row.get("verifier_exit_code") != 0 for row in rows)
    acknowledged_losses = sum(
        row.get("post_boot_classification") == "ACKNOWLEDGED_COMMIT_LOST" for row in rows
    )
    missing_artifacts = sum(
        not all((row.get("host_artifacts") or {}).get(name) for name in REQUIRED_ARTIFACTS)
        for row in rows
    )
    physical_missing = sum(
        row.get("physical_hard_power_off_executed") is not True for row in rows
    )
    environment_missing = sum(
        not row.get("environment_complete") for row in rows
    )
    complete = all(phases[phase] >= minimum_iterations for phase in REQUIRED_PHASES)
    passed = bool(rows) and all(
        value == 0
        for value in (
            duplicate_runs,
            duplicate_keys,
            unexpected_phases,
            forbidden,
            verification_errors,
            join_failures,
            nonzero_exits,
            acknowledged_losses,
            missing_artifacts,
            physical_missing,
            environment_missing,
        )
    ) and complete
    return {
        "rows": len(rows),
        "phases": phases,
        "outcomes": outcomes,
        "complete": complete,
        "duplicate_runs": duplicate_runs,
        "duplicate_keys": duplicate_keys,
        "unexpected_phases": unexpected_phases,
        "forbidden": forbidden,
        "verification_errors": verification_errors,
        "join_failures": join_failures,
        "nonzero_exits": nonzero_exits,
        "acknowledged_losses": acknowledged_losses,
        "missing_artifacts": missing_artifacts,
        "physical_missing": physical_missing,
        "environment_missing": environment_missing,
        "passed": passed,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host-results", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--minimum-iterations", type=int, default=3)
    args = parser.parse_args()
    if args.minimum_iterations < 1:
        parser.error("--minimum-iterations must be at least 1")
    try:
        assessment = assess(
            load_host_results(Path(args.host_results)), args.minimum_iterations
        )
    except Exception as exc:
        Path(args.output).write_text(
            "# E26 Power-Loss Summary\n\n"
            f"Certification result: NOT CERTIFIED\n\nError: {exc}\n",
            encoding="utf-8",
        )
        return 2
    lines = [
        "# E26 Power-Loss Summary",
        "",
        f"Total host-controlled runs: {assessment['rows']}",
        f"Required phase coverage complete: {'YES' if assessment['complete'] else 'NO'}",
        f"Duplicate runs: {assessment['duplicate_runs']}",
        f"Duplicate run keys: {assessment['duplicate_keys']}",
        f"Unexpected phases: {assessment['unexpected_phases']}",
        f"Forbidden outcomes: {assessment['forbidden']}",
        f"Verification errors: {assessment['verification_errors']}",
        f"Host/guest join failures: {assessment['join_failures']}",
        f"Non-zero verifier exits: {assessment['nonzero_exits']}",
        f"Acknowledged commit losses: {assessment['acknowledged_losses']}",
        f"Runs with missing host artifacts: {assessment['missing_artifacts']}",
        f"Runs without physical power-off marker: {assessment['physical_missing']}",
        f"Runs with incomplete environment: {assessment['environment_missing']}",
        f"Certification result: {'PASS' if assessment['passed'] else 'NOT CERTIFIED'}",
        "",
        "## Runs per phase",
    ]
    lines.extend(
        f"- {phase}: {assessment['phases'][phase]}" for phase in REQUIRED_PHASES
    )
    lines.extend(["", "## Outcomes"])
    for outcome, count in sorted(assessment["outcomes"].items()):
        lines.append(f"- {outcome}: {count}")
    Path(args.output).write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0 if assessment["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
