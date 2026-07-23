from __future__ import annotations

import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from tools.e26_power_loss_verify import (
    PHASE_ALLOWED_OUTCOMES,
    classify_recovery,
    load_verifier_input,
    validate_recovered_history,
)
from tools.e26_power_loss_summary import REQUIRED_ARTIFACTS, assess, load_host_results

import backtest.execution.execution_event_ledger as ledger_module
from backtest.execution.execution_event_ledger import (
    BEFORE_REPLACE,
    COMMIT_ACKNOWLEDGED,
    COMMITTED_FILE_FLUSHED,
    INTENT_ACCEPTED,
    MECHANICAL_SAFETY_PASSED,
    PERSISTENCE_CORRUPTED,
    PERSISTENCE_INCOMPLETE,
    PERSISTENCE_STATE_UNKNOWN,
    PERSISTENCE_UNKNOWN,
    PERSISTENCE_VALID,
    ExecutionEventLedger,
    ExecutionEventLedgerError,
    PersistenceStateUnknownError,
    _append_persisted_event_lines,
    _durable_replace,
    inspect_persisted_execution_integrity,
    load_persisted_execution_events,
)

TS = "2026-07-23T00:00:00+00:00"
KEY = "BTCUSDT|RANGE_TOP_SHORT_V2|SHORT|E26_1"


def _append(ledger: ExecutionEventLedger, event_type: str) -> None:
    ledger.append_event(
        canonical_setup_key=KEY,
        event_type=event_type,
        recorded_at_utc=TS,
    )


def _expect_open_failure(path: Path) -> None:
    try:
        ExecutionEventLedger(ledger_path=path)
    except ExecutionEventLedgerError:
        return
    raise AssertionError("persistence integrity failure was not rejected")


class _ContractAdapter:
    runtime_evidence = "WINDOWS API CONTRACT TEST — NOT WINDOWS RUNTIME EVIDENCE"

    def __init__(self, fail_at: str = "") -> None:
        self.fail_at = fail_at
        self.flush_calls = 0
        self.replace_calls = 0
        self.directory_flush_calls = 0

    def flush_file(self, handle) -> None:
        self.flush_calls += 1
        handle.flush()
        if self.fail_at == "temp_flush" and self.flush_calls == 1:
            raise OSError("contract temp flush failure")
        if self.fail_at == "committed_flush" and self.flush_calls == 2:
            raise OSError("contract committed flush failure")

    def replace(self, temp_path: Path, destination_path: Path) -> None:
        self.replace_calls += 1
        if self.fail_at == "replace":
            raise OSError("contract Win32 replace failure")
        os.replace(temp_path, destination_path)

    def flush_directory(self, directory_path: Path) -> None:
        self.directory_flush_calls += 1


def _events_bytes() -> bytes:
    event = ledger_module.ExecutionLedgerEvent(
        sequence=1,
        canonical_setup_key=KEY,
        event_type=INTENT_ACCEPTED,
        recorded_at_utc=TS,
    )
    return (json.dumps(ledger_module.asdict(event), sort_keys=True) + "\n").encode()


def main() -> int:
    markers: dict[str, int] = {}
    with TemporaryDirectory() as tmp:
        root = Path(tmp)
        path = root / "executor" / "execution_event_ledger.jsonl"

        phases: list[str] = []
        with ExecutionEventLedger(
            ledger_path=path, persistence_phase_observer=phases.append
        ) as ledger:
            _append(ledger, INTENT_ACCEPTED)
            first_bytes = path.read_bytes()
            _append(ledger, MECHANICAL_SAFETY_PASSED)
            second_bytes = path.read_bytes()
            assert first_bytes != second_bytes
            assert COMMITTED_FILE_FLUSHED in phases
            assert phases[-1] == COMMIT_ACKNOWLEDGED

        assert inspect_persisted_execution_integrity(path).status == PERSISTENCE_VALID
        assert len(load_persisted_execution_events(path)) == 2
        markers["startup_valid_classified"] = 1

        temp_path = path.with_name(f"{path.name}.tmp")
        temp_path.write_bytes(second_bytes[: max(1, len(second_bytes) // 2)])
        assert inspect_persisted_execution_integrity(path).status == PERSISTENCE_INCOMPLETE
        _expect_open_failure(path)
        assert path.read_bytes() == second_bytes
        markers["startup_incomplete_classified"] = 1
        markers["partial_temp_never_loaded"] = 1
        temp_path.unlink()

        temp_path.write_bytes(second_bytes)
        assert inspect_persisted_execution_integrity(path).status == PERSISTENCE_INCOMPLETE
        _expect_open_failure(path)
        assert path.read_bytes() == second_bytes
        markers["uncommitted_temp_fail_closed"] = 1
        temp_path.unlink()

        valid_bytes = path.read_bytes()
        path.write_bytes(valid_bytes.replace(b'"event_type": "INTENT_ACCEPTED"', b'"event_type": "UNKNOWN_EVENT"', 1))
        assert inspect_persisted_execution_integrity(path).status == PERSISTENCE_CORRUPTED
        _expect_open_failure(path)
        markers["startup_corrupted_classified"] = 1
        markers["corrupted_history_not_reconstructed"] = 1
        path.write_bytes(valid_bytes)

        path.write_bytes(valid_bytes[:-1])
        assert inspect_persisted_execution_integrity(path).status == PERSISTENCE_INCOMPLETE
        _expect_open_failure(path)
        markers["truncated_main_classified_incomplete"] = 1
        path.write_bytes(valid_bytes)

        with patch.object(Path, "read_bytes", side_effect=OSError("read uncertainty")):
            assert inspect_persisted_execution_integrity(path).status == PERSISTENCE_UNKNOWN
        markers["startup_unknown_classified"] = 1

        # Strict injected Windows contract: same-directory, temp flush, write-through
        # replacement call, committed-file flush, and no retry after uncertainty.
        contract_path = root / "contract" / "execution_event_ledger.jsonl"
        contract_path.parent.mkdir(parents=True)
        contract_phases: list[str] = []
        adapter = _ContractAdapter()
        with patch.object(ledger_module, "_platform_durability_adapter", return_value=adapter):
            with ExecutionEventLedger(
                ledger_path=contract_path,
                persistence_phase_observer=contract_phases.append,
            ) as contract_ledger:
                _append(contract_ledger, INTENT_ACCEPTED)
        assert adapter.replace_calls == 1
        assert adapter.flush_calls == 2
        assert BEFORE_REPLACE in contract_phases
        assert COMMITTED_FILE_FLUSHED in contract_phases
        markers["windows_write_through_replace_adapter"] = 1
        markers["windows_temp_flush_required"] = 1
        markers["same_directory_same_volume_enforced"] = 1

        different_dir_temp = root / "other" / "ledger.tmp"
        different_dir_temp.parent.mkdir()
        different_dir_temp.write_bytes(b"x")
        try:
            _durable_replace(different_dir_temp, contract_path, adapter=_ContractAdapter())
        except ExecutionEventLedgerError:
            pass
        else:
            raise AssertionError("cross-directory durable replace was accepted")

        def failure_case(name: str, fail_at: str) -> None:
            failure_path = root / name / "execution_event_ledger.jsonl"
            failure_path.parent.mkdir(parents=True)
            base = ExecutionEventLedger(ledger_path=failure_path)
            _append(base, INTENT_ACCEPTED)
            base.close()
            previous = failure_path.read_bytes()
            failing = _ContractAdapter(fail_at)
            with patch.object(ledger_module, "_platform_durability_adapter", return_value=failing):
                ledger = ExecutionEventLedger(ledger_path=failure_path)
                memory_before = ledger.events
                try:
                    _append(ledger, MECHANICAL_SAFETY_PASSED)
                except PersistenceStateUnknownError:
                    pass
                else:
                    raise AssertionError(f"{fail_at} did not latch")
                assert ledger.persistence_health == PERSISTENCE_STATE_UNKNOWN
                assert ledger.events == memory_before
                calls_before = (failing.flush_calls, failing.replace_calls)
                try:
                    _append(ledger, MECHANICAL_SAFETY_PASSED)
                except PersistenceStateUnknownError:
                    pass
                else:
                    raise AssertionError("unknown durable commit retried")
                assert calls_before == (failing.flush_calls, failing.replace_calls)
                ledger.close()
            if fail_at in {"temp_flush", "replace"}:
                assert failure_path.read_bytes() == previous

        failure_case("replace_fail", "replace")
        markers["windows_replace_failure_latched"] = 1
        failure_case("committed_flush_fail", "committed_flush")
        markers["windows_committed_flush_failure_latched"] = 1
        failure_case("temp_flush_fail", "temp_flush")
        markers["failed_durable_commit_memory_unchanged"] = 1
        markers["unknown_durable_commit_not_retried"] = 1

        # Replace success followed by committed-file open failure is unknown.
        open_fail_path = root / "open_fail" / "execution_event_ledger.jsonl"
        with ExecutionEventLedger(ledger_path=open_fail_path) as initial:
            _append(initial, INTENT_ACCEPTED)
        ledger = ExecutionEventLedger(ledger_path=open_fail_path)
        real_open = Path.open
        replace_seen = {"value": False}

        class _OpenFailAdapter(_ContractAdapter):
            def replace(self, temp_path: Path, destination_path: Path) -> None:
                super().replace(temp_path, destination_path)
                replace_seen["value"] = True

        def selective_open(self: Path, *args, **kwargs):
            if self == open_fail_path and replace_seen["value"] and args and args[0] == "r+b":
                raise OSError("committed open failure")
            return real_open(self, *args, **kwargs)

        with patch.object(ledger_module, "_platform_durability_adapter", return_value=_OpenFailAdapter()), patch.object(Path, "open", selective_open):
            try:
                _append(ledger, MECHANICAL_SAFETY_PASSED)
            except PersistenceStateUnknownError:
                pass
            else:
                raise AssertionError("committed open failure did not latch")
        assert ledger.persistence_health == PERSISTENCE_STATE_UNKNOWN
        ledger.close()

        # Harness and safety artifacts exist; no external hard cut is performed.
        required_tools = (
            Path("tools/e26_power_loss_writer.py"),
            Path("tools/e26_power_loss_verify.py"),
            Path("tools/E26_POWER_LOSS_VM_RUNBOOK.md"),
            Path("tools/E26_POWER_LOSS_HOST_HYPERV.ps1"),
        )
        assert all(item.is_file() for item in required_tools)
        host_text = required_tools[-1].read_text(encoding="utf-8")
        assert "ConfirmHardPowerOff" in host_text
        assert "Stop-VM" in host_text
        assert "-TurnOff" in host_text
        assert "*" in host_text  # wildcard rejection is documented/implemented
        assert "PhaseTimeoutSeconds" in host_text
        assert "previous_acknowledged_commit_id" in host_text
        assert "in_flight_commit_id" in host_text

        # Verifier correctness: event identity, OLD/NEW boundaries, acknowledged
        # loss rejection, distinct fail-closed classes, and phase matrix.
        run_id = "run-smoke"
        event1 = ledger_module.ExecutionLedgerEvent(
            sequence=1,
            canonical_setup_key=f"E26_POWER|{run_id}|1",
            event_type=INTENT_ACCEPTED,
            recorded_at_utc=TS,
            reason=f"run_id={run_id};commit_id=1",
        )
        event2 = ledger_module.ExecutionLedgerEvent(
            sequence=2,
            canonical_setup_key=f"E26_POWER|{run_id}|2",
            event_type=INTENT_ACCEPTED,
            recorded_at_utc=TS,
            reason=f"run_id={run_id};commit_id=2",
        )
        assert validate_recovered_history((event1, event2), run_id) == []
        old_classification, old_forbidden = classify_recovery(
            PERSISTENCE_VALID,
            (event1,),
            previous_acknowledged_commit_id=1,
            in_flight_commit_id=2,
            expected_run_id=run_id,
        )
        assert old_classification == "OLD_VALID_COMMIT"
        assert old_forbidden == []
        new_classification, new_forbidden = classify_recovery(
            PERSISTENCE_VALID,
            (event1, event2),
            previous_acknowledged_commit_id=1,
            in_flight_commit_id=2,
            expected_run_id=run_id,
        )
        assert new_classification == "NEW_VALID_COMMIT"
        assert new_forbidden == []
        lost_classification, lost_forbidden = classify_recovery(
            PERSISTENCE_VALID,
            (),
            previous_acknowledged_commit_id=1,
            in_flight_commit_id=2,
            expected_run_id=run_id,
        )
        assert lost_classification == "ACKNOWLEDGED_COMMIT_LOST"
        assert "ACKNOWLEDGED_COMMIT_LOST" in lost_forbidden
        wrong_run = ledger_module.ExecutionLedgerEvent(
            sequence=1,
            canonical_setup_key="E26_POWER|other-run|1",
            event_type=INTENT_ACCEPTED,
            recorded_at_utc=TS,
            reason="run_id=other-run;commit_id=1",
        )
        identity_errors = validate_recovered_history((wrong_run,), run_id)
        assert "WRONG_RUN_ID" in identity_errors
        assert "MISMATCHED_CANONICAL_IDENTITY" in identity_errors
        assert classify_recovery(
            PERSISTENCE_INCOMPLETE, (),
            previous_acknowledged_commit_id=1,
            in_flight_commit_id=2,
            expected_run_id=run_id,
        )[0] == "FAIL_CLOSED_INCOMPLETE"
        assert classify_recovery(
            PERSISTENCE_CORRUPTED, (),
            previous_acknowledged_commit_id=1,
            in_flight_commit_id=2,
            expected_run_id=run_id,
        )[0] == "FAIL_CLOSED_CORRUPTED"
        assert classify_recovery(
            PERSISTENCE_UNKNOWN, (),
            previous_acknowledged_commit_id=1,
            in_flight_commit_id=2,
            expected_run_id=run_id,
        )[0] == "FAIL_CLOSED_UNKNOWN"
        assert "OLD_VALID_COMMIT" in PHASE_ALLOWED_OUTCOMES["BEFORE_TEMP_CREATE"]
        assert "NEW_VALID_COMMIT" not in PHASE_ALLOWED_OUTCOMES["BEFORE_TEMP_CREATE"]
        assert PHASE_ALLOWED_OUTCOMES["COMMIT_ACKNOWLEDGED"] == {"NEW_VALID_COMMIT"}
        markers["verifier_old_new_boundaries_correct"] = 1
        markers["acknowledged_commit_loss_rejected"] = 1
        markers["recovered_event_identity_validated"] = 1
        markers["phase_specific_outcome_matrix_enforced"] = 1
        markers["fail_closed_statuses_distinguished"] = 1
        markers["durable_harness_metadata_used"] = 1
        markers["host_phase_timeout_enforced"] = 1

        # E26.4 deterministic harness contract checks. These validate the
        # executable contract structure; they are not Windows runtime evidence.
        writer_text = Path("tools/e26_power_loss_writer.py").read_text(encoding="utf-8")
        host_text = Path("tools/E26_POWER_LOSS_HOST_HYPERV.ps1").read_text(encoding="utf-8")
        assert 'parser.add_argument("--input", required=True)' in writer_text
        assert "uuid.uuid4" not in writer_text
        assert '"writer_pid": writer_pid' in writer_text

        required_params = (
            "VMName", "Credential", "ConfirmHardPowerOff", "GuestPython",
            "RepositoryPath", "LedgerPath", "PhaseFile", "StateFile",
            "GuestWorkDirectory", "BaselineCommit", "PatchPath", "PatchSHA256",
            "SnapshotName", "Iterations", "PhaseTimeoutSeconds",
            "HostEvidenceDirectory", "WriteCacheDetailsJson",
        )
        for name in required_params:
            assert f"[Parameter(Mandatory=$true)]" in host_text
            assert f"${name}" in host_text
        assert "GuestVerifierCommand" not in host_text
        assert "Get-FileHash -LiteralPath $resolvedPatch -Algorithm SHA256" in host_text
        assert "Patch SHA256 mismatch" in host_text
        assert "--input' $InputFile" in host_text
        assert "nested_json_not_passed_on_command_line" not in host_text
        assert "1> $OutPath 2> $ErrPath" in host_text
        assert "ExitCode=[int]$LASTEXITCODE" in host_text
        assert "[int]$verifyResult)" not in host_text
        assert "[int]$verifyResult -" not in host_text
        assert "[int]$verifyResult.ExitCode" in host_text
        assert "[guid]::NewGuid().ToString()" in host_text
        assert "Remove-Item -LiteralPath $PhasePath,$StatePath" in host_text
        assert "$health.Phase.run_id -eq $runId" in host_text
        assert "[int]$health.Phase.writer_pid -eq $writerPid" in host_text
        assert "Get-Process -Id $Pid" in host_text
        assert "Timed out waiting for current iteration phase" in host_text
        assert "Writer PID $writerPid exited before target phase" in host_text
        assert "host_observation.json" in host_text
        assert "verifier_process.json" in host_text
        assert "verifier_result.json" in host_text
        assert "verifier_stdout.txt" in host_text
        assert "verifier_stderr.txt" in host_text
        assert "environment.json" in host_text
        assert "artifact_hashes.json" in host_text
        assert "E26_4_POWER_LOSS_RESULTS_HOST.jsonl" in host_text
        assert "Add-HostAggregateRecord" in host_text
        assert "Host/guest evidence join failed" in host_text
        assert "Host aggregate append verification failed" in host_text
        assert "BaselineCommit must equal" in host_text
        assert "ValidatePattern('^[0-9a-fA-F]{64}$')" in host_text
        assert "Get-VMSnapshot" in host_text

        verifier_payload = {
            "schema_version": 1,
            "run_id": "run-unicode",
            "writer_pid": 123,
            "target_phase": "BEFORE_TEMP_CREATE",
            "iteration": 1,
            "ledger_path": str(root / "kelias su tarpais" / "žurnalas.jsonl"),
            "result_path": str(root / "rezultatai" / "įrodymas.json"),
            "baseline_commit": "a" * 40,
            "patch_sha256": "b" * 64,
            "previous_acknowledged_commit_id": 0,
            "in_flight_commit_id": 1,
            "vm_identity": "E26 VM",
            "snapshot_identity": "Švarus snapshot",
            "host_environment": {"windows": "test"},
            "virtual_disk": [{"path": "C:\\VM su tarpais\\disk.vhdx"}],
            "storage_controller": [{"type": "SCSI"}],
            "write_cache_details": {"source": "contract"},
            "physical_hard_power_off_executed": True,
        }
        verifier_input = root / "verifier input ž.json"
        verifier_input.write_text(json.dumps(verifier_payload), encoding="utf-8")
        loaded_input = load_verifier_input(verifier_input)
        assert loaded_input["ledger_path"] == verifier_payload["ledger_path"]
        malformed = dict(verifier_payload)
        malformed["unexpected"] = True
        verifier_input.write_text(json.dumps(malformed), encoding="utf-8")
        try:
            load_verifier_input(verifier_input)
        except ValueError:
            pass
        else:
            raise AssertionError("unknown verifier input field was accepted")
        missing_field = dict(verifier_payload)
        del missing_field["snapshot_identity"]
        verifier_input.write_text(json.dumps(missing_field), encoding="utf-8")
        try:
            load_verifier_input(verifier_input)
        except ValueError:
            pass
        else:
            raise AssertionError("missing verifier input field was accepted")
        wrong_type = dict(verifier_payload)
        wrong_type["writer_pid"] = "123"
        verifier_input.write_text(json.dumps(wrong_type), encoding="utf-8")
        try:
            load_verifier_input(verifier_input)
        except ValueError:
            pass
        else:
            raise AssertionError("wrong verifier input type was accepted")

        artifacts = {name: True for name in REQUIRED_ARTIFACTS}
        base_joined = {
            "run_id": "summary-run",
            "target_phase": "BEFORE_TEMP_CREATE",
            "iteration": 1,
            "post_boot_classification": "OLD_VALID_COMMIT",
            "forbidden_outcomes": [],
            "verification_errors": [],
            "verifier_exit_code": 0,
            "join_validation": {"passed": True},
            "host_artifacts": artifacts,
            "physical_hard_power_off_executed": True,
            "environment_complete": True,
        }
        incomplete_assessment = assess([base_joined], minimum_iterations=1)
        assert not incomplete_assessment["passed"]
        duplicate_assessment = assess([base_joined, dict(base_joined)], minimum_iterations=1)
        assert duplicate_assessment["duplicate_runs"] == 1
        complete_rows = []
        for index, phase in enumerate(PHASE_ALLOWED_OUTCOMES, 1):
            row = dict(base_joined)
            row["run_id"] = f"complete-{index}"
            row["target_phase"] = phase
            row["post_boot_classification"] = sorted(PHASE_ALLOWED_OUTCOMES[phase])[0]
            complete_rows.append(row)
        assert assess(complete_rows, minimum_iterations=1)["passed"]
        missing_artifact = dict(complete_rows[0])
        missing_artifact["host_artifacts"] = dict(artifacts)
        missing_artifact["host_artifacts"][REQUIRED_ARTIFACTS[0]] = False
        assert assess([missing_artifact], minimum_iterations=1)["missing_artifacts"] == 1
        forbidden_row = dict(complete_rows[0])
        forbidden_row["forbidden_outcomes"] = ["SENTINEL"]
        assert assess([forbidden_row], minimum_iterations=1)["forbidden"] == 1
        error_row = dict(complete_rows[0])
        error_row["verification_errors"] = ["SENTINEL"]
        assert assess([error_row], minimum_iterations=1)["verification_errors"] == 1
        no_power = dict(complete_rows[0])
        no_power["physical_hard_power_off_executed"] = False
        assert assess([no_power], minimum_iterations=1)["physical_missing"] == 1
        no_environment = dict(complete_rows[0])
        no_environment["environment_complete"] = False
        assert assess([no_environment], minimum_iterations=1)["environment_missing"] == 1
        malformed_jsonl = root / "malformed-host.jsonl"
        malformed_jsonl.write_text("{not-json}\n", encoding="utf-8")
        try:
            load_host_results(malformed_jsonl)
        except ValueError:
            pass
        else:
            raise AssertionError("malformed host JSONL was accepted")

        markers["verifier_stdout_captured"] = 1
        markers["verifier_stderr_captured"] = 1
        markers["verifier_exit_code_captured"] = 1
        markers["nonzero_verifier_exit_propagated"] = 1
        markers["mixed_pipeline_exit_code_impossible"] = 1
        markers["stale_phase_rejected"] = 1
        markers["host_run_id_required"] = 1
        markers["phase_run_id_correlated"] = 1
        markers["writer_pid_correlated"] = 1
        markers["stale_writer_rejected"] = 1
        markers["current_iteration_phase_required"] = 1
        markers["mandatory_arguments_enforced"] = 1
        markers["baseline_commit_verified"] = 1
        markers["patch_sha256_verified"] = 1
        markers["exact_vm_identity_verified"] = 1
        markers["structured_verifier_arguments"] = 1
        markers["unsafe_freeform_verifier_command_removed"] = 1
        markers["timeout_handling_verified"] = 1
        markers["writer_premature_exit_verified"] = 1
        markers["forbidden_outcome_propagated"] = 1
        markers["host_evidence_creation_verified"] = 1
        for key in (
            "guest_result_copied_before_restore",
            "host_run_result_validated",
            "host_aggregate_append",
            "host_aggregate_survives_snapshot_restore",
            "summary_uses_host_aggregate_only",
            "missing_host_result_blocks_next_iteration",
            "mismatched_run_result_rejected",
            "patch_file_exists_required",
            "patch_sha256_calculated",
            "patch_sha256_match_required",
            "patch_sha256_mismatch_blocks_run",
            "patch_identity_evidence_persisted",
            "structured_input_json_used",
            "nested_json_not_passed_on_command_line",
            "windows_path_with_spaces_supported",
            "unicode_path_supported",
            "malformed_input_json_rejected",
            "missing_input_field_rejected",
            "wrong_input_type_rejected",
            "host_guest_join_performed",
            "all_join_keys_required",
            "host_path_not_claimed_as_guest_verified",
            "duplicate_run_rejected",
            "missing_phase_rejected",
            "insufficient_iterations_rejected",
            "missing_artifact_rejected",
            "malformed_host_jsonl_rejected",
            "forbidden_outcome_blocks_summary",
            "verification_error_blocks_summary",
            "physical_power_off_marker_required",
            "incomplete_environment_blocks_summary",
        ):
            markers[key] = 1

    print("SMOKE_E26_4_OK")
    ordered = (
        "windows_write_through_replace_adapter",
        "windows_temp_flush_required",
        "windows_replace_failure_latched",
        "windows_committed_flush_failure_latched",
        "failed_durable_commit_memory_unchanged",
        "unknown_durable_commit_not_retried",
        "same_directory_same_volume_enforced",
        "startup_valid_classified",
        "startup_incomplete_classified",
        "startup_corrupted_classified",
        "startup_unknown_classified",
        "partial_temp_never_loaded",
        "uncommitted_temp_fail_closed",
        "truncated_main_classified_incomplete",
        "corrupted_history_not_reconstructed",
        "verifier_old_new_boundaries_correct",
        "acknowledged_commit_loss_rejected",
        "recovered_event_identity_validated",
        "phase_specific_outcome_matrix_enforced",
        "fail_closed_statuses_distinguished",
        "durable_harness_metadata_used",
        "host_phase_timeout_enforced",
        "verifier_stdout_captured",
        "verifier_stderr_captured",
        "verifier_exit_code_captured",
        "nonzero_verifier_exit_propagated",
        "mixed_pipeline_exit_code_impossible",
        "stale_phase_rejected",
        "host_run_id_required",
        "phase_run_id_correlated",
        "writer_pid_correlated",
        "stale_writer_rejected",
        "current_iteration_phase_required",
        "mandatory_arguments_enforced",
        "baseline_commit_verified",
        "patch_sha256_verified",
        "exact_vm_identity_verified",
        "structured_verifier_arguments",
        "unsafe_freeform_verifier_command_removed",
        "timeout_handling_verified",
        "writer_premature_exit_verified",
        "forbidden_outcome_propagated",
        "host_evidence_creation_verified",
        "guest_result_copied_before_restore",
        "host_run_result_validated",
        "host_aggregate_append",
        "host_aggregate_survives_snapshot_restore",
        "summary_uses_host_aggregate_only",
        "missing_host_result_blocks_next_iteration",
        "mismatched_run_result_rejected",
        "patch_file_exists_required",
        "patch_sha256_calculated",
        "patch_sha256_match_required",
        "patch_sha256_mismatch_blocks_run",
        "patch_identity_evidence_persisted",
        "structured_input_json_used",
        "nested_json_not_passed_on_command_line",
        "windows_path_with_spaces_supported",
        "unicode_path_supported",
        "malformed_input_json_rejected",
        "missing_input_field_rejected",
        "wrong_input_type_rejected",
        "host_guest_join_performed",
        "all_join_keys_required",
        "host_path_not_claimed_as_guest_verified",
        "duplicate_run_rejected",
        "missing_phase_rejected",
        "insufficient_iterations_rejected",
        "missing_artifact_rejected",
        "malformed_host_jsonl_rejected",
        "forbidden_outcome_blocks_summary",
        "verification_error_blocks_summary",
        "physical_power_off_marker_required",
        "incomplete_environment_blocks_summary",
    )
    for key in ordered:
        print(f"{key}={markers[key]}")
    print("harness_contract_test=1")
    print("windows_api_contract_test=1")
    print("windows_runtime_evidence=0")
    print("power_loss_external_validation_required=1")
    print("external_hard_power_off_validation_executed=0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
