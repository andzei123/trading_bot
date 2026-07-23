# E26.4 Power-Loss Hyper-V Runbook

Use only a disposable Windows VM with a dedicated virtual disk and one exact
clean snapshot. Never run the harness on the primary ATS workstation.

## Preconditions

1. Apply `ATS_EXECUTOR_E26_4.patch` to a clean checkout whose HEAD remains
   `a9132f7db60e7932e1d922ae4a66f673d22f2646`.
2. Create the disposable snapshot only after the patched working tree and the
   guest Python environment are ready.
3. Keep the patch file on the host. Supply both `PatchPath` and its actual
   SHA256. The harness recomputes and compares the digest before starting a VM.
4. Select a new empty host evidence directory for every matrix execution.
5. Supply factual write-cache evidence as JSON. Missing environment evidence
   prevents certification.

The host script requires exact VM and snapshot identities, explicit
`-ConfirmHardPowerOff`, a finite timeout, and all guest paths. It generates a
unique run ID, removes stale guest metadata, starts one correlated writer PID,
and accepts only a phase document matching both values.

The guest verifier consumes one schema-exact JSON input file. Nested host,
disk, and controller metadata are not transported as command-line JSON. The
guest validates only guest persistence facts. The host performs the final join
against run ID, PID, phase, iteration, baseline, patch hash, VM, and snapshot.

After every abrupt `Stop-VM -TurnOff`, the host copies the one-run verifier
result, stdout, stderr, process result, environment, and hashes into the host
run directory. It validates and flushes the canonical host JSONL before the
next snapshot restore. The final summary reads only that host JSONL.

## Required phase matrix

- `BEFORE_TEMP_CREATE`: `OLD_VALID_COMMIT` only
- `TEMP_WRITING`: `OLD_VALID_COMMIT` or `FAIL_CLOSED_INCOMPLETE`
- `TEMP_FLUSHED`: `OLD_VALID_COMMIT` or `FAIL_CLOSED_INCOMPLETE`
- `BEFORE_REPLACE`: `OLD_VALID_COMMIT` or `FAIL_CLOSED_INCOMPLETE`
- `REPLACE_RETURNED`: `OLD_VALID_COMMIT`, `NEW_VALID_COMMIT`,
  `FAIL_CLOSED_INCOMPLETE`, or explicitly allowed `FAIL_CLOSED_UNKNOWN`
- `COMMITTED_FILE_FLUSHED`: `NEW_VALID_COMMIT`,
  `FAIL_CLOSED_INCOMPLETE`, or explicitly allowed `FAIL_CLOSED_UNKNOWN`
- `COMMIT_ACKNOWLEDGED`: `NEW_VALID_COMMIT` only

Run at least three iterations for every phase. Any lost acknowledged commit,
identity mismatch, phase-matrix violation, join mismatch, missing host artifact,
non-zero verifier exit, malformed aggregate row, duplicate run, incomplete
environment, or verification exception prevents certification.

Generate the final report only with:

```powershell
py -m tools.e26_power_loss_summary `
  --host-results <HostEvidenceDirectory>\E26_4_POWER_LOSS_RESULTS_HOST.jsonl `
  --output <HostEvidenceDirectory>\E26_4_POWER_LOSS_SUMMARY.md `
  --minimum-iterations 3
```

Until that physical matrix completes successfully:

```text
POWER-LOSS DURABILITY DEMONSTRATED: NO
POWER-LOSS DURABILITY CERTIFIED: NO
GATEWAY INTEGRATION: NOT AUTHORIZED
```
