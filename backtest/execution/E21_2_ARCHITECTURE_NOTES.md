# E21.2 Final Boundary Certification Hardening

## Scope

E21.2 does not redesign the accepted E21 architecture. It hardens the single
optional ATS-to-Executor shadow observation and strengthens certification
evidence.

## Authoritative control-flow location

The production shell control flow in the supplied E21.1 repository is:

```text
Opportunity Manager final selection
    -> final selected out_df
    -> position overlap gate
    -> final admitted out_df
    -> flow-row alignment only
    -> _emit_with_optional_executor_shadow(...)
         -> _run_executor_shadow_diagnostic(...)
              -> ExecutionIntent.from_row
              -> consume_decision
              -> check_mechanical_safety(DRY_RUN)
              -> diagnostic only
         -> _emit_observation_rows(...)
```

The shell returns before this point whenever final selection or the position
overlap gate produces an empty dataframe. After the E21 hook, the wrapper calls
`_emit_observation_rows()` directly. No later strategy, lifecycle, Opportunity
Manager, risk, stale, wait-confirmation, selection, or position-overlap admission
is present between the hook and emission.

## Complete shell-safety boundary

All optional shadow-only work is inside `_run_executor_shadow_diagnostic()`'s
outer `try`:

- `out_df is None` and `out_df.empty` validation;
- `out_df.iloc[0]` row extraction;
- `dict(...)` conversion;
- cycle timestamp conversion;
- lazy Executor import;
- ExecutionIntent ingress;
- decision consumption;
- MechanicalSafetyBridge evaluation;
- result formatting and primary diagnostic output.

Any exception becomes best-effort `EXECUTOR_SHADOW_INTERNAL_ERROR` telemetry.
Even diagnostic-output failure is suppressed so the authoritative ATS emission
path remains operational.

## Authority preservation

The shadow boundary remains downstream-only. It does not add or infer ATS
selection rank, selection reason, quantity, notional, risk, or authority
snapshot identifiers. Missing required ATS facts fail closed within shadow and
cannot change `out_df` or emission.

## Forbidden paths

The E21.2 smoke uses active import, filesystem, socket, and HTTP sentinels. It
also executes the shell boundary functions at runtime with forced Executor and
row-extraction failures and proves `_emit_observation_rows()` is called exactly
once in both cases.
