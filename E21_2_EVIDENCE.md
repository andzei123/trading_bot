# E21.2 Evidence Package

## Evidence authority limitation

These are actual command outputs from the supplied/reconstructed E21.1 Git
repository at baseline commit `3d9ec5e`. This environment is not the user's
production clone and cannot independently prove the user's external locked HEAD.

## Runtime continuation smoke

```text
[EXECUTOR_SHADOW_INTERNAL_ERROR][BTCUSDT] error_type=RuntimeError error=sentinel_executor_failure
[EXECUTOR_SHADOW_INTERNAL_ERROR][BTCUSDT] error_type=RuntimeError error=sentinel_row_extraction_failure
SMOKE_E21_2_OK validated_path=1 missing_rank_fail_closed=1 missing_selection_fail_closed=1 mechanical_veto=1 unexpected_internal_error_diagnostic=1 shell_continuation_guard=1 runtime_emit_after_executor_failure=1 runtime_emit_after_row_extraction_failure=1 deterministic=1 source_immutable=1 result_immutable=1 journal_mutation_sentinel=1 exchange_call_sentinel=1 gateway_import_sentinel=1 paper_executor_import_sentinel=1 single_boundary_call=1
```

The smoke parses the actual shell source, executes the two E21.2 shell helper
functions, injects an instrumented `_emit_observation_rows`, and proves exactly
one emission call after both forced Executor failure and row-extraction failure.

## Admission control-flow evidence

The actual shell sequence is:

```text
select_newest_live_candidate / final selected out_df
position overlap gate
final admitted out_df
flow-row alignment only
_emit_with_optional_executor_shadow
  _run_executor_shadow_diagnostic
  _emit_observation_rows
```

The source-order assertions verify the shadow guard contains validation,
extraction, conversion, lazy import, executor call, formatting, and exception
handling. The wrapper source-order assertion verifies shadow runs before direct
emission. No later ATS business gate exists inside the wrapper.

## Repository-wide match classification

### `evaluate_ats_shadow_admission`

- Production: one lazy import and one invocation in
  `backtest/journal/live_observation_shell.py`; together they form one boundary.
- Executor internal: one definition in `shadow_integration_boundary.py`.
- Tests: smoke imports, calls, forced-failure patch, and source assertions.
- Documentation: prior evidence references only.

### `shadow_integration_boundary`

- Production: one lazy import in the shell.
- Executor internal: module itself.
- Tests: smoke imports and source assertion.
- Documentation: architecture/completion references.

### `ExecutionIntent.from_row`

- Production ATS boundary: one call inside `shadow_integration_boundary.py`.
- Executor internal: CSV helpers and decision-consumer row helper.
- Tests/documentation: smoke patch and architecture references.

### `consume_decision`

- Production ATS boundary: one call inside `shadow_integration_boundary.py`.
- Executor internal: definition and `consume_decision_row` delegation.
- Documentation: architecture references.

### `check_mechanical_safety`

- Production ATS boundary: one call inside `shadow_integration_boundary.py`.
- Executor internal: definition/import.
- Documentation: architecture references.

### `PaperExecutor`

- Production: no match.
- Executor internal: no implementation match in the supplied repository subset.
- Tests: forbidden import prefix sentinel.
- Documentation: prohibition statements only.

### `Gateway`

- Production: no match.
- Executor internal: no implementation match in the supplied repository subset.
- Tests: forbidden import prefix sentinel.
- Documentation: prohibition statements only.

### `testnet`

- Production: no match.
- Tests: forbidden `testnet_real_submit` import prefix sentinel only.

## Conclusion

Exactly one production ATS-to-Executor boundary exists in the supplied
repository: the shell's single lazy import/invocation pair for
`evaluate_ats_shadow_admission`, reached only through
`_run_executor_shadow_diagnostic` and followed directly by ATS emission.
