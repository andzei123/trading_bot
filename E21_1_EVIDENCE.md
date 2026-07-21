# E21.1 Evidence Package

## Validation environment

Validation was executed in a repository reconstructed from the supplied E21
current files. The patch remains repository-relative. Final application against
the user's authoritative Git HEAD must be confirmed in that repository.

## Compile

```text
python -m compileall -q backtest/execution backtest/journal/live_observation_shell.py
PASS
```

## Smoke

```text
SMOKE_E21_1_OK validated_path=1 missing_rank_fail_closed=1 missing_selection_fail_closed=1 mechanical_veto=1 unexpected_internal_error_diagnostic=1 shell_continuation_guard=1 deterministic=1 source_immutable=1 result_immutable=1 journal_mutation_sentinel=1 exchange_call_sentinel=1 gateway_import_sentinel=1 paper_executor_import_sentinel=1 single_boundary_call=1
```

Smoke instrumentation includes:

- import sentinel rejecting PaperExecutor, Command Gateway, Intent Journal,
  testnet submit, and exchange module reachability;
- `Path.open`, `Path.write_text`, and `Path.write_bytes` mutation sentinels;
- socket and `urllib.request.urlopen` exchange-call sentinels;
- forced unexpected `RuntimeError` inside `ExecutionIntent.from_row`;
- source scan asserting one shell call to `evaluate_ats_shadow_admission`;
- source ordering assertion proving guarded hook precedes unchanged
  `_emit_observation_rows()`.

## Repository-wide uniqueness search

Command:

```text
grep -RIn --exclude-dir=.git --exclude='*.md' --exclude='*.pyc' \
  'evaluate_ats_shadow_admission(' .
```

Expected implementation results:

```text
backtest/execution/shadow_integration_boundary.py: function definition
backtest/execution/tests/smoke_e21.py: test calls
backtest/journal/live_observation_shell.py: exactly one production boundary call
```

Only one production ATS to Executor call exists: the call in
`live_observation_shell.py`. Tests are non-production callers and the boundary
module contains the function definition.

## Behavioral non-interference evidence

The shell does not assign, filter, replace, or mutate `out_df` from the shadow
result. It catches every normal Python exception from lazy import and execution,
prints a diagnostic, then reaches the pre-existing unconditional:

```text
written = _emit_observation_rows(...)
```

No shadow result field is consumed after printing.
