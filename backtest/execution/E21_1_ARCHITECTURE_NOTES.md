# E21.1 — Integration Boundary Hardening

## Approved architecture preserved

E21.1 does not add a new executor layer. It hardens the existing single ATS to
Executor shadow boundary.

```text
pipeline_core / ATS lifecycle
        |
        v
Opportunity Manager final selection
        |
        v
existing final position-overlap admission gate
        |
        v
final ATS-admitted out_df row
        |
        v
E21.1 shadow hook
  ExecutionIntent.from_row
        |
  consume_decision
        |
  check_mechanical_safety(DRY_RUN)
        |
        v
STOP — diagnostic result only
        |
        v
unchanged _emit_observation_rows()
```

## Exact authority boundary

The hook is located after all existing selection and position-overlap rejection
returns. Immediately before it, the shell aligns `flow_row` to the exact final
`out_df.iloc[0]`. Immediately after the guarded hook, the existing shell calls
`_emit_observation_rows()` without consulting the shadow result.

No later ATS business admission exists between the hook and emission. The code
following the hook performs existing operational actions: emit, lifecycle
logging, fired identity recording, and position-state recording. None of these
operations is conditioned on Executor shadow output.

## Shell-safety

The shell lazily imports the E21 boundary inside `try/except Exception`. This
protects ATS from import failures as well as failures in current or future
Executor internals. An unexpected failure produces an explicit
`EXECUTOR_SHADOW_INTERNAL_ERROR` diagnostic and control continues to the
unchanged `_emit_observation_rows()` call.

The boundary itself also distinguishes:

- expected validation or mechanical failures: `shadow_ingress_blocked:*`;
- unexpected internal failures: `EXECUTOR_SHADOW_INTERNAL_ERROR:*`.

Both outcomes are immutable diagnostics and cannot authorize or block ATS.

## No synthetic ATS authority

E21.1 adds only protocol metadata (`ATS_EXECUTION_INTENT_V1`) and the shell's
actual cycle timestamp. It does not synthesize ATS authority fields.

The final ATS row must already contain, among all other required intent facts:

- `selected_for_execution`;
- `execution_rank`;
- `selection_reason`.

Missing authority metadata fails closed inside shadow evaluation only. No
fallback rank, selection reason, quantity, risk, or snapshot identity is
invented.

## Forbidden reachability

The boundary imports only E1, E2, and E4 components. It does not import or call
Intent Journal, PaperExecutor, Command Gateway, submit adapters, exchange
adapters, retry, recovery, TP/SL, fills, or production mutation code.
