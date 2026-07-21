# E21 — ATS Shell → Executor Shadow Integration Boundary

## Boundary

The single E21 hook is after final ATS candidate selection and the existing
position-overlap gate, immediately before the unchanged observation emission.

```text
ATS final admission
  -> ExecutionIntent.from_row
  -> consume_decision
  -> check_mechanical_safety(DRY_RUN)
  -> STOP
```

The shell prints the immutable result only. It does not use the result to alter
emission, lifecycle, position state, journals, gateway, submit, or exchange
behavior. E21 therefore observes the future veto boundary without granting the
executor execution authority.

## Authority

ATS remains business authority. E21 overlays only factual boundary metadata:
selected=true, rank, selection reason, schema version, and cycle timestamp. All
risk, quantity, notional, geometry, and authority snapshot values must already
exist in the ATS row. Missing values fail closed inside the shadow result.

## Prohibited paths

No Intent Journal, PaperExecutor, Command Gateway, Submit Adapter, exchange
adapter, retry, recovery, TP/SL, fill, or production file write is reachable.
