# R11 — Production Rotation Recovery State Classification

## Scope

R11 is diagnostic only. It does not authorize rotation, execute the executor,
modify journals, or implement recovery.

## Position in the chain

```text
Integration Boundary
        ↓
Authority Gate (DENY)
        ↓
Readiness (NOT_READY)
        ↓
Recovery State Classification
        ↓
Executor remains unreachable
```

## Immutable result

`LiveRotationRecoveryState` contains:

- `state`
- `recoverable`
- `reason`
- `requires_operator`
- `requires_executor`
- `diagnostic_version`

## Supported states

- `NO_ROTATION_STARTED`
- `WAITING_FOR_AUTHORITY`
- `WAITING_FOR_READINESS`
- `READY_BUT_NOT_AUTHORIZED`
- `ROTATION_NOT_STARTED`
- `CRASH_BEFORE_EXECUTOR`
- `CRASH_AFTER_AUTHORITY`
- `UNKNOWN_STATE`

Malformed or unsupported inputs fail closed to `UNKNOWN_STATE`, require an
operator, and never require or invoke the executor.

## Production default

At the R10 checkpoint, authority remains denied and readiness remains not ready.
R11 therefore classifies the production path as `ROTATION_NOT_STARTED`.

## Non-goals

R11 does not perform recovery, recovery replay, filesystem repair, journal
mutation, executor activation, scheduling, retention, or compression.
