# R8 Production Integration Boundary

## Purpose

R8 adds one isolated connection point between the live observation shell and a
future rotation executor. It does not enable production rotation.

## Configuration

The boundary reads `ATS_LIVE_ROTATION_INTEGRATION_ENABLED`.

- Unset or false: `INTEGRATION_DISABLED`.
- Explicitly true: `BOUNDARY_REACHED_EXECUTION_DISABLED`.

There is no execution-enable setting in R8. The boundary neither imports nor
calls `rotation_executor.py`.

## Safety properties

- Default production behavior and console output are unchanged.
- No journal path, writer, schema, planning, strategy, ranking, execution,
  restart, replay, WAIT, freshness, or risk code is changed.
- No archive, rename, active-file recreation, compression, retention, or
  scheduler operation is reachable.
- The shell initializes the boundary exactly once during startup and discards
  the status without producing output.

## Future phase boundary

A later, separately approved phase may replace the disabled boundary with a
validated coordinator. R8 itself cannot perform filesystem mutation.
