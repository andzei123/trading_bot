# R9 Production Rotation Authority Gate — Revision 1

The authority module is a mandatory production-shell dependency. Import failure
stops startup and cannot silently remove the gate.

The existing R8 helper evaluates authority exactly once for normal, disabled,
missing, and failed integration-boundary states. Construction/evaluation failure
returns `DENY / AUTHORITY_EVALUATION_FAILED`; unknown states return
`DENY / NOT_AUTHORIZED`. No input can produce authorization.

No executor import, filesystem mutation, scheduler, compression, retention, or
production rotation path is introduced. Planning and trading behavior remain
unchanged.
