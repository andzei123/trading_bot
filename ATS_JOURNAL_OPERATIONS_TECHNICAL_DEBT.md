# ATS Journal Operations — Technical Debt Register

This is an append-only project register. Findings are recorded here; J1 does not implement fixes.

## 2026-07-15 — J1 Journal Safety Assessment

Baseline: `pressure_diag_logging` at `6cedff6` (`ATS_JOURNAL_ROTATION_R1_R11_REPOSITORY_REPAIRED`).

| ID | Severity | Finding | Evidence / impact | Status |
| --- | --- | --- | --- | --- |
| JO-TD-001 | CRITICAL | Position authority fails empty with possible destructive subsequent overwrite | `live_observation_shell._mark_position_open()` reads existing `position_state.csv`, substitutes an empty frame after read failure, concatenates the new OPEN row and overwrites the canonical CSV. | OPEN — document only |
| JO-TD-002 | CRITICAL | Fired and terminal identity authorities fail empty | Active `identity._load_canonical_keys_from_csv(fired_setups_csv)` tries canonical identity, structural reconstruction, then legacy `setup_id`; absence of every usable canonical, structural and legacy schema returns an empty set. `_load_fired_setup_ids()` is only a legacy/helper loader. Terminal authority also fails empty after read/schema failure. | OPEN — document only |
| JO-TD-017 | CRITICAL | Visible-cache restart continuity fails empty | Cache read failure or missing required columns becomes `{}`; a later save can replace persisted visibility continuity. | OPEN — document only |
| JO-TD-018 | CRITICAL | Terminal lifecycle authority is split across canonical default paths | Shell authority is `backtest/journal/terminal_lifecycle_registry.csv`; production-reachable closer writes `backtest/journal/exports_live/terminal_lifecycle_registry.csv`. | OPEN — document only |
| JO-TD-003 | IMPORTANT | Whole-file state overwrite is non-transactional | Position/cache/summary writers overwrite canonical paths without atomic replace or recovery evidence. | OPEN — document only |
| JO-TD-004 | IMPORTANT | Append-only production CSVs have no enforced bound | Retention exists as frozen design metadata, not active execution. | OPEN — document only |
| JO-TD-005 | IMPORTANT | Pressure summary repeatedly reads complete raw history | Runtime cost grows with `raw_candidate_lifecycle_diag.csv`; threshold not measured. | OPEN — document only |
| JO-TD-006 | IMPORTANT | Existing non-empty headers are not validated consistently | Schema drift can silently misalign appended values and reader expectations. | OPEN — document only |
| JO-TD-007 | IMPORTANT | Rotation plan is unbounded and absent from design inventory | One row per observed CSV per cycle; `live_rotation_plan.csv` is not one of the 19 design entries. | OPEN — document only |
| JO-TD-008 | IMPORTANT | R1 inventory omits optional production telemetry CSVs | Pre-visible, multiple TDP trace, cluster shadow, and other CLI outputs are outside the 19-row design. | OPEN — document only |
| JO-TD-009 | MODERATE | Current-working-directory determines default journal root | Starting the same command elsewhere can create a parallel state tree. | OPEN — document only |
| JO-TD-010 | MODERATE | Logical multiple-writer ownership lacks a global lock contract | Sequential writes in one shell process are proven. Concurrent multi-process path sharing is possible but not observed or proven. | OPEN — document only |
| JO-TD-011 | MODERATE | Diagnostic persistence counters reset on restart | Sniper and TDP telemetry continuity fields are process-memory based. | OPEN — document only |
| JO-TD-012 | MODERATE | Derived-summary errors can leave stale output | Missing/corrupt source paths return without marking existing summary stale. | OPEN — document only |
| JO-TD-013 | MODERATE | Corrupt-tail and torn-row recovery are absent | No checksum, quarantine, framing, or verified last-row repair was found. | OPEN — document only |
| JO-TD-014 | LOW | Optional research telemetry remains unbounded when enabled | Disabled by default and not decision authority, but can consume disk indefinitely. | OPEN — document only |
| JO-TD-015 | INFORMATIONAL | Residual runtime evidence limits | Runtime metadata now proves existence, byte sizes, logical row counts, physical headers, last-write timestamps and parser traversal status. Still unresolved: duplicate counts, semantic schema compatibility, growth/day, runtime RAM peak, corrupt-tail detection beyond parser success and production process CWD. | OPEN — narrowed evidence gap |
| JO-TD-016 | INFORMATIONAL | Windows production open-handle compaction remains unproven | R7 validates a disposable handshake path; production rotation remains disabled and does not use it. | OPEN — evidence gap |
| JO-TD-019 | MODERATE | Production candle snapshot patterns are outside journal and rotation coverage | `run_symbol_once()` unconditionally overwrites per-symbol raw/closed snapshots after successful fetch; dynamic files are absent from the 19-row design and `live_rotation_observed_csv_paths`. | OPEN — document only |
| JO-TD-020 | IMPORTANT | Runtime metadata coverage requirement | Default metadata, candle snapshots and all 13 active custom-path targets are now incorporated. Collector CWD proves only collector context; production Python CWD remains `not proven`. | EVIDENCE SATISFIED — pending Peer/Lead review |
| JO-TD-021 | IMPORTANT | Default candidate pressure has reached material scale | Default `exports_live/candidate_pressure.csv` is 200,581,278 bytes and 4,456,443 parseable rows. It is not the current active command path, but quantitatively confirms prior unbounded-growth risk. | OPEN — runtime evidence recorded |
| JO-TD-022 | INFORMATIONAL | Process relationship classification | Parent chain confirms `py.exe` PID 17904 → venv Python PID 2668 → Python312 PID 19188. | SINGLE LAUNCHER / INTERPRETER PROCESS CHAIN CONFIRMED |
| JO-TD-023 | MODERATE | Configured active authority waterfall target is absent | `authority_waterfall.csv` was extracted from the active command but did not exist at collection. This may mean no qualifying row was emitted; no parse failure is inferred. | OPEN — document only |
| JO-TD-024 | LOW | Active sniper summary is header-only | `sniper_candidate_summary.csv` exists, parses, and has its expected header but zero data rows. | OPEN — document only |
| JO-TD-025 | IMPORTANT | Active rotation plan has material unbounded growth | Active `live_rotation_plan.csv` reached 21,033,722 bytes / 109,836 rows and was still current at collection. | OPEN — runtime evidence recorded |
| JO-TD-026 | IMPORTANT | Active flow log has material unbounded growth | Active `flow_log.csv` reached 9,012,540 bytes / 36,624 rows and was still current at collection. | OPEN — runtime evidence recorded |
| JO-TD-027 | MODERATE | Schema versions diverge across default and active directories | Default legacy versus active extended headers differ for position state, fired setups and live observation entries. Active files parse, but metadata parsing does not prove semantic compatibility across versions. | OPEN — document only |
| JO-TD-028 | IMPORTANT | Position-state multi-writer schema contraction loses wait context | Writer A, `live_observation_shell._mark_position_open()`, writes 12 columns including `wait_confirm_ts` and `wait_context_source`. Writer B, `position_closer.close_symbol_if_hit()`, reduces to 10-column `POSITION_STATE_COLUMNS` and overwrites the same CSV, discarding those two fields. Active runtime header confirms the narrower schema. Entire position-record loss is not claimed. | OPEN — document only |

## 2026-07-20 — J2 Journal Compaction Design

J2 defines contracts only. “DESIGN ADDRESSED” means the risk has an explicit fail-closed design treatment; it does not mean implementation, runtime validation, or production authorization.

| ID | Severity | Finding | Evidence / impact | Status |
| --- | --- | --- | --- | --- |
| JO-TD-029 | CRITICAL | Production writer quiescence and Windows handle proof are not implemented | Safe compaction requires a generation-token handshake, complete path-set acknowledgement, independently proven closed handles, stale-ack refusal, and new-generation resume. R7 disposable evidence does not prove the production writer protocol. | DESIGN ADDRESSED — implementation and certification required |
| JO-TD-030 | CRITICAL | OPEN-position geometry retention requires a durable dependency index and pin validation | `position_closer` resolves side/SL/TP through `live_observation_entries.csv`; J2 additionally requires entry and planned RR preservation. Missing, conflicting, or ambiguous canonical/legacy matches must block compaction. | DESIGN ADDRESSED — implementation and runtime evidence required |
| JO-TD-031 | IMPORTANT | Retention thresholds and hard resource ceilings are not evidence-approved | J1 does not establish growth/day, duplicate rates, RAM peaks, safe archive windows, or disk budgets. Arbitrary production retention values would be unsafe. | OPEN — policy values NOT AUTHORIZED |
| JO-TD-032 | IMPORTANT | Deterministic active-path registry and writer-generation authority are absent | Current paths can be CLI-overridden and defaults depend on CWD; production process CWD is not proven. A compactor cannot safely infer active authority from filenames or defaults. | DESIGN ADDRESSED — control-plane implementation required |
| JO-TD-033 | IMPORTANT | Crash-safe generation commit and recovery controller are unimplemented | Whole-file atomicity, durable commit markers, predecessor linkage, disk-full handling, and recovery across every state transition are not production-proven on Windows. | DESIGN ADDRESSED — implementation and crash certification required |
| JO-TD-034 | IMPORTANT | Full-history consumers can invalidate otherwise safe source compaction | `_rebuild_pressure_window_summary()` and the closer perform complete-file reads with current semantics. Removing raw history without coordinated reader contracts could break summaries or OPEN-position closure. | DESIGN ADDRESSED — reader bounding/coordination required before enablement |
| JO-TD-035 | MODERATE | External-memory identity, duplicate, and semantic-schema validation are not implemented | Exact duplicate/ambiguity detection and bounded-memory pin selection require disk-backed indexing/external sorting; metadata parser success is insufficient. | DESIGN ADDRESSED — implementation and scale validation required |
| JO-TD-036 | MODERATE | Archive compression and source reclamation policy remain unauthorized | No deterministic compression codec/version, dual checksum convention, or separately certified deletion/reclamation lifecycle has been approved. | OPEN — no compression or reclamation authorized |

## Register rules

- Preserve prior entries and IDs.
- Append new findings deterministically by assessment date.
- Do not close an item without implementation evidence, validation evidence, Peer Review, and Lead approval.
- Do not interpret an OPEN item as authorization to implement a fix outside the active phase.
