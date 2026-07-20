# ATS Journal Operations — J2 Journal Compaction Design

## 1. Status, scope, and non-goals

- Phase: **J2 — CRITICAL Journal Compaction Design**
- Authorized baseline: branch `pressure_diag_logging`, J1 checkpoint `fe6a291`, tag `ATS_JOURNAL_OPERATIONS_J1_APPROVED`
- Repository repair baseline: `6cedff6`, tag `ATS_JOURNAL_ROTATION_R1_R11_REPOSITORY_REPAIRED`
- Deliverable type: **design and documentation only**
- Production execution authority: **DENIED**

This document defines contracts for a future deterministic, fail-closed, bounded-memory Journal Compaction subsystem. It does not implement compaction, choose production retention values, change any CSV schema or path, modify the frozen R1–R11 rotation architecture, or authorize production enablement. J3–J6 remain locked.

## 2. Evidence baseline

The design is grounded in:

- `ATS_JOURNAL_OPERATIONS_J1_SAFETY_ASSESSMENT.md`;
- `ATS_JOURNAL_OPERATIONS_TECHNICAL_DEBT.md`;
- `ATS_J1_RUNTIME_METADATA.json`;
- `ATS_J1_ACTIVE_PATH_RUNTIME_METADATA.json`;
- read-only source inspection of `live_observation_shell.py`, `position_closer.py`, `identity.py`, pipeline diagnostics, metric writers, and frozen rotation design/support files.

The current workspace is an extracted evidence snapshot, not a Git working clone. Live `git status`, `git diff`, and HEAD verification are therefore unavailable here. The mandate-supplied checkpoint is the design baseline; a future repository integration step must verify the document against repository HEAD before checkpointing.

Runtime evidence establishes 13 configured active CSV targets, 12 existing and parse-traversable, one absent `authority_waterfall.csv`, and one valid header-only `sniper_candidate_summary.csv`. Exact physical header authority remains the two runtime JSON files. Parser traversal does not prove semantic schema compatibility or corrupt-tail absence.

## 3. Normative safety invariants

1. **No authority loss.** A committed generation must preserve every live authority, idempotency identity, terminal identity, visibility-continuity record, and OPEN-position dependency required after restart.
2. **OPEN geometry remains immediately reachable.** For every OPEN position, the exact entry, SL, TP, side, and planned RR record required by the closer must remain in the active generation or in a synchronously readable pinned store covered by the same commit. J2 does not authorize introducing such a new store; therefore the design pins the required `live_observation_entries.csv` rows in the active file.
3. **Unknown means no mutation.** Missing, ambiguous, unreadable, schema-invalid, path-ambiguous, concurrently changing, or unproven input blocks compaction for its dependency closure.
4. **No fail-empty interpretation.** An authority/state parse or schema failure is an error, never an empty state.
5. **Exact schema preservation.** The source physical header is captured exactly; output schema must match the policy-authorized schema fingerprint. Schema conversion is outside J2.
6. **Generation atomicity.** No incomplete archive or active output can become authoritative. A durable commit marker is the only authority switch.
7. **Last known-good preservation.** Source and prior committed generations remain untouched until the new generation is fully validated and durably committed; reclamation is a separate later-authorized action.
8. **Quiescence before mutation.** No active journal is compacted while any writer may append or overwrite it. Unknown Windows handle state blocks the operation.
9. **Bounded memory.** CSV parsing, counting, hashing, selection, duplicate detection, and output validation are streaming or external-memory operations with configured hard limits.
10. **Determinism and idempotency.** Identical source bytes, policy version, pins, and generation predecessor produce the same retained/archive partition and manifest content apart from the declared generation timestamp/identifier inputs.
11. **Path containment.** Every normalized source, staging, archive, manifest, and active target must be within the configured repository root and must not collide after Windows case-folding.
12. **No trading behavior change.** Compaction never changes trading decisions, ordering, identity semantics, geometry, or authority rules.

## 4. Path discovery and schema authority

The future subsystem receives an explicit, normalized absolute `repository_root`; it must not derive the root from process CWD. Existing production paths remain unchanged.

For each run it builds a path registry from: (a) the active shell command/config evidence supplied by an authenticated control plane; (b) canonical J1 defaults; (c) dynamic active-symbol candle patterns; and (d) explicitly enabled optional flags. Each registry entry records logical journal identity, declared repository-relative path, normalized absolute path, source of discovery, active/default/historical classification, and writer generation. A default and active path are never silently merged. The two default terminal-registry paths remain distinct identities until a later architecture decision resolves them.

Before reading data, the registry rejects paths outside the repository, path aliases/collisions after normalization and case-folding, symlink/reparse-point escapes, duplicate logical outputs targeting one physical file, and an active/default classification that cannot be proven. Production process CWD remains unknown and is not used as evidence.

The exact physical header bytes/fields observed at preflight are the source schema authority for that generation, cross-checked against the permitted schema versions in an approved policy registry. Runtime JSON headers are evidence, not permission to accept arbitrary drift. A header-only file is valid only when its exact header is permitted. Missing optional telemetry is recorded as absent; missing authority or required dependency blocks its dependency closure. A malformed header, inconsistent row width, invalid quoting, undecodable content, or corrupt/torn tail blocks compaction and produces `UNKNOWN_FAIL_CLOSED` or `RECOVERY_REQUIRED` according to whether durable mutation began.

## 5. Journal classification matrix

Legend: `NE` = not eligible; `CE` = conditionally eligible only after all contracts and production authorization; `R` = rebuild rather than history compaction; `B` = bounded snapshot, no compaction needed. Retention thresholds are named policy parameters, not approved numeric values.

| Journal identity | Class | Canonical/default and active discovery | Writer(s); reader(s) | Schema/write/restart authority | Eligibility, retention, OPEN pin, memory |
| --- | --- | --- | --- | --- | --- |
| `position_state.csv` | Authority/state | default `backtest/journal/exports_live/position_state.csv`; active CLI path | `_mark_position_open`; `close_symbol_if_hit`; shell gates/closer | 12-column open writer and 10-column closer overwrite; CRITICAL restart authority | **NE** until schema contraction and transactional authority handling are separately implemented/verified. Preserve complete file. OPEN rows always pinned. Streaming validation only. |
| `fired_setups.csv` | Idempotency/identity | default `backtest/journal/fired_setups.csv`; active CLI path | shell append; `identity._load_canonical_keys_from_csv` | append; CRITICAL restart identity with canonical → structural → legacy fallback | **CE**, but only identities proven no longer capable of replay and covered by terminal evidence may archive. Unresolved/duplicate identities pin. External sort/index required. |
| shell `terminal_lifecycle_registry.csv` | Authority/state | `backtest/journal/terminal_lifecycle_registry.csv` or explicit active CLI path | identity append; shell terminal loader | append; CRITICAL terminal authority | **CE** only after active path is uniquely proven. Retain policy-defined recent rows plus all identity/dependency pins. External sort/index. |
| closer `terminal_lifecycle_registry.csv` | Authority/state | derived from `position_state_csv.parent`; default `backtest/journal/exports_live/...` | closer/identity append; no unified reader | append; split default authority | **NE** while path relationship/authority is ambiguous. Preserve complete file. |
| `visible_ts_cache.csv` | Authority/state cache | hard-coded default plus verified actual | shell whole-file save/load | overwrite; CRITICAL restart continuity; maximum 50,000 entries in current writer | **NE** until atomic generation and semantic continuity validation exist. Preserve complete file; bounded streaming validation. |
| `live_observation_entries.csv` | OPEN-position dependency + telemetry | default `backtest/journal/exports_live/...`; active CLI path | shell append; closer `_load_live_entries`; audits | append; IMPORTANT close-geometry dependency | **CE** only with OPEN geometry pinning. Retain pinned rows plus policy window `live_entries_retention`; missing/ambiguous geometry blocks. External identity index; never full-file RAM. |
| `flow_log.csv` | Append-only telemetry | default exports path; active CLI path | shell append; analysis | append; no restart authority | **CE** by UTC/logical-row ranges under `flow_log_retention`; no OPEN pin unless future evidence links it. Streaming. |
| `raw_candidate_lifecycle_diag.csv` | Append-only telemetry / summary source | default exports path; active CLI path | shell append; pressure-summary rebuild/audits | append; unbounded; production-reachable full-history reader | **CE** only with a coordinated replacement summary contract or reader range support later implemented. Policy `raw_candidate_retention`; stream chunks. |
| `entry_model_pre_admission.csv` | Optional telemetry | CLI flag only | pipeline helper; audits | append; disabled by default | **CE** if present/enabled and exact active path proven; `optional_telemetry_retention`; streaming. |
| `pressure_window_summary.csv` | Reconstructable summary | default exports path; active CLI path | shell rebuild; audits | whole-file overwrite; stale-on-failure risk | **R/NE**: validate and rebuild from retained authoritative source only after summary semantics can be reproduced. Never archive as authority. |
| `sniper_candidate_diag.csv` | Append-only telemetry | default exports path; active CLI path | shell append; summary/audits | append; restart counters reset | **CE** with policy `sniper_diag_retention`; rows spanning restart semantics remain forensic in archives; streaming. |
| `sniper_candidate_summary.csv` | Reconstructable/optional summary | default exports path; active CLI path | initializer/summary path; audits | header-only valid in active evidence; update semantics not fully proven | **NE** until durable writer/rebuild semantics are proven. Preserve current file; header-only is not an error. |
| `opportunity_manager_snapshot.csv` | Optional append-only telemetry | CLI flag | shell append; audits | append; duplicates possible | **CE** under `opportunity_snapshot_retention`; duplicate handling is forensic, not silent dedupe; external sorting if needed. |
| `authority_waterfall.csv` | Optional telemetry with authority semantics in audits | CLI flag | shell append; audits | append; active target may be absent | **NE when absent or semantic role unresolved**. Absence is recorded, never synthesized. If present, treat as authority-sensitive and pin unresolved identities. |
| `parity_filter_diagnostics.csv` | Optional telemetry | CLI flag | shell append; audits | append; low production authority | **CE** under `optional_telemetry_retention`; streaming. |
| `tdp_stale_shadow_oos.csv` | Optional telemetry | CLI flag | shell append; audits | append; restart counters reset | **CE** under `optional_telemetry_retention`; preserve restart-boundary evidence. |
| `structural_ts_shadow_oos.csv` | Optional telemetry | CLI flag | shell append; audits | append | **CE** under `optional_telemetry_retention`; streaming. |
| `candidate_pressure.csv` | Append-only telemetry | default exports path; active CLI path | pipeline append; audits | append, write errors swallowed; historical default >200 MB/4.4M rows | **CE** under `candidate_pressure_retention`; source-stability checks mandatory; streaming. |
| `symbol_performance.csv` | Reconstructable summary | metrics path if enabled | metrics tracker; dashboards | normally overwrite; enablement not proven | **NE/R** until enablement and rebuild inputs are proven. |
| `equity_curve.csv` | Reconstructable/growing summary | metrics path if enabled | metrics tracker; dashboard | overwrite/growing derived output; enablement not proven | **NE/R** until semantics and source are proven; streaming rebuild required. |
| `{symbol}_raw_live_candles.csv` | Bounded snapshot | `backtest/journal/exports_live/candles_debug/`; discover only active-symbol pattern | `run_symbol_once`; no production decision reader after write | unconditional whole-file overwrite; bounded fetched frame | **B**, exclude from compaction. Validate containment/header; future snapshot replacement safety is separate. |
| `{symbol}_closed_live_candles.csv` | Bounded snapshot | same dynamic discovery | `run_symbol_once`; in-memory frame used by current cycle | unconditional overwrite; bounded frame minus forming candle | **B**, exclude from compaction. |
| `live_rotation_plan.csv` | Generated plan/telemetry | default root or active configured path | frozen R4 writer; audits | append, unbounded; not itself in 19-row design | **CE** as forensic plan history only, never as rotation authority. `rotation_plan_retention`; streaming. Does not enable R1–R11. |
| `pre_visible_entry_exposure.csv` | Optional telemetry | CLI flag | shell append; audits | append | **CE** under `optional_telemetry_retention`; streaming. |
| `tdp_visible_assignment_trace.csv` | Optional telemetry | CLI flag | pipeline append; audits | append | **CE** under `optional_telemetry_retention`; streaming. |
| `tdp_identity_resurfacing_trace.csv` | Optional telemetry | CLI flag | pipeline append; audits | append | **CE** under `optional_telemetry_retention`; identity fields are retained as forensic data, not authority. |
| `tdp_disappearance_trace.csv` | Optional telemetry | CLI flag | shell/pipeline append; audits | append | **CE** under `optional_telemetry_retention`; streaming. |
| `tdp_true_birth_trace.csv` | Optional telemetry | CLI flag | shell/pipeline append; audits | append | **CE** under `optional_telemetry_retention`; streaming. |
| `cluster_score_shadow_v2.csv` | Optional telemetry | CLI flag | shadow append; audits | append | **CE** under `optional_telemetry_retention`; streaming. |
| `live_journal_rotation_design.csv` | Configuration/policy input | repository canonical file only | maintained offline; rotation manager reader | frozen configuration, not runtime journal | **NE**. Version and checksum in manifest evidence only; never compact. |
| `live_rotation_plan` design/observed-path inputs | Configuration/policy inputs | repository canonical modules/config | frozen R1–R11 | design metadata | **NE**. Read-only evidence; J2 does not extend inventory or enable rotation. |
| future compaction manifest/recovery files | Forensic manifest and recovery state | repository-relative operations root chosen by approved configuration | future compactor/recovery controller | immutable manifests plus append/atomic state records | Not subject to ordinary journal compaction. Retain full chain; bounded streaming verification. |

### 5.1 Common class contracts

Every `CE` journal requires an approved policy containing `policy_id`, `policy_version`, retention unit (`logical rows`, `UTC event range`, or both), threshold parameter, hard maximum source bytes/rows per run, maximum memory bytes, maximum temporary-disk bytes, archive format, permitted schema fingerprints, and dependency/pin rules. With no approved threshold the policy state is `NOT_AUTHORIZED` and zero source mutation is allowed.

Archives use a byte-preserving CSV segment plus a UTF-8 JSON manifest. CSV is selected because it preserves the current logical format; optional compression is not authorized until deterministic codec/version and checksum semantics are approved. Archive naming is:

`backtest/journal/operations/archive/<journal_id>/<generation_sequence>-<generation_id>/<journal_id>.<range_id>.csv`

Staging and recovery state live under:

`backtest/journal/operations/staging/<generation_id>/`

Paths are repository-relative in the manifest and resolved against the explicit repository root. A generation directory without a valid completion marker is incomplete and never authoritative.

After a successful compacted-generation commit, an append journal contains the exact physical header, all pinned records, and the policy-retained active range in original logical order. Authority/state files retain the full validated state unless a journal-specific rule explicitly proves a safe projection. Summary files are rebuilt, not sliced. Snapshot/configuration files are unchanged.

## 6. OPEN-position geometry preservation contract

Source inspection proves `position_closer._lookup_entry_row()` loads `live_observation_entries.csv`, prefers exact `canonical_setup_key` when the OPEN row supplies one, otherwise matches legacy `setup_id`, sorts matches by parsed `timestamp` when present, and selects the latest row. It then requires usable `side`, `sl`, and `tp`; J2 additionally requires `entry` and `planned_rr` because the mandated restart geometry is broader than the present closer access.

Preflight performs the following for every row whose normalized `status == OPEN`:

1. Validate the position-state physical header against an allowed source version without converting it. Require `symbol`, `status`, `opened_ts`, and at least one non-empty identity field.
2. If `canonical_setup_key` is non-empty, it is authoritative. Match only exact canonical keys. A legacy-key match cannot override a failed canonical match.
3. If canonical identity is absent, permit exact `setup_id` fallback only under an explicitly approved legacy schema version. Empty identity blocks compaction.
4. Stream `live_observation_entries.csv` once and build a disk-backed, generation-scoped identity index. Store record byte/logical offsets, identity, timestamp, schema fingerprint, and a record checksum; do not retain whole rows in RAM.
5. Require at least one matching row with the same symbol and parseable finite `entry`, `sl`, `tp`, and `planned_rr`, plus a valid side. Identity collisions across symbol/model/side are ambiguity unless the canonical key and schema contract prove uniqueness.
6. For duplicate matches, reproduce current selection only when the timestamp ordering is total and the latest record is unique and geometry-consistent. Equal/invalid latest timestamps, conflicting geometry for the selected identity, or multiple byte-distinct candidates without a deterministic proof block compaction.
7. Add every selected geometry record checksum to `open_position_pins`; copy the exact logical record into the retained active generation regardless of age/threshold. Do not deduplicate or rewrite it.
8. Validate the compacted active output by re-running the same disk-backed lookup against every OPEN position and comparing the selected record checksum and all required geometry fields with preflight.
9. After commit and before writer resume, repeat the lookup from the committed active generation. Any mismatch enters `RECOVERY_REQUIRED`; the writer remains stopped and the last known-good generation remains available.
10. On restart, recovery must validate the commit marker, active generation, position state, and every OPEN geometry pin before permitting writer resume or future compaction.

Missing, corrupt, ambiguous, duplicate-unresolved, schema-incompatible, or unreachable geometry yields `UNKNOWN_FAIL_CLOSED` before mutation. If discovered after durable staging/commit activity, it yields `RECOVERY_REQUIRED`. No archive-only lookup is permitted for current OPEN positions under J2, because the existing closer does not read archives.

## 7. Retention and eligibility model

Eligibility is evaluated per dependency closure, not per file in isolation. A closure contains the target, its authority/identity inputs, OPEN geometry dependencies, summary consumers, active writer generation, policy, and predecessor manifest. Any `NE`, unknown, or failed member blocks the closure.

Retention values are not justified by J1. The future configuration must supply named values for each `*_retention` parameter and hard bounds for bytes, rows, wall-clock duration, memory, and temporary disk. Conservative default is `NOT_AUTHORIZED`: archive construction may be simulated read-only, but active-file replacement and source reclamation are forbidden. Evidence required before authorization includes measured growth/day, duplicate rates, semantic schema validation, RAM/disk benchmarks, corrupt-tail tests, Windows handle tests, and restart/crash drills.

Selection is stable and deterministic: preserve original logical-row order, classify each row as `PINNED`, `RETAINED`, or `ARCHIVED`, and prove `source_count = retained_count + archived_count` with no dropped or duplicated logical records. If a row lacks the timestamp/identity needed by its policy, it is pinned rather than guessed; if this makes a hard bound impossible, compaction is blocked.

## 8. Manifest and forensic contract

Each immutable generation manifest contains:

- manifest format/version, compaction policy ID/version, journal class;
- journal logical identity, declared relative path, normalized absolute source path hash, active/default classification;
- generation ID, monotonic generation sequence, predecessor manifest checksum, UTC creation timestamp;
- source size, logical row count, exact header capture (encoded bytes and parsed fields), schema fingerprint, source SHA-256;
- retained and archived byte/row counts and deterministic range descriptors;
- SHA-256 and byte count for every staged active/archive output;
- complete OPEN-position pin list with position identity, selected record checksum, logical offset/range, and geometry-field hashes;
- writer process-chain identity, writer generation token, quiescence request/ack tokens and UTC timestamps;
- path-registry checksum, permitted-schema registry version, tool build/version;
- validation results, anomaly counts, duplicate/ambiguity results, memory/temp-disk maxima observed;
- commit state, completion-marker checksum, recovery status, and previous-generation linkage.

The manifest is first written as an uncommitted staging artifact. `completion.json` is written only after validated outputs and manifest are durable and the active-generation pointer/commit record is durable. Checksums cover bytes before compression; if compression is later authorized, both uncompressed content and container checksums are recorded. UTC uses RFC 3339 with `Z`; local time is prohibited. A generation sequence is allocated by a durable compare-and-set registry and may not be reused.

## 9. Deterministic compaction state machine

| State / transition | Inputs and guards | Durable evidence and allowed mutation | Failure, restart, idempotency/recovery |
| --- | --- | --- | --- |
| `DISCOVER → CLASSIFY` | explicit repo root; authenticated active command/config; no path collision | read-only path registry draft | unknown active path → `UNKNOWN_FAIL_CLOSED`; repeatable |
| `CLASSIFY → PREFLIGHT` | every path mapped to one class/policy/dependency closure | durable intent record may be created under new staging generation only | absent policy or class → `ABORTED`; source untouched |
| `PREFLIGHT → QUIESCE_REQUESTED` | exact headers permitted; streaming parse/checksum/count pass; disk/memory capacity; OPEN geometry proven; source fingerprint captured | durable preflight report and intent; no source mutation | any authority/geometry/schema/corruption unknown → `UNKNOWN_FAIL_CLOSED` |
| `QUIESCE_REQUESTED → QUIESCENCE_PROVEN` | unique writer chain/generation; authenticated request token; all relevant writers acknowledge and close handles; source fingerprint stable | durable request/ack records only | timeout, stale ack, extra process, changing source, open-handle unknown → `ABORTED`; restart revalidates token |
| `QUIESCENCE_PROVEN → SNAPSHOT` | acknowledgement still current; process chain unchanged | read-only source snapshot/handle or byte-for-byte staged source copy under generation directory | inability to obtain stable snapshot → `RECOVERY_REQUIRED`; writers remain quiesced until safe abort/resume |
| `SNAPSHOT → VALIDATE_SOURCE` | staged/source bytes match preflight fingerprint | staging writes only | mismatch → `RECOVERY_REQUIRED`; discard uncommitted staging after evidence capture |
| `VALIDATE_SOURCE → BUILD_COMPACTED_GENERATION` | second streaming CSV validation succeeds; pins/index valid | write archive and active candidate under staging only | failure → `RECOVERY_REQUIRED`; source remains authority |
| `BUILD_COMPACTED_GENERATION → VALIDATE_GENERATION` | all logical records classified | staging only | count/hash/order/schema/pin failure → `RECOVERY_REQUIRED` |
| `VALIDATE_GENERATION → WRITE_MANIFEST` | outputs valid; predecessor still current | write/fsync immutable uncommitted manifest | failure → `RECOVERY_REQUIRED` |
| `WRITE_MANIFEST → COMMIT_GENERATION` | writer still quiescent; source and predecessor fingerprints unchanged; commit preconditions valid | atomically publish generation pointer/commit record using a later Windows-validated primitive; never delete source | ambiguous publish → `RECOVERY_REQUIRED`; recovery determines old/new authority from durable marker, never guesses |
| `COMMIT_GENERATION → REOPEN_OR_RESUME` | committed marker and output checksums valid; active path resolves to committed generation | writer-control metadata only; resume is separately authenticated | failure leaves writer stopped and enters `RECOVERY_REQUIRED` |
| `REOPEN_OR_RESUME → POST_COMMIT_VALIDATE` | writer acknowledges new generation token and target; no old-generation writes | append acknowledgement evidence | stale/old-target writer → stop/refuse and `RECOVERY_REQUIRED` |
| `POST_COMMIT_VALIDATE → COMPLETE` | active header/hash baseline valid; OPEN geometry lookup matches; writer target/generation proven | write durable completion marker/status | failure → `RECOVERY_REQUIRED`; no source reclamation |
| `* → ABORTED` | safe failure before commit and no ambiguous mutation | abort record; staging may be quarantined later | source remains authority; resume only after original fingerprint validation |
| `* → RECOVERY_REQUIRED` | durable mutation began or commit/resume state ambiguous | recovery record only | restart runs recovery controller; no normal compaction/writer resume until resolved |
| `* → UNKNOWN_FAIL_CLOSED` | authority, identity, path, schema, geometry, handle, or state cannot be proven | anomaly evidence only | no source mutation; manual evidence/approval required |

Every transition is compare-and-set on `(generation_id, expected_state, predecessor_checksum)`. Replaying a completed transition with identical evidence is a no-op; conflicting evidence fails closed. Rollback means continuing to use a proven old generation before commit. After a valid commit, recovery is forward-only to validate/resume the committed generation; it never silently restores stale authority.

## 10. Windows quiescence and handle-safety protocol

The design requires a future production writer-control protocol; R7 disposable validation is not production proof and frozen R1–R11 is not modified.

1. The writer publishes a cryptographically random process-generation token, process-chain identity, target path registry checksum, and current generation.
2. The compactor proves a single launcher/interpreter chain, not merely three matching command lines. Any independent process targeting a closure blocks compaction.
3. A quiescence request includes request ID, expected writer token, path-set checksum, deadline, and intended generation.
4. The writer reaches a safe cycle boundary, stops new CSV work, flushes/closes every relevant handle, and acknowledges the exact request/path set with a monotonic acknowledgement sequence.
5. The compactor independently verifies source fingerprints remain unchanged and uses Windows handle/process evidence approved by later validation. An acknowledgement alone is insufficient.
6. Stale token, late acknowledgement, PID reuse, path mismatch, timeout, incomplete path coverage, or unknown handle state refuses compaction.
7. After commit, resume names the committed writer generation and path registry. A writer may not reopen the old generation. Old-generation write detection triggers `RECOVERY_REQUIRED`.
8. Crash after acknowledgement but before commit: recovery proves the old commit marker remains authoritative, validates the old source fingerprint, then permits an authenticated resume; uncommitted staging is quarantined.
9. Crash after commit but before resume: recovery validates the new commit marker, outputs, pins, and predecessor link, then resumes only the new generation.
10. The last known-good generation and manifest remain immutable. Delete/rename/replace behavior with open handles is never assumed.

## 11. Crash and restart matrix

| Crash point | Authoritative state | Required recovery |
| --- | --- | --- |
| before quiescence acknowledgement | old active generation | expire request; verify writer/source; no filesystem mutation |
| after acknowledgement, before snapshot | old generation; writer stopped | validate old fingerprint; resume old writer or retry with new request |
| during staged snapshot/build | old generation | quarantine incomplete staging; validate/resume old generation |
| after output validation, before manifest | old generation | retain forensic staging; no publish |
| after manifest, before commit | old generation | predecessor/current pointer proves no commit; quarantine or idempotently retry |
| during commit with ambiguous return | unknown until durable marker inspection | `RECOVERY_REQUIRED`; inspect checksummed pointer/marker and both generations; never infer from filenames/timestamps |
| after commit, before resume | new committed generation | validate manifest/output/pins, then resume new token |
| after resume, before post-validation | new generation, writer state unproven | stop/refuse further compaction; validate target and OPEN geometry |
| after completion marker | new generation | idempotent verification; old generation retained pending separately authorized reclamation |

On process restart, the recovery controller scans intent/recovery records before normal operation. Multiple current markers, broken predecessor links, missing outputs, checksum mismatch, or incomplete OPEN pins produce `UNKNOWN_FAIL_CLOSED`. J2 does not authorize automatic repair.

## 12. Bounded-memory contract

- Parse CSV as logical records with a bounded record buffer. A record exceeding configured `max_record_bytes` blocks the run; the value is mandatory and not defaulted.
- Compute SHA-256, byte counts, logical-row counts, header capture, and structural validation in one or more streaming passes.
- Partition output by streaming copy using deterministic selection metadata; never create an unbounded DataFrame.
- Use external merge sort or an embedded disk-backed key/value index scoped to staging for identity/time lookup and duplicate detection. Memory caches have configured maximum entries/bytes and spill deterministically.
- OPEN-position state is small in current evidence but is not assumed bounded. Stream it and place identity/index data on disk.
- Exact duplicate counts use external sorting by `(identity, record_checksum, logical_offset)`; approximate sketches are insufficient for authority decisions.
- Validate outputs with independent streaming passes; do not trust writer counters alone.
- Enforce configured memory, temporary-disk, output-size, record-size, row-count, and duration ceilings before and during work. Exceeding a ceiling aborts before commit without dropping data.
- The current full-history `_rebuild_pressure_window_summary()` is not invoked by compaction. A later implementation must eliminate or bound that consumer before raw-history removal can be authorized.

## 13. Corruption and unknown-state behavior

No tail is truncated or repaired automatically. On malformed quoting, inconsistent field count, undecodable bytes, checksum mismatch, file change during scan, or semantic schema violation, the source is preserved byte-for-byte, anomaly evidence is written outside the source tree, and the closure fails closed. Header-only files are accepted only by policy. Missing optional files are recorded as `ABSENT_OPTIONAL`; missing authority/dependency files are `UNKNOWN_FAIL_CLOSED`. Duplicate records are counted and preserved unless an approved journal-specific semantic contract proves safe deduplication; J2 approves none.

## 14. Observability requirements for future implementation

Future code must emit bounded operational events for state transition, policy/generation IDs, normalized journal identity, source/output counts and hashes, schema fingerprint, pin counts, duplicate/ambiguity counts, writer/quiescence tokens, memory/temp-disk high-water marks, duration, result, and recovery status. Logs must not contain secrets or complete row payloads. Observability itself must have a bounded retention contract and must never become trading authority.

## 15. Implementation boundaries and validation plan

Later implementation may add only an isolated Journal Operations subsystem, manifest schemas, disk-backed streaming utilities, and an authenticated writer-control boundary after separate approval. It may not change strategy, risk, ranking, WAIT, freshness, exchange integration, executor authority, frozen rotation behavior, or journal schemas.

Before production enablement, validation must include unit/property tests for logical CSV parsing and deterministic partitioning; schema-version fixtures; multi-gigabyte bounded-memory tests; duplicate/ambiguous identity tests; OPEN geometry pin tests for canonical and legacy paths; malformed header/quote/tail tests; path collision/reparse tests; Windows open-handle and independent-process tests; crash injection at every state; restart recovery from every durable boundary; disk-full/permission/checksum faults; idempotent replay; clean-clone documentation verification; and a disposable filesystem certification. Production remains disabled until Peer Review, Lead Approval, later implementation review, certification, and an explicit authority change.

## 16. Unresolved decisions and production-enablement blockers

1. No retention threshold, hard resource ceiling, or production policy registry is approved.
2. Production writer quiescence/control and Windows handle proof do not exist.
3. The active path registry and production process CWD cannot currently be independently proven by the compactor.
4. Terminal lifecycle default authority remains split.
5. Position-state fail-empty overwrite and 12→10 schema contraction remain open defects.
6. Fired/terminal/visibility fail-empty authority behavior remains open.
7. OPEN geometry duplicate/ambiguity rates and semantic schemas are not measured.
8. The closer still loads full `live_observation_entries.csv`; archive-aware reading is not authorized, so pins must remain active.
9. Full-history pressure-summary consumption must be bounded or redesigned in a later authorized phase before its raw source can compact.
10. Atomic/durable Windows commit primitive, directory fsync semantics, disk-full behavior, and recovery controller are unimplemented and uncertified.
11. Archive compression format and deterministic codec are unapproved.
12. Source reclamation/deletion is outside this design and remains unauthorized.

Any one blocker keeps production compaction at `DENY`.

## 17. J2 design conclusion

The design supplies a deterministic fail-closed protocol, full J1 journal classification, explicit OPEN-position geometry pinning, bounded-memory mechanisms, forensic manifests, Windows quiescence requirements, and crash/restart recovery contracts. It deliberately does not claim these contracts are implemented or production-safe today.

Production Mutation: **NONE**  
Trading Behavior Change: **ZERO**  
Production Shell Restarted: **NO**  
Executor Execution: **NONE**  
Production Rotation: **NOT STARTED**  
J3–J6: **NOT STARTED**  
Commit: **NOT PERFORMED**

Verdict: **J2_DESIGN_COMPLETE_READY_FOR_PEER_REVIEW**
