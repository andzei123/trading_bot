# ATS Journal Operations — J3 Directory Reconstruction Design

## 1. Status, scope, and non-goals

- Phase: **J3 — Journal Directory Reconstruction**
- Authorized branch: `pressure_diag_logging`
- Authorized HEAD: `05eed38be2830c0b0cc5b197e7d8dc271d627dd6`
- J2 approval tag: `ATS_JOURNAL_OPERATIONS_J2_APPROVED`
- Deliverable: assessment, architecture, directory design, dependency reconstruction, migration contract, and documentation only
- Production migration authority: **DENY**

This document reconstructs operational path ownership and defines a deterministic target directory architecture and future migration protocol. It does not create the target tree, move/copy/rename/delete runtime data, implement a resolver or adapter, modify `.gitignore`, execute J2 compaction, change production Python, or start J4–J6. R1–R11 remain frozen.

## 2. Evidence and limitations

Evidence reviewed:

- approved J1 safety assessment and runtime metadata;
- approved J2 compaction design;
- technical-debt register through `JO-TD-036`;
- `live_observation_shell.py`, `position_closer.py`, `identity.py`, diagnostic/metric writers;
- CLI defaults and active command-line overrides;
- `live_journal_rotation_design.csv` and frozen rotation support files;
- repository `.gitignore` evidence and extracted directory structure.

The workspace is an extracted evidence snapshot, not a working clone at `05eed38`. Git commands cannot independently verify branch, HEAD, tracked status, or exact repository diff here. The supplied baseline is accepted for design; repository integration must verify the final documents against HEAD before checkpointing. Production process CWD remains not proven. No large runtime file was scanned for J3.

## 3. Current directory map and findings

Current operational objects are distributed across:

```text
backtest/journal/
├── live_journal_rotation_design.csv        # tracked canonical configuration
├── live_rotation_plan.csv                  # generated plan
├── fired_setups.csv                        # default identity authority
├── terminal_lifecycle_registry.csv         # shell default terminal authority
├── visible_ts_cache.csv                    # hard-coded restart cache
├── exports_live/
│   ├── position_state.csv
│   ├── live_observation_entries.csv
│   ├── flow_log.csv
│   ├── candidate_pressure.csv
│   ├── raw_candidate_lifecycle_diag.csv
│   ├── pressure_window_summary.csv
│   ├── sniper_candidate_diag.csv
│   ├── sniper_candidate_summary.csv
│   ├── terminal_lifecycle_registry.csv      # closer-derived default path
│   └── candles_debug/
│       ├── {symbol}_raw_live_candles.csv
│       └── {symbol}_closed_live_candles.csv
└── SHADOW_R11_REGRESSION_20260713_165605/  # active CLI-overridden run root
    └── 13 configured targets; 12 existed at J1 collection
```

Configuration, authority state, append telemetry, summaries, generated plans, debug snapshots, simulations, and historical copies share the journal subtree. Default files can coexist with an active custom run, and canonical-looking duplicates do not establish authority. The active J1 command redirected 13 CSV arguments into the shadow directory; `visible_ts_cache.csv` and candle snapshots remained on hard-coded/default paths. Thus one live process can already span more than one physical root.

The broad `*.csv` ignore rule and overlapping journal-specific ignore rules correctly suppress much runtime output but also suppress canonical CSV configuration unless explicit exceptions are maintained. `live_journal_rotation_design.csv` requires tracked-configuration treatment; `live_rotation_plan.csv` must remain generated/ignored. Clean-clone reconstruction cannot rely on ignored empty runtime directories or live files.

## 4. Current path-dependency inventory

Classification: `C` canonical/default, `A` active override, `H` historical/default not active, `D` dynamic, `F` future. Tracking status is based on source/ignore evidence and must be confirmed in the Git clone. `DENY` means migration cannot occur until the named compatibility and safety mechanism exists.

| Logical object | Discovered path/pattern; class; path source | Writer; reader; configurability | Integrity/restart/schema/ambiguity | Future class; migration eligibility and compatibility |
| --- | --- | --- | --- | --- |
| `position_state` | `backtest/journal/exports_live/position_state.csv` C/H; active shadow path A; CLI default/argument | `_mark_position_open`, `close_symbol_if_hit`; shell gates/closer; CLI | ignored/generated; CRITICAL authority; 12→10 schema contraction; duplicate locations | `runtime/active/<gen>/authority/position_state.csv`; **DENY** until resolver, coherent authority bundle, exact schema validation, quiescence, and OPEN closure proof |
| `fired_setups` | `backtest/journal/fired_setups.csv` C/H; active shadow A; CLI | shell append; canonical identity loader; CLI | ignored/generated; CRITICAL idempotency; canonical/structural/legacy schemas; duplicate roots | `.../authority/fired_setups.csv`; **DENY** until atomic authority-bundle cutover and loader compatibility |
| shell terminal registry | `backtest/journal/terminal_lifecycle_registry.csv` C/H; active shadow A; CLI | identity append; shell loader; CLI | CRITICAL authority; default split; default absent in J1 metadata | `.../authority/terminal_lifecycle_registry.csv`; **DENY** until one physical authority is proven |
| closer terminal registry | `position_state.parent/terminal_lifecycle_registry.csv`; default exports C/H; active shadow A | closer append; no unified reader; derived, not independent CLI | CRITICAL split authority; can collide with shell path only under co-location | same single authority path; **DENY** until shell and closer resolution are identical by contract |
| `visible_ts_cache` | `backtest/journal/visible_ts_cache.csv` C/A; hard-coded constant | shell overwrite/load; not CLI | ignored/generated; CRITICAL continuity; fail-empty; outside custom active root | `.../authority/visible_ts_cache.csv`; **DENY** until fixed reader/writer use resolver and atomic bundle |
| `live_observation_entries` | exports default C/H; active shadow A; CLI | shell append; closer geometry lookup/audits | IMPORTANT and OPEN dependency; legacy/default versus active schema divergence | `.../position_dependencies/live_observation_entries.csv`; **DENY** until OPEN pins and same-generation lookup proven |
| `flow_log` | exports default C/H; active shadow A; CLI | shell append; audits | ignored/generated; append telemetry; active 9 MB/36,624 rows at J1 | `.../telemetry/flow_log.csv`; eligible only in future offline bundle after resolver/quiescence |
| `raw_candidate_lifecycle_diag` | exports C/H; active shadow A; CLI | shell append; pressure-summary/audits | unbounded; full-history live consumer | `.../telemetry/raw_candidate_lifecycle_diag.csv`; **DENY** until reader contract is generation-aware/bounded |
| `entry_model_pre_admission` | optional CLI path | pipeline diagnostic; audits | absent unless enabled; append; exact active path required | `.../telemetry/optional/`; migrate only if enabled and discovered |
| `pressure_window_summary` | exports C/H; active shadow A; CLI | full rebuild; audits | derived overwrite; stale-on-failure; schema from source | `.../summaries/pressure_window_summary.csv`; build in same target generation after source validation |
| `sniper_candidate_diag` | exports C/H; active shadow A; CLI | shell append; summary/audits | unbounded; restart counter semantics | `.../telemetry/sniper_candidate_diag.csv`; conditional future migration |
| `sniper_candidate_summary` | exports C/H; active shadow A; CLI | initializer/summary; audits | active header-only valid; durable update semantics unresolved | `.../summaries/sniper_candidate_summary.csv`; preserve header-only; **DENY** until semantics proven |
| `opportunity_manager_snapshot` | optional CLI; active shadow A | shell append; audits | optional append, duplicates allowed | `.../telemetry/optional/`; conditional if present; no synthesis |
| `authority_waterfall` | optional CLI; active shadow target absent | shell append; audits | missing optional target is valid evidence, not an empty file | `.../telemetry/optional/`; record `ABSENT_OPTIONAL`; do not create during migration |
| `parity_filter_diagnostics` | optional CLI | shell append; audits | optional/low authority | `.../telemetry/optional/`; conditional |
| `tdp_stale_shadow_oos` | optional CLI | shell append; audits | restart-local counters | `.../telemetry/optional/`; conditional |
| `structural_ts_shadow_oos` | optional CLI | shell append; audits | optional append | `.../telemetry/optional/`; conditional |
| `candidate_pressure` | exports C/H; active shadow A; CLI | pipeline append; audits | historical default 200 MB/4.4M rows; errors swallowed | `.../telemetry/candidate_pressure.csv`; future streaming migration only |
| `symbol_performance` | exports/configured metrics path | metric writer; dashboards | enablement not proven; derived summary | `.../summaries/`; **DENY** until active ownership/rebuild semantics proven |
| `equity_curve` | exports/configured metrics path | metric writer; dashboard | enablement not proven; derived/growing | `.../summaries/`; **DENY** until active ownership/rebuild semantics proven |
| `{symbol}_raw_live_candles` | `exports_live/candles_debug/{symbol}_raw_live_candles.csv` D/A; hard-coded | `run_symbol_once`; no production file reader found | ignored/generated bounded overwrite; old BNB files may be historical; active symbol set required | `.../snapshots/candles/{symbol}.raw.csv`; **DENY** until hard-coded writer uses resolver; no timestamp-based symbol selection |
| `{symbol}_closed_live_candles` | matching closed pattern D/A; hard-coded | `run_symbol_once`; current cycle uses memory | same bounded overwrite and stale-symbol ambiguity | `.../snapshots/candles/{symbol}.closed.csv`; **DENY** until resolver compatibility |
| `live_rotation_plan` | `backtest/journal/live_rotation_plan.csv` C/H; active shadow A; CLI | frozen R4 writer; audits | generated/ignored; unbounded; not canonical policy | `.../plans/live_rotation_plan.csv`; later migration must not enable rotation |
| `pre_visible_entry_exposure` | optional CLI | shell append; audits | optional forensic telemetry | `.../telemetry/optional/`; conditional |
| `tdp_visible_assignment_trace` | optional CLI | pipeline append; audits | optional forensic trace | `.../telemetry/optional/`; conditional |
| `tdp_identity_resurfacing_trace` | optional CLI | pipeline append; audits | optional identity trace, not authority | `.../telemetry/optional/`; conditional |
| `tdp_disappearance_trace` | optional CLI | shell/pipeline append; audits | optional | `.../telemetry/optional/`; conditional |
| `tdp_true_birth_trace` | optional CLI | shell/pipeline append; audits | optional | `.../telemetry/optional/`; conditional |
| `cluster_score_shadow_v2` | optional CLI | shadow writer; audits | optional | `.../telemetry/optional/`; conditional |
| frozen 19-object rotation design | `backtest/journal/live_journal_rotation_design.csv` C; constant/Git | offline maintenance; rotation manager | canonical tracked configuration; broad CSV ignore conflict | `journal_operations/config/rotation/`; tracked; migration only as reviewed Git change, never runtime migration |
| frozen rotation source/policy modules | `backtest/journal/live_*rotation*.py` C | source; frozen readers | tracked source, not operational data | remain source tree; no J3 movement |
| J2 manifests | future `backtest/journal/operations/...` F | future compactor/recovery | immutable forensic authority | `journal_operations/runtime/manifests/`; migration-generation evidence; full-chain retention |
| J2 recovery/quiescence state | future operations root F | future controller/writer | control authority; not implemented | `journal_operations/runtime/control/`; part of atomic authority bundle |
| active shadow run root | `backtest/journal/SHADOW_R11_REGRESSION_20260713_165605/` A | current CLI-targeted writers/readers | mixed authority/telemetry/plan; authoritative for 13 overrides at collection, not for hard-coded objects | source generation only; never selected by name/recency; future migration requires process/path proof |
| `exports_live` | `backtest/journal/exports_live/` C/H+A for hard-coded files | multiple writers/readers | mixed default, hard-coded active, historical, snapshots | source namespace only; cannot be wholesale declared active or historical |
| archives/backups/manual copies | `exports_old`, `exports_history`, `__OLD`, downloads/backups when found | manual/tools | ignored/historical; provenance variable | `legacy/reference/` or external backup custody; never authority; migration requires manifest or remains untouched |
| audit/download exports | local audit bundles, downloaded CSVs, reports | auditors/tools | ignored/generated; may duplicate canonical-looking names | `journal_operations/audit_exports/<audit_id>/`; read-only evidence, never runtime authority |
| simulations/backtests | run-specific output directories | simulators/backtests | generated; must not share active root | `journal_operations/simulations/<run_id>/`; never production authority |
| disposable tests | temp/validation fixtures | tests only | generated/disposable | `journal_operations/tests/disposable/<run_id>/`; containment and deletion limited to test root |
| quarantine | none implemented F | future migration/recovery controller | immutable failed-generation evidence | `journal_operations/runtime/quarantine/<migration_id>/`; never active authority |

The 19 frozen R1 objects are individually present in the table: the five state/replay objects, nine diagnostic/shadow objects, and five summaries/telemetry objects. The additional J1/J2 objects and directory-level artifacts are also covered.

## 5. Duplicate-authority analysis

1. Default and active CLI paths coexist. At J1 collection the active custom directory superseded default paths only for explicitly overridden arguments; it did not supersede hard-coded `visible_ts_cache.csv` or candle snapshots.
2. Terminal lifecycle authority has two default derivations. Under the active run, co-locating `position_state.csv` and the shell registry in one directory happened to make both writers converge, but default architecture still diverges.
3. `position_state.csv`, `fired_setups.csv`, and `live_observation_entries.csv` exist in default/historical and active directories with different physical headers. Parser success does not establish cross-version compatibility.
4. Stale BNB candle snapshots may coexist with current BTC/ETH/XRP/SOL snapshots. Filename existence or newest timestamp cannot identify the active symbol set.
5. Manual copies, Downloads, backups, audit bundles, and simulation outputs can carry canonical-looking filenames. They are non-authoritative without a committed generation manifest and active pointer.
6. A future source and target may both exist during migration. Neither directory name nor modification time is authoritative; only a valid predecessor-linked commit marker can select a generation.

No two locations may be writable authorities. Detection of multiple writer targets, duplicate generation markers, path aliases, or mixed dependency generations results in `UNKNOWN_FAIL_CLOSED`.

## 6. Proposed target directory tree

This tree is a design only and is not created by J3:

```text
journal_operations/
├── config/                               # tracked canonical policy
│   ├── schemas/
│   ├── path_registry/
│   ├── retention/
│   └── rotation/                         # frozen design reference
├── contracts/                            # tracked directory/generation contracts
├── runtime/                              # ignored operational root
│   ├── active/
│   │   └── <generation_id>/
│   │       ├── authority/
│   │       ├── position_dependencies/
│   │       ├── telemetry/
│   │       │   └── optional/
│   │       ├── summaries/
│   │       ├── snapshots/candles/
│   │       └── plans/
│   ├── control/                          # generation pointer/quiescence/recovery
│   ├── manifests/                        # immutable forensic manifests
│   ├── archive/<journal_id>/<generation_id>/
│   ├── staging/<migration_or_compaction_id>/
│   └── quarantine/<operation_id>/
├── audit_exports/<audit_id>/             # ignored local evidence exports
├── simulations/<run_id>/                 # ignored non-production output
├── tests/disposable/<run_id>/             # ignored, test-contained output
└── legacy/reference/                     # non-authoritative curated references
```

`runtime/active/<generation_id>` is immutable after its authority files cease being written; the currently active generation is mutable only through its declared writers. The generation pointer is control authority and cannot be inferred from directory ordering.

## 7. Directory ownership matrix

| Directory | Owner; allowed writers/readers | Classes; tracking/retention/mutability | Restart, naming, schema/generation, containment |
| --- | --- | --- | --- |
| `config/` | repository maintainers; runtime read-only | configuration/policies/schemas; tracked; Git review; immutable at runtime | no live authority; semantic versions and checksums; under repo root |
| `contracts/` | architects/maintainers; tools read-only | documentation/contracts; tracked | clean-clone reconstructable; no generation data |
| `runtime/active/<gen>/authority/` | declared production authority writers/readers only | state/identity/cache; ignored; retain per authority contract; mutable only while active | restart-critical; exact schema version; one active generation |
| `.../position_dependencies/` | observation writer and closer reader | OPEN geometry dependency; ignored; pin-aware | same generation as authority bundle; mismatch denied |
| `.../telemetry/` | declared telemetry writers; audits/readers | append-only; ignored; J2 retention authority | `<logical_name>.csv`; manifest schema fingerprint |
| `.../summaries/` | declared rebuild writers; audits | reconstructable summaries; ignored | same generation/source linkage; header-only allowed by policy |
| `.../snapshots/` | declared snapshot writers; diagnostics | bounded overwrite snapshots/cache; ignored | symbol and kind in canonical lower/upper convention fixed by registry |
| `.../plans/` | frozen planning writer; audits | generated plans; ignored | not configuration or authority; never enables rotation |
| `runtime/control/` | future recovery/generation controller and authenticated writer | pointer, intent, quiescence, recovery; ignored but durable | restart authority; atomic records; monotonic generation sequence |
| `runtime/manifests/` | future migration/compaction controller; read-only consumers | immutable forensic manifests; ignored operationally, externally backed up per policy | exact checksum chain; UTC; no overwrite |
| `runtime/archive/` | future approved compactor only | immutable archives; ignored; J2 policy | journal/generation naming; repository containment |
| `runtime/staging/` | one authenticated operation | incomplete candidates; ignored; bounded cleanup | never authority; operation ID; reparse-free |
| `runtime/quarantine/` | recovery controller; read-only forensic access | failed/ambiguous artifacts; ignored; explicit disposition | never authority; immutable evidence |
| `audit_exports/` | auditors/tools | exports/bundles; ignored/local retention | never production read path; audit ID and manifest |
| `simulations/` | simulation/backtest tools | non-production data; ignored | run ID; marker states `NON_PRODUCTION` |
| `tests/disposable/` | test harness only | deterministic fixtures may be tracked elsewhere; outputs ignored | hard containment; no production paths |
| `legacy/reference/` | repository maintainers or external custody | curated read-only historical references; tracked only if small/licensed/reviewed, otherwise ignored | explicit `NON_AUTHORITY` marker and provenance |

All operational roots reject symlinks, junctions, mount/reparse substitution, case-fold collisions, and escape outside their approved normalized absolute root.

## 8. Authority co-location and OPEN-position dependency rules

The **Authority Dependency Group (ADG)** is one atomic logical migration set:

```text
position_state.csv
fired_setups.csv
terminal_lifecycle_registry.csv (all discovered logical authorities)
visible_ts_cache.csv
live_observation_entries.csv
active-generation pointer
migration/compaction manifest
writer-quiescence and recovery evidence
```

No member may come from a different source or target generation. Optional telemetry is not part of the ADG unless source analysis proves a restart/identity dependency. `live_observation_entries.csv` is mandatory because the closer resolves OPEN geometry from it.

For every OPEN position, future migration streams `position_state.csv`, computes identity closure using exact non-empty `canonical_setup_key` first and approved legacy `setup_id` fallback only when canonical identity is absent, then uses a disk-backed index of `live_observation_entries.csv`. It requires one deterministic latest usable geometry record consistent with current reader semantics and containing schema-version-valid entry, SL, TP, side, and planned RR representation. Missing identity, mixed symbol, invalid timestamp ordering, conflicting geometry, duplicate ambiguity, incompatible schema, or absent record denies migration.

The selected record checksum and logical offset are pinned in the migration manifest. Target validation repeats the join and compares the exact record/geometry checksum. Both files are built in a new inactive generation; neither is moved individually. Commit publishes the whole ADG through one generation pointer only after all members and pins validate. Rollback before commit retains the source ADG. After commit, recovery is forward-only to the validated target; the source becomes immutable predecessor evidence and cannot remain writable.

Because current readers are not generation-aware, **physical migration is denied** until they all resolve the same explicit active generation. J3 does not copy geometry into a new authority file and does not redesign the position schema.

## 9. Deterministic path-resolution contract

Future resolution inputs are explicit absolute `repository_root`, configured `runtime_root`, committed `active_generation_id`, and policy-defined `archive_root`, `staging_root`, and `audit_root`. Process CWD is never an implicit base.

Resolution procedure:

1. Canonicalize the repository root using Windows final-path semantics and require it to exist.
2. Resolve tracked configuration from repository root. Resolve operational logical identities from the authenticated active-generation registry, not from filename defaults.
3. Relative CLI paths must declare a base enum (`REPOSITORY_ROOT`, `RUNTIME_ROOT`, or `ACTIVE_GENERATION_ROOT`). Missing base is invalid; legacy relative CLI behavior requires an explicit compatibility mode and cannot be production-approved until verified.
4. Normalize separators, drive/UNC form, Unicode, trailing dots/spaces, and case-folded comparison key. Record normalized absolute and repository-relative forms.
5. Reject `..` escape, alternate data streams, device names, wildcard components, path collision/alias, and source-target overlap.
6. Resolve every ancestor and reject symlink, junction, mount point, or other reparse point unless a future specific policy authorizes its exact identity. J3 authorizes none.
7. Deny UNC/network paths by default. Separate authorization must define availability, locking, durability, and identity rules.
8. Require all operational paths inside their approved roots. A missing/ambiguous root fails closed.
9. Select the active generation only through a checksummed, predecessor-linked commit record. Never use latest timestamp, lexical directory order, or filename.
10. Record the resolver version, registry checksum, root identities, and results in every manifest.

## 10. Git tracking and artifact policy

Future tracked content:

- source code;
- canonical policies and path registries;
- schema definitions/fingerprints;
- architecture/documentation;
- small deterministic test fixtures explicitly approved;
- frozen `live_journal_rotation_design.csv` or its reviewed future canonical location.

Future ignored content:

- active runtime state and CSVs;
- generated plans and summaries;
- archives, staging, recovery runtime records, and quarantine payloads;
- large telemetry;
- local audit bundles/downloads/manual copies;
- simulations/backtests and disposable output.

The broad `*.csv` rule is too coarse to express integrity ownership by itself. A future `.gitignore` change should keep broad runtime protection while adding narrow, explicit exceptions for named canonical configuration/schema fixtures under tracked-only roots, for example an exception scoped to `journal_operations/config/**/*.csv`, followed by tests proving runtime trees remain ignored. J3 does not modify `.gitignore`.

A clean clone must recreate source, documents, schema contracts, tracked configuration, and empty-directory creation logic. It must not ship active generation pointers, live state, or runtime CSVs and must not impersonate production authority. Runtime initialization must fail closed until a separately provisioned/validated authority generation exists.

## 11. Compatibility strategy

Options assessed:

- **Symlink/junction compatibility:** rejected. It introduces reparse ambiguity and Windows handle/case risks.
- **Dual-write:** rejected. It creates two writable authorities and non-atomic divergence.
- **Uncontrolled dual-read:** rejected. Reader ordering can mix generations.
- **Read-only alias period:** acceptable only for offline verification tools, never production authority selection.
- **In-place path moves with current fixed readers:** rejected. A crash can split ADG files and hard-coded candle/cache paths remain wrong.
- **Explicit runtime root plus generation-aware resolver and offline cutover:** selected future architecture.

Selected recommendation: implement one shared generation-aware path resolver behind all writers/readers, preserve existing CLI arguments through a validated compatibility adapter that converts them into explicit registry entries, then perform migration during a separately authorized controlled offline window. Source is single-write before commit; target is inactive build-only; after atomic ADG commit, target becomes the sole writable authority. Fixed candle paths, closer-derived terminal path, identity/restart loaders, frozen plan output, and optional/header-only behavior must all be compatibility-tested. No production cutover occurs in J3.

## 12. Directory migration state machine

| Transition/state | Inputs and guards | Future permitted action and durable evidence | Failure, crash/restart, idempotency and recovery |
| --- | --- | --- | --- |
| `DISCOVER → CLASSIFY` | explicit roots, source command/config/source evidence | read-only registry draft | ambiguous root/path → `UNKNOWN_FAIL_CLOSED` |
| `CLASSIFY → MAP_DEPENDENCIES` | every object has class/authority status | durable intent may be created only in future staging | unknown object/owner → `MIGRATION_DENIED` |
| `MAP_DEPENDENCIES → VALIDATE_SOURCE` | complete ADG and reader/writer graph | read-only dependency manifest | missing ADG member → `MIGRATION_DENIED` |
| `VALIDATE_SOURCE → DETECT_DUPLICATES` | streaming schema/count/hash; OPEN closure proven | validation evidence only | parse/schema/geometry failure → `UNKNOWN_FAIL_CLOSED` |
| `DETECT_DUPLICATES → DETECT_ACTIVE_WRITERS` | all canonical-looking copies inventoried | duplicate report | unresolved authority/mixed generations → `MIGRATION_DENIED` |
| `DETECT_ACTIVE_WRITERS → PREFLIGHT` | process chains/targets identified | process evidence | unknown/multiple writer → `MIGRATION_DENIED` |
| `PREFLIGHT → QUIESCE_REQUESTED` | capacity, policy, resolver compatibility, target absent/owned, all checks pass | durable request only | any blocker → `MIGRATION_DENIED` |
| `QUIESCE_REQUESTED → QUIESCENCE_PROVEN` | authenticated generation/path token; safe boundary; all handles closed | request/ack/handle evidence | stale ack, timeout, PID reuse, reader/writer handle unknown → `ABORTED` |
| `QUIESCENCE_PROVEN → SNAPSHOT_SOURCE` | source fingerprints stable | immutable staged snapshot/copy; source unchanged | change/partial copy → `RECOVERY_REQUIRED` |
| `SNAPSHOT_SOURCE → BUILD_TARGET_GENERATION` | snapshot checksum validated; target new and contained | create inactive target directories/files only | target exists/collision/disk failure → `RECOVERY_REQUIRED` |
| `BUILD_TARGET_GENERATION → VALIDATE_TARGET` | every ADG and selected non-ADG object copied/rebuilt | target writes only; streaming verification | mismatch/missing pin/mixed schema → `RECOVERY_REQUIRED` |
| `VALIDATE_TARGET → WRITE_MIGRATION_MANIFEST` | counts/hashes/headers/dependencies match | write/fsync uncommitted manifest | failure → `RECOVERY_REQUIRED` |
| `WRITE_MIGRATION_MANIFEST → COMMIT_DIRECTORY_GENERATION` | source/predecessor stable; writer quiescent; target valid | atomically publish generation commit/pointer; never delete source | ambiguous result → `RECOVERY_REQUIRED` |
| `COMMIT_DIRECTORY_GENERATION → REOPEN_OR_RESUME` | committed target/pointer validate | authenticated resolver/writer resume only | reopen old path or mismatch → `RECOVERY_REQUIRED` |
| `REOPEN_OR_RESUME → POST_COMMIT_VALIDATE` | all readers/writers report same root/gen | operational acknowledgements | split root/generation → stop/fail closed |
| `POST_COMMIT_VALIDATE → COMPLETE` | OPEN joins, authorities, paths and write target revalidated | durable completion marker | failure → `RECOVERY_REQUIRED` |
| `* → MIGRATION_DENIED` | policy or safety precondition absent | denial record only | no production mutation; new evidence required |
| `* → ABORTED` | safe pre-commit refusal with proven old authority | abort evidence; source unchanged | retry uses new operation ID/token |
| `* → RECOVERY_REQUIRED` | build/commit/resume mutation began or authority ambiguous | recovery record only | inspect durable markers; no automatic guess |
| `* → UNKNOWN_FAIL_CLOSED` | root/path/generation/geometry/state cannot be proven | anomaly evidence only | manual evidence and approval required |

Every transition is compare-and-set on migration ID, expected state, source generation, predecessor checksum, and resolver/path-registry version. Repeating an already completed transition with identical inputs is a no-op. Before commit, rollback means retain/resume the proven source generation. After valid commit, forward recovery validates/resumes the target; it never silently restores a stale source.

## 13. Windows migration safety matrix

| Condition | Mandatory result |
| --- | --- |
| writer or reader holds relevant file | deny until authenticated quiescence and independent handle proof |
| rename/replace/delete refused | no retry loop that changes authority; preserve source; abort/recovery evidence |
| stale acknowledgement or PID reuse | reject token; require new process-generation proof |
| launcher/interpreter chain | classify as one chain only with parent evidence; detect additional independent chains by target registry |
| antivirus/indexer handle | unknown handle blocks commit; timeout safely |
| permission failure | preserve source; quarantine incomplete target evidence |
| partial directory/file copy | target remains inactive; checksum and completion marker absent |
| disk full/free-space change | stop before commit; keep source; record exact failure |
| checksum/header/row mismatch | `RECOVERY_REQUIRED`; never publish |
| target already exists | deny unless it is the exact idempotent operation with matching manifest |
| source changes during copy | invalidate snapshot and deny/recover |
| writer reopens old path | stop/refuse operation; old/new cannot both write |
| readers resolve different roots | `UNKNOWN_FAIL_CLOSED` before resume |
| restart with both roots | choose only checksummed committed pointer; otherwise fail closed |
| junction/reparse substitution | reject path and quarantine operation evidence |
| case-only rename/collision | do not use case-only migration; require distinct non-colliding target and normalized-key checks |

Disposable R7 evidence is not production migration proof.

## 14. Crash and restart matrix

| Restart point | Authority and deterministic action |
| --- | --- |
| before migration/quiescence | source generation remains authority; rediscover and validate |
| during snapshot/target build | source remains authority; incomplete target is staging only and quarantined after evidence capture |
| before commit | source remains authority; manifest cannot be mistaken for completion |
| during commit | inspect checksummed commit record, predecessor link, and target manifest; ambiguity → `UNKNOWN_FAIL_CLOSED` |
| after commit before resume | target is authority only if commit validates; validate ADG and OPEN pins before resume |
| source and target both present | presence/timestamp irrelevant; committed pointer alone selects authority |
| incomplete manifest | cannot be authority; recovery required |
| checksum mismatch | stop; preserve both; unknown fail closed |
| mixed authority generations | stop all migration/resume; unknown fail closed |
| missing OPEN geometry | migration denied or recovery required; never resume target |
| unknown active root | unknown fail closed; no default-path fallback |

An old source is not deleted in J3 or initial future cutover. It becomes immutable predecessor evidence after target commit and requires separate retention/reclamation authority.

## 15. Forensic migration manifest

The immutable manifest records migration ID, design/policy/resolver/tool versions, repository commit, source/target roots and normalized identities, every logical object and path, source/target generation IDs, predecessor checksum, exact header bytes/fields, schema fingerprints, file byte sizes, logical row counts, SHA-256 checksums, source last-write/fingerprint evidence, dependency groups, every OPEN-position identity and geometry-record pin/checksum, process chain and writer-generation evidence, quiescence tokens/times/path set, copy/build/validation/commit state, anomalies/duplicates, rollback or forward-recovery decision, resource high-water marks, completion-marker checksum, and RFC 3339 UTC timestamps.

The uncommitted manifest and target live only in staging. The completion marker is written after generation commit and post-commit validation. Missing or invalid completion evidence cannot appear complete.

## 16. Bounded-resource contract

Future migration uses streaming logical CSV parsing, streaming SHA-256/counting, bounded record buffers, disk-backed identity/duplicate indexes, and external sorting. It requires configured memory, temporary-disk, record-size, source-size, duration, and cancellation ceilings. Preflight proves target/staging free space using source size plus safety margin parameter; no arbitrary value is approved in J3. Resource exhaustion preserves source authority and forensic failure evidence. Cleanup may remove only proven uncommitted staging under the exact operation root after evidence retention approval; it never follows reparse points or deletes source/target generations.

## 17. Validation strategy

Before future production approval:

- clean-clone reconstruction and Git ignore/exception tests;
- complete default/active/optional path-registry fixtures;
- CWD-independence, case-fold, traversal, UNC, device-name, symlink/junction/reparse tests;
- canonical and legacy OPEN-geometry joins, duplicates/conflicts, mixed-generation rejection;
- dual terminal-registry and hard-coded candle/cache compatibility tests;
- header-only and missing-optional behavior;
- multi-gigabyte bounded-memory/cancellation/disk-full tests;
- independent writer-chain, stale ack, PID reuse, reader handle, antivirus/indexer simulations on Windows;
- crash injection at every migration transition and restart matrix state;
- source/target/predecessor checksum and idempotent retry tests;
- shadow resolver comparison followed by disposable offline migration drills;
- Peer Review, Lead Approval, implementation review, certification, and explicit migration authority.

Production runtime testing, restart, and migration are prohibited in J3.

## 18. Unresolved decisions and blockers

1. No production generation-aware resolver or explicit runtime root exists.
2. Current fixed-path readers/writers, cache, candles, and closer terminal derivation are not compatible with target generations.
3. Production process CWD and authoritative active-root control plane are not proven.
4. Writer quiescence and Windows handle evidence are not implemented/certified.
5. Terminal lifecycle default authority remains split.
6. Position fail-empty behavior and schema contraction remain open.
7. Semantic schema compatibility and OPEN duplicate/ambiguity rates remain unmeasured.
8. Broad CSV ignore exceptions and clean-clone directory initialization are not implemented/tested.
9. J2 generation publication/recovery is approved design only, not implementation.
10. Retention, backup, old-source disposition, free-space margin, and resource ceilings remain unauthorized.
11. Active shadow and default/historical objects cannot be selected by timestamp or filename.
12. Any unknown or mixed generation denies migration.

## 19. Future implementation boundaries

A later authorized phase may implement the isolated path registry/resolver, schemas, manifest/recovery controller, compatibility adapter, and disposable migration tooling. Production integration requires a separately reviewed minimal change to every affected reader/writer and an offline cutover plan. It may not change trading logic, identity semantics, OPEN geometry, R1–R11 authority, J2 policy, executor reachability, or risk/ranking/WAIT/freshness behavior. It may not introduce dual-write authority or reparse aliases.

## 20. J3 conclusion

The target architecture supplies one explicit generation-scoped location for each operational class and keeps tracked configuration separate from ignored runtime authority. The ADG prevents position, identity, terminal, visibility, and geometry files from crossing generations. The selected future mechanism is a shared generation-aware resolver plus controlled offline single-writer cutover. Physical migration remains denied until all blockers are implemented and certified.

Production Mutation: **NONE**  
Trading Behavior Change: **ZERO**  
Production Shell Restarted: **NO**  
Runtime CSV Mutation: **NONE**  
Executor Execution: **NONE**  
Production Rotation: **NOT STARTED**  
Production Compaction: **NOT IMPLEMENTED**  
Production Migration: **DENIED**  
J4–J6: **NOT STARTED**  
Commit: **NOT PERFORMED**

Verdict: **J3_DESIGN_COMPLETE_READY_FOR_PEER_REVIEW**
