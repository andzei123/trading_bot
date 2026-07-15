# ATS Journal Operations — J1 Safety Assessment

## Status and scope

- Phase: **J1 — IMPORTANT Journal Safety Assessment**
- Canonical branch: `pressure_diag_logging`
- Canonical HEAD: `6cedff6`
- Canonical tag: `ATS_JOURNAL_ROTATION_R1_R11_REPOSITORY_REPAIRED`
- Assessment date: 2026-07-15
- Method: static, read-only inspection of `ATS_J1_SOURCE.zip` regenerated from HEAD `6cedff6`, plus supplied branch/HEAD/status evidence and the untracked journal-source bundle.
- Production mutation: **NONE**
- Trading behavior change: **ZERO**
- Executable code changed: **NONE**

This report assesses current behavior. It does not authorize rotation, compaction, repair, retention, schema changes, or J2 work.

## Repository baseline and evidence limits

The supplied UTF-16 evidence decodes to branch `pressure_diag_logging` and HEAD `6cedff6`. The supplied Git status is not clean: it contains a large pre-existing untracked working tree. Those items predate J1 and were preserved. The source ZIP comment contains full commit `6cedff69d741c7a7fe6e96b34eafecaf21f97a03`, consistent with the declared short HEAD.

The assessment environment is an extracted source snapshot, not a working clone, so live `git diff` cannot be generated here. Required runtime metadata was not supplied in this turn. Consequently, current absolute paths, existence, byte sizes, row counts, live headers, last-write timestamps, parse status, working directory, active command, and enabled optional CSV flags are **not proven from supplied evidence**. These metadata are mandatory before final J1 closure; full CSV contents are neither required nor requested.

## Inspection method

The source snapshot contained 555 files, including 186 Python files. Inspection covered:

1. all `.csv` literals and `Path` defaults in tracked Python, Markdown, and rotation design files;
2. the canonical production writer `backtest/journal/live_observation_shell.py`;
3. pipeline diagnostics in `backtest/live_pipeline/pipeline_core.py` and `backtest/live/pipeline_helpers/diagnostics.py`;
4. state closing in `backtest/journal/position_closer.py` and lifecycle helpers;
5. all 19 entries in `backtest/journal/live_journal_rotation_design.csv`;
6. observe-only planning in `backtest/journal/live_rotation_plan_writer.py`;
7. creation, parent-directory, header, append/overwrite, missing/empty/malformed/corrupt handling, restart behavior, and whole-file reads.

Historical backtest outputs, candle inputs, reports, backups, smoke fixtures, and analyst-only CSVs were searched, but are not classified as active production journals unless the current production shell or its direct pipeline references them.

## Canonical production inventory

Abbreviations: **A** = append; **O** = overwrite/rebuild; **M** = mixed. Unless noted otherwise, relative defaults resolve from the process current working directory. “Fail-empty” means a missing, unreadable, empty, or structurally invalid file may be treated as no state/data rather than raising a hard failure.

| CSV | Default repository-relative path | Canonical writer / call site | Known production reader | Create / schema / mode | Restart and fault behavior | Growth / RAM | Importance |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `position_state.csv` | `backtest/journal/exports_live/position_state.csv` | Writer A: `live_observation_shell._mark_position_open`; Writer B: `position_closer.close_symbol_if_hit` | shell position gates; closer | Writer A reads existing state, substitutes an empty frame after read failure, concatenates a new OPEN row and overwrites with 12 columns including `wait_confirm_ts` and `wait_context_source`. Writer B loads/reduces to the 10-column `POSITION_STATE_COLUMNS` and overwrites with `state.to_csv(...)`, omitting those two wait-context fields. Parent created; **O** | persisted restart authority; separate CRITICAL fail-empty risk remains. A production close can contract the physical schema from 12 to 10 columns and discard only `wait_confirm_ts` and `wait_context_source`; it does not prove loss of the entire position record | historical rows unbounded; full-file reads and rewrites | **CRITICAL** authority plus **IMPORTANT** schema contraction |
| `fired_setups.csv` | `backtest/journal/fired_setups.csv` | shell `_append_fired_setup_ids` | active `run_symbol_once()` authority: `identity._load_canonical_keys_from_csv(fired_setups_csv)`; `_load_fired_setup_ids()` is a separate legacy/helper loader | parent created; first batch defines physical header; **A** | active loader recovers identity in order: `canonical_setup_key`; reconstruction from `symbol`, `model`, `side`, `setup_created_ts`; legacy `setup_id`; absence of every usable canonical, structural and legacy identity schema becomes an empty set | unbounded; full file loaded to an identity set during checks | **CRITICAL** |
| `terminal_lifecycle_registry.csv` | shell authority: `backtest/journal/terminal_lifecycle_registry.csv`; closer authority: `backtest/journal/exports_live/terminal_lifecycle_registry.csv` under canonical defaults | shell calls identity `_append_terminal_lifecycle_row`; `run_symbol_once()` calls `close_symbol_if_hit()`, which derives `position_state_csv.parent / "terminal_lifecycle_registry.csv"` | shell `_load_terminal_canonical_keys` reads only its CLI authority path | both parents/first-header then **A** | default paths already split lifecycle authority; both loaders fail empty on read/schema failure | both unbounded; shell authority registry is fully loaded | **CRITICAL** |
| `visible_ts_cache.csv` | hard-coded `backtest/journal/visible_ts_cache.csv` | shell `_save_visible_ts_cache` | module-import `_load_visible_ts_cache` | parent created; inline two-column schema; **O** | missing/read error/bad header becomes empty cache; capped to 50,000 entries before save | bounded by entry count, but full load and full rewrite | **CRITICAL** |
| `live_observation_entries.csv` | `backtest/journal/exports_live/live_observation_entries.csv` | shell `_ensure_output_csv`, `_emit_observation_rows` / `_append_df` | position closer `_load_live_entries`; offline replay/analysis | parent created; locked shell columns; **A** | survives restart; closer treats parse/bad identity schema as empty, preventing close lookup; no demonstrated corruption recovery | unbounded; closer loads full file for each lookup | **IMPORTANT** |
| `flow_log.csv` | `backtest/journal/exports_live/flow_log.csv` | shell `_append_flow_row` | `analyze_flow_log.py`, audits | parent created; `FLOW_LOG_COLUMNS`; **A** | new header only for absent/zero-byte file; existing malformed header is not validated | unbounded; analysis loads full file | **MODERATE** |
| `raw_candidate_lifecycle_diag.csv` | `backtest/journal/exports_live/raw_candidate_lifecycle_diag.csv` | shell `_append_raw_candidate_lifecycle_diag` | shell `_rebuild_pressure_window_summary`; audits | parent created; constant column list; **A** | existing non-empty header not validated; malformed/corrupt file makes summary rebuild silently return | unbounded, high-rate | **IMPORTANT** |
| `entry_model_pre_admission.csv` | disabled by default; CLI-supplied path | entry-model/pipeline diagnostic helper | audits only found | parent created by helper; constant/constructed schema; **A** | opt-in; header only on absent/empty; no proven schema validation | unbounded when enabled | **MODERATE** |
| `pressure_window_summary.csv` | `backtest/journal/exports_live/pressure_window_summary.csv` | shell `_rebuild_pressure_window_summary` | audits | parent created; constant schema; **O** | derived; if raw source missing, unreadable, empty, or missing required columns, old summary may remain unchanged | rebuild reads entire raw diagnostic into RAM and rewrites entire summary each invocation | **IMPORTANT** |
| `sniper_candidate_diag.csv` | `backtest/journal/exports_live/sniper_candidate_diag.csv` | shell `_append_sniper_candidate_diag` | summary/audits | proactively creates header; constant schema; **A** | malformed non-empty header not repaired or rejected; in-memory persistence counters reset on process restart | unbounded, high-rate | **MODERATE** |
| `sniper_candidate_summary.csv` | `backtest/journal/exports_live/sniper_candidate_summary.csv` | shell output initializer; summary behavior tied to sniper pipeline | audits | proactively creates empty header; summary schema; current inspected shell does not prove durable row updates in every path | persistence and completeness across restart not proven | expected derived growth/overwrite not proven | **MODERATE** |
| `opportunity_manager_snapshot.csv` | disabled by default; CLI-supplied path | shell `_append_opportunity_manager_snapshot` | audits | parent created; constant schema; **A** | opt-in; no existing-header validation; repeated cycles can append the same candidate identity | unbounded when enabled; no full production read found | **MODERATE** |
| `authority_waterfall.csv` | disabled by default; CLI-supplied path | shell authority-waterfall append helper | audits | parent created; constant schema; **A** | opt-in; header only on absent/empty; uniqueness is not enforced at filesystem layer | unbounded when enabled | **MODERATE** |
| `parity_filter_diagnostics.csv` | disabled by default; CLI-supplied path | shell `_append_parity_filter_diagnostics` | audits | parent created; constant schema; **A** | opt-in research output; no header validation | unbounded when enabled | **LOW** for production safety |
| `tdp_stale_shadow_oos.csv` | disabled by default; CLI-supplied path | shell `_append_tdp_stale_shadow_rows` | audits | parent created; constant schema; **A** | in-memory persistence counters reset on restart; duplicate identity/cycle rows are allowed | unbounded when enabled | **LOW** |
| `structural_ts_shadow_oos.csv` | disabled by default; CLI-supplied path | shell `_append_structural_ts_shadow_rows` | audits | parent created; constant schema; **A** | diagnostic only; no header/corruption validation | unbounded when enabled | **LOW** |
| `candidate_pressure.csv` | `backtest/journal/exports_live/candidate_pressure.csv` | pipeline `_write_candidate_pressure_row` | offline lifecycle/audits | parent created; row-derived schema; **A**, exception-swallowing | missing/empty safely creates header; existing schema not validated; write failure is silent | unbounded, one row per symbol/cycle; no live full read found | **MODERATE** |
| `symbol_performance.csv` | generally `backtest/journal/exports_live/symbol_performance.csv` | metrics tracker | dashboards/analysis | parent behavior delegated; derived summary, normally **O** | current production shell does not directly prove it is enabled | bounded summary expected, not proven | **LOW** |
| `equity_curve.csv` | generally `backtest/journal/exports_live/equity_curve.csv` | metrics tracker | dashboard/analysis | derived from trades; **O** | current production shell does not directly prove it is enabled | can grow with trades and is commonly loaded fully | **MODERATE** |
| `{symbol}_raw_live_candles.csv` | `backtest/journal/exports_live/candles_debug/{symbol}_raw_live_candles.csv` | `run_symbol_once()` immediately after successful retrieval, normalization and directory creation; not guarded by `--debug` | no production decision reader found after the write | directory created unconditionally after successful non-empty fetch; dynamic symbol filename; frame schema; **O** | refreshed each successful symbol cycle; CWD-relative; not in R1 design or observed rotation paths | bounded by fetched frame (`--bybit_candles`, default 260); whole-frame write | **MODERATE** |
| `{symbol}_closed_live_candles.csv` | `backtest/journal/exports_live/candles_debug/{symbol}_closed_live_candles.csv` | `run_symbol_once()` after removing the forming candle; not guarded by `--debug` | current cycle continues using the in-memory frame, not this snapshot | same unconditional directory; dynamic symbol filename; frame schema; **O** | refreshed on every successful cycle with at least two rows; CWD-relative; not in R1 design or observed rotation paths | bounded by fetched frame minus one row; whole-frame write | **MODERATE** |

## Additional active or configurable CSVs absent from the 19-row design

| CSV | Default / activation | Writer and behavior | Safety classification |
| --- | --- | --- | --- |
| `live_rotation_plan.csv` | `backtest/journal/live_rotation_plan.csv`, enabled by production shell | R4 writer appends one planning row per observed CSV per cycle; it does not create the parent directory, rotate, move, compress, or delete | **IMPORTANT** growth: potentially very high and unbounded; outside its own 19-file design inventory |
| `pre_visible_entry_exposure.csv` | CLI path, disabled by default | shell diagnostic append; header on absent/empty | **LOW**, unbounded if enabled |
| `tdp_visible_assignment_trace.csv` | CLI path, disabled by default | pipeline append | **LOW**, unbounded if enabled |
| `tdp_identity_resurfacing_trace.csv` | CLI path, disabled by default | pipeline append | **LOW**, unbounded if enabled |
| `tdp_disappearance_trace.csv` | CLI path, disabled by default | shell/pipeline diagnostic append | **LOW**, unbounded if enabled |
| `tdp_true_birth_trace.csv` | CLI path, disabled by default | shell/pipeline diagnostic append | **LOW**, unbounded if enabled |
| `cluster_score_shadow_v2.csv` | CLI path, disabled by default | shadow-score diagnostic append | **LOW**, unbounded if enabled |
| `candles_debug/{symbol}_raw_live_candles.csv` and `{symbol}_closed_live_candles.csv` | production-reachable default patterns, one pair per active symbol | unconditional whole-file snapshots after successful candle retrieval; dynamic names; absent from `live_rotation_observed_csv_paths` | **MODERATE** inventory and rotation-coverage gap; bounded per file |

These files must be reconciled with the Journal Operations inventory before a future design can claim coverage of every production journal. This is an inventory finding only; no design change is made in J1.

## Risk findings by severity

### CRITICAL

1. **Position authority fails empty with destructive follow-on risk.** `_load_position_state()` and shell position reads tolerate missing, empty, malformed, or unreadable state as empty. `live_observation_shell._mark_position_open()` reads the existing `position_state.csv`, substitutes an empty frame after read failure, concatenates the new OPEN row and overwrites the canonical `position_state.csv`.
2. **Fired and terminal identity authority fail empty.** The principal active fired authority in `run_symbol_once()` is `identity._load_canonical_keys_from_csv(fired_setups_csv)`. It attempts `canonical_setup_key`, then structural reconstruction from `symbol`, `model`, `side`, and `setup_created_ts`, then legacy `setup_id`; absence of every usable canonical, structural and legacy identity schema returns an empty set. `_load_fired_setup_ids()` is only a separate legacy/helper fail-empty loader. `_load_terminal_canonical_keys()` returns an empty set after read/schema failure.
3. **Visible-cache restart continuity fails empty.** `_load_visible_ts_cache()` returns `{}` after read failure or missing required columns, losing persisted first-visibility continuity for the restarted process; the next save can replace the old file.
4. **Terminal lifecycle authority is split across canonical default paths.** The shell reads/writes `backtest/journal/terminal_lifecycle_registry.csv`. `run_symbol_once()` production-reachably calls `close_symbol_if_hit()`, whose terminal append derives `backtest/journal/exports_live/terminal_lifecycle_registry.csv` from the default position-state parent. This split exists without custom configuration.

### IMPORTANT

1. `live_observation_entries.csv`, raw diagnostics, and restart registries have no size bounds or active compaction.
2. Production-reachable `_rebuild_pressure_window_summary()` loads and processes the complete unbounded `raw_candidate_lifecycle_diag.csv` under default configuration and rewrites the summary repeatedly. Exact RAM and CPU failure thresholds are not measured.
3. Position and identity readers load full CSVs; position state is rewritten non-atomically with `to_csv(path)`.
4. Existing non-empty CSV headers are generally trusted; append writers do not reject schema drift.
5. `live_rotation_plan.csv` grows by the number of observed CSVs each cycle and is not listed in its own design inventory.
6. The 19-row design does not cover all optional production telemetry paths or the two unconditional per-symbol candle snapshot patterns; candle snapshots are also absent from `live_rotation_observed_csv_paths`.
7. Runtime existence, byte sizes, logical row counts, physical headers, last-write timestamps and parser traversal are now available. Duplicate counts, semantic schema compatibility, growth/day, runtime RAM peak, corrupt-tail detection beyond parser success and production process CWD remain unresolved.
8. **Position-state multi-writer schema contraction.** `live_observation_shell._mark_position_open()` writes 12 columns including `wait_confirm_ts` and `wait_context_source`. Production-reachable `position_closer.close_symbol_if_hit()` reduces the frame to its 10-column `POSITION_STATE_COLUMNS` and overwrites the same CSV, discarding those two wait-context fields. Active runtime metadata confirms the narrower 10-column physical header. Proven loss is limited to the two omitted fields, not the entire position authority record.

### MODERATE

1. Logical multiple-writer ownership is confirmed for lifecycle/state artifacts, and sequential writes within the canonical single shell process are proven. No inter-process lock was found. Concurrent multi-process access is possible if multiple processes target the same path, but was **not observed or proven** by J1.
2. In-memory persistence counters for sniper/TDP diagnostic fields reset on restart, so telemetry semantics cross a restart boundary inconsistently.
3. Derived summary failures often return silently and may leave stale output that appears current.
4. CLI paths may intentionally point multiple logical streams to one file; no central path-collision validation was found.

### LOW

1. Optional research/shadow CSVs are disabled by default and do not feed production decisions, but are unbounded when enabled.
2. Repository-relative defaults simplify portability but are not anchored to repository root.

### INFORMATIONAL

1. No hard-coded Windows drive letter or user-profile absolute path was found in the active production shell defaults.
2. R1–R11 authority remains DENY and the production executor remains unreachable; J1 found no evidence of automatic production rotation.

## Restart and filesystem safety

- All active defaults are relative `Path` values. Launching from a different current working directory can create/read a second journal tree at a different location.
- Most writers create parent directories. `live_rotation_plan_writer` explicitly does not; it depends on an already-existing parent.
- Append writers write headers only if a file is absent or zero bytes. They do not verify that a non-empty file has the expected header or column order.
- Most CSV appends are ordinary pandas/csv writes without file locking, `fsync`, atomic replace, checksum, journal transaction, or torn-row detection.
- `position_state.csv`, `visible_ts_cache.csv`, and derived summaries use whole-file overwrite. A crash during overwrite can leave an empty, partial, or malformed file.
- Each active symbol also has unconditional raw and closed candle snapshot overwrites after successful retrieval. Their directory and paths are CWD-relative; their size is bounded by the fetched candle frame.
- Windows open-handle safety was validated by R7 for the future handshake/executor prototype, but current production writers do not use that handshake because rotation is disabled. Therefore open-handle-safe production compaction is **not proven**.
- Restart preserves files on disk, but some diagnostic counters live only in process memory. Restart continuity of those calculated fields is not preserved.
- No production mutation test was run against live files during J1.

## CSV growth and RAM findings

Unbounded append growth is present for fired setups, terminal registry, live observations, flow log, raw candidates, candidate pressure, enabled optional telemetry, and the rotation plan. Retention labels in the frozen R1 design are policy metadata only; they are not active retention enforcement.

Confirmed whole-file reads include:

- `position_state.csv` for open-position checks and close processing;
- `fired_setups.csv` for idempotency IDs;
- `terminal_lifecycle_registry.csv` for terminal identities;
- `visible_ts_cache.csv` at process import/start;
- `live_observation_entries.csv` during position close lookup;
- `raw_candidate_lifecycle_diag.csv` during each pressure-summary rebuild;
- analyst/report readers for flow, equity, and performance outputs.

The most direct current runtime RAM/CPU amplification risk is the pressure-summary path: an append-only high-rate raw file is repeatedly loaded and regrouped in full. The exact failure threshold is **not proven** without runtime sizes and profiling.

## Duplicate, schema, and corruption assessment

- Filesystem writers do not generally enforce row uniqueness. Duplicate prevention exists in trading logic for selected identities, not as a generic CSV integrity guarantee.
- Append helpers assume the existing physical column order matches the current constant list. Code/header drift can shift values under old column names.
- A zero-byte file is normally recreated with a header. A header-only file is usually treated as empty.
- A malformed non-empty header is variously fail-empty, padded with missing columns, silently ignored, or appended to without validation.
- No common quarantine, backup-before-overwrite, checksum, row framing, or corrupt-tail recovery layer was found.
- Logical multiple-writer ownership exists for state/lifecycle artifacts. Canonical shell call paths prove sequential writes inside one process. Simultaneous multi-process writes are possible but not proven to occur.

## Unknown or not-proven properties

The following remain not proven from supplied evidence:

- current production byte sizes, row counts, growth/day, and RAM peak;
- actual on-disk headers versus source constants;
- real duplicate rows, duplicate headers, truncated final rows, encoding defects, and CSV quoting corruption;
- whether the production process always starts from repository root;
- whether two shell/simulator processes ever target the same CSV concurrently;
- whether antivirus, backup, sync, or another Windows process holds journal handles;
- atomicity and durability after power loss, disk-full, or abrupt process termination;
- which optional CLI telemetry streams are enabled in the current live command;
- whether every historical/analyst reader can consume compacted multi-part history.

## Mandatory open runtime metadata

Final J1 closure requires metadata-only, read-only evidence with these fields for every target:

`absolute_path`, `filename`, `exists`, `size_bytes`, `row_count`, `header`, `last_write_timestamp`, `parse_success`.

Priority targets:

- `backtest/journal/exports_live/position_state.csv`
- `backtest/journal/fired_setups.csv`
- `backtest/journal/terminal_lifecycle_registry.csv`
- `backtest/journal/exports_live/terminal_lifecycle_registry.csv`
- `backtest/journal/visible_ts_cache.csv`
- `backtest/journal/exports_live/live_observation_entries.csv`
- `backtest/journal/exports_live/raw_candidate_lifecycle_diag.csv`
- `backtest/journal/exports_live/pressure_window_summary.csv`
- `backtest/journal/live_rotation_plan.csv`
- `backtest/journal/exports_live/candidate_pressure.csv`
- every existing `backtest/journal/exports_live/candles_debug/*_raw_live_candles.csv`
- every existing `backtest/journal/exports_live/candles_debug/*_closed_live_candles.csv`

The evidence must also record the production shell working directory if independently provable, active live command, and enabled optional CSV flags. Collection must stream records and must not copy or fully load CSV contents. `collector_working_directory` proves only the collector PowerShell process directory; it does **not** prove the already-running production Python process CWD. If that production CWD cannot be independently recovered, record it as `not proven`.

### First-pass runtime metadata incorporated

Evidence file: `ATS_J1_RUNTIME_METADATA.json`, collected at `2026-07-15T19:57:52.7041906Z`. It contains 20 file records. Every existing file reported `parse_success=true`; absent files correctly have no size/header/timestamp and `parse_success=false`.

| Default target | Exists | Size bytes | Rows | Last write UTC | Runtime observation |
| --- | ---: | ---: | ---: | --- | --- |
| `exports_live/position_state.csv` | yes | 303 | 2 | 2026-04-27T15:24:34.6067585Z | legacy six-column header; not the active command path |
| `fired_setups.csv` | yes | 535,491 | 2,106 | 2026-06-11T14:54:47.9058443Z | legacy identity header with `setup_id`; not active command path |
| root `terminal_lifecycle_registry.csv` | no | — | 0 | — | default authority absent |
| `exports_live/terminal_lifecycle_registry.csv` | no | — | 0 | — | default closer authority absent |
| `visible_ts_cache.csv` | yes | 281,338 | 3,385 | 2026-07-15T19:00:16.3423497Z | canonical hard-coded cache is current/recent |
| `exports_live/live_observation_entries.csv` | yes | 1,245 | 2 | 2026-04-27T08:00:06.4323723Z | legacy header; not active command path |
| `exports_live/raw_candidate_lifecycle_diag.csv` | no | — | 0 | — | default path absent |
| `exports_live/pressure_window_summary.csv` | no | — | 0 | — | default path absent |
| root `live_rotation_plan.csv` | yes | 119,624 | 624 | 2026-07-07T15:51:00.6953019Z | not active command path |
| `exports_live/candidate_pressure.csv` | yes | 200,581,278 | 4,456,443 | 2026-07-06T07:31:15.8263749Z | material unbounded-growth evidence; not active command path |

Five symbol pairs were found. BTCUSDT, ETHUSDT, XRPUSDT and SOLUSDT raw/closed snapshots were updated seconds before collection, each with 260/259 rows and approximately 25–28 KB. This proves current production reachability and bounded snapshot size. The BNBUSDT pair has the same 260/259 shape but last write `2026-05-31`, so it is historical/stale relative to the four-symbol active command.

The active command uses symbols `BTCUSDT,ETHUSDT,XRPUSDT,SOLUSDT`, `--debug`, `--use_wait_confirmation`, `--cluster_score_mode SHADOW_SCORE_V3_TDP_ONLY`, `--cluster_max_per_group 2`, and `--poll_seconds 15`. It explicitly redirects these CSV outputs into `backtest/journal/SHADOW_R11_REGRESSION_20260713_165605/`:

- `live_observation_entries.csv`
- `position_state.csv`
- `fired_setups.csv`
- `terminal_lifecycle_registry.csv`
- `flow_log.csv`
- `candidate_pressure.csv`
- `raw_candidate_lifecycle_diag.csv`
- `pressure_window_summary.csv`
- `sniper_candidate_diag.csv`
- `sniper_candidate_summary.csv`
- `opportunity_manager_snapshot.csv`
- `authority_waterfall.csv`
- `live_rotation_plan.csv`

The first pass inspected default targets, not these 13 active custom targets. Supplemental evidence collected at `2026-07-15T20:05:09.4045429Z` subsequently covered all 13 active custom targets.

First-pass process relationship was not proven. Supplemental `ParentProcessId` evidence establishes `py.exe` PID 17904 → `.venv` Python PID 2668 → Python312 PID 19188, with adjacent creation timestamps. Classification: **SINGLE LAUNCHER / INTERPRETER PROCESS CHAIN CONFIRMED**. It is not three independent concurrent shell instances.

`collector_working_directory` is `C:\\Users\\Anjusik\\PythonProject`; this proves only collector context. Production Python process CWD remains `not proven`.

### Supplemental active-path streaming collector

Run the following in a separate PowerShell window from `C:\Users\Anjusik\PythonProject`. It selects one matching command line, extracts every `--*_csv` argument without hard-coding the shadow folder, de-duplicates paths, uses a quoted-field-aware streaming parser with shared read access, adds process-tree evidence, and writes metadata only:

```powershell
Set-Location C:\Users\Anjusik\PythonProject
Add-Type -AssemblyName Microsoft.VisualBasic

$procs = @(Get-CimInstance Win32_Process |
  Where-Object { $_.CommandLine -match '(?i)-m\s+backtest\.journal\.live_observation_shell(?:\s|$)' })
$source = $procs |
  Sort-Object @{Expression={ if ($_.ExecutablePath -match '\\.venv\\Scripts\\python\.exe$') { 0 } else { 1 } }}, ProcessId |
  Select-Object -First 1
if (-not $source) { throw 'No active live_observation_shell command found' }

$matches = [regex]::Matches(
  $source.CommandLine,
  '(?i)(?:^|\s)--[A-Za-z0-9_]*csv(?:\s+|=)(?:"([^"]+)"|''([^'']+)''|(\S+))'
)
$activePaths = @($matches | ForEach-Object {
  $value = if ($_.Groups[1].Success) { $_.Groups[1].Value } elseif ($_.Groups[2].Success) { $_.Groups[2].Value } else { $_.Groups[3].Value }
  [IO.Path]::GetFullPath($value)
} | Sort-Object -Unique)

$metadata = foreach ($absolute in $activePaths) {
  $exists = [IO.File]::Exists($absolute)
  $count = 0L; $header = $null; $parse = $false; $size = $null; $lastWrite = $null
  if ($exists) {
    $item = Get-Item -LiteralPath $absolute
    $size = $item.Length; $lastWrite = $item.LastWriteTimeUtc.ToString('o')
    try {
      $stream = [IO.File]::Open($absolute, 'Open', 'Read', 'ReadWrite')
      $reader = [Microsoft.VisualBasic.FileIO.TextFieldParser]::new($stream)
      $reader.TextFieldType = [Microsoft.VisualBasic.FileIO.FieldType]::Delimited
      $reader.SetDelimiters(','); $reader.HasFieldsEnclosedInQuotes = $true
      try {
        if (-not $reader.EndOfData) { $header = ($reader.ReadFields() -join ',') }
        while (-not $reader.EndOfData) { $null = $reader.ReadFields(); $count++ }
        $parse = ($null -ne $header -and -not $reader.ErrorLine)
      } finally { $reader.Dispose(); $stream.Dispose() }
    } catch { $parse = $false }
  }
  [pscustomobject]@{
    absolute_path=$absolute; filename=[IO.Path]::GetFileName($absolute); exists=$exists
    size_bytes=$size; row_count=$count; header=$header
    last_write_timestamp=$lastWrite; parse_success=$parse
  }
}

[pscustomobject]@{
  collected_at_utc=[DateTime]::UtcNow.ToString('o')
  collector_working_directory=(Get-Location).Path
  production_process_cwd='not proven'
  source_process_id=$source.ProcessId
  active_live_process=@($procs | Select-Object ProcessId,ParentProcessId,CreationDate,ExecutablePath,CommandLine)
  active_csv_paths=$activePaths
  files=$metadata
} | ConvertTo-Json -Depth 7 | Set-Content ATS_J1_ACTIVE_PATH_RUNTIME_METADATA.json -Encoding UTF8
```

Supply only `ATS_J1_ACTIVE_PATH_RUNTIME_METADATA.json`. The command does not stop the shell, does not write production CSVs, and never holds a complete CSV in RAM.

### Active custom-path runtime metadata incorporated

Evidence file: `ATS_J1_ACTIVE_PATH_RUNTIME_METADATA.json`, collected at `2026-07-15T20:05:09.4045429Z`. All 13 `--*_csv` paths extracted from the active command are present in the evidence.

**Exact physical header authority:** `ATS_J1_RUNTIME_METADATA.json` and `ATS_J1_ACTIVE_PATH_RUNTIME_METADATA.json` are the authoritative evidence for complete runtime header strings. The table below reproduces short headers in full where compact; entries labelled “present; exact header in runtime JSON” are summaries only and must not be interpreted as complete physical headers. Metadata parser success proves traversal, not semantic schema correctness.

Common active root:

`C:\Users\Anjusik\PythonProject\backtest\journal\SHADOW_R11_REGRESSION_20260713_165605\`

| Absolute path / filename | Exists | Size bytes | Rows | Header evidence | Last write UTC | Parse | Classification |
| --- | ---: | ---: | ---: | --- | --- | --- | --- |
| `C:\Users\Anjusik\PythonProject\backtest\journal\SHADOW_R11_REGRESSION_20260713_165605\live_rotation_plan.csv` | yes | 21,033,722 | 109,836 | `cycle_ts,csv_name,rotation_policy,retention_policy,compression_policy,restart_safety,decision,planned_action,target_name,notes` | 2026-07-15T20:04:55.5959074Z | success | active, current, unbounded append |
| `C:\Users\Anjusik\PythonProject\backtest\journal\SHADOW_R11_REGRESSION_20260713_165605\flow_log.csv` | yes | 9,012,540 | 36,624 | present; exact complete header in active runtime JSON | 2026-07-15T20:04:55.5944025Z | success | active, current, unbounded append |
| `C:\Users\Anjusik\PythonProject\backtest\journal\SHADOW_R11_REGRESSION_20260713_165605\candidate_pressure.csv` | yes | 31,906 | 707 | `timestamp,symbol,raw_candidate_count,cluster_group_count,groups_gt1,groups_gt2,groups_gt3` | 2026-07-15T20:00:18.1909484Z | success | active, activity-dependent, unbounded append |
| `C:\Users\Anjusik\PythonProject\backtest\journal\SHADOW_R11_REGRESSION_20260713_165605\raw_candidate_lifecycle_diag.csv` | yes | 15,466 | 16 | present; exact complete header in active runtime JSON | 2026-07-15T19:15:11.5258885Z | success | active, activity-dependent, unbounded append; full-history summary source |
| `C:\Users\Anjusik\PythonProject\backtest\journal\SHADOW_R11_REGRESSION_20260713_165605\sniper_candidate_diag.csv` | yes | 10,777 | 16 | present; exact complete header in active runtime JSON | 2026-07-15T19:15:11.5293943Z | success | active, activity-dependent, unbounded append |
| `C:\Users\Anjusik\PythonProject\backtest\journal\SHADOW_R11_REGRESSION_20260713_165605\live_observation_entries.csv` | yes | 2,594 | 2 | present; exact complete header in active runtime JSON | 2026-07-14T12:00:12.4609176Z | success | active, low-event, unbounded append |
| `C:\Users\Anjusik\PythonProject\backtest\journal\SHADOW_R11_REGRESSION_20260713_165605\pressure_window_summary.csv` | yes | 2,100 | 7 | present; exact complete header in active runtime JSON | 2026-07-15T19:15:11.5679100Z | success | active derived whole-file overwrite |
| `C:\Users\Anjusik\PythonProject\backtest\journal\SHADOW_R11_REGRESSION_20260713_165605\opportunity_manager_snapshot.csv` | yes | 2,036 | 2 | present; exact complete header in active runtime JSON | 2026-07-14T12:00:12.4539142Z | success | active optional telemetry, unbounded append |
| `C:\Users\Anjusik\PythonProject\backtest\journal\SHADOW_R11_REGRESSION_20260713_165605\terminal_lifecycle_registry.csv` | yes | 1,439 | 6 | `canonical_setup_key,setup_id,symbol,model,side,setup_created_ts,terminal_stage,lifecycle_state,terminal_ts,reason` | 2026-07-14T12:30:00.7187452Z | success | active restart/terminal authority, unbounded append |
| `C:\Users\Anjusik\PythonProject\backtest\journal\SHADOW_R11_REGRESSION_20260713_165605\fired_setups.csv` | yes | 967 | 2 | present; exact complete header in active runtime JSON | 2026-07-14T12:00:12.4664171Z | success | active idempotency authority, unbounded append |
| `C:\Users\Anjusik\PythonProject\backtest\journal\SHADOW_R11_REGRESSION_20260713_165605\position_state.csv` | yes | 616 | 2 | `symbol,canonical_setup_key,setup_id,setup_created_ts,signal_ts,opened_ts,status,closed_ts,close_reason,lifecycle_state` | 2026-07-14T12:30:00.7197442Z | success | active position authority, whole-file read/overwrite |
| `C:\Users\Anjusik\PythonProject\backtest\journal\SHADOW_R11_REGRESSION_20260713_165605\sniper_candidate_summary.csv` | yes | 95 | 0 | `symbol,model,death_stage,death_reason,candidate_expansion_state,candidate_count,emitted_count` | 2026-07-13T13:56:23.7958086Z | success | active target, header-only, no data rows |
| `C:\Users\Anjusik\PythonProject\backtest\journal\SHADOW_R11_REGRESSION_20260713_165605\authority_waterfall.csv` | no | — | 0 | — | — | not applicable | configured active target missing; optional writer has not created it |

#### Quantitative active findings

- Largest active file and highest active row count: `live_rotation_plan.csv`, 21,033,722 bytes / 109,836 rows.
- Second largest: `flow_log.csv`, 9,012,540 bytes / 36,624 rows.
- Active unbounded-growth journals: rotation plan, flow log, candidate pressure, raw candidate diagnostic, sniper diagnostic, live observations, opportunity snapshot, fired setups, terminal registry, and authority waterfall when created. State and summary files use whole-file overwrite semantics instead.
- Missing active target: `authority_waterfall.csv`. Metadata does not prove a parse failure; the file does not exist and the optional writer may simply not have emitted a row.
- Header-only active file: `sniper_candidate_summary.csv` (95 bytes, zero rows, parse success).
- Parse failures: none among the 12 existing active files. Metadata-only parse success does not establish semantic schema correctness.
- Staleness: plan and flow were written seconds before collection. Candidate pressure was approximately five minutes old. Raw/sniper/pressure summary were approximately 50 minutes old, and event/authority/state files were last changed on 2026-07-14. Because these writers are event-dependent, metadata alone does not prove a runtime fault; it establishes last observed write only.
- Visible header differences between default and active `position_state.csv`, `fired_setups.csv`, and `live_observation_entries.csv` reflect legacy/default artifacts versus the current extended active schemas. This proves schema-version divergence across directories, not malformed active headers.
- Production-reachable `_rebuild_pressure_window_summary()` currently reads an active raw source of only 15,466 bytes / 16 rows, so no present RAM/CPU failure is shown. The algorithm still performs a complete-file read and its source remains unbounded. The default historical `candidate_pressure.csv` at 200,581,278 bytes / 4,456,443 rows provides quantitative evidence that diagnostic journals can reach material scale, but it is not the summary's active raw input.

#### Active versus default conclusion

The default-path files are historical or inactive for command-line-overridden outputs, except the hard-coded `visible_ts_cache.csv` and shared `candles_debug` snapshots. The active custom directory is the authoritative runtime evidence for the 13 overridden CSVs. Both default terminal registry paths were absent, while the explicitly configured active terminal registry exists and parses. This does not remove the source-level canonical default path-divergence defect; it only shows that this run directs the shell registry to the custom directory.

### Smallest safe Windows collection procedure

Run from `C:\Users\Anjusik\PythonProject` in a separate PowerShell window. This reads files with shared read/write access, keeps only one record in memory, and emits metadata JSON only:

```powershell
Set-Location C:\Users\Anjusik\PythonProject
Add-Type -AssemblyName Microsoft.VisualBasic

$fixed = @(
  'backtest/journal/exports_live/position_state.csv',
  'backtest/journal/fired_setups.csv',
  'backtest/journal/terminal_lifecycle_registry.csv',
  'backtest/journal/exports_live/terminal_lifecycle_registry.csv',
  'backtest/journal/visible_ts_cache.csv',
  'backtest/journal/exports_live/live_observation_entries.csv',
  'backtest/journal/exports_live/raw_candidate_lifecycle_diag.csv',
  'backtest/journal/exports_live/pressure_window_summary.csv',
  'backtest/journal/live_rotation_plan.csv',
  'backtest/journal/exports_live/candidate_pressure.csv'
)
$snapshots = Get-ChildItem 'backtest/journal/exports_live/candles_debug' -File -ErrorAction SilentlyContinue |
  Where-Object { $_.Name -like '*_raw_live_candles.csv' -or $_.Name -like '*_closed_live_candles.csv' } |
  ForEach-Object { $_.FullName }
$targets = @($fixed) + @($snapshots)

$metadata = foreach ($target in $targets) {
  $absolute = [IO.Path]::GetFullPath($target)
  $exists = [IO.File]::Exists($absolute)
  $count = 0L; $header = $null; $parse = $false; $size = $null; $lastWrite = $null
  if ($exists) {
    $item = Get-Item -LiteralPath $absolute
    $size = $item.Length; $lastWrite = $item.LastWriteTimeUtc.ToString('o')
    try {
      $stream = [IO.File]::Open($absolute, 'Open', 'Read', 'ReadWrite')
      $reader = [Microsoft.VisualBasic.FileIO.TextFieldParser]::new($stream)
      $reader.TextFieldType = [Microsoft.VisualBasic.FileIO.FieldType]::Delimited
      $reader.SetDelimiters(',')
      $reader.HasFieldsEnclosedInQuotes = $true
      try {
        if (-not $reader.EndOfData) { $header = ($reader.ReadFields() -join ',') }
        while (-not $reader.EndOfData) { $null = $reader.ReadFields(); $count++ }
        $parse = ($null -ne $header -and -not $reader.ErrorLine)
      } finally { $reader.Dispose(); $stream.Dispose() }
    } catch { $parse = $false }
  }
  [pscustomobject]@{
    absolute_path=$absolute; filename=[IO.Path]::GetFileName($absolute); exists=$exists
    size_bytes=$size; row_count=$count; header=$header
    last_write_timestamp=$lastWrite; parse_success=$parse
  }
}

$process = Get-CimInstance Win32_Process |
  Where-Object { $_.CommandLine -match 'live_observation_shell' } |
  Select-Object ProcessId, ExecutablePath, CommandLine

[pscustomobject]@{
  collected_at_utc=[DateTime]::UtcNow.ToString('o')
  collector_working_directory=(Get-Location).Path # collector only; production process CWD not proven
  active_live_process=$process
  files=$metadata
} | ConvertTo-Json -Depth 6 | Set-Content ATS_J1_RUNTIME_METADATA.json -Encoding UTF8
```

`row_count` is a streaming logical CSV-record count after the header. `parse_success` proves that the complete file was traversed by a quoted-field-aware CSV parser without a parse exception; it does not prove semantic agreement with the canonical schema. `collector_working_directory` is collector context only. Production Python process CWD remains `not proven` unless independently recovered. Only `ATS_J1_RUNTIME_METADATA.json` should be supplied; do not copy the underlying CSVs.

## J1 verdict

`J1_COMPLETE_READY_FOR_FINAL_PEER_REVIEW`

Source-level, default-path and active custom-path evidence are incorporated. No active parse failure or critical active-file condition was demonstrated. The missing optional authority waterfall and header-only sniper summary remain recorded findings. J1 is ready for final Independent Peer Architect review; J2 must not begin before Peer and Lead approval.
