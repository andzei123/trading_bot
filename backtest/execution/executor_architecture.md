# ATS Executor Architecture

## 1. Purpose

The ATS Executor is a separate execution subsystem for the ANJUSIK Trading System. Its purpose is to receive an already-authorized ATS execution intent and move that intent through executor-local safety, formatting, audit, simulation, testnet, and reconciliation layers.

The executor does not create trading ideas. It does not rank setups. It does not calculate risk. It does not change entry, stop, target, freshness, WAIT, or authority decisions. ATS decides; the executor executes or blocks mechanically.

## 2. Architecture Overview

The approved boundary is:

```text
ATS
  ↓
Execution Intent
  ↓
Executor
  ↓
Exchange / Testnet / Read-only Exchange State
```

The executor lives under `backtest/execution/` and is intentionally isolated from production ATS modules. During the E1-E19 program, `live_observation_shell.py` remains frozen and is not imported, modified, or integrated.

## 3. Executor Philosophy

The executor is a downstream mechanical system. Its design principles are:

- fail closed;
- deterministic behavior;
- immutable input and output models;
- append-only ledger state;
- no hidden strategy logic;
- no ATS risk recalculation;
- explicit mode gates for mutation paths;
- no production file writes unless a future certified integration explicitly allows them.

The executor is allowed to say no. It is not allowed to invent a better trade.

## 4. Layer-by-Layer Explanation

### E1 — Foundation

**Responsibility:** establish executor-local package primitives and dry-run-safe foundations.

**Inputs:** ATS-shaped intent data.

**Outputs:** executor-local objects suitable for later stages.

**Safety guarantees:** no exchange communication, no production writes, and no order submission.

### E2 — Decision Consumer

**Responsibility:** accept only complete ATS execution intents and return immutable `ValidatedExecutionIntent`.

**Inputs:** `ExecutionIntent` or intent row.

**Outputs:** `ValidatedExecutionIntent`.

**Safety guarantees:** validates shape and completeness only; does not calculate risk, quantity, RR, ranking, freshness, WAIT, or authority.

### E3 — Intent Journal

**Responsibility:** append validated intents to an executor diagnostics journal.

**Inputs:** `ValidatedExecutionIntent`.

**Outputs:** append-only intent journal record.

**Safety guarantees:** journal is diagnostic only and never influences execution decisions.

### E4 — Mechanical Safety Bridge

**Responsibility:** check executor-local mechanical conditions around a validated intent.

**Inputs:** `ValidatedExecutionIntent`.

**Outputs:** immutable `MechanicalSafetyResult`.

**Safety guarantees:** checks only mechanical safety such as executor mode, kill switch presence, malformed identity, geometry shape, expiration, local pending identity, and optional executor-local upper-bound guardrails. It is not ATS Risk Management.

### E5 — Execution Identity Registry

**Responsibility:** protect against duplicate execution attempts within the executor session.

**Inputs:** `canonical_setup_key`.

**Outputs:** immutable `IdempotencyResult`.

**Safety guarantees:** in-memory only at this stage; no persistence, no production `position_state.csv`, and no exchange reconciliation.

### E6 — Exchange Quantity Converter

**Responsibility:** convert ATS-authorized quantity into exchange-ready formatting.

**Inputs:** `ValidatedExecutionIntent` and optional mechanical quantity rules.

**Outputs:** immutable `ExchangeReadyIntent`.

**Safety guarantees:** floor-only deterministic rounding; no strategic sizing, no risk recalculation, no RR/entry/stop/target changes.

### E7 — Read-Only Exchange Adapter

**Responsibility:** define read-only exchange adapter models and interface.

**Inputs:** adapter implementation output.

**Outputs:** immutable balance, position, open-order, instrument-rule, and read-result snapshots.

**Safety guarantees:** protocol exposes read-only methods only. Known mutation method names are forbidden.

### E8 — Execution Simulator

**Responsibility:** produce deterministic local simulated execution events.

**Inputs:** `ExchangeReadyIntent` and selected simulated outcome.

**Outputs:** immutable simulated execution event.

**Safety guarantees:** no Bybit, no REST, no WebSocket, no paper/live orders.

### E9 — Recovery Simulator

**Responsibility:** model deterministic recovery decisions for uncertain simulated states.

**Inputs:** simulated execution events.

**Outputs:** immutable recovery decision.

**Safety guarantees:** classification only. No exchange polling, no retry, no cancel, no recovery action.

### E10 — Execution Event Ledger / State Snapshot

**Responsibility:** provide append-only executor-local event sourcing and deterministic state reconstruction.

**Inputs:** executor-local events.

**Outputs:** immutable `ExecutionStateSnapshot`.

**Safety guarantees:** ledger writes only to an explicitly supplied executor-local path; no default production journal path.

### E11 — Paper Executor Local Lifecycle

**Responsibility:** compose existing executor-local components into a paper lifecycle.

**Inputs:** `ValidatedExecutionIntent`.

**Outputs:** `ExecutionStateSnapshot` rebuilt from E10 ledger.

**Safety guarantees:** no separate paper authority and no `paper_state.csv`. E10 ledger remains the only executor state truth.

### E12 — Real Read-Only Exchange Validation

**Responsibility:** validate E7 read-only adapter behavior against Bybit read-only operations when credentials are supplied.

**Inputs:** external read-only credentials for optional validation.

**Outputs:** normalized read-only snapshots.

**Safety guarantees:** missing credentials fail closed. Smoke tests use mock adapters only. No mutation endpoints.

### E13 — Exchange Reconciliation Engine

**Responsibility:** compare executor-local state with supplied read-only exchange snapshots.

**Inputs:** `ExecutionStateSnapshot`, open positions, open orders, balances, and optional instrument rules.

**Outputs:** immutable `ReconciliationResult`.

**Safety guarantees:** no exchange calls inside reconciler, no automatic recovery, and no order mutation.

### E14 — Testnet Command Gateway Skeleton

**Responsibility:** prepare a TESTNET-only command reservation behind reconciliation and manual enable gates.

**Inputs:** `ExchangeReadyIntent`, `ReconciliationResult`, and E10 ledger.

**Outputs:** immutable `CommandDecision` and pre-submit reservation event.

**Safety guarantees:** LIVE is always blocked. TESTNET requires explicit manual enable. No submit occurs in E14.

### E15 — Testnet Submit Adapter Skeleton

**Responsibility:** build a normalized TESTNET submit request and classify stubbed submit outcomes.

**Inputs:** E14-approved `CommandDecision` and `ExchangeReadyIntent`.

**Outputs:** immutable `SubmitResult`.

**Safety guarantees:** no live mode, no TP/SL, no retry, no protection orders, and smoke tests use fake responses only.

### E16 — Submit Outcome Handler

**Responsibility:** convert submit outcomes into append-only ledger events and rebuild snapshots.

**Inputs:** `SubmitResult` and E10 ledger.

**Outputs:** updated `ExecutionStateSnapshot`.

**Safety guarantees:** no retry, no reconciliation, no exchange polling, no hidden execution loop.

### E17 — Real Testnet Submit

**Responsibility:** provide a real Bybit TESTNET submit path behind all E14/E15/E16 gates.

**Inputs:** approved command, exchange-ready intent, testnet credentials, manual enable, emergency notional cap, and local ledger.

**Outputs:** normalized `SubmitResult` and E16 snapshot.

**Safety guarantees:** LIVE is impossible. No retry. No TP/SL. No cancel/amend. No production ATS writes. Offline smoke never calls Bybit.

### E18 — Post-Submit Reconciliation Drill

**Responsibility:** classify post-submit consistency using `SubmitResult`, local snapshot, and supplied read-only exchange state.

**Inputs:** `SubmitResult`, `ExecutionStateSnapshot`, and `ExchangeStateSnapshot`.

**Outputs:** immutable `PostSubmitReconciliationResult`.

**Safety guarantees:** classification only. No exchange calls, no retry, no cancel, no TP/SL, and no automatic recovery.

### E19 — End-to-End Testnet Drill and Documentation

**Responsibility:** validate E1-E18 as one deterministic local drill and provide this canonical architecture document.

**Inputs:** `ValidatedExecutionIntent`, fake/stubbed submit outcome, and supplied read-only exchange state.

**Outputs:** `EndToEndDrillResult`, E10 ledger events, and final `ExecutionStateSnapshot`.

**Safety guarantees:** adds no new execution layer and no exchange mutation. Smoke uses fake exchange responses only.

## 5. Complete Execution Flow

```text
ATS Execution Intent
  ↓
E2 Decision Consumer
  ↓
ValidatedExecutionIntent
  ↓
E3 Intent Journal
  ↓
E4 Mechanical Safety Bridge
  ↓
E5 Execution Identity Registry
  ↓
E6 Exchange Quantity Converter
  ↓
ExchangeReadyIntent
  ↓
E13 Read-only Reconciliation Gate
  ↓
E14 TESTNET Command Gateway Reservation
  ↓
E15 Submit Adapter / E17 Real TESTNET Submit Path
  ↓
E16 Submit Outcome Handler
  ↓
E10 Execution Event Ledger
  ↓
E18 Post-Submit Reconciliation
  ↓
ExecutionStateSnapshot
```

Simulation and paper branches use E8, E9, and E11 while keeping the same ledger authority model.

## 6. Executor Authority Boundaries

ATS decides:

- symbol;
- side;
- model;
- entry;
- stop;
- target;
- authorized risk;
- authorized quantity/notional;
- ranking;
- freshness;
- WAIT;
- authority waterfall;
- whether a setup is selected for execution.

Executor executes or blocks mechanically. The executor never recalculates strategy, never recalculates risk, never modifies ATS quantity decisions, and never substitutes its own authority.

## 7. Safety Model

### READ_ONLY

Default safe posture. Read-only adapter models may inspect exchange state without mutation.

### TESTNET

TESTNET mutation requires explicit mode, manual enable, an E14 reservation, reconciliation OK, credentials, client order id, exchange-ready quantity, and emergency notional cap compliance.

### Future LIVE

LIVE is not implemented in E1-E19 and must remain impossible until a separate production certification phase adds stronger gates, restart persistence, reconciliation, protection orders, and operational runbooks.

## 8. Ledger Authority

E10 ledger is the executor source of truth because it is append-only, local, deterministic, and replayable. Runtime snapshots are derived from events rather than maintained as separate mutable state. This prevents paper/live shadow state from becoming a second hidden authority.

## 9. Reconciliation Model

Reconciliation is passive comparison. E13 compares local executor snapshot against read-only exchange positions and open orders. E18 compares submit outcome against read-only post-submit state. Both fail closed on unknown or read failure. Neither performs automatic recovery or exchange mutation.

## 10. Submit Lifecycle

The submit lifecycle is:

```text
ExchangeReadyIntent
  ↓
Reconciliation OK
  ↓
E14 Pre-submit Reservation
  ↓
E15/E17 Submit Result
  ↓
E16 Ledger Outcome
  ↓
E18 Post-submit Reconciliation
```

A reservation must exist before any future mutation path. Submit outcomes can be ACK, REJECT, TIMEOUT_UNKNOWN, SUBMIT_UNKNOWN, or BLOCKED. Unknown states are not retried automatically.

## 11. Recovery Model

Recovery is currently modeled, not automated. E9 classifies simulated unknown or partial states. E16 and E18 create blocking/manual-review snapshots for unknown submit outcomes. Future restart recovery must use ledger replay plus read-only exchange reconciliation before any new orders are allowed.

## 12. Production Safety Rules

- Do not modify `live_observation_shell.py` while production shadow validation is active.
- Do not write production ATS files.
- Do not modify `position_state.csv`.
- Do not alter ATS telemetry.
- Do not calculate strategy risk in executor.
- Do not alter entry, stop, target, RR, authority, ranking, freshness, or WAIT.
- Do not enable LIVE mode without a separate certification stage.
- Unknown exchange or submit state must fail closed.

## 13. Future Roadmap

Future work is explicitly outside E1-E19:

- TP/SL placement;
- restart recovery persistence;
- partial fill manager;
- live safety gates;
- production certification;
- exchange-specific operational runbooks;
- structured monitoring and alerting.

Each item must be introduced as a small independently validated phase with compile, smoke, parity or reconciliation checks where applicable, commit, and tag.

## 14. Engineering Rules

- Production shell is frozen during executor development.
- Executor remains isolated under `backtest/execution/`.
- No ATS logic duplication.
- One small patch per phase.
- Compile after every phase.
- Smoke test after every phase.
- Use temp directories for tests.
- Append-only ledger for executor truth.
- Fail closed on uncertainty.
- No exchange mutation in smoke tests.

## 15. Glossary

**ATS:** The authoritative strategy system that produces execution intents.

**Execution Intent:** ATS-approved downstream instruction containing symbol, side, entry, stop, target, authorized quantity/risk context, and identity.

**ValidatedExecutionIntent:** Immutable E2 wrapper proving the executor accepted a complete ATS intent.

**Mechanical Safety Bridge:** Executor-local mechanical gate. Not strategy risk management.

**Execution Identity Registry:** Session-local duplicate-attempt protection by canonical setup key.

**ExchangeReadyIntent:** Quantity-formatted intent that preserves ATS decisions.

**Execution Event Ledger:** Append-only executor event source and snapshot reconstruction authority.

**Reconciliation:** Passive comparison between local executor state and supplied read-only exchange state.

**CommandDecision:** E14 TESTNET gateway decision and reservation result.

**SubmitResult:** Normalized submit outcome from stubbed or gated TESTNET submit path.

**PostSubmitReconciliationResult:** E18 classification comparing submit outcome with read-only exchange state.

**Fail closed:** Block new orders and require manual review or safe resolution when state is unknown or inconsistent.
