# ATS Executor Kill-Switch Audit

## Audit Classification

```text
Executor-local kill-switch mechanism: PRESENT
Executor-local kill-switch integration into E17 submit path: NOT PROVEN
Current ATS strategic kill-switch implementation: PARTIALLY IDENTIFIED, NOT PROVEN FROM CURRENT HEAD
Unified ATS + Executor kill-switch authority: NOT IMPLEMENTED / NOT PROVEN
Production readiness: BLOCKED
```

## 1. Current ATS Kill-Switch Implementation

Available evidence identifies `evaluate_policy_kill_switch()` in `backtest/risk/policy_engine.py` and a dependency on `backtest.live.kill_switch.rolling_r_guard`.

Because the current authoritative repository HEAD for `policy_engine.py` and `backtest/live/kill_switch.py` was not supplied, the following are not certified:

- exact current thresholds;
- exact input state and lookback;
- fail-open/fail-closed behavior on missing or malformed data;
- whether the result is mandatory in the final execution-authority handoff;
- whether the executor receives a durable ATS kill-switch state;
- whether restart can lose the ATS kill-switch decision.

Conclusion: **current ATS kill-switch semantics are NOT PROVEN FROM CURRENT HEAD**.

## 2. Executor-Local Kill Switch

Evidence: `backtest/execution/mechanical_safety_bridge.py`.

`check_mechanical_safety()` accepts optional `kill_switch_path`. When a path is supplied and exists, it returns:

```text
allowed = False
reason = kill_switch_present
severity = CRITICAL
```

Properties:

- fail-closed when the supplied marker exists;
- no exchange call;
- no ATS mutation;
- no strategy/risk recalculation;
- immutable decision result.

Limitations:

- the path is optional;
- no canonical default path is defined;
- no ownership/creation/removal protocol is defined;
- no atomicity or filesystem authority is documented;
- no evidence proves E14/E17 always call E4 with this path;
- no ledger event is guaranteed for a kill-switch block;
- restart behavior depends on whether the same path is supplied again.

Conclusion: the local mechanism exists, but **mandatory enforcement at the mutation boundary is not proven**.

## 3. Emergency Stop Logic

E4 can block on a kill-switch marker and executor mode. E17 includes hard gates and an emergency maximum notional cap.

No separate centralized emergency-stop authority was identified that atomically blocks:

- command preparation;
- submission;
- post-timeout restart;
- all symbols and identities;
- both ATS and executor processes.

Conclusion: emergency stopping is distributed across local checks rather than represented by one certified authority object.

## 4. Mode Gating

### READ_ONLY / DISABLED

E14 defaults to `DISABLED`; E7 requires `READ_ONLY` for exchange reads. These are safe defaults.

### TESTNET

E14/E17 require explicit `TESTNET`, manual enable, reconciliation OK, reservation, deterministic client order ID, credentials, exchange-ready quantity, and endpoint/cap checks.

### LIVE

E14/E15/E17 explicitly hard-block `LIVE`. LIVE is not implemented.

Conclusion: current mode gating is strong for preventing accidental LIVE access.

## 5. Reservation Safety

E14 writes `RESERVED_PRE_SUBMIT` into E10 before command readiness. E15/E17 require `reserved_in_ledger == True`.

Strengths:

- reservation precedes mutation;
- deterministic client order ID;
- append-only event;
- no blind retry on unknown submit outcome.

Limitations:

- reservation evidence is represented both in the ledger and as a boolean on `CommandDecision`;
- E17 gate validation relies on the decision boolean and does not independently query/rebuild the ledger to prove that the matching reservation event exists;
- no persistent uniqueness constraint exists across restarts;
- no durable reservation state load is mandatory.

Conclusion: reservation ordering is architecturally correct but restart-safe reservation authority is incomplete.

## 6. Unknown-State Handling

E16 classifies timeout/unknown submit outcomes into ledger events that set:

```text
has_unknown_state = True
submit_unknown = True
block_new_orders = True
requires_manual_review = True
```

E13 blocks when local snapshot is unknown or exchange read fails. E18 classifies post-submit mismatch and unknown state without automatic recovery.

Strength: unknown means block, and retry is prohibited.

Gap: mandatory process-wide propagation of one identity's unknown state is not proven. Blocking is snapshot/result based and may be bypassed by a fresh process unless startup reconstruction and reconciliation are compulsory.

## 7. Reconciliation Blocks

E13 returns blocking/manual-review results for:

- local open but exchange missing;
- exchange open but local missing;
- open order without local state;
- local unknown state;
- exchange read failure;
- pre-existing local manual-review/block state.

E14 requires `RECONCILIATION_OK` before reservation. E17 again requires reconciliation OK.

Conclusion: reconciliation gates are correctly placed before submit. Startup reconciliation is still not mandatory.

## 8. Runtime Fail-Closed Behavior

### Proven fail-closed paths

- unsupported executor mode;
- local kill-switch marker present when configured;
- invalid/missing identity or geometry;
- expired intent when expiry is provided;
- duplicate identity in the same process;
- impossible/minimum quantity;
- read-only exchange failure;
- reconciliation mismatch;
- missing manual enable;
- missing reservation;
- missing credentials;
- non-TESTNET/LIVE mode;
- timeout/unknown submit outcome;
- emergency max-notional breach.

### Not proven fail-closed paths

- restart before identity registry is rebuilt;
- restart after reservation but before submit outcome is recorded;
- restart after timeout/unknown state;
- missing or unreadable executor kill-switch path;
- stale ATS authority after handoff where no expiry is present;
- unified ATS kill-switch state reaching E17;
- corrupted/truncated ledger startup handling;
- duplicate executor process using separate in-memory registries.

## 9. Risk Authority

ATS remains strategic risk authority. The executor does not re-run portfolio or correlation caps.

E4 local max-risk/max-notional values are mechanical upper-bound guardrails. E6 computes post-rounding risk only to ensure floor conversion does not exceed the already-authorized amount. E17 emergency max-notional is another execution-local cap.

These checks can only block or reduce mechanical quantity representation; they do not authorize additional risk.

## 10. Final Findings

| ID | Finding | Severity | Status |
|---|---|---|---|
| KS-01 | Current ATS kill-switch code and wiring not available from authoritative HEAD | CRITICAL | NOT PROVEN |
| KS-02 | Executor kill-switch path is optional | CRITICAL | OPEN |
| KS-03 | E17 mandatory kill-switch enforcement not proven | CRITICAL | OPEN |
| KS-04 | E5 identity registry is session-local | CRITICAL | OPEN |
| KS-05 | Startup ledger reconstruction/reconciliation not mandatory | CRITICAL | OPEN |
| KS-06 | Reservation boolean is trusted without independent ledger proof at submit boundary | HIGH | OPEN |
| KS-07 | Unknown-state blocking may not survive fresh process startup | CRITICAL | OPEN |
| KS-08 | Safety-block decisions are not uniformly ledger-observable | HIGH | OPEN |
| KS-09 | Duplicate executor processes can hold separate registries | HIGH | OPEN |
| KS-10 | LIVE remains hard blocked | INFO | PROVEN IN E14-E17 |

## Audit Decision

```text
E20 inventory: COMPLETE WITH EVIDENCE LIMITATIONS
TESTNET enablement: NOT AUTHORIZED BY E20
LIVE enablement: FORBIDDEN
Production readiness: NOT CERTIFIED
Required next authority work: restart-safe durable safety authority and unified kill-switch integration
```
