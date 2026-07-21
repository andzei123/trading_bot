# ATS Executor Safety Authority Map

## E20 Scope

This document inventories authority and safety boundaries only. E20 introduces no execution behavior, no TESTNET enablement, no LIVE enablement, and no production integration.

## Evidence Boundary

Executor evidence was reviewed from the E19 implementation under `backtest/execution/`.

The current production ATS HEAD files were not supplied to this audit workspace. Therefore, current ATS kill-switch wiring beyond the known `policy_engine` references is classified **NOT PROVEN FROM CURRENT HEAD**. Historical donor skeletons were not used as implementation authority.

## Authority Chain

```text
ATS Generator / Authority / Ranking / Opportunity Manager / Risk
    |
    | produces already-authorized Execution Intent
    v
E2 Decision Consumer
    |
    | validates completeness; does not grant strategy authority
    v
E4 Mechanical Safety Bridge
    |
    | executor-local mechanical checks; may block, never override ATS
    v
E5 Execution Identity Registry
    |
    | session-local duplicate-attempt block
    v
E6 Exchange Quantity Converter
    |
    | deterministic exchange formatting; floor-only mechanical conversion
    v
E13 Reconciliation Result
    |
    | must be RECONCILIATION_OK
    v
E14 Command Gateway
    |
    | TESTNET + manual enable + pre-submit ledger reservation
    v
E17 Real TESTNET Submit
    |
    | credentials + reservation + reconciliation + endpoint + emergency cap
    v
E16 Submit Outcome Handler
    |
    | append-only outcome event; unknown means block
    v
E18 Post-Submit Reconciliation
    |
    | read-only classification; mismatch means block/manual review
    v
E10 Execution Event Ledger
    |
    v
ExecutionStateSnapshot
```

## Ultimate Execution Permission

### Strategic permission

**ATS is the ultimate strategic authority.** It decides symbol, side, model, entry, stop, target, authorized risk, authorized quantity/notional, ranking, freshness, WAIT, and whether a setup is selected.

### Mechanical permission

The executor can only reduce permission by blocking. It cannot convert an ATS rejection into permission and cannot create a new strategy decision.

For the current TESTNET path, an exchange mutation is reachable only when all downstream gates agree:

1. immutable validated intent exists;
2. mechanical safety passes;
3. identity is not already accepted in the current session;
4. quantity conversion returns exchange-ready output;
5. read-only reconciliation is `RECONCILIATION_OK`;
6. E14 mode is exactly `TESTNET`;
7. manual enable is true;
8. pre-submit reservation is appended to E10 ledger;
9. deterministic `client_order_id` is present;
10. E17 TESTNET credentials and TESTNET endpoint checks pass;
11. emergency max-notional guardrail passes.

## Required Questions

### Who ultimately decides execution permission?

ATS decides strategic permission. The executor applies additional mechanical vetoes. No executor component is authorized to create strategy permission.

### Can executor override ATS?

**No, by architecture.** Decision Consumer, Mechanical Safety Bridge, Quantity Converter, Command Gateway, Submit Adapter, and Reconciliation layers only validate, format, reserve, submit an already-authorized TESTNET command, classify, or block.

### Does executor duplicate ATS risk logic?

**No strategic risk logic is intentionally duplicated.** E4 optional caps are documented as executor-local upper-bound guardrails. E6 checks that mechanical floor rounding does not increase authorized risk; it does not re-run portfolio, correlation, ranking, or strategy risk.

### Can stale state accidentally allow execution?

A known stale/expired intent is blocked by E4 when `entry_window_expires_ts` is present and expired. Unknown local or exchange state is blocked by E13/E16/E18. However, complete protection against stale state after process restart is **not proven**, because E5 is session-local and restart persistence/recovery is not implemented.

### Can restart bypass safety?

**Potentially, yes, unless startup is externally gated.** E5 identities are in-memory only. E10 can persist events when an explicit path is supplied, but E1-E19 do not implement mandatory restart ledger loading, startup reconciliation, or a global restart lock. Restart safety therefore remains a production blocker.

### Can unknown exchange state produce duplicate submit?

The architecture says unknown submit outcomes must block and must not retry. E16 records timeout/unknown as blocking/manual-review states. But because restart persistence and mandatory startup reconciliation are absent, duplicate submit after a process restart is not fully prevented by the current implementation alone.

### Are TESTNET and LIVE independently gated?

TESTNET is explicitly gated by mode, manual enable, reservation, reconciliation, credentials, endpoint validation, quantity readiness, and emergency cap. LIVE is hard-blocked in E14-E17. A certified independent LIVE gate does not exist because LIVE is intentionally unimplemented.

### Are safety decisions observable from logs or ledger?

Partially. Reservation and submit outcomes are represented in E10 ledger and reconstructable into immutable snapshots. Mechanical safety failures, idempotency blocks, and reconciliation classifications are returned as immutable result objects but are not guaranteed to be appended to the ledger on every blocked path. Full forensic observability is therefore incomplete.

## Authority Classification

| Area | Authority | Can Allow? | Can Block? | Persistent Evidence |
|---|---|---:|---:|---|
| ATS strategy/authority | ATS | Yes | Yes | ATS-owned telemetry/state |
| Decision completeness | E2 | No | Yes | Returned immutable intent/error |
| Mechanical safety | E4 | No | Yes | Result object; ledger not guaranteed |
| Session idempotency | E5 | No | Yes | In-memory only |
| Quantity formatting | E6 | No | Yes | Immutable `ExchangeReadyIntent` |
| Reconciliation | E13/E18 | No | Yes | Result object; optional ledger event path only where implemented |
| Command reservation | E14 | TESTNET command readiness only | Yes | E10 `RESERVED_PRE_SUBMIT` |
| TESTNET submit | E17 | Executes only after all prior gates | Yes | E16 ledger outcome |
| State authority | E10 | Reconstructs, does not authorize strategy | Yes through derived blocking state | Append-only executor ledger |
| LIVE execution | Not implemented | No | Hard blocked | N/A |

## E20 Conclusion

The executor follows a veto-only downstream authority model and cannot intentionally override ATS. The strongest current guarantees are TESTNET mode gating, reservation-before-submit, deterministic client order identity, fail-closed unknown handling, and read-only reconciliation.

The principal unresolved authority gap is restart: session-local idempotency and optional ledger persistence do not yet form a mandatory restart-safe execution lock.
