                    DEBUG FLOW MAP
════════════════════════════════════════════════════════════

1. MARKET DATA
────────────────────────────────────────────────────────────
Input:
candles
macro
liquidations
cross-asset context

Possible failure:
- no fresh candles
- missing macro inputs
- bad timestamp alignment
- stale feed

Debug:
[BOOT]
[WATCHDOG]
[FEED_WATCHDOG]

Key checks:
- latest_ts updates
- candle count enough
- no NaN on close/high/low


                        │
                        ▼

2. CONTEXT ENGINE
────────────────────────────────────────────────────────────
build_ctx()

Creates:
trend_dir
trend_strength
impulse_recent
range_hi / range_lo
macro_bias
volatility context

Possible failure:
- ctx built but wrong trend_dir
- trend strength unstable
- macro fields missing
- BOS / structure context not represented correctly

Debug:
CTX_SNAPSHOT
trend_dir
trend_strength
macro_bias
ctx_sub_label

Root cause signs:
- trend_dir inconsistent with price structure
- ctx_sub_label missing when setup expected


                        │
                        ▼

3. REGIME ENGINE
────────────────────────────────────────────────────────────
phase_router.py
decide_phase()

Outputs:
PHASE_TREND_UP
PHASE_TREND_DOWN
PHASE_RANGE

Possible failure:
- wrong phase selected
- macro incorrectly tilts ambiguous case
- runner overrides router decision
- phase drift after setup creation

Debug:
PHASE_DECISION
setup_phase
current_phase
setup_trend_dir
current_trend_dir

Root cause signs:
- trend_phase != final_phase often
- runner rewrites phase
- setup created in one phase, filtered in another


                        │
                        ▼

4. SIGNAL ENGINE
────────────────────────────────────────────────────────────
entry_model.py

Generates:
TDP_REENTRY
RANGE_TOP_SHORT_V2

Possible failure:
- BOS exists but setup not created
- entry_model too strict
- RR logic suppresses valid setups
- phase contract blocks correct model

Debug:
SETUP_CREATED
model
side
phase
rr
ctx_sub_label
entry / tp / sl

Root cause signs:
- expected BOS visible in chart but no SETUP_CREATED
- only one model type appears
- RR unexpectedly low/high


                        │
                        ▼

5. SETUP LIFECYCLE ENGINE
────────────────────────────────────────────────────────────
setup_detected
↓
setup_active
↓
entry_window
↓
invalidated
↓
closed

Possible failure:
- setup expires too early
- setup invalidated before entry chance
- setup lifecycle re-evaluated using current regime
- setup contract not frozen

Debug:
SETUP_EMIT_CHECK
SETUP_EXPIRED
SETUP_INVALIDATED

Important fields:
setup_age_candles
live_max_setup_age_candles
emit_last_candles
setup_phase
setup_rr
setup_max_age_candles

Root cause signs:
- lots of SETUP_CREATED → SETUP_EXPIRED
- lots of SETUP_INVALIDATED reason=entry_touched
- no emit despite valid BOS


                        │
                        ▼

6. SIGNAL FILTERS
────────────────────────────────────────────────────────────
Filters:
phase alignment
signal_cluster_filter
TTS gate
macro permission
context gate
liquidity checks

Possible failure:
- cluster filter removes best setups
- macro permission too strict
- context gate blocks almost everything
- phase alignment uses current state instead of setup state

Debug:
SETUP_FILTERED
filter_name
reason
macro_bias
macro_strength
context_allow

Root cause signs:
- many SETUP_CREATED → SETUP_FILTERED
- same filter dominates all rejects
- filtered setups look visually valid


                        │
                        ▼

7. CAPITAL ENGINE
────────────────────────────────────────────────────────────
A / B / C tiers
symbol weights
allocation logic

Possible failure:
- setup valid but allocated 0 capital
- symbol deprioritized too hard
- tier logic too strict

Debug:
CAPITAL_DECISION
tier
allocated_risk
symbol_weight
priority_score

Root cause signs:
- many emitted setups but few trades
- BTC/ETH always chosen, others starved


                        │
                        ▼

8. RISK ENGINE
────────────────────────────────────────────────────────────
Controls:
max risk per trade
max daily risk
correlation cap
equity governor
kill switch

Possible failure:
- risk block too aggressive
- correlation cap suppresses flow
- defensive mode never exits
- kill switch silently active

Debug:
RISK_BLOCK
PORTFOLIO_FILTER
KILL_SWITCH
REGIME_DECISION

Root cause signs:
- emitted setups exist but no TRADE_OPENED
- max_positions reached too often
- profile stuck in DEFENSIVE/OFF


                        │
                        ▼

9. EXECUTION ENGINE
────────────────────────────────────────────────────────────
order manager
position manager
TP/SL handler
timeout handler

Possible failure:
- setup emitted but trade not opened
- executor selects only one from many
- position manager rejects duplicate symbol
- latency / stale snapshot

Debug:
SETUP_EMITTED
TRADE_OPENED
EXECUTION_REJECT
POSITION_BLOCK

Root cause signs:
- many SETUP_EMITTED but 0 TRADE_OPENED
- only last emitted setup becomes trade
- symbol-level lock blocks entries


                        │
                        ▼

10. MONITORING / CSV
────────────────────────────────────────────────────────────
Outputs:
live_entries.csv
runner_diagnostics.jsonl
trades.csv
equity_curve.csv

Possible failure:
- pipeline works but logs hide real issue
- state file prevents rerun
- CSV overwritten instead of appended

Debug:
TRACE
STATUS SNAPSHOT
runner_diagnostics.jsonl

Root cause signs:
- diagnostics inconsistent with actual behavior
- filtered and emitted same setup
- state causes hidden no_new_candle skip

FAST ROOT CAUSE GUIDE
A)
SETUP_CREATED
→ SETUP_EXPIRED

Problem:
setup lifecycle / emit window too strict


B)
PHASE_DECISION mismatch
trend_phase != final_phase

Problem:
phase conflict / runner override


C)
SETUP_CREATED
→ SETUP_FILTERED filter_name=macro_filter

Problem:
macro layer too strict


D)
SETUP_CREATED
→ SETUP_INVALIDATED reason=entry_touched

Problem:
invalidation too aggressive


E)
SETUP_EMITTED
→ no TRADE_OPENED

Problem:
execution / portfolio / risk layer


F)
No SETUP_CREATED at all

Problem:
signal engine / BOS / ctx / phase router
MINIMAL DEBUG LOG CONTRACT
PHASE_DECISION
SETUP_CREATED
SETUP_EMIT_CHECK
SETUP_FILTERED
SETUP_INVALIDATED
SETUP_EMITTED
TRADE_OPENED

Papildomi naudingi:

REGIME_DECISION
KILL_SWITCH
PORTFOLIO_FILTER
CONTEXT_GATE
NO_NEW_CANDLE
3 MOST IMPORTANT DEBUG RULES
1. New market state creates new setups
   It must not rewrite old setups

2. BOS validates setup existence
   Runner only manages lifecycle

3. Runner = orchestrator only
   It must not replace regime engine decisions
WHAT TO CHECK FIRST IN ANY FAILURE
1. Was phase correct?
2. Was setup created?
3. Which exact filter killed it?
4. Did it survive lifecycle?
5. Did it get emitted?
6. Why did execution accept/reject it?
DEBUG PRIORITY ORDER
1. Phase conflict
2. Setup lifecycle
3. Signal filters
4. Portfolio/risk blocks
5. Execution issues
6. Monitoring/logging mismatches
FINAL DEBUG PHILOSOPHY
If setups are not created:
    problem is upstream (context / regime / signal engine)

If setups are created but not emitted:
    problem is lifecycle / filters

If setups are emitted but trades do not open:
    problem is portfolio / risk / execution