                        MARKET DATA LAYER
────────────────────────────────────────────────────────
candles
volume
funding
macro signals
orderbook
liquidations
cross-asset context
onchain signals
                        │
                        ▼

                    CONTEXT ENGINE
────────────────────────────────────────────────────────
build_ctx()

Generuoja context features:

trend_dir
trend_strength
dev_up / dev_dn
impulse_recent
range_hi / range_lo
macro_bias
volatility
                        │
                        ▼

                    REGIME ENGINE
────────────────────────────────────────────────────────
phase_router.py
decide_phase()

Rezultatas:

PHASE_TREND_UP
PHASE_TREND_DOWN
PHASE_RANGE

IMPORTANT RULE:

Regime = structural market state  
Macro gali:

allow
reduce
block extreme

Macro NEGALI override'inti regime.
                        │
                        ▼

                    SIGNAL ENGINE
────────────────────────────────────────────────────────
entry_model.py

Modeliai:

TDP_REENTRY
RANGE_TOP_SHORT_V2

Edge branduolys:

TDP_REENTRY
+ TREND phases

Signal Engine generuoja:

setup candidates

Signal ≠ Trade
                        │
                        ▼

                SETUP LIFECYCLE ENGINE
────────────────────────────────────────────────────────

setup_detected
        │
        ▼
setup_active
        │
        ▼
entry_window
        │
        ▼
invalidated
        │
        ▼
closed

Lifecycle parametrai:

setup_age_candles
emit_last_candles
live_max_setup_age_candles

Setup yra struktūrinis objektas,
kuris gali gyventi kelias žvakes.
                        │
                        ▼

                    SIGNAL FILTERS
────────────────────────────────────────────────────────
Filtrų sluoksnis:

phase alignment
signal_cluster_filter
TTS gate
macro permission
context gate
liquidity checks

Tik setup'ai kurie praeina filtrus tampa:

TRADE CANDIDATES
                        │
                        ▼

                    CAPITAL ENGINE
────────────────────────────────────────────────────────
Capital allocation logika.

Setup tiers:

A setups → 0.2%
B setups → 0.1%
C setups → 0–0.05%

Symbol weights:

BTC → core
ETH → core
SOL → secondary
XRP → optional

Capital engine sprendžia:

allocation
priority
symbol exposure
                        │
                        ▼

                    RISK ENGINE
────────────────────────────────────────────────────────
Risk engine saugo kapitalą.

Controls:

max risk per trade
max daily risk
max open trades
correlation cap
portfolio exposure
drawdown guard
equity governor
kill switch

GLOBAL_RISK_GUARD
                        │
                        ▼

                    EXECUTION ENGINE
────────────────────────────────────────────────────────
Order execution.

Components:

order manager
position manager
TP / SL handler
timeout handler

Monitoring:

latency
slippage
fill quality
                        │
                        ▼

                    MONITORING LAYER
────────────────────────────────────────────────────────
System monitoring:

runner watchdog
signal watchdog
feed watchdog
system health monitor
performance alerts
                        │
                        ▼

                    CSV DATA WAREHOUSE
────────────────────────────────────────────────────────
Visi rezultatai saugomi CSV:

trades.csv
live_entries.csv
equity_curve.csv
symbol_performance.csv

CSV leidžia:

edge analysis
performance diagnostics
strategy research
                        │
                        ▼

                    RESEARCH PLATFORM
────────────────────────────────────────────────────────

CSV
 ↓
edge analysis
 ↓
model improvements
 ↓
new deployments

Research agentai:

CSV_ANALYST
ENTRY_MODEL_AUDITOR
FILTER_CLUSTER_OWNER
                        │
                        ▼

                STRATEGY EVOLUTION LOOP
────────────────────────────────────────────────────────

Market data
 ↓
Trading
 ↓
CSV logs
 ↓
Analysis
 ↓
Model updates
 ↓
New deployment

Sistema evoliucionuoja
be strategijos degradacijos.

CORE SYSTEM PRINCIPLES
1. Deterministic execution
Same input → same output

2. Layer separation
Kiekvienas komponentas turi vieną atsakomybę

3. Runner = Orchestrator only
Runner nedaro signal decisions

4. Signal ≠ Trade
Signal turi praeiti filtrus

5. Setup lifecycle
Setup gyvena kelias žvakes
FINAL SYSTEM GOAL
AI-managed systematic trading platform

multi-strategy trading
capital allocation
risk governance
portfolio intelligence
research automation