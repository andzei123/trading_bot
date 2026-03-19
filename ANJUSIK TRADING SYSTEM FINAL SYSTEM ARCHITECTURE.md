1. SYSTEM PURPOSE

Anjusik Trading System yra systematic crypto trading platform, kuri:

identifikuoja rinkos struktūrą

generuoja setup'us

filtruoja signalus pagal kontekstą

paskirsto kapitalą

vykdo orderius

analizuoja rezultatus per CSV research pipeline

Sistema sukurta kaip:

Quant Research Platform
+
Automated Trading Engine
2. CORE SYSTEM PIPELINE

Galutinis trading pipeline:

Market Data
↓
Context Engine
↓
Regime Engine
↓
Signal Engine
↓
Setup Lifecycle Engine
↓
Signal Filters
↓
Capital Engine
↓
Risk Engine
↓
Execution Engine
↓
Monitoring
↓
CSV Data Warehouse
↓
Research / AI Analysis
3. DATA LAYER

Market data šaltiniai:

candles
volume
range levels
cross-asset context
macro signals

Data apdorojama per:

build_ctx()

Rezultatas:

context features

Pvz:

trend_dir
trend_strength
dev_up
dev_dn
impulse_recent
range_hi
range_lo
4. REGIME ENGINE

Regime engine nustato rinkos struktūrą.

Naudojamas:

phase_router
decide_phase()

Rezultatai:

PHASE_TREND_UP
PHASE_TREND_DOWN
PHASE_RANGE

Regime yra strategijos pagrindas.

Macro negali override'inti regime.

Macro gali tik:

allow
reduce
block extreme
5. SIGNAL ENGINE

Signal engine generuoja setup'us.

Pagrindiniai modeliai:

TDP_REENTRY
RANGE_TOP_SHORT_V2

Edge branduolys:

TDP_REENTRY
+
TREND phases

Signal engine tik generuoja setup'us.

Signal ≠ trade.

6. SETUP LIFECYCLE

Setup'ai turi gyvenimo ciklą.

setup_detected
↓
setup_active
↓
entry_window
↓
invalidated
↓
closed

Parametrai:

setup_age_candles
emit_last_candles
live_max_setup_age_candles

Tai leidžia:

entry kelias žvakes po setup
7. SIGNAL FILTERS

Signalai filtruojami per kelis sluoksnius.

Filtrai:

phase alignment
signal cluster filter
TTS gate
macro permission
risk filters

Tik signalai, kurie praeina filtrus, tampa:

trade candidates
8. CAPITAL ENGINE

Capital engine paskirsto kapitalą.

Setup tiers:

A setups
B setups
C setups

Risk:

A = 0.2%
B = 0.1%
C = 0–0.05%

Symbol weighting:

BTC = core
ETH = core
SOL = secondary
XRP = optional
9. RISK ENGINE

Risk engine saugo kapitalą.

Taisyklės:

max risk per trade
max risk per day
max open trades
max correlated exposure
drawdown protection

Svarbus komponentas:

GLOBAL_RISK_GUARD
10. EXECUTION ENGINE

Execution engine vykdo trade'us.

Komponentai:

order manager
position manager
TP/SL handler
timeout handler

Stebima:

latency
slippage
fills

Agentai:

RUNNER_OWNER
LATENCY_MONITOR
SLIPPAGE_MONITOR
11. MONITORING

Monitoring sistema seka:

system health
runner state
signal pipeline
market data feed

Watchdog agentai:

SYSTEM_HEALTH_MONITOR
RUNNER_WATCHDOG
FEED_WATCHDOG
SIGNAL_WATCHDOG
12. CSV DATA WAREHOUSE

Visi rezultatai saugomi CSV.

Pvz:

trades.csv
live_entries.csv
equity_curve.csv
symbol_performance.csv

Šie failai leidžia atlikti:

edge analysis
performance diagnostics
strategy evolution
13. RESEARCH PLATFORM

CSV duomenys naudojami research cikle.

CSV
↓
edge analysis
↓
model improvements
↓
new deployments

Agentai:

CSV_ANALYST
ENTRY_MODEL_AUDITOR
FILTER_CLUSTER_OWNER
14. AI GOVERNANCE

Sistema turi AI organizaciją.

Sluoksniai:

Governance
Research
Strategy
Execution
Risk
Monitoring
Delivery

Tai leidžia sistemai:

analizuoti save
aptikti problemas
siūlyti patobulinimus
15. STRATEGY EVOLUTION LOOP

Sistema evoliucionuoja per research ciklą.

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

Tai apsaugo nuo:

strategy decay
16. FINAL SYSTEM GOAL

Galutinis tikslas:

AI managed systematic trading platform

Sistema turi:

multiple strategies
capital allocation
risk management
research automation