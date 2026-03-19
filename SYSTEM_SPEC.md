Trading System Architecture & Specification

Version: 1.0
Status: Active
Purpose: Define deterministic architecture for trading system.

1. SYSTEM GOAL

Sistema skirta:

systematic multi-strategy trading
risk-governed capital allocation
portfolio-level intelligence

Galutinis tikslas:

institutional-grade trading engine
2. CORE DESIGN PRINCIPLES

Sistema turi laikytis šių principų.

2.1 Deterministic execution

Tas pats input → tas pats output.

2.2 Layer separation

Kiekvienas komponentas turi vieną atsakomybę.

2.3 No hidden side effects

Pipeline funkcijos:

no file writes
no exchange calls
no prints
2.4 Runner = Orchestrator only

Runner tik koordinuoja komponentus.

3. SYSTEM ARCHITECTURE

Sistema padalinta į sluoksnius.

DATA LAYER
SIGNAL LAYER
RISK LAYER
EXECUTION LAYER
PORTFOLIO LAYER
MONITORING LAYER
4. DATA LAYER

Atsakingas už visus duomenis.

Data sources
candles
macro indicators
funding rates
orderbook
liquidations
onchain
Modules
market_data_bybit.py
market_data_csv.py
liq_ws.py
macro_provider.py
5. SIGNAL LAYER

Generuoja trade candidates.

Components
phase detection
entry models
strategy registry
Pipeline order
PHASE_PRE
PHASE_ROUTER
ENTRY_MODEL
Output

Signal layer grąžina:

trade candidates
6. RISK LAYER

Valdo visas rizikos kontrolės sistemas.

Components
context gate
macro gate
news gate
liquidity gate
TTS gate
correlation cap
capital budget
equity governor
kill switch
volatility targeting
Risk pipeline
context gate
↓
TTS gate
↓
regime filter
↓
correlation cap
↓
capital budget
↓
equity governor
↓
kill switch
7. EXECUTION LAYER

Valdo order execution.

Components
slippage estimator
order sizing
order routing
order slicing
8. PORTFOLIO LAYER

Valdo visą kapitalą.

Components
exposure engine
capital allocator
regime allocator
strategy correlation
capital throttle
9. MONITORING LAYER

Sistema turi turėti monitoring.

Components
watchdog
performance analytics
risk alerts
trade journal
equity tracking
10. CORE PIPELINE

Pagrindinė pipeline funkcija:

run_cycle(ctx) -> result
ctx turi
candles
macro snapshot
liq snapshot
portfolio state
run_mode
system flags
result turi
entries
dropped_rows
events
status
11. PIPELINE ORDER

Pilnas pipeline:

DATA LOAD
↓
PHASE_PRE
↓
PHASE_ROUTER
↓
ENTRY_MODEL
↓
CONTEXT_GATE
↓
TTS_GATE
↓
REGIME_FILTER
↓
CORR_CAP
↓
CAPITAL_BUDGET
↓
EQUITY_GOVERNOR
↓
KILL_SWITCH
↓
PORTFOLIO_FILTER
↓
EMIT_SIGNALS
12. LOG CONTRACT

Sistema turi naudoti standartizuotus log tagus.

BOOT
[BOOT] symbols=[...] interval=...
RUN MODE
[RUN_MODE] mode=KPI_VALIDATION
LIQUIDATION ENGINE
[LIQ] WS started for N symbols
REGIME
[REGIME] NORMAL | reason
WATCHDOG
[WATCHDOG] ok lag_s=...
CONTEXT
[CONTEXT] allow=True risk=0.5
TTS GATE
[TTS_GATE] allow=True bias=UP
PHASE
[PHASE] RANGE | macro_bias=ALT_SHORT
KILL SWITCH
[KILL_SWITCH] threshold=-10 window=7d rolling_R=-11
13. STRATEGY REGISTRY

Strategijos registruojamos:

strategies/registry/

Example:

range_short_v2.yaml
trend_pullback_v1.yaml
Strategy metadata
strategy:
    capital: 0.2
    risk_per_trade: 0.5%
14. PERFORMANCE VALIDATION

Strategijos turi praeiti KPI gates.

KPI requirements
KPI	Threshold
Sharpe	>1.5
Max DD	<20%
Profit factor	>1.3
Winrate	>40%
Expectancy	>0
Walk forward
Train 2023
Test 2024
Forward 2025
Monte Carlo robustness

Randomize:

slippage
execution delay
entry order

Jei Sharpe krenta <0.3:

model fragile
15. LIVE GOVERNANCE

Sistema turi automatiškai išjungti strategijas.

Example:

if rolling_30d_sharpe < 0:
    disable strategy
16. CAPITAL ENGINE

Kapitalas paskirstomas pagal:

strategy performance
market regime
portfolio volatility

Example:

if strategy_R_30d < 0:
    allocation *= 0.5
17. PORTFOLIO INTELLIGENCE

Sistema turi:

strategy correlation matrix
volatility targeting
capital throttle
Volatility targeting

Target:

portfolio volatility = 15%
Capital throttle

Jei:

drawdown > X
volatility HIGH
liquidity LOW

↓

reduce capital
18. DEVELOPMENT RULES

Programuotojai turi laikytis šių taisyklių.

Rule 1

Pipeline funkcijos:

no IO
no prints
no external calls
Rule 2

Runner turi būti:

orchestrator only
Rule 3

Visi moduliai turi turėti:

clear responsibility
Rule 4

Kiekvienas naujas modulis turi turėti:

log contract
19. TARGET STATE

Po pilnos evoliucijos sistema tampa:

multi-strategy capital engine
dynamic allocation
portfolio intelligence
risk governed

Tai yra:

institutional-grade trading infrastructure