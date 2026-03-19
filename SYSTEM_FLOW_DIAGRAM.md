1. GLOBAL SYSTEM FLOW

Visa sistema:

MARKET DATA
    ↓
DATA LAYER
    ↓
SIGNAL LAYER
    ↓
RISK LAYER
    ↓
PORTFOLIO LAYER
    ↓
EXECUTION LAYER
    ↓
MONITORING
2. DATA FLOW

Sistema pradeda nuo duomenų.

Bybit API
CSV data
Macro data
Liquidation WS
Funding rates

↓

DATA LAYER
DATA LAYER modules
market_data_bybit.py
market_data_csv.py
liq_ws.py
macro_provider.py
funding_provider.py
3. SIGNAL GENERATION FLOW

Kai data paruošta, prasideda signal generation.

CANDLES
    ↓
PHASE_PRE
    ↓
PHASE_ROUTER
    ↓
ENTRY_MODEL
PHASE_PRE

Nustato:

trend direction
structure
context
PHASE_ROUTER

Nustato:

TREND
RANGE
ROTATION
ENTRY_MODEL

Generuoja:

trade candidates

Pvz:

TDP
TTS
pullback
breakout
4. CONTEXT FILTERS

Po signal generation veikia context filters.

ENTRY CANDIDATES
    ↓
MACRO_GATE
    ↓
NEWS_GATE
    ↓
LIQUIDITY_GATE
MACRO_GATE

Tikrina:

BTC trend
DXY
market risk
NEWS_GATE

Blokuoja trades per:

major news
events
LIQ_GATE

Tikrina:

liquidation pressure
volatility spikes
5. TTS CONFIRMATION GATE

TTS naudojamas kaip:

confirmation filter

Pipeline:

CONTEXT_PASS
    ↓
TTS_GATE
TTS_GATE tikrina
HTF confirmation
retest structure
momentum
output
allow_trade = True / False
6. REGIME FILTER

Regime controller nustato strategijų leidimą.

trade candidates
    ↓
REGIME_CONTROLLER
regimes
NORMAL
DEFENSIVE
OFF
regime controls
allow_models
block_models
max_positions
7. RISK ENGINE

Po regime filter veikia risk engine.

REGIME_PASS
    ↓
CORR_CAP
    ↓
CAPITAL_BUDGET
    ↓
EQUITY_GOVERNOR
    ↓
KILL_SWITCH
CORR_CAP

Riboja:

high correlation trades
CAPITAL_BUDGET

Kontroliuoja:

capital per side
capital per regime
global capital
EQUITY_GOVERNOR

Tikrina:

current drawdown
risk scaling
KILL_SWITCH

Sustabdo trading jei:

rolling loss threshold hit
8. PORTFOLIO ENGINE

Valdo visą kapitalą.

VALID TRADES
    ↓
PORTFOLIO ENGINE
components
exposure engine
capital allocator
regime allocator
portfolio controls
BTC exposure
ALT exposure
sector exposure
strategy exposure
9. EXECUTION ENGINE

Kai trade patvirtintas:

EXECUTION ENGINE
components
order sizing
slippage estimation
order routing
output
place order
10. MONITORING SYSTEM

Sistema viską registruoja.

TRADES
    ↓
MONITORING
monitoring modules
trade journal
equity curve
risk alerts
watchdog
performance analytics
11. SYSTEM PIPELINE (FULL)

Pilnas pipeline:

MARKET DATA
    ↓
DATA LAYER
    ↓
PHASE_PRE
    ↓
PHASE_ROUTER
    ↓
ENTRY_MODEL
    ↓
MACRO_GATE
    ↓
NEWS_GATE
    ↓
LIQ_GATE
    ↓
TTS_GATE
    ↓
REGIME_CONTROLLER
    ↓
CORR_CAP
    ↓
CAPITAL_BUDGET
    ↓
EQUITY_GOVERNOR
    ↓
KILL_SWITCH
    ↓
PORTFOLIO_ENGINE
    ↓
EXECUTION_ENGINE
    ↓
MONITORING
12. SYSTEM COMPONENT MAP

Visa sistema:

backtest/
│
├─ data
│
├─ live_pipeline
│   ├─ pipeline_core.py
│   └─ adapters
│
├─ strategies
│   ├─ entry_model.py
│   └─ registry
│
├─ risk
│   ├─ corr_cap.py
│   ├─ capital_budget.py
│   ├─ equity_governor.py
│   ├─ kill_switch.py
│   └─ volatility_targeting.py
│
├─ portfolio
│   ├─ capital_allocator.py
│   ├─ regime_allocator.py
│   └─ strategy_correlation.py
│
├─ execution
│   ├─ order_router.py
│   └─ slippage_model.py
│
└─ monitoring
    ├─ watchdog.py
    └─ performance_tracker.py
13. FINAL TARGET STATE

Kai visi komponentai veikia, sistema tampa:

multi strategy trading engine
risk governed
capital scalable
portfolio intelligent

Tai yra:

institutional trading infrastructure