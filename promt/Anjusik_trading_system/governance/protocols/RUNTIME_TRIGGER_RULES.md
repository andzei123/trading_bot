# RUNTIME TRIGGER RULES
## Anjusik Trading System


--------------------------------
PURPOSE
--------------------------------

Define runtime conditions that trigger system responses in risk, intelligence, and operations layers.


--------------------------------
INTELLIGENCE TRIGGERS
--------------------------------

Responsible agents:

INTELLIGENCE_DIRECTOR  
MACRO_OWNER  
LIQUIDITY_OWNER  
NEWS_OWNER  


Trigger conditions may include:

1. Macro Regime Shift

Examples:

- strong macro trend change
- global market regime change

Action:

INTELLIGENCE_DIRECTOR reports regime shift to:

JUMI  
RYZYK


2. Liquidity Shock

Examples:

- large liquidation cascade
- abnormal order book imbalance

Action:

LIQUIDITY_OWNER alerts INTELLIGENCE_DIRECTOR.


3. News Event

Examples:

- major macroeconomic announcement
- regulatory shock

Action:

NEWS_OWNER flags event and logs in intelligence input logs.


--------------------------------
RISK TRIGGERS
--------------------------------

Responsible agents:

RISK_DIRECTOR  
GLOBAL_RISK_GUARD  


Risk triggers may include:

1. Drawdown Threshold

Example:

system drawdown exceeds predefined limit.

Action:

GLOBAL_RISK_GUARD alerts:

RISK_DIRECTOR  
JUMI


2. Volatility Spike

Example:

sudden abnormal volatility detected.

Action:

RISK layer recommends exposure reduction.


3. Liquidity Collapse

Example:

market liquidity deteriorates rapidly.

Action:

GLOBAL_RISK_GUARD recommends strategy pause.


--------------------------------
EXECUTION TRIGGERS
--------------------------------

Responsible agents:

EXECUTION_DIRECTOR  
RUNNER_OWNER  


Triggers include:

- execution failures
- runner crashes
- abnormal latency


Action:

RUNNER_OWNER alerts:

EXECUTION_DIRECTOR  
RYZYK


--------------------------------
SYSTEM MONITORING TRIGGERS
--------------------------------

Responsible agents:

SYSTEM_HEALTH_MONITOR  
RUNNER_WATCHDOG  
FEED_WATCHDOG  
SIGNAL_WATCHDOG


Triggers include:

- feed interruption
- signal pipeline failure
- monitoring anomalies


Action:

SYSTEM_HEALTH_MONITOR escalates to:

RYZYK  
JUMI


--------------------------------
ESCALATION LEVELS
--------------------------------

Level 1 – Operational issue

Handled by:

RYZYK


Level 2 – System instability

Escalate to:

JUMI


Level 3 – Critical system risk

Escalate to:

ANJUSIK


--------------------------------
FINAL RULE
--------------------------------

Owner decisions override all runtime triggers.

Owner:

ANJUSIK