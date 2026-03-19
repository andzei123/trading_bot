# ANJUSIK TRADING SYSTEM
## AI AGENT OPERATING RULES


--------------------------------
SYSTEM GOVERNANCE
--------------------------------

All AI agents operate under the authority of the project owner.

Owner:

ANJUSIK  
Andžej Volosevič

Lead Orchestrator:

JUMI

All AI actions must follow the governance structure defined in:

PROJECT_CONSTITUTION.md


--------------------------------
CORE PRINCIPLES
--------------------------------

AI agents must follow these principles.

1. Safety first

No change may introduce risk to the trading system.


2. Minimal change policy

Changes must be as small and targeted as possible.


3. Diagnostic before modification

Agents must investigate problems before proposing fixes.


4. No silent changes

All changes must be visible and documented.


5. Owner authority

The owner may override any AI decision.


--------------------------------
PATCH RULES
--------------------------------

All system modifications must follow the patch workflow.

Agents must provide:

Patch summary  
Unified diff  
Files changed  
Validation commands  
Expected logs


Agents must NOT provide full file rewrites unless necessary.


--------------------------------
FORBIDDEN ACTIONS
--------------------------------

AI agents are not allowed to:

- modify strategy logic without owner approval
- modify risk rules without owner approval
- modify treasury distribution
- execute live trading changes automatically
- delete core system components
- modify governance documents


--------------------------------
STRATEGY SAFETY
--------------------------------

Strategy logic includes:

entry models  
filter clusters  
phase routing  
signal scoring


Agents must treat strategy logic as sensitive.

Any strategy change requires explicit owner approval.


--------------------------------
RISK SAFETY
--------------------------------

Risk layer must never be weakened.

Risk components include:

drawdown limits  
position limits  
exposure monitoring


Agents must not bypass risk systems.


--------------------------------
TREASURY SAFETY
--------------------------------

Treasury accounting is maintained by:

CSV_ACCOUNTANT


AI agents must not modify:

distribution percentages  
treasury ledger history


Only the owner may change treasury rules.


--------------------------------
INTELLIGENCE SAFETY
--------------------------------

Market intelligence includes:

macro signals  
liquidity events  
news events


Intelligence decisions are handled by:

INTELLIGENCE_DIRECTOR  
RYZYK  
ACCOUNTING_ASSISTANT


AI agents must not suppress intelligence signals.


--------------------------------
EXECUTION SAFETY
--------------------------------

Execution systems must remain stable.

Agents must not modify:

order routing  
execution pipeline  
runner logic

without validation.


--------------------------------
VALIDATION REQUIREMENTS
--------------------------------

All patches must include validation instructions.

Examples:

run unit tests  
run runner diagnostics  
verify logs


Agents must specify expected logs.


--------------------------------
DOCUMENTATION RULES
--------------------------------

All significant system changes must update documentation.

Examples:

PROJECT_DASHBOARD.md  
AUTOMATED_AGENT_REPORT.md  
CHANGELOG_AI_TEAM.md


--------------------------------
ERROR HANDLING
--------------------------------

When encountering errors:

1. investigate root cause  
2. report findings  
3. propose minimal patch  


Agents must not guess solutions.


--------------------------------
COMMUNICATION RULES
--------------------------------

Agents must report through proper channels.

Governance decisions:

CORE_GOVERNANCE_CHAT


Operations coordination:

RYZYK


Intelligence discussions:

INTELLIGENCE_DECISION_CHAT


--------------------------------
OWNER OVERRIDE
--------------------------------

The owner may override any AI decision.

Owner commands take priority over all AI instructions.


--------------------------------
FINAL RULE
--------------------------------

AI agents exist to assist the owner.

They do not replace the owner's authority.

The system operates under human supervision unless explicitly configured otherwise.


ERROR HANDLING

If an agent detects uncertainty,
missing information,
or conflicting instructions,
the agent must request clarification
before proceeding.

Agents must not fabricate information
or assume missing technical details.

DOCUMENTATION RULE

All significant actions must be documented.

Agents must produce:

reports
patch summaries
validation outputs

MINIMAL CHANGE PRINCIPLE

Agents must prefer minimal,
targeted modifications.

Agents must not redesign architecture
unless explicitly instructed.