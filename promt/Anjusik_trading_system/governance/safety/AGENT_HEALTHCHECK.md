# AGENT HEALTHCHECK
## Anjusik Trading System


--------------------------------
PURPOSE
--------------------------------

This document defines how to audit the health and completeness of the AI agent organization.


--------------------------------
HEALTHCHECK OBJECTIVES
--------------------------------

The healthcheck verifies:

- all registry agents exist
- all prompt files exist
- no duplicate agents exist
- hierarchy is consistent
- prompts contain required sections
- protected governance remains intact


--------------------------------
HEALTHCHECK CHECKLIST
--------------------------------

1. REGISTRY CHECK

Verify that:
- every active agent is listed in AGENT_REGISTRY.md
- no inactive or duplicate agent remains in the registry


2. FILE CHECK

Verify that:
- each listed agent has exactly one prompt file
- no duplicate agent exists in multiple folders
- folder paths match the registry


3. PROMPT QUALITY CHECK

Verify that prompts include:

- Agent Name
- Agent Role
- Agent Layer
- Supervisor
- Project Context
- Agent Purpose
- Responsibilities
- Input Sources
- Outputs
- Escalation Rules
- Restrictions
- Owner Authority


4. HIERARCHY CHECK

Verify that:
- supervisors are valid
- authority chain is coherent
- no agent bypasses Owner authority
- governance agents are not subordinated incorrectly


5. GOVERNANCE CHECK

Verify that:
- AGENT_REGISTRY.md is protected
- PROJECT_CONSTITUTION.md is protected
- TREASURY_PROTOCOL.txt is protected
- OWNER_PROTOCOL.txt is protected


6. DUPLICATE CHECK

Verify that:
- there are no duplicate agents in multiple folders
- there are no stale old prompt files


7. PROTECTED AUTHORITY CHECK

Verify that no prompt implies:
- agent ownership
- treasury control
- governance override
- legal authority above ANJUSIK


--------------------------------
HEALTH STATUS LEVELS
--------------------------------

GREEN
All checks pass.

YELLOW
Minor inconsistencies exist but system remains safe.

RED
Critical governance or structural problem detected.


--------------------------------
RECOMMENDED AUDIT ORDER
--------------------------------

1. Owner / Leadership
2. Legal
3. Accounting
4. Intelligence
5. Strategy
6. Execution
7. Risk
8. Data
9. Monitoring
10. Research
11. Capital Allocation
12. Security
13. Support


--------------------------------
FAILURE CONDITIONS
--------------------------------

A RED condition exists if:

- Owner authority is weakened
- duplicate critical agent exists
- protected documents can be modified by non-owner agents
- supervisor chain is broken
- treasury agents can change distribution rules


--------------------------------
OUTPUT FORMAT
--------------------------------

Agent
Registry status
File status
Prompt quality
Hierarchy status
Health result
Notes


--------------------------------
FINAL RULE
--------------------------------

All healthcheck actions are subordinate to ANJUSIK.