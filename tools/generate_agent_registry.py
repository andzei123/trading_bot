from __future__ import annotations

import re
from pathlib import Path
from collections import defaultdict
from typing import Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def find_prompt_root(project_root: Path) -> Path:
    candidates = []

    for base_name in ("promt", "prompt"):
        base_dir = project_root / base_name
        if base_dir.exists() and base_dir.is_dir():
            for child in base_dir.iterdir():
                if child.is_dir() and child.name.lower().startswith("anjusik") and "system" in child.name.lower():
                    candidates.append(child)

    if not candidates:
        raise FileNotFoundError(
            "Could not find prompt root. Expected something like "
            "'promt/Anjusik_trading_system' or 'prompt/Anjusik_trading_system'."
        )

    return candidates[0]


PROMPT_ROOT = find_prompt_root(PROJECT_ROOT)
AGENTS_DIR = PROMPT_ROOT / "Anjusik" / "agents"
GOVERNANCE_DIR = PROMPT_ROOT / "governance"
GENERATED_DIR = GOVERNANCE_DIR / "generated"

MANUAL_REGISTRY = AGENTS_DIR / "AGENT_REGISTRY.md"
GENERATED_REGISTRY = GENERATED_DIR / "AGENT_REGISTRY_GENERATED.md"
DIFF_REPORT = GENERATED_DIR / "AGENT_REGISTRY_DIFF.md"

FIELD_PATTERNS = {
    "agent_name": re.compile(r"^\s*AGENT NAME:\s*(.*?)\s*$", re.IGNORECASE),
    "agent_role": re.compile(r"^\s*AGENT ROLE:\s*(.*?)\s*$", re.IGNORECASE),
    "supervisor": re.compile(r"^\s*Supervisor:\s*(.*?)\s*$", re.IGNORECASE),
    "agent_layer": re.compile(r"^\s*Agent Layer:\s*(.*?)\s*$", re.IGNORECASE),
}


def extract_prompt_field(text: str, field: str) -> str:
    pattern = FIELD_PATTERNS[field]
    lines = text.splitlines()

    for i, line in enumerate(lines):
        match = pattern.match(line)
        if match:
            value = match.group(1).strip()
            if value:
                return value
            if i + 1 < len(lines):
                return lines[i + 1].strip()

    return "MISSING"


def folder_title(folder_name: str) -> str:
    return folder_name.replace("_", " ").replace("-", " ").title()


def scan_prompt_agents() -> Dict[str, List[dict]]:
    grouped: Dict[str, List[dict]] = defaultdict(list)

    for path in sorted(AGENTS_DIR.rglob("*.txt")):
        if path.name.startswith("."):
            continue

        rel_path = path.relative_to(AGENTS_DIR).as_posix()
        folder = path.parent.relative_to(AGENTS_DIR).as_posix()
        text = path.read_text(encoding="utf-8", errors="ignore")

        entry = {
            "path": rel_path,
            "agent_name": extract_prompt_field(text, "agent_name"),
            "agent_role": extract_prompt_field(text, "agent_role"),
            "supervisor": extract_prompt_field(text, "supervisor"),
            "agent_layer": extract_prompt_field(text, "agent_layer"),
        }
        grouped[folder].append(entry)

    for folder in grouped:
        grouped[folder] = sorted(grouped[folder], key=lambda x: x["agent_name"])

    return dict(sorted(grouped.items(), key=lambda x: x[0]))


def flatten_prompt_agents(grouped: Dict[str, List[dict]]) -> Dict[str, dict]:
    result: Dict[str, dict] = {}
    for agents in grouped.values():
        for agent in agents:
            result[agent["agent_name"]] = agent
    return result


def parse_manual_registry(path: Path) -> Dict[str, dict]:
    if not path.exists():
        return {}

    text = path.read_text(encoding="utf-8", errors="ignore")
    lines = text.splitlines()

    agents: Dict[str, dict] = {}
    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Matches lines like: "1. JUMI"
        m = re.match(r"^(\d+)\.\s+([A-Z0-9_]+)$", line)
        if not m:
            i += 1
            continue

        number = m.group(1)
        name = m.group(2)

        role = "MISSING"
        prompt_file = "MISSING"
        supervisor = "MISSING"
        agent_layer = "MISSING"

        i += 1
        while i < len(lines):
            current = lines[i].strip()

            # Stop if next agent block starts
            if re.match(r"^\d+\.\s+[A-Z0-9_]+$", current):
                break

            if current == "Role:" and i + 1 < len(lines):
                role = lines[i + 1].strip()

            if current == "Prompt File:" and i + 1 < len(lines):
                prompt_file = lines[i + 1].strip()

            if current == "Supervisor:" and i + 1 < len(lines):
                supervisor = lines[i + 1].strip()

            if current == "Operational Supervisor:" and i + 1 < len(lines):
                supervisor = lines[i + 1].strip()

            if current == "Reporting Line:" and i + 1 < len(lines):
                supervisor = lines[i + 1].strip()

            i += 1

        agents[name] = {
            "number": number,
            "agent_name": name,
            "agent_role": role,
            "prompt_file": prompt_file,
            "supervisor": supervisor,
            "agent_layer": agent_layer,  # manual registry doesn't explicitly store it
        }

    return agents


def render_generated_registry(grouped: Dict[str, List[dict]]) -> str:
    total_agents = sum(len(v) for v in grouped.values())

    lines: List[str] = []
    lines.append("# AI AGENT REGISTRY GENERATED")
    lines.append("## Anjusik Trading System")
    lines.append("")
    lines.append("--------------------------------")
    lines.append("SYSTEM METADATA")
    lines.append("--------------------------------")
    lines.append("")
    lines.append("Owner:")
    lines.append("ANJUSIK")
    lines.append("")
    lines.append("Legal Identity:")
    lines.append("Andžej Volosevič")
    lines.append("")
    lines.append("Registry Source:")
    lines.append("Auto-generated from agent prompt files")
    lines.append("")
    lines.append("Prompt Root:")
    lines.append(str(PROMPT_ROOT))
    lines.append("")
    lines.append("Total Agents Found:")
    lines.append(str(total_agents))
    lines.append("")
    lines.append("--------------------------------")
    lines.append("OWNER SOVEREIGNTY")
    lines.append("--------------------------------")
    lines.append("")
    lines.append("All agents operate under the authority of ANJUSIK.")
    lines.append("This generated registry is diagnostic and must not override the manual registry.")
    lines.append("")

    for folder, agents in grouped.items():
        lines.append("--------------------------------")
        lines.append(folder_title(folder))
        lines.append("--------------------------------")
        lines.append("")
        for agent in agents:
            lines.append(f"Agent Name: {agent['agent_name']}")
            lines.append(f"Agent Role: {agent['agent_role']}")
            lines.append(f"Agent Layer: {agent['agent_layer']}")
            lines.append(f"Supervisor: {agent['supervisor']}")
            lines.append(f"Prompt File: Anjusik/agents/{agent['path']}")
            lines.append("")
        lines.append("")

    lines.append("--------------------------------")
    lines.append("FINAL RULE")
    lines.append("--------------------------------")
    lines.append("")
    lines.append("Any governance-sensitive registry change must still be approved by ANJUSIK.")
    lines.append("")

    return "\n".join(lines)


def render_diff_report(prompt_agents: Dict[str, dict], manual_agents: Dict[str, dict]) -> str:
    prompt_names = set(prompt_agents.keys())
    manual_names = set(manual_agents.keys())

    missing_in_manual = sorted(prompt_names - manual_names)
    missing_in_prompts = sorted(manual_names - prompt_names)
    shared = sorted(prompt_names & manual_names)

    mismatches: List[str] = []

    for name in shared:
        p = prompt_agents[name]
        m = manual_agents[name]

        if p["agent_role"] != m["agent_role"]:
            mismatches.append(
                f"- {name}: AGENT ROLE mismatch\n"
                f"  prompt: {p['agent_role']}\n"
                f"  manual: {m['agent_role']}\n"
            )

        if p["supervisor"] != m["supervisor"]:
            mismatches.append(
                f"- {name}: Supervisor mismatch\n"
                f"  prompt: {p['supervisor']}\n"
                f"  manual: {m['supervisor']}\n"
            )

        expected_prompt_file = f"Anjusik/agents/{p['path']}"
        if expected_prompt_file != m["prompt_file"]:
            mismatches.append(
                f"- {name}: Prompt File mismatch\n"
                f"  prompt: {expected_prompt_file}\n"
                f"  manual: {m['prompt_file']}\n"
            )

    lines: List[str] = []
    lines.append("# AGENT REGISTRY DIFF REPORT")
    lines.append("## Anjusik Trading System")
    lines.append("")
    lines.append("--------------------------------")
    lines.append("PURPOSE")
    lines.append("--------------------------------")
    lines.append("")
    lines.append("Compare prompt-derived agent structure against the manual governance registry.")
    lines.append("")

    lines.append("--------------------------------")
    lines.append("AGENTS FOUND IN PROMPTS BUT MISSING IN MANUAL REGISTRY")
    lines.append("--------------------------------")
    lines.append("")
    if missing_in_manual:
        lines.extend(f"- {name}" for name in missing_in_manual)
    else:
        lines.append("None")
    lines.append("")

    lines.append("--------------------------------")
    lines.append("AGENTS FOUND IN MANUAL REGISTRY BUT MISSING IN PROMPTS")
    lines.append("--------------------------------")
    lines.append("")
    if missing_in_prompts:
        lines.extend(f"- {name}" for name in missing_in_prompts)
    else:
        lines.append("None")
    lines.append("")

    lines.append("--------------------------------")
    lines.append("FIELD MISMATCHES")
    lines.append("--------------------------------")
    lines.append("")
    if mismatches:
        lines.extend(mismatches)
    else:
        lines.append("None")
    lines.append("")

    lines.append("--------------------------------")
    lines.append("SUMMARY")
    lines.append("--------------------------------")
    lines.append("")
    lines.append(f"Prompt agents found: {len(prompt_agents)}")
    lines.append(f"Manual registry agents found: {len(manual_agents)}")
    lines.append("")

    lines.append("--------------------------------")
    lines.append("FINAL RULE")
    lines.append("--------------------------------")
    lines.append("")
    lines.append("Manual governance registry remains the source of truth unless updated by ANJUSIK.")
    lines.append("Generated output is for consistency checking only.")
    lines.append("")

    return "\n".join(lines)


def main() -> None:
    if not AGENTS_DIR.exists():
        raise FileNotFoundError(f"Agents directory not found: {AGENTS_DIR}")

    GENERATED_DIR.mkdir(parents=True, exist_ok=True)

    grouped = scan_prompt_agents()
    prompt_agents = flatten_prompt_agents(grouped)
    manual_agents = parse_manual_registry(MANUAL_REGISTRY)

    generated_registry_content = render_generated_registry(grouped)
    diff_report_content = render_diff_report(prompt_agents, manual_agents)

    GENERATED_REGISTRY.write_text(generated_registry_content, encoding="utf-8")
    DIFF_REPORT.write_text(diff_report_content, encoding="utf-8")

    print(f"Prompt root found: {PROMPT_ROOT}")
    print(f"Generated registry: {GENERATED_REGISTRY}")
    print(f"Diff report: {DIFF_REPORT}")
    print(f"Prompt agents found: {len(prompt_agents)}")
    print(f"Manual registry agents found: {len(manual_agents)}")


if __name__ == "__main__":
    main()