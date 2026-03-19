from __future__ import annotations

import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROMPT_ROOT = PROJECT_ROOT / "prompt" / "Anjusik_traiding_system"
AGENTS_DIR = PROMPT_ROOT / "Anjusik" / "agents"

REQUIRED_FIELDS = [
    ("AGENT NAME", re.compile(r"^\s*AGENT NAME:\s*", re.IGNORECASE | re.MULTILINE)),
    ("AGENT ROLE", re.compile(r"^\s*AGENT ROLE:\s*", re.IGNORECASE | re.MULTILINE)),
    ("Agent Layer", re.compile(r"^\s*Agent Layer:\s*", re.IGNORECASE | re.MULTILINE)),
    ("Supervisor", re.compile(r"^\s*Supervisor:\s*", re.IGNORECASE | re.MULTILINE)),
    ("RESTRICTIONS", re.compile(r"^\s*RESTRICTIONS\s*$", re.IGNORECASE | re.MULTILINE)),
    ("OWNER AUTHORITY", re.compile(r"^\s*OWNER AUTHORITY\s*$", re.IGNORECASE | re.MULTILINE)),
]

def main() -> None:
    failures = []

    if not AGENTS_DIR.exists():
        raise FileNotFoundError(f"Agents directory not found: {AGENTS_DIR}")

    for path in sorted(AGENTS_DIR.rglob("*.txt")):
        text = path.read_text(encoding="utf-8", errors="ignore")
        missing = [name for name, pattern in REQUIRED_FIELDS if not pattern.search(text)]
        if missing:
            failures.append((path.relative_to(PROJECT_ROOT).as_posix(), missing))

    if not failures:
        print("All agent prompts passed integrity check.")
        return

    print("Prompt integrity issues found:\n")
    for file_path, missing in failures:
        print(f"- {file_path}")
        for item in missing:
            print(f"  * Missing: {item}")
        print()

if __name__ == "__main__":
    main()