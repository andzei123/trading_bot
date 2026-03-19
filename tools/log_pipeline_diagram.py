#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple, Dict


# --- Tag patterns (edit if your logs evolve) ---
TAG_PATTERNS: List[Tuple[str, re.Pattern]] = [
    ("BOOT", re.compile(r"\[BOOT\]")),
    ("RUN_MODE", re.compile(r"\[RUN_MODE\]")),
    ("SYMBOL_PERF", re.compile(r"\[SYMBOL_PERF\]")),
    ("PYRAMID", re.compile(r"\[PYRAMID\]")),
    ("PORTFOLIO_CAP", re.compile(r"\[PORTFOLIO_CAP\]")),
    ("LIQ", re.compile(r"\[LIQ\]")),
    ("REGIME", re.compile(r"\[REGIME\]")),
    ("WATCHDOG", re.compile(r"\[WATCHDOG\]")),
    ("CROSS_ASSET", re.compile(r"\[CROSS_ASSET\]")),
    ("PHASE_PRE", re.compile(r"\[PHASE_PRE\]")),
    ("ENTRY_DIAG", re.compile(r"\[ENTRY_DIAG\]")),
    ("DEBUG_ENTRY_FORCE", re.compile(r"\[DEBUG_ENTRY_FORCE\]")),
    ("CLUSTER_FILTER", re.compile(r"\[CLUSTER_FILTER\]")),
    ("EXEC_QUALITY", re.compile(r"\[EXEC_QUALITY\]")),
    ("VOL_REGIME", re.compile(r"\[VOL_REGIME\]")),
    ("CORR_CAP", re.compile(r"\[CORR_CAP\]")),
    ("BUDGET", re.compile(r"\[BUDGET\]")),
    ("CONTEXT", re.compile(r"\[CONTEXT\]")),
    ("EQUITY_GOVERNOR", re.compile(r"\[EQUITY_GOVERNOR\]")),
    ("KILL_SWITCH", re.compile(r"\[KILL_SWITCH\]")),
    ("PHASE", re.compile(r"\[PHASE\]")),
]

# governance contract tags you want always visible
REQUIRED_AT_LEAST_ONCE = [
    "WATCHDOG",
    "CORR_CAP",
    "BUDGET",
    "EQUITY_GOVERNOR",
    "KILL_SWITCH",
]

# contract expectation (example): LIQ exactly once per run (you can relax)
EXPECTED_EXACTLY_ONCE = ["BOOT"]  # you can add "LIQ" if you enforce it


@dataclass
class ParseResult:
    sequence: List[str]
    counts: Dict[str, int]
    warnings: List[str]


def parse_log(text: str) -> ParseResult:
    seq: List[str] = []
    counts: Dict[str, int] = {name: 0 for name, _ in TAG_PATTERNS}

    for line in text.splitlines():
        for name, pat in TAG_PATTERNS:
            if pat.search(line):
                counts[name] += 1
                # only record a step when it changes (avoid spam)
                if not seq or seq[-1] != name:
                    seq.append(name)

    warnings: List[str] = []

    # required tags check
    for name in REQUIRED_AT_LEAST_ONCE:
        if counts.get(name, 0) == 0:
            warnings.append(f"Missing required tag: [{name}] (count=0)")

    # exactly once checks
    for name in EXPECTED_EXACTLY_ONCE:
        c = counts.get(name, 0)
        if c != 1:
            warnings.append(f"Expected exactly once: [{name}] but count={c}")

    # KPI_VALIDATION vs debug forced check
    if re.search(r"\[RUN_MODE\]\s+mode=KPI_VALIDATION", text):
        if counts.get("DEBUG_ENTRY_FORCE", 0) > 0:
            warnings.append("KPI_VALIDATION mode but DEBUG_ENTRY_FORCE appeared (should be 0)")

    # LIQ contract suggestion
    if counts.get("LIQ", 0) == 0:
        warnings.append("No [LIQ] line seen. If LIQ is part of bootstrap contract, enforce 1 line per run.")

    return ParseResult(sequence=seq, counts=counts, warnings=warnings)


def mermaid_flow(seq: List[str]) -> str:
    if not seq:
        return "flowchart TD\n  A[No tags detected]"
    lines = ["flowchart TD"]
    # nodes
    for name in seq:
        lines.append(f"  {name}[{name}]")
    # edges
    for a, b in zip(seq, seq[1:]):
        lines.append(f"  {a} --> {b}")
    return "\n".join(lines)


def graphviz_dot(seq: List[str]) -> str:
    if not seq:
        return "digraph G { label=\"No tags detected\"; }"
    lines = [
        "digraph G {",
        "  rankdir=LR;",
        "  node [shape=box];",
    ]
    for name in seq:
        lines.append(f"  {name} [label=\"{name}\"];")
    for a, b in zip(seq, seq[1:]):
        lines.append(f"  {a} -> {b};")
    lines.append("}")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate pipeline diagram from live_signal_runner logs.")
    ap.add_argument("--in", dest="inp", required=True, help="Path to log file (txt)")
    ap.add_argument("--outdir", default="artifacts/log_diagrams", help="Output directory")
    args = ap.parse_args()

    inp = Path(args.inp)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    text = inp.read_text(encoding="utf-8", errors="replace")
    res = parse_log(text)

    # write artifacts
    (outdir / "pipeline.mmd").write_text(mermaid_flow(res.sequence), encoding="utf-8")
    (outdir / "pipeline.dot").write_text(graphviz_dot(res.sequence), encoding="utf-8")

    # summary report
    report_lines = []
    report_lines.append(f"INPUT: {inp}")
    report_lines.append("")
    report_lines.append("SEQUENCE:")
    report_lines.append("  " + " -> ".join(res.sequence) if res.sequence else "  (none)")
    report_lines.append("")
    report_lines.append("COUNTS:")
    for name, _ in TAG_PATTERNS:
        c = res.counts.get(name, 0)
        if c:
            report_lines.append(f"  {name}: {c}")
    report_lines.append("")
    report_lines.append("WARNINGS:")
    if res.warnings:
        for w in res.warnings:
            report_lines.append(f"  - {w}")
    else:
        report_lines.append("  (none)")

    (outdir / "report.txt").write_text("\n".join(report_lines), encoding="utf-8")

    print(f"Wrote:\n  {outdir/'pipeline.mmd'}\n  {outdir/'pipeline.dot'}\n  {outdir/'report.txt'}")
    if res.warnings:
        print("\nWARNINGS:")
        for w in res.warnings:
            print(" -", w)


if __name__ == "__main__":
    main()