#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple


# --- Contract patterns (minimal, stable) ---
CONTRACT_PATTERNS: Dict[str, re.Pattern] = {
    "BOOT": re.compile(r"^\[BOOT\]\s"),
    "RUN_MODE": re.compile(r"^\[RUN_MODE\]\s"),
    "SYMBOL_PERF": re.compile(r"^\[SYMBOL_PERF\]\s"),
    "PYRAMID": re.compile(r"^\[PYRAMID\]\s"),
    "PORTFOLIO_CAP": re.compile(r"^\[PORTFOLIO_CAP\]\s"),
    # LIQ has timestamp prefix in your logs
    "LIQ": re.compile(r"^\[\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\]\s+\[LIQ\]\s"),
    "WATCHDOG": re.compile(r"^\[WATCHDOG\]\s"),
    "CROSS_ASSET_STRENGTH": re.compile(r"^\[CROSS_ASSET\].*\bstrength="),
    "CORR_CAP": re.compile(r"^\[\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\]\s+\[CORR_CAP\]"),
    "BUDGET": re.compile(r"^\[\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\]\s+\[BUDGET\]"),
    "EQUITY_GOVERNOR": re.compile(r"^\[\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\]\s+\[EQUITY_GOVERNOR\]"),
    "KILL_SWITCH": re.compile(r"^\[\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\]\s+\[KILL_SWITCH\]"),
}

# --- Useful signals for "chaos" detection ---
DEBUG_PATTERNS: Dict[str, re.Pattern] = {
    "DEBUG_ENTRY_FORCE": re.compile(r"\[DEBUG_ENTRY_FORCE\]"),
    "DEBUG_FORCED_MODEL": re.compile(r"\bDEBUG_FORCED_(LONG|SHORT)\b"),
    "ENTRY_DIAG": re.compile(r"\[ENTRY_DIAG\]"),
}

ROUTER_HINTS: Dict[str, re.Pattern] = {
    # Heuristics: if both appear heavily, likely hybrid routing in play
    "REGIME_ROUTER": re.compile(r"\[REGIME\]\s"),
    "MACRO_GATE": re.compile(r"\bMACRO_GATE\b|\[CONTEXT\].*macro_allow="),
    "PHASE_ROUTER": re.compile(r"\[PHASE(_PRE)?\]"),
    "ROTATION": re.compile(r"\[ROTATION_(PHASE|ALLOC)\]"),
}


@dataclass
class HealthReport:
    counts: Dict[str, int]
    debug_counts: Dict[str, int]
    router_counts: Dict[str, int]
    warnings: List[str]
    notes: List[str]


def _count_matches(lines: List[str], patterns: Dict[str, re.Pattern]) -> Dict[str, int]:
    out = {k: 0 for k in patterns.keys()}
    for ln in lines:
        for k, rx in patterns.items():
            if rx.search(ln):
                out[k] += 1
    return out


def _first_run_mode(lines: List[str]) -> str | None:
    for ln in lines:
        m = re.search(r"^\[RUN_MODE\]\s+mode=(\S+)", ln)
        if m:
            return m.group(1)
    return None


def analyze(text: str) -> HealthReport:
    lines = [ln.rstrip("\n") for ln in text.splitlines()]

    counts = _count_matches(lines, CONTRACT_PATTERNS)
    debug_counts = _count_matches(lines, DEBUG_PATTERNS)
    router_counts = _count_matches(lines, ROUTER_HINTS)

    warnings: List[str] = []
    notes: List[str] = []

    # --- Contract stability checks ---
    # BOOT should be exactly 1 in typical runner output (smoke_test includes it)
    if counts["BOOT"] == 0:
        warnings.append("Missing [BOOT] — runner bootstrap not visible in log.")
    elif counts["BOOT"] > 1:
        warnings.append(f"[BOOT] appears {counts['BOOT']} times — duplicate boot sequence?")

    # LIQ contract expectation: 1 line, never 0 or >1 (if that's your contract)
    if counts["LIQ"] == 0:
        warnings.append("Missing [LIQ] — liquidation stream status line not printed (contract drift).")
    elif counts["LIQ"] > 1:
        warnings.append(f"[LIQ] appears {counts['LIQ']} times — should be exactly 1 per run.")

    # RUN_MODE should be 0 or 1 (depending on whether feature enabled)
    if counts["RUN_MODE"] > 1:
        warnings.append(f"[RUN_MODE] appears {counts['RUN_MODE']} times — should be at most 1.")
    run_mode = _first_run_mode(lines)
    if run_mode:
        notes.append(f"Detected RUN_MODE={run_mode}")

    # Phase2 contract tags should exist once (or at least once)
    for k in ["SYMBOL_PERF", "PYRAMID", "PORTFOLIO_CAP"]:
        if counts[k] == 0:
            warnings.append(f"Missing [{k}] contract log.")
        elif counts[k] > 1:
            warnings.append(f"[{k}] appears {counts[k]} times — expected 1 per run.")

    if counts["CROSS_ASSET_STRENGTH"] == 0:
        warnings.append("Missing [CROSS_ASSET] strength=... (phase2 contract drift).")
    elif counts["CROSS_ASSET_STRENGTH"] > 1:
        warnings.append(f"[CROSS_ASSET strength] appears {counts['CROSS_ASSET_STRENGTH']} times — expected 1 per run.")

    # Governance tags: these are per-symbol typically, so we just sanity-check non-zero
    for k in ["WATCHDOG", "CORR_CAP", "BUDGET", "EQUITY_GOVERNOR", "KILL_SWITCH"]:
        if counts[k] == 0:
            warnings.append(f"Missing [{k}] — governance output not present (unexpected).")

    # --- Debug forced entries / KPI_VALIDATION gating ---
    if run_mode == "KPI_VALIDATION":
        if debug_counts["DEBUG_ENTRY_FORCE"] > 0 or debug_counts["DEBUG_FORCED_MODEL"] > 0:
            warnings.append(
                "KPI_VALIDATION but DEBUG forced entries detected — gating might be broken."
            )
        else:
            notes.append("KPI_VALIDATION: OK (no DEBUG_ENTRY_FORCE / DEBUG_FORCED_*)")
    else:
        if debug_counts["DEBUG_ENTRY_FORCE"] > 0:
            notes.append(f"DEBUG_ENTRY_FORCE detected: {debug_counts['DEBUG_ENTRY_FORCE']} (expected in DEV_DEBUG)")
        if debug_counts["DEBUG_FORCED_MODEL"] > 0:
            notes.append(f"DEBUG_FORCED_* model mentions: {debug_counts['DEBUG_FORCED_MODEL']}")

    # --- Router conflict heuristic (not a hard error) ---
    # If both REGIME and MACRO_GATE are present strongly, it's "hybrid" by design now.
    if router_counts["REGIME_ROUTER"] > 0 and router_counts["MACRO_GATE"] > 0:
        notes.append(
            f"Hybrid routing signals present: REGIME={router_counts['REGIME_ROUTER']}, MACRO_GATE={router_counts['MACRO_GATE']}"
        )
        # If you want, mark as warning only when *too many* routers fire
        active_router_keys = [k for k, v in router_counts.items() if v > 0]
        if len(active_router_keys) >= 4:
            warnings.append(
                f"Many routing layers active ({', '.join(active_router_keys)}) — consider simplifying pipeline."
            )

    return HealthReport(
        counts=counts,
        debug_counts=debug_counts,
        router_counts=router_counts,
        warnings=warnings,
        notes=notes,
    )


def render_report(rep: HealthReport) -> str:
    lines: List[str] = []
    lines.append("SYSTEM HEALTH CHECK\n")

    lines.append("CONTRACT COUNTS:")
    for k in sorted(rep.counts.keys()):
        lines.append(f"  {k:20s} : {rep.counts[k]}")
    lines.append("")

    lines.append("DEBUG COUNTS:")
    for k in sorted(rep.debug_counts.keys()):
        lines.append(f"  {k:20s} : {rep.debug_counts[k]}")
    lines.append("")

    lines.append("ROUTER HINT COUNTS:")
    for k in sorted(rep.router_counts.keys()):
        lines.append(f"  {k:20s} : {rep.router_counts[k]}")
    lines.append("")

    if rep.warnings:
        lines.append("WARNINGS:")
        for w in rep.warnings:
            lines.append(f"  - {w}")
    else:
        lines.append("WARNINGS: (none)")

    if rep.notes:
        lines.append("\nNOTES:")
        for n in rep.notes:
            lines.append(f"  - {n}")

    lines.append("")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="Path to log file (utf-8).")
    ap.add_argument("--out", dest="out", default=None, help="Optional output report file.")
    args = ap.parse_args()

    inp = Path(args.inp)
    text = inp.read_text(encoding="utf-8", errors="replace")
    rep = analyze(text)
    report = render_report(rep)

    print(report)
    if args.out:
        outp = Path(args.out)
        outp.parent.mkdir(parents=True, exist_ok=True)
        outp.write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()