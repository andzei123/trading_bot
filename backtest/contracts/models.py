from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass
class EntryCandidate:
    symbol: str
    model: str
    side: str
    entry: Any
    sl: Any
    tp: Any
    rr: Any


@dataclass
class RouterDecision:
    symbol: str
    phase: str
    trend_dir: str
    model_hint: str


@dataclass
class RiskDecision:
    symbol: str
    allow: bool
    reason: str
    risk_multiplier: float


# ✅ STAGE 5 / STEP 5 — NEW DTO
@dataclass
class MacroDecision:
    symbol: str
    regime: str
    macro_bias: str
    allow: bool


@dataclass
class ExecutionIntent:
    symbol: str
    model: str
    side: str
    entry: Any
    sl: Any
    tp: Any
    rr: Any
    mode: str


def df_to_entry_candidates(
    df: pd.DataFrame,
    *,
    symbol: str,
) -> list[EntryCandidate]:
    out: list[EntryCandidate] = []

    for _, row in df.iterrows():
        out.append(
            EntryCandidate(
                symbol=str(symbol),
                model=str(row.get("model", "")),
                side=str(row.get("side", "")),
                entry=row.get("entry"),
                sl=row.get("sl"),
                tp=row.get("tp"),
                rr=row.get("rr"),
            )
        )

    return out


def entry_candidate_to_execution_intent(
    candidate: EntryCandidate,
    *,
    mode: str,
) -> ExecutionIntent:
    return ExecutionIntent(
        symbol=str(candidate.symbol),
        model=str(candidate.model),
        side=str(candidate.side),
        entry=candidate.entry,
        sl=candidate.sl,
        tp=candidate.tp,
        rr=candidate.rr,
        mode=str(mode),
    )


def df_to_execution_intents(
    df: pd.DataFrame,
    *,
    symbol: str,
    mode: str,
) -> list[ExecutionIntent]:
    return [
        entry_candidate_to_execution_intent(candidate, mode=mode)
        for candidate in df_to_entry_candidates(df, symbol=symbol)
    ]


def df_to_router_decisions(
    df: pd.DataFrame,
    *,
    symbol: str,
    phase: str,
    trend_dir: str,
) -> list[RouterDecision]:
    out: list[RouterDecision] = []

    for _, row in df.iterrows():
        out.append(
            RouterDecision(
                symbol=str(row.get("symbol", symbol)),
                phase=str(row.get("phase", phase)),
                trend_dir=str(row.get("trend_dir", trend_dir or "")),
                model_hint=str(row.get("model", "")),
            )
        )

    return out


# ✅ STAGE 5 / STEP 5 — PURE ADAPTER
def df_to_macro_decisions(
    df: pd.DataFrame,
    *,
    symbol: str,
) -> list[MacroDecision]:
    out: list[MacroDecision] = []

    for _, row in df.iterrows():
        out.append(
            MacroDecision(
                symbol=str(row.get("symbol", symbol)),
                regime=str(row.get("regime", "")),
                macro_bias=str(row.get("macro_bias", "")),
                allow=bool(row.get("context_allow", True)),
            )
        )

    return out


def df_to_risk_decisions(
    df: pd.DataFrame,
    *,
    symbol: str,
) -> list[RiskDecision]:
    out: list[RiskDecision] = []

    for _, row in df.iterrows():
        out.append(
            RiskDecision(
                symbol=str(row.get("symbol", symbol)),
                allow=bool(row.get("context_allow", True)),
                reason=str(row.get("block_reason", "")),
                risk_multiplier=row.get("risk_multiplier", 1.0),
            )
        )

    return out