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


def df_to_entry_candidates(
    df: pd.DataFrame,
    *,
    symbol: str,
) -> list[EntryCandidate]:
    """
    Narrow adapter only:
    - no filtering
    - no scoring
    - no policy
    - no mutation
    """
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