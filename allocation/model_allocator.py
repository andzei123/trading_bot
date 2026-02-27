# allocation/model_allocator.py
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict

import pandas as pd

log = logging.getLogger(__name__)

PERF_FILE = Path("data/performance/model_performance.csv")

TOP_BOOST = 1.20
BOTTOM_CUT = 0.80


def load_model_performance(path: Path = PERF_FILE) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"model_performance file not found: {path}")
    df = pd.read_csv(path)
    required = {"model", "winrate", "R_sum"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in performance file: {missing}")
    return df


def compute_allocation_multipliers(path: Path = PERF_FILE) -> Dict[str, float]:
    """
    Returns dict:
        { model_name: allocation_multiplier }
    """
    df = load_model_performance(path).copy()

    # score = winrate + normalized R_sum
    df["R_norm"] = (df["R_sum"] - df["R_sum"].mean()) / (df["R_sum"].std() + 1e-9)
    df["score"] = df["winrate"] + df["R_norm"]

    df = df.sort_values("score", ascending=False).reset_index(drop=True)

    n = len(df)
    top_cut = max(1, int(0.3 * n))
    bot_cut = max(1, int(0.3 * n))

    multipliers: Dict[str, float] = {}

    for i, row in df.iterrows():
        model = str(row["model"])

        if i < top_cut:
            mult = TOP_BOOST
        elif i >= n - bot_cut:
            mult = BOTTOM_CUT
        else:
            mult = 1.0

        multipliers[model] = mult
        log.debug("[MODEL_ALLOC] model=%s multiplier=%.2f", model, mult)

    return multipliers
