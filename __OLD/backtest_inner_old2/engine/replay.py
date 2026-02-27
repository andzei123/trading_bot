from __future__ import annotations
from typing import Optional
import pandas as pd


class ReplayEngine:
    def __init__(self, candles_15m: pd.DataFrame, context_builder=None):
        self.candles = candles_15m
        self.context_builder = context_builder
        self.index = 0

    def next_candle(self) -> Optional[pd.Series]:
        if self.index >= len(self.candles):
            return None

        candle = self.candles.iloc[self.index]  # ✅ eilutė pagal indeksą
        self.index += 1
        return candle
