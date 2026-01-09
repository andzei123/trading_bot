from pathlib import Path

CANDLES_PATH = Path("backtest/journal/candles_ohlc.csv")
TRADES_PATH  = Path("backtest/journal/trades.csv")

HTF = "4h"
CTX_WIN = 80
ATR_WIN = 14

# TTS
IMPULSE_ATR_MIN = 1.0
RANGE_ATR_MAX = 3.0
DEV_MAX = 1
BREAKOUT_ATR_MIN = 0.3

# TDP
EXTREME_Q = 0.2
DEV_MIN_TDP = 4
TDP_RANGE_ATR_MAX = 14.0

EMA_FAST = 20
EMA_SLOW = 50

REQUIRE_TREND_FOR_TTS = True
REQUIRE_TREND_FOR_TDP = False
REQUIRE_EXTREME_FOR_TDP = True
