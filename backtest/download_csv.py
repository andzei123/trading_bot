from pybit.unified_trading import HTTP
import pandas as pd
from datetime import datetime

# PUBLIC endpoint – jokių API raktų nereikia
session = HTTP(testnet=False)

symbol = "BTCUSDT"
interval = 15   # 15m
limit = 1000    # max per request

all_rows = []
end_time = None

# ~30k žvakių ≈ ~300 dienų (užteks pradžiai)
for _ in range(30):
    res = session.get_kline(
        category="linear",
        symbol=symbol,
        interval=interval,
        limit=limit,
        end=end_time
    )

    rows = res["result"]["list"]
    if not rows:
        break

    all_rows.extend(rows)
    end_time = int(rows[-1][0]) - 1

# Bybit grąžina atvirkščiai – apverčiam
df = pd.DataFrame(all_rows, columns=[
    "timestamp", "open", "high", "low", "close", "volume", "turnover"
])

df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
df = df.sort_values("timestamp")

df[["open", "high", "low", "close", "volume"]] = \
    df[["open", "high", "low", "close", "volume"]].astype(float)

df = df[["timestamp", "open", "high", "low", "close", "volume"]]

df.to_csv("backtest/data/BTCUSDT_15m.csv", index=False)

print("✅ BTCUSDT_15m.csv sukurtas")
