import pandas as pd

signals = pd.read_csv("backtest/journal/legacy_signals_live_full.csv")
candles = pd.read_csv("backtest/journal/candles_ohlc.csv")

# ===== auto-detect columns =====
cols = candles.columns.tolist()
print("CANDLE COLUMNS:", cols)

# surandam time
time_col = None
for c in ["time","timestamp","open_time","datetime"]:
    if c in candles.columns:
        time_col = c
        break

if time_col is None:
    raise Exception(f"No time column found. Columns: {cols}")

# surandam high/low jei kitaip vadinasi
high_col = None
low_col = None

for c in candles.columns:
    if c.lower() == "high":
        high_col = c
    if c.lower() == "low":
        low_col = c

if high_col is None or low_col is None:
    raise Exception(f"High/Low not found. Columns: {cols}")

# normalize
candles = candles.rename(columns={time_col: "time"})
candles = candles.sort_values("time")

# ===== SOL setups =====
sol = signals[signals["symbol"] == "SOLUSDT"][["entry","tp","sl"]].copy()

print("\n=== SOL SETUPS ===")
print(sol)

results = []

for i, row in sol.iterrows():
    entry = row["entry"]
    tp = row["tp"]
    sl = row["sl"]

    outcome = "NO_HIT"
    bars = 0

    for _, c in candles.iterrows():
        high = c[high_col]
        low = c[low_col]

        bars += 1

        # SHORT logika
        if low <= tp:
            outcome = "TP"
            break
        if high >= sl:
            outcome = "SL"
            break

    results.append({
        "entry": entry,
        "tp": tp,
        "sl": sl,
        "outcome": outcome,
        "bars_to_result": bars
    })

res = pd.DataFrame(results)

print("\n=== OUTCOME ===")
print(res)