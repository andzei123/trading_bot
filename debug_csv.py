import pandas as pd

df = pd.read_csv("backtest/journal/candles.csv")

print("COLUMNS:")
print(df.columns.tolist())

print("\nFIRST ROWS:")
print(df.head(5))
