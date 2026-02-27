import pandas as pd

# Load trades
df = pd.read_csv("backtest/journal/exports_trades/trades_simulated.csv")

# Parse timestamp
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
df = df.dropna(subset=["timestamp"])

# Create year-month column
df["year_month"] = df["timestamp"].dt.to_period("M").astype(str)

# Monthly report
report = (
    df.groupby("year_month")
    .agg(
        trades=("R", "count"),
        wins=("R", lambda x: (x > 0).sum()),
        losses=("R", lambda x: (x < 0).sum()),
        expectancy_R=("R", "mean"),
        total_R=("R", "sum"),
    )
    .reset_index()
    .sort_values("year_month")
)

print("\n=== MONTHLY PERFORMANCE (R UNITS) ===")
print(report.to_string(index=False))
