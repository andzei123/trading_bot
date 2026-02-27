import pandas as pd

# Load simulated trades
df = pd.read_csv("backtest/journal/exports_trades/trades_simulated.csv")

# Parse timestamp
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
df = df.dropna(subset=["timestamp"])

# Add year & quarter
df["year"] = df["timestamp"].dt.year
df["quarter"] = df["timestamp"].dt.to_period("Q").astype(str)

# Quarterly performance report
report = (
    df.groupby(["year", "quarter"])
    .agg(
        trades=("R", "count"),
        winrate_pct=("R", lambda x: round((x > 0).mean() * 100, 2)),
        expectancy_R=("R", "mean"),
        total_R=("R", "sum"),
    )
    .reset_index()
    .sort_values(["year", "quarter"])
)

print("\n=== QUARTERLY PERFORMANCE REPORT ===")
print(report.to_string(index=False))
