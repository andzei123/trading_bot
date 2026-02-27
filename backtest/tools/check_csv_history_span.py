from pathlib import Path
import pandas as pd

CSV_PATH = "backtest/data/BTCUSDT_15m.csv "  # <- pakeisk simbolį

p = Path(CSV_PATH)
if not p.exists():
    raise SystemExit(f"File not found: {p}")

df = pd.read_csv(p)

if "timestamp" not in df.columns:
    raise SystemExit("CSV neturi 'timestamp' stulpelio")

ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
ts = ts.dropna()

if ts.empty:
    raise SystemExit("Timestamp stulpelis tuščias arba netinkamas")

start = ts.min()
end = ts.max()
span_days = (end - start).days
span_years = span_days / 365.25

print(f"File: {p.name}")
print(f"Rows: {len(ts):,}")
print(f"Start: {start}")
print(f"End:   {end}")
print(f"Span:  {span_days} days (~{span_years:.2f} years)")
