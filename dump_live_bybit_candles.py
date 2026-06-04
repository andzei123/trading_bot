from pathlib import Path
from backtest.journal.live_observation_shell import load_bybit_latest

out = Path("backtest/data_live_parity")
out.mkdir(parents=True, exist_ok=True)

for sym in ["BTCUSDT", "ETHUSDT", "XRPUSDT"]:
    df = load_bybit_latest("linear", sym, "15", 260)
    print(sym, df["timestamp"].min(), df["timestamp"].max(), len(df))
    df.to_csv(out / f"{sym}.csv", index=False)
