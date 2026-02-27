# Live test (locked config)

This repo includes a repeatable **signal-only live test** wrapper using `config/live.json`.

## 1) Set up

```bash
python -m venv .venv
. .venv/bin/activate   # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
```

## 2) Configure

Edit:
- `config/live.json` (symbols, Bybit interval, etc.)
- `config/news_events.json` (optional manual blackout events)

## 3) Run (signal-only, 30m)

```bash
python run_live_signal_only_30m.py
```

Outputs:
- `backtest/journal/live_entries.csv`
- `backtest/journal/live_state.txt` (created even if no signals are emitted)

## Notes
- Default config uses **ETHUSDT, XRPUSDT, SOLUSDT** on Bybit Linear, 30m.
- If you want to include BTC, edit `config/live.json`.