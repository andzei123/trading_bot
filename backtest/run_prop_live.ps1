# Step 14 - Prop live daemon (PowerShell)
# Activate venv first, then:
#   .\run_prop_live.ps1

$ErrorActionPreference = "Stop"
$PY = "python"

& $PY -m backtest.journal.prop_live_daemon `
  --loop_minutes 5 `
  --emit_last_candles 50 `
  --top_n 5 `
  --metric total_R `
  --bad_month_r -10 `
  --bad_month_min_trades 20 `
  --maxdd_threshold -25 `
  --killswitch_r -10 `
  --killswitch_window_days 7 `
  --monthly_action neutral `
  --bybit_interval 30 --bybit_candles 1500 `
  --debug_regime
