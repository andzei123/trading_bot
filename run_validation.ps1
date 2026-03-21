# kiek porų daryti
$pairs = 6

# pauzė sekundėmis (15 min = 900)
$delay = 900

# simboliai
$symbols = "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,BNBUSDT"

# katalogas
$dir = "backtest/journal/validation_runs"
New-Item -ItemType Directory -Force -Path $dir | Out-Null

for ($i=1; $i -le $pairs; $i++) {

    Write-Host "`n==============================="
    Write-Host "RUN PAIR $i"
    Write-Host "==============================="

    $legacy_out = "$dir/legacy_$i.csv"
    $signal_out = "$dir/signal_$i.csv"

    Write-Host "→ LEGACY"
    python -m backtest.journal.live_signal_runner --once --paper `
        --out $legacy_out `
        --symbols $symbols `
        --source bybit `
        --bybit_category linear `
        --bybit_interval 15 `
        --bybit_candles 1500 `
        --emit_last_candles 6 `
        --risk_guard_csv backtest/journal/tmp/risk_guard_step5.csv `
        --risk_guard_month 2026-03 `
        --risk_guard_bad_month_r -10 `
        --risk_guard_min_trades 20 `
        --risk_guard_action defensive

    Write-Host "→ SIGNAL_SCORE"
    python -m backtest.journal.live_signal_runner --once --paper `
        --out $signal_out `
        --symbols $symbols `
        --source bybit `
        --bybit_category linear `
        --bybit_interval 15 `
        --bybit_candles 1500 `
        --emit_last_candles 6 `
        --risk_guard_csv backtest/journal/tmp/risk_guard_step5.csv `
        --risk_guard_month 2026-03 `
        --risk_guard_bad_month_r -10 `
        --risk_guard_min_trades 20 `
        --risk_guard_action defensive `
        --cluster_score_mode SIGNAL_SCORE

    if ($i -lt $pairs) {
        Write-Host "`nSleeping for $delay seconds..."
        Start-Sleep -Seconds $delay
    }
}

Write-Host "`n✅ DONE: All runs completed"