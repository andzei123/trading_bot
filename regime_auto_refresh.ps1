$Out = "backtest\journal\exports_trades\market_regime.csv"
$Tmp = "backtest\journal\exports_trades\market_regime.tmp.csv"
$Log = "backtest\journal\exports_trades\market_regime_refresh.log"

while ($true) {
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content $Log "[$ts] REGIME_REFRESH_START"

    py -m backtest.journal.market_regime `
      --refresh `
      --symbol BTCUSDT `
      --bybit_category linear `
      --bybit_interval 15 `
      --candles 2000 `
      --keep_days 90 `
      --out $Tmp

    if (Test-Path $Tmp) {
        Move-Item -Force $Tmp $Out
        $last = (Import-Csv $Out).timestamp[-1]
        Add-Content $Log "[$ts] REGIME_REFRESH_OK last_ts=$last"
    } else {
        Add-Content $Log "[$ts] REGIME_REFRESH_FAIL tmp_missing"
    }

    Start-Sleep -Seconds 14400
}
