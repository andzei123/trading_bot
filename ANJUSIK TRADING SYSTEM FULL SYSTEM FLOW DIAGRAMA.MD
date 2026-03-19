                           MARKET DATA
                     (candles / volume / levels)
                                  │
                                  ▼
                         CONTEXT ENGINE
                           build_ctx()
                                  │
                                  ▼
                         REGIME ENGINE
                         phase_router
                         decide_phase()
                                  │
                                  ▼
                         SIGNAL ENGINE
                     TDP_REENTRY / RANGE_V2
                                  │
                                  ▼
                     SETUP LIFECYCLE ENGINE
                (setup_age / emit_last_candles)
                                  │
                                  ▼
                         SIGNAL FILTERS
        ┌─────────────────────────────────────────┐
        │ phase alignment                         │
        │ TTS gate                                │
        │ signal cluster filter                   │
        │ macro permission layer                  │
        │ correlation filters                     │
        └─────────────────────────────────────────┘
                                  │
                                  ▼
                         CAPITAL ENGINE
                   (setup scoring / symbol weight)
                                  │
                                  ▼
                           RISK ENGINE
                 (risk per trade / exposure limits)
                                  │
                                  ▼
                         EXECUTION ENGINE
               order manager / TP / SL / timeout
                                  │
                                  ▼
                            MONITORING
              system_health / watchdog / telemetry
                                  │
                                  ▼
                         CSV DATA WAREHOUSE
        trades.csv / equity_curve.csv / symbol_stats.csv
                                  │
                                  ▼
                         RESEARCH PIPELINE
                   CSV analysis / edge discovery
                                  │
                                  ▼
                      STRATEGY EVOLUTION LOOP
                  model improvement / redeployment