"""Shared production decision pipeline.

This package exists to guarantee a single 1:1 decision layer for both:
 - backtest.journal.live_signal_runner (live orchestrator)
 - backtest.offline.offline_live_runner_backtest (offline replay)

Only the data source and execution differ.
"""
