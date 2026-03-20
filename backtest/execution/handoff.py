from __future__ import annotations

from typing import Any, Callable

import pandas as pd

from backtest.contracts.models import df_to_execution_intents


def handoff_signals(
    *,
    df_e: pd.DataFrame,
    paper: bool,
    bybit_symbol: str,
    latest_ts: Any,
    rr: float | None,
    once: bool,
    diag_log: Callable[..., None],
    now_utc_str: Callable[[], str],
) -> None:
    if paper:
        try:
            # Stage 5 / Step 2:
            # narrow, fail-open contract shaping only
            try:
                _ = df_to_execution_intents(
                    df_e,
                    symbol=bybit_symbol,
                    mode="paper",
                )
            except Exception:
                pass

            from backtest.execution.paper_adapter import run_paper_adapter

            run_paper_adapter(
                df_e=df_e,
                bybit_symbol=bybit_symbol,
                latest_ts=latest_ts,
                rr=rr,
                diag_log=diag_log,
                now_utc_str=now_utc_str,
            )
        except Exception as _e:
            print(f"[{now_utc_str()}] [PAPER][WARN] {repr(_e)}")
    else:
        try:
            diag_log(
                "POST_EMIT_SKIPPED",
                symbol=str(bybit_symbol),
                reason="paper_false_no_post_emit_execution_path",
                count=int(len(df_e)),
                once=bool(once),
            )
        except Exception:
            pass