from __future__ import annotations

import pandas as pd

import backtest.journal.filter_trades as ft


def build_context(
    candles: pd.DataFrame,
    *,
    diag_always: bool = False,
) -> pd.DataFrame:
    """
    Stage 1 controlled extraction:
    - candles -> ctx transformation
    - ctx-level diagnostic attr enrichment

    Semantics intentionally identical to the runner's prior inline logic.
    """
    ctx = ft.build_ctx(candles)

    try:
        if hasattr(ctx, "attrs") and isinstance(ctx.attrs, dict):
            ctx.attrs["diag_always"] = bool(diag_always)
    except Exception:
        pass

    return ctx
