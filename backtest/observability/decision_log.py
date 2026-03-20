from __future__ import annotations

from typing import Any, Callable


def build_trade_opened_payload(
    *,
    symbol: str,
    model: str,
    entry: float | None,
    tp: float | None,
    sl: float | None,
    mode: str,
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "model": model,
        "entry": entry,
        "tp": tp,
        "sl": sl,
        "mode": mode,
    }


def decision_log(
    diag_log: Callable[..., None],
    event: str,
    **kwargs: Any,
) -> None:
    """
    Thin wrapper over diag_log.

    Stage 6 goal:
    - centralize decision logging entrypoint
    - no behavior change
    - no formatting changes
    """
    try:
        diag_log(event, **kwargs)
    except Exception:
        # must be fail-open
        pass
