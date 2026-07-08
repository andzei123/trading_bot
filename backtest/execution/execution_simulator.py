from __future__ import annotations

import hashlib
from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime, timezone

from .quantity_converter import ExchangeReadyIntent


class ExecutionSimulationError(ValueError):
    """Raised when a local execution simulation request is invalid."""


ACKED = "ACKED"
REJECTED = "REJECTED"
PARTIALLY_FILLED = "PARTIALLY_FILLED"
FILLED = "FILLED"
TIMEOUT_UNKNOWN = "TIMEOUT_UNKNOWN"
EXCHANGE_UNAVAILABLE = "EXCHANGE_UNAVAILABLE"

SUPPORTED_SIMULATED_OUTCOMES = frozenset(
    {
        ACKED,
        REJECTED,
        PARTIALLY_FILLED,
        FILLED,
        TIMEOUT_UNKNOWN,
        EXCHANGE_UNAVAILABLE,
    }
)


@dataclass(frozen=True)
class SimulatedExecutionEvent:
    """Immutable local simulator event for E8.

    This event is executor-local diagnostic data only. It does not submit,
    cancel, amend, poll, reconcile, or mutate exchange/ATS state.
    """

    canonical_setup_key: str
    symbol: str
    side: str
    client_order_id: str
    event_type: str
    status: str
    filled_qty: Decimal
    remaining_qty: Decimal
    fill_price: Decimal | None
    reject_reason: str
    simulated_at_utc: str


@dataclass(frozen=True)
class ExecutionSimulationRequest:
    """Controlled deterministic simulation input for E8 smoke and future tests."""

    outcome: str
    simulated_at_utc: str | None = None
    client_order_id: str | None = None
    fill_price: Decimal | None = None
    filled_qty: Decimal | None = None
    reject_reason: str = ""


class ExecutionSimulator:
    """Deterministic local exchange outcome simulator.

    E8 intentionally does not call exchanges, submit orders, cancel orders,
    perform retries, poll status, write production files, calculate strategy
    risk, or alter quantity/entry/stop/target decisions.
    """

    def simulate(
        self,
        exchange_ready_intent: ExchangeReadyIntent,
        request: ExecutionSimulationRequest,
    ) -> SimulatedExecutionEvent:
        if not isinstance(exchange_ready_intent, ExchangeReadyIntent):
            raise ExecutionSimulationError("simulator requires ExchangeReadyIntent")
        if not exchange_ready_intent.exchange_ready:
            raise ExecutionSimulationError("ExchangeReadyIntent is not exchange_ready")
        if not isinstance(request, ExecutionSimulationRequest):
            raise ExecutionSimulationError("simulator requires ExecutionSimulationRequest")

        outcome = request.outcome.strip().upper()
        if outcome not in SUPPORTED_SIMULATED_OUTCOMES:
            raise ExecutionSimulationError(f"unsupported simulated outcome: {request.outcome}")

        total_qty = exchange_ready_intent.rounded_quantity
        if not total_qty.is_finite() or total_qty <= 0:
            raise ExecutionSimulationError("ExchangeReadyIntent rounded_quantity must be finite and positive")

        simulated_at = request.simulated_at_utc or _utc_now()
        client_order_id = request.client_order_id or deterministic_simulated_client_order_id(exchange_ready_intent)

        if outcome == ACKED:
            filled_qty = Decimal("0")
            remaining_qty = total_qty
            fill_price = None
            reject_reason = ""
        elif outcome == REJECTED:
            filled_qty = Decimal("0")
            remaining_qty = total_qty
            fill_price = None
            reject_reason = request.reject_reason or "simulated_reject"
        elif outcome == PARTIALLY_FILLED:
            filled_qty = request.filled_qty if request.filled_qty is not None else total_qty / Decimal("2")
            _validate_filled_qty(filled_qty, total_qty)
            if filled_qty == total_qty:
                raise ExecutionSimulationError("partial fill must leave remaining quantity")
            remaining_qty = total_qty - filled_qty
            fill_price = request.fill_price if request.fill_price is not None else exchange_ready_intent.entry
            reject_reason = ""
        elif outcome == FILLED:
            filled_qty = request.filled_qty if request.filled_qty is not None else total_qty
            if filled_qty != total_qty:
                raise ExecutionSimulationError("filled simulation must fill full rounded quantity")
            remaining_qty = Decimal("0")
            fill_price = request.fill_price if request.fill_price is not None else exchange_ready_intent.entry
            reject_reason = ""
        elif outcome == TIMEOUT_UNKNOWN:
            filled_qty = Decimal("0")
            remaining_qty = total_qty
            fill_price = None
            reject_reason = "timeout_unknown"
        elif outcome == EXCHANGE_UNAVAILABLE:
            filled_qty = Decimal("0")
            remaining_qty = total_qty
            fill_price = None
            reject_reason = "exchange_unavailable"
        else:  # pragma: no cover - guarded by SUPPORTED_SIMULATED_OUTCOMES
            raise ExecutionSimulationError(f"unhandled simulated outcome: {outcome}")

        return SimulatedExecutionEvent(
            canonical_setup_key=exchange_ready_intent.canonical_setup_key,
            symbol=exchange_ready_intent.symbol,
            side=exchange_ready_intent.side,
            client_order_id=client_order_id,
            event_type="SIMULATED_EXECUTION_EVENT",
            status=outcome,
            filled_qty=filled_qty,
            remaining_qty=remaining_qty,
            fill_price=fill_price,
            reject_reason=reject_reason,
            simulated_at_utc=simulated_at,
        )


def deterministic_simulated_client_order_id(exchange_ready_intent: ExchangeReadyIntent) -> str:
    """Build a deterministic simulator-only client order id."""

    basis = "|".join(
        [
            exchange_ready_intent.canonical_setup_key,
            exchange_ready_intent.symbol,
            exchange_ready_intent.side,
            format(exchange_ready_intent.rounded_quantity.normalize(), "f"),
            format(exchange_ready_intent.entry.normalize(), "f"),
        ]
    )
    digest = hashlib.sha256(basis.encode("utf-8")).hexdigest()[:24].upper()
    return f"ATS_SIM_{digest}"


def simulate_execution(
    exchange_ready_intent: ExchangeReadyIntent,
    request: ExecutionSimulationRequest,
) -> SimulatedExecutionEvent:
    """Convenience wrapper for one deterministic local simulation."""

    return ExecutionSimulator().simulate(exchange_ready_intent, request)


def _validate_filled_qty(filled_qty: Decimal, total_qty: Decimal) -> None:
    if not filled_qty.is_finite() or filled_qty <= 0:
        raise ExecutionSimulationError("filled_qty must be finite and positive")
    if filled_qty > total_qty:
        raise ExecutionSimulationError("filled_qty exceeds rounded quantity")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
