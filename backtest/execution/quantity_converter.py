from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass
from decimal import Decimal, ROUND_FLOOR

from .intent import ExecutionIntent


class QuantityConversionError(ValueError):
    """Raised when a dry-run order cannot be mechanically built safely."""


@dataclass(frozen=True)
class DryRunOrder:
    client_order_id: str
    symbol: str
    side: str
    order_type: str
    qty: str
    entry: str
    sl: str
    tp: str
    notional: str
    risk_usd_after_rounding: str
    risk_not_increased: bool
    send_enabled: bool = False

    def to_row(self) -> dict[str, str | bool]:
        return asdict(self)


def deterministic_client_order_id(intent: ExecutionIntent) -> str:
    basis = "|".join(
        [
            intent.symbol,
            intent.model,
            intent.side,
            intent.canonical_setup_key,
            intent.setup_id,
            intent.intended_entry_ts,
            str(intent.execution_rank),
        ]
    )
    digest = hashlib.sha256(basis.encode("utf-8")).hexdigest()[:24].upper()
    return f"ATSX1_{digest}"


def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        raise QuantityConversionError("qty_step must be positive")
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step


def convert_intent_to_dry_run_order(
    intent: ExecutionIntent,
    *,
    qty_step: Decimal = Decimal("0.000001"),
    min_qty: Decimal = Decimal("0"),
    min_notional: Decimal = Decimal("0"),
) -> DryRunOrder:
    """Build exchange order parameters without submitting anything.

    E1 may only perform mechanical conversion. Quantity is rounded down to the
    exchange step so the resulting risk cannot exceed ATS-authorized risk.
    """

    rounded_qty = _floor_to_step(intent.authorized_qty, qty_step)
    if rounded_qty <= 0:
        raise QuantityConversionError("rounded quantity is zero")
    if min_qty and rounded_qty < min_qty:
        raise QuantityConversionError("rounded quantity is below min_qty")

    notional = rounded_qty * intent.entry
    if min_notional and notional < min_notional:
        raise QuantityConversionError("rounded notional is below min_notional")

    risk_distance = abs(intent.entry - intent.sl)
    risk_after_rounding = rounded_qty * risk_distance
    if risk_after_rounding > intent.authorized_risk_usd:
        raise QuantityConversionError("rounding would increase authorized risk")

    return DryRunOrder(
        client_order_id=deterministic_client_order_id(intent),
        symbol=intent.symbol,
        side="Buy" if intent.side == "LONG" else "Sell",
        order_type="Limit",
        qty=format(rounded_qty.normalize(), "f"),
        entry=format(intent.entry.normalize(), "f"),
        sl=format(intent.sl.normalize(), "f"),
        tp=format(intent.tp.normalize(), "f"),
        notional=format(notional.normalize(), "f"),
        risk_usd_after_rounding=format(risk_after_rounding.normalize(), "f"),
        risk_not_increased=True,
        send_enabled=False,
    )
