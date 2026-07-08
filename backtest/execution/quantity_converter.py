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


@dataclass(frozen=True)
class ExchangeQuantityRules:
    """Immutable exchange quantity formatting rules for E6.

    These rules are mechanical exchange constraints only. They do not represent
    ATS risk management, strategy sizing, portfolio caps, ranking, or authority.
    """

    quantity_step: Decimal = Decimal("0.000001")
    min_quantity: Decimal = Decimal("0")
    quantity_precision: int = 6


@dataclass(frozen=True)
class ExchangeReadyIntent:
    """Immutable intent after deterministic exchange quantity formatting.

    E6 preserves ATS entry, stop, target, side, symbol, authority, and risk
    decisions. It only exposes the already-authorized quantity in a format that
    can be consumed by a later order-building stage.
    """

    canonical_setup_key: str
    symbol: str
    side: str
    quantity: Decimal
    rounded_quantity: Decimal
    quantity_precision: int
    exchange_ready: bool
    conversion_reason: str
    entry: Decimal
    stop: Decimal
    target: Decimal


def _format_decimal(value: Decimal, precision: int) -> Decimal:
    if precision < 0:
        raise QuantityConversionError("quantity_precision must be non-negative")
    quantizer = Decimal("1").scaleb(-precision)
    return value.quantize(quantizer)


def convert_validated_intent_quantity(
    validated_intent: "ValidatedExecutionIntent",
    rules: ExchangeQuantityRules | None = None,
) -> ExchangeReadyIntent:
    """Convert ATS-authorized quantity into exchange-ready quantity.

    This function is deterministic and executor-local. It does not call an
    exchange, read exchange state, calculate strategy risk, resize positions,
    change RR, or alter ATS authority. Rounding is floor-only so quantity is not
    increased above the ATS-authorized quantity.
    """

    from .decision_consumer import ValidatedExecutionIntent

    if not isinstance(validated_intent, ValidatedExecutionIntent):
        raise QuantityConversionError("quantity converter requires ValidatedExecutionIntent")

    active_rules = rules or ExchangeQuantityRules()
    quantity = validated_intent.authorized_qty

    if not quantity.is_finite() or quantity <= 0:
        raise QuantityConversionError("authorized quantity must be finite and positive")
    if not active_rules.quantity_step.is_finite() or active_rules.quantity_step <= 0:
        raise QuantityConversionError("quantity_step must be finite and positive")
    if active_rules.min_quantity < 0:
        raise QuantityConversionError("min_quantity must be non-negative")

    rounded_quantity = _floor_to_step(quantity, active_rules.quantity_step)
    if rounded_quantity <= 0:
        raise QuantityConversionError("rounded quantity is zero")
    if active_rules.min_quantity and rounded_quantity < active_rules.min_quantity:
        raise QuantityConversionError("rounded quantity is below min_quantity")

    rounded_quantity = _format_decimal(rounded_quantity, active_rules.quantity_precision)

    return ExchangeReadyIntent(
        canonical_setup_key=validated_intent.canonical_setup_key,
        symbol=validated_intent.symbol,
        side=validated_intent.side,
        quantity=quantity,
        rounded_quantity=rounded_quantity,
        quantity_precision=active_rules.quantity_precision,
        exchange_ready=True,
        conversion_reason="quantity_floor_rounded_to_exchange_step",
        entry=validated_intent.entry,
        stop=validated_intent.sl,
        target=validated_intent.tp,
    )
