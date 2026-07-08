from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Generic, Protocol, Sequence, TypeVar


class ExchangeReadAdapterError(RuntimeError):
    """Raised when a read-only exchange snapshot cannot be produced safely."""


@dataclass(frozen=True)
class ExchangeBalanceSnapshot:
    """Immutable normalized account-balance view from a read-only adapter.

    E7 snapshots are diagnostic inputs for future reconciliation only. They do
    not authorize trading, calculate strategy risk, resize positions, or mutate
    exchange state.
    """

    exchange: str
    account_type: str
    asset: str
    wallet_balance: Decimal
    available_balance: Decimal
    equity: Decimal
    observed_at_utc: str


@dataclass(frozen=True)
class ExchangePositionSnapshot:
    """Immutable normalized open-position view from a read-only adapter."""

    exchange: str
    symbol: str
    side: str
    quantity: Decimal
    entry_price: Decimal | None
    mark_price: Decimal | None
    unrealized_pnl: Decimal | None
    observed_at_utc: str


@dataclass(frozen=True)
class ExchangeOpenOrderSnapshot:
    """Immutable normalized open-order view from a read-only adapter."""

    exchange: str
    symbol: str
    side: str
    order_id: str
    client_order_id: str
    order_type: str
    price: Decimal | None
    quantity: Decimal
    remaining_quantity: Decimal
    status: str
    observed_at_utc: str


@dataclass(frozen=True)
class ExchangeInstrumentRules:
    """Immutable normalized instrument precision and size rules."""

    exchange: str
    symbol: str
    quantity_step: Decimal
    min_quantity: Decimal
    quantity_precision: int
    price_tick: Decimal
    price_precision: int
    min_notional: Decimal | None
    observed_at_utc: str


T = TypeVar("T")


@dataclass(frozen=True)
class ExchangeReadResult(Generic[T]):
    """Immutable result envelope for read-only exchange adapter calls."""

    ok: bool
    result: T | None
    error: str
    observed_at_utc: str

    @classmethod
    def success(cls, result: T, *, observed_at_utc: str) -> "ExchangeReadResult[T]":
        return cls(ok=True, result=result, error="", observed_at_utc=observed_at_utc)

    @classmethod
    def failure(cls, error: str, *, observed_at_utc: str) -> "ExchangeReadResult[T]":
        return cls(ok=False, result=None, error=error, observed_at_utc=observed_at_utc)


@dataclass(frozen=True)
class ExchangeStateSnapshot:
    """Immutable aggregate of read-only exchange state.

    This object is intentionally passive. It is not a reconciliation decision,
    execution decision, or risk-management decision.
    """

    exchange: str
    server_time_utc: str | None
    balances: tuple[ExchangeBalanceSnapshot, ...]
    positions: tuple[ExchangePositionSnapshot, ...]
    open_orders: tuple[ExchangeOpenOrderSnapshot, ...]
    instrument_rules: tuple[ExchangeInstrumentRules, ...]
    observed_at_utc: str


class ReadOnlyExchangeAdapter(Protocol):
    """Protocol for E7 read-only exchange adapters.

    Implementations may read exchange state and normalize it into immutable
    executor-local models. They must not expose mutation methods such as order
    submission, cancellation, amendment, TP/SL placement, or position closing.
    """

    mode: str

    def read_server_time(self) -> ExchangeReadResult[str]:
        """Return exchange/server time if available."""

    def read_balance(self) -> ExchangeReadResult[tuple[ExchangeBalanceSnapshot, ...]]:
        """Return normalized balances."""

    def read_open_positions(self) -> ExchangeReadResult[tuple[ExchangePositionSnapshot, ...]]:
        """Return normalized open positions."""

    def read_open_orders(self) -> ExchangeReadResult[tuple[ExchangeOpenOrderSnapshot, ...]]:
        """Return normalized open orders."""

    def read_instrument_rules(self) -> ExchangeReadResult[tuple[ExchangeInstrumentRules, ...]]:
        """Return normalized instrument precision and size rules."""


MUTATION_METHOD_NAMES = frozenset(
    {
        "submit_order",
        "place_order",
        "cancel_order",
        "amend_order",
        "set_tp_sl",
        "set_trading_stop",
        "close_position",
    }
)


def assert_read_only_adapter(adapter: object) -> None:
    """Fail closed if an E7 adapter exposes known mutation entry points."""

    mode = getattr(adapter, "mode", None)
    if mode != "READ_ONLY":
        raise ExchangeReadAdapterError("exchange adapter mode must be READ_ONLY")

    present_mutators = sorted(name for name in MUTATION_METHOD_NAMES if hasattr(adapter, name))
    if present_mutators:
        raise ExchangeReadAdapterError(
            "read-only adapter exposes mutation methods: " + ",".join(present_mutators)
        )


def require_credentials_configured(
    *, api_key: str | None, api_secret: str | None, observed_at_utc: str
) -> ExchangeReadResult[None]:
    """Return a fail-closed result when read credentials are not configured.

    Import, compile, and smoke tests must not require credentials. A real
    adapter can call this before making read-only network requests.
    """

    if not api_key or not api_secret:
        return ExchangeReadResult.failure(
            "missing read-only exchange credentials", observed_at_utc=observed_at_utc
        )
    return ExchangeReadResult.success(None, observed_at_utc=observed_at_utc)
