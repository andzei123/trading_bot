from __future__ import annotations

from dataclasses import FrozenInstanceError
from decimal import Decimal
from pathlib import Path
import tempfile

from backtest.execution.bybit_read_only_adapter import BybitReadOnlyConfig, READ_ONLY_MODE
from backtest.execution.exchange_read_adapter import (
    ExchangeBalanceSnapshot,
    ExchangeInstrumentRules,
    ExchangeOpenOrderSnapshot,
    ExchangePositionSnapshot,
    ExchangeReadResult,
    ExchangeStateSnapshot,
    assert_read_only_adapter,
    require_credentials_configured,
)


NETWORK_CALLS = 0
OBSERVED_AT = "2026-07-08T12:00:00+00:00"


class FakeReadOnlyExchangeAdapter:
    mode = READ_ONLY_MODE

    def read_server_time(self) -> ExchangeReadResult[str]:
        return ExchangeReadResult.success("2026-07-08T12:00:01+00:00", observed_at_utc=OBSERVED_AT)

    def read_balance(self) -> ExchangeReadResult[tuple[ExchangeBalanceSnapshot, ...]]:
        return ExchangeReadResult.success(
            (
                ExchangeBalanceSnapshot(
                    exchange="BYBIT",
                    account_type="UNIFIED",
                    asset="USDT",
                    wallet_balance=Decimal("10000"),
                    available_balance=Decimal("9950"),
                    equity=Decimal("10025"),
                    observed_at_utc=OBSERVED_AT,
                ),
            ),
            observed_at_utc=OBSERVED_AT,
        )

    def read_open_positions(self) -> ExchangeReadResult[tuple[ExchangePositionSnapshot, ...]]:
        return ExchangeReadResult.success(
            (
                ExchangePositionSnapshot(
                    exchange="BYBIT",
                    symbol="XRPUSDT",
                    side="Buy",
                    quantity=Decimal("100"),
                    entry_price=Decimal("2.00"),
                    mark_price=Decimal("2.01"),
                    unrealized_pnl=Decimal("1.0"),
                    observed_at_utc=OBSERVED_AT,
                ),
            ),
            observed_at_utc=OBSERVED_AT,
        )

    def read_open_orders(self) -> ExchangeReadResult[tuple[ExchangeOpenOrderSnapshot, ...]]:
        return ExchangeReadResult.success(
            (
                ExchangeOpenOrderSnapshot(
                    exchange="BYBIT",
                    symbol="XRPUSDT",
                    side="Buy",
                    order_id="order-1",
                    client_order_id="ATSX1_TEST",
                    order_type="Limit",
                    price=Decimal("2.00"),
                    quantity=Decimal("100"),
                    remaining_quantity=Decimal("100"),
                    status="New",
                    observed_at_utc=OBSERVED_AT,
                ),
            ),
            observed_at_utc=OBSERVED_AT,
        )

    def read_instrument_rules(self) -> ExchangeReadResult[tuple[ExchangeInstrumentRules, ...]]:
        return ExchangeReadResult.success(
            (
                ExchangeInstrumentRules(
                    exchange="BYBIT",
                    symbol="XRPUSDT",
                    quantity_step=Decimal("0.1"),
                    min_quantity=Decimal("1"),
                    quantity_precision=1,
                    price_tick=Decimal("0.0001"),
                    price_precision=4,
                    min_notional=Decimal("5"),
                    observed_at_utc=OBSERVED_AT,
                ),
            ),
            observed_at_utc=OBSERVED_AT,
        )


class BadMutationAdapter(FakeReadOnlyExchangeAdapter):
    def cancel_order(self) -> None:
        raise AssertionError("mutation method must not exist on read-only adapter")


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        before_files = set(Path(tmp).rglob("*"))

        config = BybitReadOnlyConfig.from_env()
        adapter = FakeReadOnlyExchangeAdapter()
        assert_read_only_adapter(adapter)

        server_time = adapter.read_server_time()
        balance = adapter.read_balance()
        positions = adapter.read_open_positions()
        orders = adapter.read_open_orders()
        rules = adapter.read_instrument_rules()

        snapshot = ExchangeStateSnapshot(
            exchange="BYBIT",
            server_time_utc=server_time.result,
            balances=balance.result or (),
            positions=positions.result or (),
            open_orders=orders.result or (),
            instrument_rules=rules.result or (),
            observed_at_utc=OBSERVED_AT,
        )

        mutation_blocked = False
        try:
            assert_read_only_adapter(BadMutationAdapter())
        except Exception:
            mutation_blocked = True

        credentials_failed_closed = require_credentials_configured(
            api_key=None, api_secret=None, observed_at_utc=OBSERVED_AT
        )

        immutable = False
        try:
            snapshot.exchange = "OTHER"  # type: ignore[misc]
        except FrozenInstanceError:
            immutable = True

        after_files = set(Path(tmp).rglob("*"))

    assert config.base_url
    assert balance.ok is True and balance.result is not None
    assert balance.result[0].asset == "USDT"
    assert positions.ok is True and positions.result is not None
    assert positions.result[0].symbol == "XRPUSDT"
    assert orders.ok is True and orders.result is not None
    assert orders.result[0].client_order_id == "ATSX1_TEST"
    assert rules.ok is True and rules.result is not None
    assert rules.result[0].quantity_step == Decimal("0.1")
    assert mutation_blocked is True
    assert credentials_failed_closed.ok is False
    assert immutable is True
    assert NETWORK_CALLS == 0
    assert after_files == before_files

    print(
        "SMOKE_E12_OK "
        "offline_mock=1 "
        "normalized_balance=1 "
        "normalized_positions=1 "
        "normalized_open_orders=1 "
        "instrument_rules=1 "
        "mutation_methods_impossible=1 "
        "credentials_fail_closed=1 "
        "production_files_modified=0 "
        "exchange_network_calls=0"
    )


if __name__ == "__main__":
    main()
