from __future__ import annotations

from backtest.execution.bybit_read_only_adapter import BybitReadOnlyAdapter, BybitReadOnlyConfig
from backtest.execution.exchange_read_adapter import assert_read_only_adapter


def _require_ok(name: str, result: object) -> None:
    ok = getattr(result, "ok", False)
    error = getattr(result, "error", "")
    if not ok:
        raise SystemExit(f"E12_REAL_READ_ONLY_FAIL {name}: {error}")


def main() -> None:
    config = BybitReadOnlyConfig.from_env()
    adapter = BybitReadOnlyAdapter(config)
    assert_read_only_adapter(adapter)

    server_time = adapter.read_server_time()
    _require_ok("server_time", server_time)

    balance = adapter.read_balance()
    positions = adapter.read_open_positions()
    open_orders = adapter.read_open_orders()
    instrument_rules = adapter.read_instrument_rules()

    _require_ok("balance", balance)
    _require_ok("positions", positions)
    _require_ok("open_orders", open_orders)
    _require_ok("instrument_rules", instrument_rules)

    print(
        "E12_REAL_READ_ONLY_OK "
        f"balances={len(balance.result or ())} "
        f"positions={len(positions.result or ())} "
        f"open_orders={len(open_orders.result or ())} "
        f"instrument_rules={len(instrument_rules.result or ())} "
        "exchange_mutations=0 persistent_writes=0"
    )


if __name__ == "__main__":
    main()
