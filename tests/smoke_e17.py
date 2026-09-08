from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from backtest.execution.exchange_reconciler import LOCAL_UNKNOWN_STATE, RECONCILIATION_OK, ReconciliationResult
from backtest.execution.execution_event_ledger import ExecutionEventLedger, SUBMIT_ACK, SUBMIT_REJECT, SUBMIT_TIMEOUT_UNKNOWN
from backtest.execution.quantity_converter import ExchangeReadyIntent
from backtest.execution.testnet_command_gateway import CommandDecision, MODE_LIVE, MODE_READ_ONLY, MODE_TESTNET
from backtest.execution.testnet_real_submit import (
    BLOCKED_MANUAL_ENABLE_MISSING,
    BLOCKED_MISSING_TESTNET_CREDENTIALS,
    BLOCKED_RECONCILIATION_NOT_OK,
    BybitTestnetCredentials,
    BybitTestnetSubmitConfig,
    BybitTestnetSubmitRequest,
    RealTestnetSubmitExecutor,
    UrllibBybitTestnetSubmitTransport,
    TESTNET_ACK_CONFIRMED,
    TESTNET_REJECT_CONFIRMED,
    TESTNET_TIMEOUT_UNKNOWN,
)
from backtest.execution.testnet_submit_adapter import BLOCKED_LIVE_FORBIDDEN, BLOCKED_NOT_RESERVED, BLOCKED_NOT_TESTNET


PRODUCTION_FILES = (
    Path("live_observation_shell.py"),
    Path("pipeline_core.py"),
    Path("policy_engine.py"),
    Path("position_state.csv"),
)


class FakeBybitTestnetTransport:
    def __init__(self, response: str) -> None:
        self.response = response
        self.calls: list[BybitTestnetSubmitRequest] = []

    def post_order(self, *, request: BybitTestnetSubmitRequest, credentials: BybitTestnetCredentials) -> dict[str, Any]:
        assert credentials.complete
        assert request.testnet_only
        assert request.endpoint == "https://api-testnet.bybit.com"
        assert request.path == "/v5/order/" + "create"
        assert "takeProfit" not in request.payload()
        assert "stopLoss" not in request.payload()
        self.calls.append(request)
        if self.response == "timeout":
            raise TimeoutError("fake timeout")
        if self.response == "reject":
            return {"retCode": 10001, "retMsg": "fake reject", "result": {}}
        return {"retCode": 0, "retMsg": "OK", "result": {"orderId": "TESTNET-ORDER-1"}}


def _file_snapshot() -> dict[str, tuple[bool, int | None, int | None]]:
    snapshot: dict[str, tuple[bool, int | None, int | None]] = {}
    for path in PRODUCTION_FILES:
        if path.exists():
            stat = path.stat()
            snapshot[str(path)] = (True, stat.st_size, stat.st_mtime_ns)
        else:
            snapshot[str(path)] = (False, None, None)
    return snapshot


def _intent(key: str = "setup-e17") -> ExchangeReadyIntent:
    return ExchangeReadyIntent(
        canonical_setup_key=key,
        symbol="BTCUSDT",
        side="LONG",
        quantity=Decimal("0.010000"),
        rounded_quantity=Decimal("0.01"),
        quantity_precision=3,
        exchange_ready=True,
        conversion_reason="smoke_e17_exchange_ready",
        entry=Decimal("100"),
        stop=Decimal("99"),
        target=Decimal("103"),
    )


def _command(intent: ExchangeReadyIntent, *, mode: str = MODE_TESTNET, allowed: bool = True, reserved: bool = True, client_order_id: str = "ATS-E17-CLIENT-1") -> CommandDecision:
    return CommandDecision(
        allowed=allowed,
        mode=mode,
        client_order_id=client_order_id,
        command_status="COMMAND_READY_TESTNET" if allowed else "BLOCKED",
        reason="smoke_e17_command",
        canonical_setup_key=intent.canonical_setup_key,
        symbol=intent.symbol,
        side=intent.side,
        reserved_in_ledger=reserved,
        requires_manual_review=False,
        decided_at_utc="2026-07-09T00:00:00+00:00",
    )


def _reconciliation(intent: ExchangeReadyIntent, *, ok: bool = True) -> ReconciliationResult:
    return ReconciliationResult(
        canonical_setup_key=intent.canonical_setup_key,
        symbol=intent.symbol,
        reconciliation_status=RECONCILIATION_OK if ok else LOCAL_UNKNOWN_STATE,
        local_state="NO_EVENTS",
        exchange_position_status="MISSING",
        open_orders_count=0,
        mismatch_detected=not ok,
        block_new_orders=not ok,
        requires_manual_review=not ok,
        reason="smoke_e17_reconciliation",
        checked_at_utc="2026-07-09T00:00:01+00:00",
    )


def _config(*, mode: str = MODE_TESTNET, manual_enable: bool = True) -> BybitTestnetSubmitConfig:
    return BybitTestnetSubmitConfig(
        mode=mode,
        manual_enable=manual_enable,
        emergency_max_notional=Decimal("1000"),
    )


def main() -> None:
    before_files = _file_snapshot()
    executor = RealTestnetSubmitExecutor()
    retired_submit = False
    retired_transport = False
    try:
        executor.submit_once_result()
    except RuntimeError as exc:
        retired_submit = "retired by R1" in str(exc)
    try:
        UrllibBybitTestnetSubmitTransport().post_order(request=None, credentials=None)
    except RuntimeError as exc:
        retired_transport = "retired by R1" in str(exc)
    assert retired_submit and retired_transport
    assert _file_snapshot() == before_files
    print("SMOKE_E17_OK legacy_real_submit_retired=1 legacy_real_transport_retired=1 real_exchange_calls=0 production_files_modified=0")


if __name__ == "__main__":
    main()
