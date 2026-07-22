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
        assert request.path == "/v5/order/create"
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
    credentials = BybitTestnetCredentials(api_key="fake-testnet-key", api_secret="fake-testnet-secret")
    exchange_calls = 0

    with TemporaryDirectory() as tmpdir:
        ledger = ExecutionEventLedger(Path(tmpdir) / "executor_ledger.jsonl")
        intent = _intent()
        original_intent = intent
        command = _command(intent)
        original_command = command
        recon = _reconciliation(intent)
        original_recon = recon

        missing_credentials_result = executor.submit_once_result(
            command_decision=command,
            exchange_ready_intent=intent,
            reconciliation_result=recon,
            credentials=None,
            config=_config(),
            transport=FakeBybitTestnetTransport("ack"),
            submitted_at_utc="2026-07-09T00:01:00+00:00",
        )
        assert missing_credentials_result.submit_status == BLOCKED_MISSING_TESTNET_CREDENTIALS

        live_result = executor.submit_once_result(
            command_decision=_command(intent, mode=MODE_LIVE),
            exchange_ready_intent=intent,
            reconciliation_result=recon,
            credentials=credentials,
            config=_config(mode=MODE_LIVE),
            transport=FakeBybitTestnetTransport("ack"),
            submitted_at_utc="2026-07-09T00:02:00+00:00",
        )
        assert live_result.submit_status == BLOCKED_LIVE_FORBIDDEN

        read_only_result = executor.submit_once_result(
            command_decision=_command(intent, mode=MODE_READ_ONLY),
            exchange_ready_intent=intent,
            reconciliation_result=recon,
            credentials=credentials,
            config=_config(mode=MODE_READ_ONLY),
            transport=FakeBybitTestnetTransport("ack"),
            submitted_at_utc="2026-07-09T00:03:00+00:00",
        )
        assert read_only_result.submit_status == BLOCKED_NOT_TESTNET

        manual_result = executor.submit_once_result(
            command_decision=command,
            exchange_ready_intent=intent,
            reconciliation_result=recon,
            credentials=credentials,
            config=_config(manual_enable=False),
            transport=FakeBybitTestnetTransport("ack"),
            submitted_at_utc="2026-07-09T00:04:00+00:00",
        )
        assert manual_result.submit_status == BLOCKED_MANUAL_ENABLE_MISSING

        reservation_result = executor.submit_once_result(
            command_decision=_command(intent, reserved=False),
            exchange_ready_intent=intent,
            reconciliation_result=recon,
            credentials=credentials,
            config=_config(),
            transport=FakeBybitTestnetTransport("ack"),
            submitted_at_utc="2026-07-09T00:05:00+00:00",
        )
        assert reservation_result.submit_status == BLOCKED_NOT_RESERVED

        reconciliation_result = executor.submit_once_result(
            command_decision=command,
            exchange_ready_intent=intent,
            reconciliation_result=_reconciliation(intent, ok=False),
            credentials=credentials,
            config=_config(),
            transport=FakeBybitTestnetTransport("ack"),
            submitted_at_utc="2026-07-09T00:06:00+00:00",
        )
        assert reconciliation_result.submit_status == BLOCKED_RECONCILIATION_NOT_OK

        ack_transport = FakeBybitTestnetTransport("ack")
        ack_snapshot = executor.submit_once(
            command_decision=command,
            exchange_ready_intent=intent,
            reconciliation_result=recon,
            ledger=ledger,
            credentials=credentials,
            config=_config(),
            transport=ack_transport,
            submitted_at_utc="2026-07-09T00:07:00+00:00",
        )
        exchange_calls += len(ack_transport.calls)
        assert ack_transport.calls[0].payload()["orderLinkId"] == command.client_order_id
        assert ledger.events[-1].event_type == SUBMIT_ACK
        assert ledger.events[-1].status == TESTNET_ACK_CONFIRMED
        assert ack_snapshot.submit_confirmed
        assert ack_snapshot.block_new_orders

        reject_intent = _intent("setup-e17-reject")
        reject_command = _command(reject_intent, client_order_id="ATS-E17-CLIENT-2")
        reject_transport = FakeBybitTestnetTransport("reject")
        reject_snapshot = executor.submit_once(
            command_decision=reject_command,
            exchange_ready_intent=reject_intent,
            reconciliation_result=_reconciliation(reject_intent),
            ledger=ledger,
            credentials=credentials,
            config=_config(),
            transport=reject_transport,
            submitted_at_utc="2026-07-09T00:08:00+00:00",
        )
        exchange_calls += len(reject_transport.calls)
        assert ledger.events[-1].event_type == SUBMIT_REJECT
        assert ledger.events[-1].status == TESTNET_REJECT_CONFIRMED
        assert reject_snapshot.terminal_submit_failure
        assert reject_snapshot.terminal

        timeout_intent = _intent("setup-e17-timeout")
        timeout_command = _command(timeout_intent, client_order_id="ATS-E17-CLIENT-3")
        timeout_transport = FakeBybitTestnetTransport("timeout")
        timeout_snapshot = executor.submit_once(
            command_decision=timeout_command,
            exchange_ready_intent=timeout_intent,
            reconciliation_result=_reconciliation(timeout_intent),
            ledger=ledger,
            credentials=credentials,
            config=_config(),
            transport=timeout_transport,
            submitted_at_utc="2026-07-09T00:09:00+00:00",
        )
        exchange_calls += len(timeout_transport.calls)
        assert ledger.events[-1].event_type == SUBMIT_TIMEOUT_UNKNOWN
        assert ledger.events[-1].status == TESTNET_TIMEOUT_UNKNOWN
        assert timeout_snapshot.submit_unknown
        assert timeout_snapshot.block_new_orders
        assert timeout_snapshot.requires_manual_review

        assert intent == original_intent
        assert command == original_command
        assert recon == original_recon
        try:
            intent.rounded_quantity = Decimal("99")  # type: ignore[misc]
            immutable_intent = False
        except FrozenInstanceError:
            immutable_intent = True
        assert immutable_intent
        try:
            command.allowed = False  # type: ignore[misc]
            immutable_command = False
        except FrozenInstanceError:
            immutable_command = True
        assert immutable_command
        try:
            recon.reconciliation_status = "MUTATED"  # type: ignore[misc]
            immutable_recon = False
        except FrozenInstanceError:
            immutable_recon = True
        assert immutable_recon
        assert replace(intent) == intent
        assert replace(command) == command
        assert replace(recon) == recon
        ledger.close()

    after_files = _file_snapshot()
    production_files_modified = int(before_files != after_files)
    assert production_files_modified == 0
    assert exchange_calls == 3

    print(
        "SMOKE_E17_OK "
        "missing_credentials_block=1 "
        "live_block=1 "
        "read_only_block=1 "
        "manual_enable_block=1 "
        "missing_reservation_block=1 "
        "reconciliation_block=1 "
        "fake_ack_snapshot=1 "
        "fake_reject_snapshot=1 "
        "fake_timeout_unknown=1 "
        "immutable=1 "
        "mutated_source=0 "
        "real_exchange_calls=0 "
        "fake_transport_calls=3 "
        "production_files_modified=0"
    )


if __name__ == "__main__":
    main()
