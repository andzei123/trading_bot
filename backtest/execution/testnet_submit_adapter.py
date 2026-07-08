from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from .quantity_converter import ExchangeReadyIntent
from .testnet_command_gateway import CommandDecision, MODE_LIVE, MODE_TESTNET, deterministic_testnet_client_order_id


TESTNET_SUBMIT_READY = "TESTNET_SUBMIT_READY"
TESTNET_SUBMIT_STUBBED = "TESTNET_SUBMIT_STUBBED"
BLOCKED_COMMAND_NOT_ALLOWED = "BLOCKED_COMMAND_NOT_ALLOWED"
BLOCKED_NOT_TESTNET = "BLOCKED_NOT_TESTNET"
BLOCKED_LIVE_FORBIDDEN = "BLOCKED_LIVE_FORBIDDEN"
BLOCKED_NOT_RESERVED = "BLOCKED_NOT_RESERVED"
BLOCKED_MISSING_CLIENT_ORDER_ID = "BLOCKED_MISSING_CLIENT_ORDER_ID"
BLOCKED_INVALID_INTENT = "BLOCKED_INVALID_INTENT"
TESTNET_ACK_SIMULATED = "TESTNET_ACK_SIMULATED"
TESTNET_REJECT_SIMULATED = "TESTNET_REJECT_SIMULATED"
TESTNET_TIMEOUT_UNKNOWN_SIMULATED = "TESTNET_TIMEOUT_UNKNOWN_SIMULATED"

SIMULATED_ACK = "ACK"
SIMULATED_REJECT = "REJECT"
SIMULATED_TIMEOUT_UNKNOWN = "TIMEOUT_UNKNOWN"


@dataclass(frozen=True)
class TestnetSubmitRequest:
    """Immutable normalized TESTNET submit request skeleton.

    E15 builds request data only after the E14 gateway has already allowed the
    command and reserved it in the executor-local ledger. This object is not
    sent to Bybit in E15. It contains no TP/SL, retry, cancel, amend, protection
    order, live execution, or strategy/risk recalculation behavior.
    """

    mode: str
    client_order_id: str
    canonical_setup_key: str
    symbol: str
    side: str
    order_type: str
    quantity: Decimal
    price: Decimal
    reduce_only: bool
    testnet_only: bool


@dataclass(frozen=True)
class SubmitResult:
    """Immutable E15 testnet submit skeleton result.

    E15 never enables LIVE and smoke tests never reach any exchange mutation
    path. Results classify stubbed TESTNET outcomes only.
    """

    submitted: bool
    mode: str
    client_order_id: str
    submit_status: str
    exchange_order_id: str
    exchange_ret_code: str
    exchange_ret_message: str
    reason: str
    canonical_setup_key: str
    symbol: str
    side: str
    submitted_at_utc: str


class TestnetSubmitAdapterSkeleton:
    """TESTNET-only submit adapter boundary behind E14 CommandDecision.

    The adapter accepts only an E14-approved TESTNET command with a deterministic
    client_order_id and an existing pre-submit reservation. It does not call
    Bybit, submit live orders, create protection orders, retry, poll, or write
    ATS production files.
    """

    def build_submit_request(
        self,
        *,
        command_decision: CommandDecision,
        exchange_ready_intent: ExchangeReadyIntent,
    ) -> TestnetSubmitRequest:
        validation = _validate_submit_inputs(command_decision, exchange_ready_intent)
        if validation is not None:
            raise ValueError(validation.reason)

        return TestnetSubmitRequest(
            mode=command_decision.mode,
            client_order_id=command_decision.client_order_id,
            canonical_setup_key=exchange_ready_intent.canonical_setup_key,
            symbol=exchange_ready_intent.symbol,
            side="Buy" if exchange_ready_intent.side == "LONG" else "Sell",
            order_type="Limit",
            quantity=exchange_ready_intent.rounded_quantity,
            price=exchange_ready_intent.entry,
            reduce_only=False,
            testnet_only=True,
        )

    def submit(
        self,
        *,
        command_decision: CommandDecision,
        exchange_ready_intent: ExchangeReadyIntent,
        simulated_response: str = SIMULATED_ACK,
        submitted_at_utc: str | None = None,
    ) -> SubmitResult:
        validation = _validate_submit_inputs(command_decision, exchange_ready_intent, submitted_at_utc=submitted_at_utc)
        if validation is not None:
            return validation

        request = self.build_submit_request(command_decision=command_decision, exchange_ready_intent=exchange_ready_intent)
        response = str(simulated_response or SIMULATED_ACK).strip().upper()
        submitted_ts = submitted_at_utc or _utc_now()

        if response == SIMULATED_REJECT:
            return SubmitResult(
                submitted=False,
                mode=request.mode,
                client_order_id=request.client_order_id,
                submit_status=TESTNET_REJECT_SIMULATED,
                exchange_order_id="",
                exchange_ret_code="10001",
                exchange_ret_message="simulated testnet reject",
                reason="stubbed TESTNET submit classified as reject; no exchange mutation performed",
                canonical_setup_key=request.canonical_setup_key,
                symbol=request.symbol,
                side=exchange_ready_intent.side,
                submitted_at_utc=submitted_ts,
            )
        if response == SIMULATED_TIMEOUT_UNKNOWN:
            return SubmitResult(
                submitted=False,
                mode=request.mode,
                client_order_id=request.client_order_id,
                submit_status=TESTNET_TIMEOUT_UNKNOWN_SIMULATED,
                exchange_order_id="",
                exchange_ret_code="UNKNOWN",
                exchange_ret_message="simulated unknown timeout",
                reason="stubbed TESTNET submit outcome unknown; no retry attempted in E15",
                canonical_setup_key=request.canonical_setup_key,
                symbol=request.symbol,
                side=exchange_ready_intent.side,
                submitted_at_utc=submitted_ts,
            )

        return SubmitResult(
            submitted=True,
            mode=request.mode,
            client_order_id=request.client_order_id,
            submit_status=TESTNET_ACK_SIMULATED,
            exchange_order_id=f"SIM-{request.client_order_id[-12:]}",
            exchange_ret_code="0",
            exchange_ret_message="OK",
            reason="stubbed TESTNET submit ack classified; no exchange mutation performed",
            canonical_setup_key=request.canonical_setup_key,
            symbol=request.symbol,
            side=exchange_ready_intent.side,
            submitted_at_utc=submitted_ts,
        )


def _validate_submit_inputs(
    command_decision: CommandDecision,
    exchange_ready_intent: ExchangeReadyIntent,
    *,
    submitted_at_utc: str | None = None,
) -> SubmitResult | None:
    submitted_ts = submitted_at_utc or _utc_now()

    if not isinstance(command_decision, CommandDecision):
        return _blocked_result(
            mode="",
            status=BLOCKED_COMMAND_NOT_ALLOWED,
            reason="submit adapter requires E14 CommandDecision",
            key="",
            symbol="",
            side="",
            client_order_id="",
            submitted_at_utc=submitted_ts,
        )
    if not isinstance(exchange_ready_intent, ExchangeReadyIntent):
        return _blocked_result(
            mode=command_decision.mode,
            status=BLOCKED_INVALID_INTENT,
            reason="submit adapter requires ExchangeReadyIntent",
            key=command_decision.canonical_setup_key,
            symbol=command_decision.symbol,
            side=command_decision.side,
            client_order_id=command_decision.client_order_id,
            submitted_at_utc=submitted_ts,
        )
    if command_decision.mode == MODE_LIVE:
        return _blocked_result(
            mode=command_decision.mode,
            status=BLOCKED_LIVE_FORBIDDEN,
            reason="LIVE mode is forbidden in E15 submit adapter",
            key=command_decision.canonical_setup_key,
            symbol=command_decision.symbol,
            side=command_decision.side,
            client_order_id=command_decision.client_order_id,
            submitted_at_utc=submitted_ts,
        )
    if command_decision.mode != MODE_TESTNET:
        return _blocked_result(
            mode=command_decision.mode,
            status=BLOCKED_NOT_TESTNET,
            reason="submit adapter is disabled unless E14 approved explicit TESTNET mode",
            key=command_decision.canonical_setup_key,
            symbol=command_decision.symbol,
            side=command_decision.side,
            client_order_id=command_decision.client_order_id,
            submitted_at_utc=submitted_ts,
        )
    if not command_decision.allowed:
        return _blocked_result(
            mode=command_decision.mode,
            status=BLOCKED_COMMAND_NOT_ALLOWED,
            reason="E14 CommandDecision is not allowed",
            key=command_decision.canonical_setup_key,
            symbol=command_decision.symbol,
            side=command_decision.side,
            client_order_id=command_decision.client_order_id,
            submitted_at_utc=submitted_ts,
        )
    if not command_decision.reserved_in_ledger:
        return _blocked_result(
            mode=command_decision.mode,
            status=BLOCKED_NOT_RESERVED,
            reason="pre-submit reservation is required before TESTNET submit skeleton",
            key=command_decision.canonical_setup_key,
            symbol=command_decision.symbol,
            side=command_decision.side,
            client_order_id=command_decision.client_order_id,
            submitted_at_utc=submitted_ts,
        )
    if not command_decision.client_order_id:
        return _blocked_result(
            mode=command_decision.mode,
            status=BLOCKED_MISSING_CLIENT_ORDER_ID,
            reason="deterministic client_order_id is required",
            key=command_decision.canonical_setup_key,
            symbol=command_decision.symbol,
            side=command_decision.side,
            client_order_id="",
            submitted_at_utc=submitted_ts,
        )
    expected_client_order_id = deterministic_testnet_client_order_id(exchange_ready_intent)
    if command_decision.client_order_id != expected_client_order_id:
        return _blocked_result(
            mode=command_decision.mode,
            status=BLOCKED_MISSING_CLIENT_ORDER_ID,
            reason="client_order_id is not deterministic for supplied ExchangeReadyIntent",
            key=command_decision.canonical_setup_key,
            symbol=command_decision.symbol,
            side=command_decision.side,
            client_order_id=command_decision.client_order_id,
            submitted_at_utc=submitted_ts,
        )
    if command_decision.canonical_setup_key != exchange_ready_intent.canonical_setup_key:
        return _blocked_result(
            mode=command_decision.mode,
            status=BLOCKED_INVALID_INTENT,
            reason="CommandDecision and ExchangeReadyIntent canonical_setup_key mismatch",
            key=command_decision.canonical_setup_key,
            symbol=command_decision.symbol,
            side=command_decision.side,
            client_order_id=command_decision.client_order_id,
            submitted_at_utc=submitted_ts,
        )
    if not exchange_ready_intent.exchange_ready:
        return _blocked_result(
            mode=command_decision.mode,
            status=BLOCKED_INVALID_INTENT,
            reason="ExchangeReadyIntent is not exchange_ready",
            key=exchange_ready_intent.canonical_setup_key,
            symbol=exchange_ready_intent.symbol,
            side=exchange_ready_intent.side,
            client_order_id=command_decision.client_order_id,
            submitted_at_utc=submitted_ts,
        )

    return None


def _blocked_result(
    *,
    mode: str,
    status: str,
    reason: str,
    key: str,
    symbol: str,
    side: str,
    client_order_id: str,
    submitted_at_utc: str,
) -> SubmitResult:
    return SubmitResult(
        submitted=False,
        mode=mode,
        client_order_id=client_order_id,
        submit_status=status,
        exchange_order_id="",
        exchange_ret_code="",
        exchange_ret_message="",
        reason=reason,
        canonical_setup_key=key,
        symbol=symbol,
        side=side,
        submitted_at_utc=submitted_at_utc,
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
