from __future__ import annotations

import hashlib
import hmac
import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Protocol

from .exchange_reconciler import RECONCILIATION_OK, ReconciliationResult
from .execution_event_ledger import ExecutionEventLedger, ExecutionStateSnapshot
from .quantity_converter import ExchangeReadyIntent
from .submit_outcome_handler import SubmitOutcomeHandler, TESTNET_SUBMIT_UNKNOWN, TESTNET_TIMEOUT_UNKNOWN
from .testnet_command_gateway import CommandDecision, MODE_LIVE, MODE_TESTNET
from .testnet_submit_adapter import (
    BLOCKED_COMMAND_NOT_ALLOWED,
    BLOCKED_INVALID_INTENT,
    BLOCKED_LIVE_FORBIDDEN,
    BLOCKED_MISSING_CLIENT_ORDER_ID,
    BLOCKED_NOT_RESERVED,
    BLOCKED_NOT_TESTNET,
    SubmitResult,
)


TESTNET_ACK_CONFIRMED = "TESTNET_ACK_CONFIRMED"
TESTNET_REJECT_CONFIRMED = "TESTNET_REJECT_CONFIRMED"
BLOCKED_RECONCILIATION_NOT_OK = "BLOCKED_RECONCILIATION_NOT_OK"
BLOCKED_MANUAL_ENABLE_MISSING = "BLOCKED_MANUAL_ENABLE_MISSING"
BLOCKED_MISSING_TESTNET_CREDENTIALS = "BLOCKED_MISSING_TESTNET_CREDENTIALS"
BLOCKED_NOTIONAL_CAP_EXCEEDED = "BLOCKED_NOTIONAL_CAP_EXCEEDED"

BYBIT_TESTNET_ENDPOINT = "https://api-testnet.bybit." + "com"
BYBIT_CREATE_ORDER_PATH = "/v5/order/" + "create"


@dataclass(frozen=True)
class BybitTestnetCredentials:
    """Externally supplied Bybit TESTNET credentials.

    Secrets are never hardcoded by E17. Missing credentials fail closed before
    any mutation transport can be called.
    """

    api_key: str
    api_secret: str

    @property
    def complete(self) -> bool:
        return bool(self.api_key.strip() and self.api_secret.strip())


@dataclass(frozen=True)
class BybitTestnetSubmitConfig:
    """Immutable E17 TESTNET submit configuration.

    LIVE mode is forbidden. This config contains no TP/SL, retry, cancel,
    amend, close-position, protection-order, or strategy/risk recalculation
    behavior.
    """

    mode: str = MODE_TESTNET
    manual_enable: bool = False
    endpoint: str = BYBIT_TESTNET_ENDPOINT
    category: str = "linear"
    order_type: str = "Limit"
    time_in_force: str = "GTC"
    recv_window_ms: int = 5000
    emergency_max_notional: Decimal = Decimal("0")


@dataclass(frozen=True)
class BybitTestnetSubmitRequest:
    """Normalized Bybit TESTNET order-create request.

    It is built only after all E17 gates pass. It deliberately contains no
    TP/SL or protection-order fields.
    """

    endpoint: str
    path: str
    category: str
    symbol: str
    side: str
    order_type: str
    qty: str
    price: str
    time_in_force: str
    order_link_id: str
    reduce_only: bool
    testnet_only: bool

    def payload(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "symbol": self.symbol,
            "side": self.side,
            "orderType": self.order_type,
            "qty": self.qty,
            "price": self.price,
            "timeInForce": self.time_in_force,
            "orderLinkId": self.order_link_id,
            "reduceOnly": self.reduce_only,
        }


class BybitTestnetSubmitTransport(Protocol):
    """Tiny mutation transport boundary for TESTNET order-create only."""

    def post_order(self, *, request: BybitTestnetSubmitRequest, credentials: BybitTestnetCredentials) -> dict[str, Any]:
        ...


class UrllibBybitTestnetSubmitTransport:
    """Retired E17 direct network mutator. R1 production owns all mutation transport."""
    def post_order(self, *, request: BybitTestnetSubmitRequest, credentials: BybitTestnetCredentials) -> dict[str, Any]:
        raise RuntimeError("direct TESTNET mutation transport retired by R1; use R1TestnetProduct")


class RealTestnetSubmitExecutor:
    """Retired production-public E17 mutation entrypoint.

    R1 requires all CREATE/CANCEL mutation to pass through R1TestnetProduct.
    The legacy type remains importable only so old callers fail closed explicitly.
    """
    def submit_once(self, **kwargs):
        raise RuntimeError("legacy direct TESTNET submit authority retired by R1")
    def submit_once_result(self, **kwargs):
        raise RuntimeError("legacy direct TESTNET submit authority retired by R1")


def build_bybit_testnet_submit_request(
    *,
    exchange_ready_intent: ExchangeReadyIntent,
    command_decision: CommandDecision,
    config: BybitTestnetSubmitConfig,
) -> BybitTestnetSubmitRequest:
    """Build one normalized TESTNET order-create request after gate validation."""

    return BybitTestnetSubmitRequest(
        endpoint=config.endpoint.rstrip("/"),
        path=BYBIT_CREATE_ORDER_PATH,
        category=config.category,
        symbol=exchange_ready_intent.symbol,
        side="Buy" if exchange_ready_intent.side == "LONG" else "Sell",
        order_type=config.order_type,
        qty=_decimal_to_plain_string(exchange_ready_intent.rounded_quantity),
        price=_decimal_to_plain_string(exchange_ready_intent.entry),
        time_in_force=config.time_in_force,
        order_link_id=command_decision.client_order_id,
        reduce_only=False,
        testnet_only=True,
    )


def normalize_bybit_testnet_submit_response(
    *,
    response: dict[str, Any],
    command_decision: CommandDecision,
    exchange_ready_intent: ExchangeReadyIntent,
    submitted_at_utc: str,
) -> SubmitResult:
    """Normalize one Bybit TESTNET response into the existing SubmitResult."""

    ret_code = str(response.get("retCode", ""))
    ret_message = str(response.get("retMsg", ""))
    result = response.get("result") if isinstance(response.get("result"), dict) else {}
    exchange_order_id = str(result.get("orderId", "")) if isinstance(result, dict) else ""

    if ret_code == "0":
        return _submit_result(
            submitted=True,
            status=TESTNET_ACK_CONFIRMED,
            reason="Bybit TESTNET submit acknowledged; E17 performed no retry and no TP/SL",
            command_decision=command_decision,
            exchange_ready_intent=exchange_ready_intent,
            submitted_at_utc=submitted_at_utc,
            exchange_order_id=exchange_order_id,
            exchange_ret_code=ret_code,
            exchange_ret_message=ret_message or "OK",
        )

    return _submit_result(
        submitted=False,
        status=TESTNET_REJECT_CONFIRMED,
        reason="Bybit TESTNET submit rejected; terminal submit failure without retry",
        command_decision=command_decision,
        exchange_ready_intent=exchange_ready_intent,
        submitted_at_utc=submitted_at_utc,
        exchange_ret_code=ret_code,
        exchange_ret_message=ret_message,
    )


def _validate_e17_gates(
    *,
    command_decision: CommandDecision,
    exchange_ready_intent: ExchangeReadyIntent,
    reconciliation_result: ReconciliationResult,
    credentials: BybitTestnetCredentials | None,
    config: BybitTestnetSubmitConfig,
    submitted_at_utc: str,
) -> SubmitResult | None:
    if not isinstance(command_decision, CommandDecision):
        return _blocked(status=BLOCKED_COMMAND_NOT_ALLOWED, reason="E17 requires E14 CommandDecision", submitted_at_utc=submitted_at_utc)
    if not isinstance(exchange_ready_intent, ExchangeReadyIntent):
        return _blocked_from_command(
            command_decision,
            status=BLOCKED_INVALID_INTENT,
            reason="E17 requires ExchangeReadyIntent",
            submitted_at_utc=submitted_at_utc,
        )
    if not isinstance(reconciliation_result, ReconciliationResult):
        return _blocked_from_command(
            command_decision,
            status=BLOCKED_RECONCILIATION_NOT_OK,
            reason="E17 requires ReconciliationResult",
            submitted_at_utc=submitted_at_utc,
        )
    if not isinstance(config, BybitTestnetSubmitConfig):
        return _blocked_from_command(
            command_decision,
            status=BLOCKED_NOT_TESTNET,
            reason="E17 requires BybitTestnetSubmitConfig",
            submitted_at_utc=submitted_at_utc,
        )
    if command_decision.mode == MODE_LIVE or config.mode == MODE_LIVE:
        return _blocked_from_command(
            command_decision,
            status=BLOCKED_LIVE_FORBIDDEN,
            reason="LIVE mode is impossible in E17",
            submitted_at_utc=submitted_at_utc,
        )
    if command_decision.mode != MODE_TESTNET or config.mode != MODE_TESTNET:
        return _blocked_from_command(
            command_decision,
            status=BLOCKED_NOT_TESTNET,
            reason="E17 real submit requires explicit TESTNET mode",
            submitted_at_utc=submitted_at_utc,
        )
    if not config.endpoint.startswith(BYBIT_TESTNET_ENDPOINT):
        return _blocked_from_command(
            command_decision,
            status=BLOCKED_LIVE_FORBIDDEN,
            reason="E17 endpoint must be Bybit TESTNET endpoint",
            submitted_at_utc=submitted_at_utc,
        )
    if not config.manual_enable:
        return _blocked_from_command(
            command_decision,
            status=BLOCKED_MANUAL_ENABLE_MISSING,
            reason="manual_enable=True is required for E17 TESTNET submit",
            submitted_at_utc=submitted_at_utc,
        )
    if not command_decision.allowed:
        return _blocked_from_command(
            command_decision,
            status=BLOCKED_COMMAND_NOT_ALLOWED,
            reason="E14 CommandDecision is not allowed",
            submitted_at_utc=submitted_at_utc,
        )
    if not command_decision.reserved_in_ledger:
        return _blocked_from_command(
            command_decision,
            status=BLOCKED_NOT_RESERVED,
            reason="E14 pre-submit reservation is required before E17 submit",
            submitted_at_utc=submitted_at_utc,
        )
    if not command_decision.client_order_id:
        return _blocked_from_command(
            command_decision,
            status=BLOCKED_MISSING_CLIENT_ORDER_ID,
            reason="deterministic client_order_id is required",
            submitted_at_utc=submitted_at_utc,
        )
    if command_decision.canonical_setup_key != exchange_ready_intent.canonical_setup_key:
        return _blocked_from_command(
            command_decision,
            status=BLOCKED_INVALID_INTENT,
            reason="CommandDecision and ExchangeReadyIntent canonical_setup_key mismatch",
            submitted_at_utc=submitted_at_utc,
        )
    if not exchange_ready_intent.exchange_ready or exchange_ready_intent.rounded_quantity <= 0:
        return _blocked_from_command(
            command_decision,
            status=BLOCKED_INVALID_INTENT,
            reason="ExchangeReadyIntent must be exchange_ready with positive rounded_quantity",
            submitted_at_utc=submitted_at_utc,
        )
    if reconciliation_result.reconciliation_status != RECONCILIATION_OK or reconciliation_result.block_new_orders:
        return _blocked_from_command(
            command_decision,
            status=BLOCKED_RECONCILIATION_NOT_OK,
            reason="E17 submit requires RECONCILIATION_OK and no block_new_orders",
            submitted_at_utc=submitted_at_utc,
        )
    if credentials is None or not credentials.complete:
        return _blocked_from_command(
            command_decision,
            status=BLOCKED_MISSING_TESTNET_CREDENTIALS,
            reason="external Bybit TESTNET credentials are required",
            submitted_at_utc=submitted_at_utc,
        )
    notional = exchange_ready_intent.rounded_quantity * exchange_ready_intent.entry
    if config.emergency_max_notional and notional > config.emergency_max_notional:
        return _blocked_from_command(
            command_decision,
            status=BLOCKED_NOTIONAL_CAP_EXCEEDED,
            reason="executor-local emergency max notional cap exceeded",
            submitted_at_utc=submitted_at_utc,
        )
    return None


def _submit_result(
    *,
    submitted: bool,
    status: str,
    reason: str,
    command_decision: CommandDecision,
    exchange_ready_intent: ExchangeReadyIntent,
    submitted_at_utc: str,
    exchange_order_id: str = "",
    exchange_ret_code: str = "",
    exchange_ret_message: str = "",
) -> SubmitResult:
    return SubmitResult(
        submitted=submitted,
        mode=command_decision.mode,
        client_order_id=command_decision.client_order_id,
        submit_status=status,
        exchange_order_id=exchange_order_id,
        exchange_ret_code=exchange_ret_code,
        exchange_ret_message=exchange_ret_message,
        reason=reason,
        canonical_setup_key=exchange_ready_intent.canonical_setup_key,
        symbol=exchange_ready_intent.symbol,
        side=exchange_ready_intent.side,
        submitted_at_utc=submitted_at_utc,
    )


def _blocked_from_command(command_decision: CommandDecision, *, status: str, reason: str, submitted_at_utc: str) -> SubmitResult:
    return SubmitResult(
        submitted=False,
        mode=command_decision.mode,
        client_order_id=command_decision.client_order_id,
        submit_status=status,
        exchange_order_id="",
        exchange_ret_code="",
        exchange_ret_message="",
        reason=reason,
        canonical_setup_key=command_decision.canonical_setup_key,
        symbol=command_decision.symbol,
        side=command_decision.side,
        submitted_at_utc=submitted_at_utc,
    )


def _blocked(*, status: str, reason: str, submitted_at_utc: str) -> SubmitResult:
    return SubmitResult(
        submitted=False,
        mode="",
        client_order_id="",
        submit_status=status,
        exchange_order_id="",
        exchange_ret_code="",
        exchange_ret_message="",
        reason=reason,
        canonical_setup_key="",
        symbol="",
        side="",
        submitted_at_utc=submitted_at_utc,
    )


def _decimal_to_plain_string(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
