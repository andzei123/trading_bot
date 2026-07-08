from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone

from .exchange_reconciler import RECONCILIATION_OK, ReconciliationResult
from .execution_event_ledger import ExecutionEventLedger, RESERVED_PRE_SUBMIT
from .quantity_converter import ExchangeReadyIntent


MODE_DISABLED = "DISABLED"
MODE_READ_ONLY = "READ_ONLY"
MODE_TESTNET = "TESTNET"
MODE_LIVE = "LIVE"

COMMAND_READY_TESTNET = "COMMAND_READY_TESTNET"
BLOCKED_NOT_TESTNET = "BLOCKED_NOT_TESTNET"
BLOCKED_LIVE_FORBIDDEN = "BLOCKED_LIVE_FORBIDDEN"
BLOCKED_RECONCILIATION_NOT_OK = "BLOCKED_RECONCILIATION_NOT_OK"
BLOCKED_MANUAL_ENABLE_MISSING = "BLOCKED_MANUAL_ENABLE_MISSING"
BLOCKED_INVALID_INTENT = "BLOCKED_INVALID_INTENT"
RESERVED_PRE_SUBMIT_STATUS = "RESERVED_PRE_SUBMIT"


@dataclass(frozen=True)
class CommandDecision:
    """Immutable E14 command-gateway decision.

    E14 is a TESTNET-only gateway skeleton. It can reserve a future command in
    the executor-local ledger, but it never submits/cancels/amends orders,
    calls mutation endpoints, enables LIVE, writes ATS production files,
    recalculates strategy risk, changes quantity, or integrates with the shell.
    """

    allowed: bool
    mode: str
    client_order_id: str
    command_status: str
    reason: str
    canonical_setup_key: str
    symbol: str
    side: str
    reserved_in_ledger: bool
    requires_manual_review: bool
    decided_at_utc: str


class TestnetCommandGateway:
    """Gated TESTNET command skeleton with append-only pre-submit reservation.

    Default mode is disabled/read-only safe. LIVE is always blocked in E14, even
    when manually enabled. No exchange mutation adapter is accepted or called.
    """

    def __init__(self, *, mode: str = MODE_DISABLED, manual_enable: bool = False) -> None:
        self.mode = _normalize_mode(mode)
        self.manual_enable = bool(manual_enable)

    def prepare_command(
        self,
        *,
        exchange_ready_intent: ExchangeReadyIntent,
        reconciliation_result: ReconciliationResult,
        ledger: ExecutionEventLedger,
        decided_at_utc: str | None = None,
    ) -> CommandDecision:
        if not isinstance(exchange_ready_intent, ExchangeReadyIntent):
            return _blocked(
                mode=self.mode,
                status=BLOCKED_INVALID_INTENT,
                reason="command gateway requires ExchangeReadyIntent",
                key="",
                symbol="",
                side="",
                manual=True,
                decided_at_utc=decided_at_utc,
            )
        if not isinstance(reconciliation_result, ReconciliationResult):
            return _blocked(
                mode=self.mode,
                status=BLOCKED_RECONCILIATION_NOT_OK,
                reason="command gateway requires ReconciliationResult",
                key=exchange_ready_intent.canonical_setup_key,
                symbol=exchange_ready_intent.symbol,
                side=exchange_ready_intent.side,
                manual=True,
                decided_at_utc=decided_at_utc,
            )
        if not isinstance(ledger, ExecutionEventLedger):
            return _blocked(
                mode=self.mode,
                status=BLOCKED_INVALID_INTENT,
                reason="command gateway requires ExecutionEventLedger",
                key=exchange_ready_intent.canonical_setup_key,
                symbol=exchange_ready_intent.symbol,
                side=exchange_ready_intent.side,
                manual=True,
                decided_at_utc=decided_at_utc,
            )

        key = exchange_ready_intent.canonical_setup_key.strip()
        symbol = exchange_ready_intent.symbol.strip().upper()
        side = exchange_ready_intent.side.strip().upper()
        decided_ts = decided_at_utc or _utc_now()

        if not key or not symbol or side not in {"LONG", "SHORT"} or not exchange_ready_intent.exchange_ready:
            return _blocked(
                mode=self.mode,
                status=BLOCKED_INVALID_INTENT,
                reason="exchange-ready intent is incomplete or not exchange_ready",
                key=key,
                symbol=symbol,
                side=side,
                manual=True,
                decided_at_utc=decided_ts,
            )
        if self.mode == MODE_LIVE:
            return _blocked(
                mode=self.mode,
                status=BLOCKED_LIVE_FORBIDDEN,
                reason="LIVE mode is forbidden in E14 command gateway",
                key=key,
                symbol=symbol,
                side=side,
                manual=True,
                decided_at_utc=decided_ts,
            )
        if self.mode != MODE_TESTNET:
            return _blocked(
                mode=self.mode,
                status=BLOCKED_NOT_TESTNET,
                reason="command gateway is disabled unless explicit TESTNET mode is selected",
                key=key,
                symbol=symbol,
                side=side,
                manual=False,
                decided_at_utc=decided_ts,
            )
        if not self.manual_enable:
            return _blocked(
                mode=self.mode,
                status=BLOCKED_MANUAL_ENABLE_MISSING,
                reason="TESTNET command gateway requires explicit manual enable flag",
                key=key,
                symbol=symbol,
                side=side,
                manual=False,
                decided_at_utc=decided_ts,
            )
        if reconciliation_result.reconciliation_status != RECONCILIATION_OK or reconciliation_result.block_new_orders:
            return _blocked(
                mode=self.mode,
                status=BLOCKED_RECONCILIATION_NOT_OK,
                reason="reconciliation is not OK; command preparation blocked",
                key=key,
                symbol=symbol,
                side=side,
                manual=reconciliation_result.requires_manual_review,
                decided_at_utc=decided_ts,
            )

        client_order_id = deterministic_testnet_client_order_id(exchange_ready_intent)
        ledger.append_event(
            canonical_setup_key=key,
            event_type=RESERVED_PRE_SUBMIT,
            recorded_at_utc=decided_ts,
            symbol=symbol,
            side=side,
            client_order_id=client_order_id,
            status=RESERVED_PRE_SUBMIT_STATUS,
            block_new_orders=True,
            requires_manual_review=False,
            reason="testnet pre-submit reservation created; no exchange mutation performed",
        )
        return CommandDecision(
            allowed=True,
            mode=self.mode,
            client_order_id=client_order_id,
            command_status=COMMAND_READY_TESTNET,
            reason="TESTNET command reserved in executor-local ledger; no exchange mutation performed",
            canonical_setup_key=key,
            symbol=symbol,
            side=side,
            reserved_in_ledger=True,
            requires_manual_review=False,
            decided_at_utc=decided_ts,
        )


def deterministic_testnet_client_order_id(intent: ExchangeReadyIntent) -> str:
    """Return deterministic testnet client order id without exchange access."""

    basis = "|".join(
        [
            intent.canonical_setup_key.strip(),
            intent.symbol.strip().upper(),
            intent.side.strip().upper(),
            str(intent.rounded_quantity),
            str(intent.entry),
            str(intent.stop),
            str(intent.target),
        ]
    )
    digest = hashlib.sha256(basis.encode("utf-8")).hexdigest()[:24].upper()
    return f"ATS_TESTNET_{digest}"


def _blocked(
    *,
    mode: str,
    status: str,
    reason: str,
    key: str,
    symbol: str,
    side: str,
    manual: bool,
    decided_at_utc: str | None,
) -> CommandDecision:
    return CommandDecision(
        allowed=False,
        mode=mode,
        client_order_id="",
        command_status=status,
        reason=reason,
        canonical_setup_key=key,
        symbol=symbol,
        side=side,
        reserved_in_ledger=False,
        requires_manual_review=manual,
        decided_at_utc=decided_at_utc or _utc_now(),
    )


def _normalize_mode(mode: str) -> str:
    normalized = str(mode or MODE_DISABLED).strip().upper()
    return normalized or MODE_DISABLED


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
