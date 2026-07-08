from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Sequence

from .exchange_read_adapter import ExchangeBalanceSnapshot, ExchangeInstrumentRules, ExchangeOpenOrderSnapshot, ExchangePositionSnapshot
from .execution_event_ledger import ExecutionStateSnapshot


RECONCILIATION_OK = "RECONCILIATION_OK"
LOCAL_OPEN_EXCHANGE_MISSING = "LOCAL_OPEN_EXCHANGE_MISSING"
EXCHANGE_OPEN_LOCAL_MISSING = "EXCHANGE_OPEN_LOCAL_MISSING"
OPEN_ORDER_WITHOUT_LOCAL_STATE = "OPEN_ORDER_WITHOUT_LOCAL_STATE"
LOCAL_UNKNOWN_STATE = "LOCAL_UNKNOWN_STATE"
EXCHANGE_READ_FAILED = "EXCHANGE_READ_FAILED"
MANUAL_REVIEW_REQUIRED = "MANUAL_REVIEW_REQUIRED"


@dataclass(frozen=True)
class ReconciliationResult:
    """Immutable read-only reconciliation classification.

    E13 compares executor-local state with already-supplied read-only exchange
    snapshots. It does not call exchanges, submit/cancel/amend orders, close
    positions, write ATS production files, calculate strategy risk, change
    quantity decisions, or perform automatic recovery actions.
    """

    canonical_setup_key: str
    symbol: str
    reconciliation_status: str
    local_state: str
    exchange_position_status: str
    open_orders_count: int
    mismatch_detected: bool
    block_new_orders: bool
    requires_manual_review: bool
    reason: str
    checked_at_utc: str


class ExchangeReconciler:
    """Deterministic read-only classifier for local/exchange state mismatch.

    The reconciler receives immutable executor and exchange snapshots produced by
    earlier layers. It is intentionally passive: it returns a classification and
    safety recommendation only. It never reads exchange state directly and never
    mutates either ATS or exchange state.
    """

    def reconcile(
        self,
        *,
        canonical_setup_key: str,
        symbol: str,
        local_snapshot: ExecutionStateSnapshot | None,
        exchange_positions: Sequence[ExchangePositionSnapshot] = (),
        exchange_open_orders: Sequence[ExchangeOpenOrderSnapshot] = (),
        exchange_balances: Sequence[ExchangeBalanceSnapshot] = (),
        instrument_rules: Sequence[ExchangeInstrumentRules] = (),
        exchange_read_ok: bool = True,
        exchange_read_error: str = "",
        checked_at_utc: str | None = None,
    ) -> ReconciliationResult:
        del exchange_balances, instrument_rules  # optional future comparison inputs; no decisions yet

        normalized_key = canonical_setup_key.strip()
        normalized_symbol = symbol.strip().upper()
        if not normalized_key:
            raise ValueError("canonical_setup_key is required")
        if not normalized_symbol:
            raise ValueError("symbol is required")

        checked_ts = checked_at_utc or _utc_now()
        matching_positions = tuple(
            position for position in exchange_positions if _same_symbol(position.symbol, normalized_symbol) and _nonzero(position.quantity)
        )
        matching_orders = tuple(
            order for order in exchange_open_orders if _same_symbol(order.symbol, normalized_symbol) and _nonzero(order.remaining_quantity)
        )
        open_orders_count = len(matching_orders)
        has_exchange_position = bool(matching_positions)
        exchange_position_status = "OPEN" if has_exchange_position else "MISSING"
        local_state = local_snapshot.current_state if local_snapshot is not None else "NO_LOCAL_STATE"

        if not exchange_read_ok:
            return _result(
                key=normalized_key,
                symbol=normalized_symbol,
                status=EXCHANGE_READ_FAILED,
                local_state=local_state,
                exchange_position_status="UNKNOWN",
                open_orders_count=open_orders_count,
                mismatch=True,
                block=True,
                manual=True,
                reason=exchange_read_error or "read-only exchange snapshot failed; fail closed",
                checked_at_utc=checked_ts,
            )

        if local_snapshot is not None and local_snapshot.has_unknown_state:
            return _result(
                key=normalized_key,
                symbol=normalized_symbol,
                status=LOCAL_UNKNOWN_STATE,
                local_state=local_state,
                exchange_position_status=exchange_position_status,
                open_orders_count=open_orders_count,
                mismatch=True,
                block=True,
                manual=True,
                reason="local executor snapshot is unknown; block new orders until reconciliation",
                checked_at_utc=checked_ts,
            )

        if local_snapshot is None or local_snapshot.current_state == "NO_EVENTS":
            if open_orders_count > 0:
                return _result(
                    key=normalized_key,
                    symbol=normalized_symbol,
                    status=OPEN_ORDER_WITHOUT_LOCAL_STATE,
                    local_state=local_state,
                    exchange_position_status=exchange_position_status,
                    open_orders_count=open_orders_count,
                    mismatch=True,
                    block=True,
                    manual=True,
                    reason="exchange has open orders but executor has no local state",
                    checked_at_utc=checked_ts,
                )
            if has_exchange_position:
                return _result(
                    key=normalized_key,
                    symbol=normalized_symbol,
                    status=EXCHANGE_OPEN_LOCAL_MISSING,
                    local_state=local_state,
                    exchange_position_status=exchange_position_status,
                    open_orders_count=open_orders_count,
                    mismatch=True,
                    block=True,
                    manual=True,
                    reason="exchange position exists but executor has no local state",
                    checked_at_utc=checked_ts,
                )
            return _result(
                key=normalized_key,
                symbol=normalized_symbol,
                status=RECONCILIATION_OK,
                local_state=local_state,
                exchange_position_status=exchange_position_status,
                open_orders_count=0,
                mismatch=False,
                block=False,
                manual=False,
                reason="no local or exchange exposure detected",
                checked_at_utc=checked_ts,
            )

        local_expects_position = local_snapshot.current_state == "FILLED_CONFIRMED"
        if local_expects_position and not has_exchange_position:
            return _result(
                key=normalized_key,
                symbol=normalized_symbol,
                status=LOCAL_OPEN_EXCHANGE_MISSING,
                local_state=local_state,
                exchange_position_status=exchange_position_status,
                open_orders_count=open_orders_count,
                mismatch=True,
                block=True,
                manual=True,
                reason="executor local state is filled/open but exchange position is missing",
                checked_at_utc=checked_ts,
            )
        if has_exchange_position and not local_expects_position and local_snapshot.terminal:
            return _result(
                key=normalized_key,
                symbol=normalized_symbol,
                status=EXCHANGE_OPEN_LOCAL_MISSING,
                local_state=local_state,
                exchange_position_status=exchange_position_status,
                open_orders_count=open_orders_count,
                mismatch=True,
                block=True,
                manual=True,
                reason="exchange position exists but local terminal state does not expect exposure",
                checked_at_utc=checked_ts,
            )
        if local_snapshot.requires_manual_review or local_snapshot.block_new_orders:
            return _result(
                key=normalized_key,
                symbol=normalized_symbol,
                status=MANUAL_REVIEW_REQUIRED,
                local_state=local_state,
                exchange_position_status=exchange_position_status,
                open_orders_count=open_orders_count,
                mismatch=True,
                block=True,
                manual=True,
                reason="local executor snapshot already requires blocking/manual review",
                checked_at_utc=checked_ts,
            )

        return _result(
            key=normalized_key,
            symbol=normalized_symbol,
            status=RECONCILIATION_OK,
            local_state=local_state,
            exchange_position_status=exchange_position_status,
            open_orders_count=open_orders_count,
            mismatch=False,
            block=False,
            manual=False,
            reason="local executor state matches supplied read-only exchange snapshot",
            checked_at_utc=checked_ts,
        )


def _result(
    *,
    key: str,
    symbol: str,
    status: str,
    local_state: str,
    exchange_position_status: str,
    open_orders_count: int,
    mismatch: bool,
    block: bool,
    manual: bool,
    reason: str,
    checked_at_utc: str,
) -> ReconciliationResult:
    return ReconciliationResult(
        canonical_setup_key=key,
        symbol=symbol,
        reconciliation_status=status,
        local_state=local_state,
        exchange_position_status=exchange_position_status,
        open_orders_count=open_orders_count,
        mismatch_detected=mismatch,
        block_new_orders=block,
        requires_manual_review=manual,
        reason=reason,
        checked_at_utc=checked_at_utc,
    )


def _same_symbol(left: str, right: str) -> bool:
    return left.strip().upper() == right.strip().upper()


def _nonzero(value: Decimal) -> bool:
    return value.copy_abs() > Decimal("0")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
