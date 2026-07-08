from __future__ import annotations

import argparse
import os
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

from backtest.execution.exchange_reconciler import RECONCILIATION_OK, ReconciliationResult
from backtest.execution.execution_event_ledger import ExecutionEventLedger
from backtest.execution.quantity_converter import ExchangeReadyIntent
from backtest.execution.testnet_command_gateway import CommandDecision, MODE_TESTNET
from backtest.execution.testnet_real_submit import (
    BybitTestnetCredentials,
    BybitTestnetSubmitConfig,
    RealTestnetSubmitExecutor,
    UrllibBybitTestnetSubmitTransport,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Optional E17 real Bybit TESTNET submit validation. Places at most one tiny TESTNET order.")
    parser.add_argument("--i-understand-this-places-one-testnet-order", action="store_true", required=True)
    parser.add_argument("--manual-enable", action="store_true", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--side", choices=("LONG", "SHORT"), required=True)
    parser.add_argument("--qty", required=True)
    parser.add_argument("--price", required=True)
    parser.add_argument("--max-notional", required=True)
    parser.add_argument("--client-order-id", required=True)
    parser.add_argument("--canonical-setup-key", default="E17_REAL_TESTNET_VALIDATION")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    api_key = os.environ.get("BYBIT_TESTNET_API_KEY", "")
    api_secret = os.environ.get("BYBIT_TESTNET_API_SECRET", "")
    if not api_key or not api_secret:
        raise SystemExit("Missing BYBIT_TESTNET_API_KEY / BYBIT_TESTNET_API_SECRET")

    qty = Decimal(args.qty)
    price = Decimal(args.price)
    intent = ExchangeReadyIntent(
        canonical_setup_key=args.canonical_setup_key,
        symbol=args.symbol.upper(),
        side=args.side,
        quantity=qty,
        rounded_quantity=qty,
        quantity_precision=8,
        exchange_ready=True,
        conversion_reason="manual_e17_real_testnet_validation",
        entry=price,
        stop=price,
        target=price,
    )
    command = CommandDecision(
        allowed=True,
        mode=MODE_TESTNET,
        client_order_id=args.client_order_id,
        command_status="COMMAND_READY_TESTNET",
        reason="manual E17 real TESTNET validation",
        canonical_setup_key=intent.canonical_setup_key,
        symbol=intent.symbol,
        side=intent.side,
        reserved_in_ledger=True,
        requires_manual_review=False,
        decided_at_utc="manual",
    )
    reconciliation = ReconciliationResult(
        canonical_setup_key=intent.canonical_setup_key,
        symbol=intent.symbol,
        reconciliation_status=RECONCILIATION_OK,
        local_state="MANUAL_TESTNET_VALIDATION",
        exchange_position_status="MISSING",
        open_orders_count=0,
        mismatch_detected=False,
        block_new_orders=False,
        requires_manual_review=False,
        reason="manual E17 validation assumes pre-check completed",
        checked_at_utc="manual",
    )

    with TemporaryDirectory() as tmpdir:
        ledger = ExecutionEventLedger(Path(tmpdir) / "e17_real_testnet_ledger.jsonl")
        snapshot = RealTestnetSubmitExecutor().submit_once(
            command_decision=command,
            exchange_ready_intent=intent,
            reconciliation_result=reconciliation,
            ledger=ledger,
            credentials=BybitTestnetCredentials(api_key=api_key, api_secret=api_secret),
            config=BybitTestnetSubmitConfig(manual_enable=args.manual_enable, emergency_max_notional=Decimal(args.max_notional)),
            transport=UrllibBybitTestnetSubmitTransport(),
        )
        print("E17_REAL_TESTNET_SUBMIT_RESULT")
        print(snapshot)
        print("ledger_events", ledger.events)


if __name__ == "__main__":
    main()
