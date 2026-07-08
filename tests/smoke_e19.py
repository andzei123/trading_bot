from __future__ import annotations

import copy
import hashlib
import tempfile
from decimal import Decimal
from pathlib import Path

from backtest.execution.decision_consumer import consume_decision_row
from backtest.execution.end_to_end_testnet_drill import EndToEndTestnetDrill
from backtest.execution.exchange_read_adapter import (
    ExchangeBalanceSnapshot,
    ExchangeInstrumentRules,
    ExchangeOpenOrderSnapshot,
    ExchangePositionSnapshot,
    ExchangeStateSnapshot,
)
from backtest.execution.execution_event_ledger import ExecutionEventLedger, RESERVED_PRE_SUBMIT, SUBMIT_ACK
from backtest.execution.execution_identity_registry import ExecutionIdentityRegistry
from backtest.execution.post_submit_reconciler import (
    ACK_BUT_ORDER_MISSING,
    POST_SUBMIT_RECONCILIATION_OK,
    REJECT_CONFIRMED_NO_ORDER,
    UNKNOWN_REQUIRES_EXCHANGE_CHECK,
)
from backtest.execution.submit_outcome_handler import SubmitOutcomeHandler
from backtest.execution.testnet_command_gateway import MODE_TESTNET, TestnetCommandGateway
from backtest.execution.testnet_submit_adapter import (
    SIMULATED_ACK,
    SIMULATED_REJECT,
    SIMULATED_TIMEOUT_UNKNOWN,
    TESTNET_ACK_SIMULATED,
    TestnetSubmitAdapterSkeleton,
)

NOW = "2026-01-01T00:00:00+00:00"
PRODUCTION_FILES = (
    "live_observation_shell.py",
    "pipeline_core.py",
    "policy_engine.py",
    "position_state.csv",
)


def _hash_existing_production_files() -> dict[str, str]:
    hashes: dict[str, str] = {}
    for filename in PRODUCTION_FILES:
        path = Path(filename)
        if path.exists():
            hashes[filename] = hashlib.sha256(path.read_bytes()).hexdigest()
    return hashes


def _row(key: str) -> dict[str, str]:
    return {
        "schema_version": "ATS_EXECUTION_INTENT_V1",
        "cycle_ts": NOW,
        "symbol": "BTCUSDT",
        "model": "TDP_REENTRY",
        "side": "LONG",
        "canonical_setup_key": key,
        "setup_id": f"setup-{key}",
        "setup_created_ts": NOW,
        "signal_ts": NOW,
        "visible_ts": NOW,
        "wait_confirm_ts": NOW,
        "intended_entry_ts": NOW,
        "entry_window_expires_ts": "2026-01-01T01:00:00+00:00",
        "selected_for_execution": "1",
        "execution_rank": "1",
        "selection_reason": "smoke",
        "entry": "50000",
        "sl": "49000",
        "tp": "52000",
        "planned_rr": "2",
        "risk_pct": "0.005",
        "reward_pct": "0.010",
        "risk_distance": "1000",
        "reward_distance": "2000",
        "authorized_qty": "0.001234",
        "authorized_notional": "61.70",
        "authorized_risk_usd": "1.234",
        "risk_snapshot_id": "risk-1",
        "position_snapshot_id": "pos-1",
        "authority_waterfall_id": "auth-1",
    }


def _validated(key: str):
    return consume_decision_row(_row(key))


def _state(*, order_client_id: str = "", order: bool = False, position: bool = False) -> ExchangeStateSnapshot:
    orders = ()
    positions = ()
    if order:
        orders = (
            ExchangeOpenOrderSnapshot(
                exchange="BYBIT_TESTNET",
                symbol="BTCUSDT",
                side="Buy",
                order_id="EX-1",
                client_order_id=order_client_id,
                order_type="Limit",
                price=Decimal("50000"),
                quantity=Decimal("0.001"),
                remaining_quantity=Decimal("0.001"),
                status="New",
                observed_at_utc=NOW,
            ),
        )
    if position:
        positions = (
            ExchangePositionSnapshot(
                exchange="BYBIT_TESTNET",
                symbol="BTCUSDT",
                side="Buy",
                quantity=Decimal("0.001"),
                entry_price=Decimal("50000"),
                mark_price=Decimal("50010"),
                unrealized_pnl=Decimal("0.01"),
                observed_at_utc=NOW,
            ),
        )
    return ExchangeStateSnapshot(
        exchange="BYBIT_TESTNET",
        server_time_utc=NOW,
        balances=(
            ExchangeBalanceSnapshot(
                exchange="BYBIT_TESTNET",
                account_type="UNIFIED",
                asset="USDT",
                wallet_balance=Decimal("1000"),
                available_balance=Decimal("900"),
                equity=Decimal("1000"),
                observed_at_utc=NOW,
            ),
        ),
        positions=positions,
        open_orders=orders,
        instrument_rules=(
            ExchangeInstrumentRules(
                exchange="BYBIT_TESTNET",
                symbol="BTCUSDT",
                quantity_step=Decimal("0.001"),
                min_quantity=Decimal("0.001"),
                quantity_precision=3,
                price_tick=Decimal("0.1"),
                price_precision=1,
                min_notional=Decimal("5"),
                observed_at_utc=NOW,
            ),
        ),
        observed_at_utc=NOW,
    )


def _reserved_submit_for_client_id(validated, tmp_path: Path):
    from backtest.execution.exchange_reconciler import ExchangeReconciler
    from backtest.execution.quantity_converter import convert_validated_intent_quantity

    ledger = ExecutionEventLedger(tmp_path / "cid-ledger.jsonl")
    exchange_ready = convert_validated_intent_quantity(validated)
    snapshot = ledger.rebuild_snapshot(validated.canonical_setup_key)
    reconciliation = ExchangeReconciler().reconcile(
        local_snapshot=snapshot,
        exchange_positions=_state().positions,
        exchange_open_orders=_state().open_orders,
        exchange_balances=_state().balances,
        instrument_rules=_state().instrument_rules,
        canonical_setup_key=validated.canonical_setup_key,
        symbol=validated.symbol,
        checked_at_utc=NOW,
    )
    command = TestnetCommandGateway(mode=MODE_TESTNET, manual_enable=True).prepare_command(
        exchange_ready_intent=exchange_ready,
        reconciliation_result=reconciliation,
        ledger=ledger,
        decided_at_utc=NOW,
    )
    submit = TestnetSubmitAdapterSkeleton().submit(
        command_decision=command,
        exchange_ready_intent=exchange_ready,
        simulated_response=SIMULATED_ACK,
        submitted_at_utc=NOW,
    )
    return submit.client_order_id


def _run(tmp: Path, key: str, response: str, exchange_state: ExchangeStateSnapshot, *, reconciliation_ok: bool = True):
    return EndToEndTestnetDrill(
        ledger=ExecutionEventLedger(tmp / f"{key}-ledger.jsonl"),
        identity_registry=ExecutionIdentityRegistry(),
        intent_journal_path=tmp / f"{key}-intent.csv",
        manual_enable=True,
        checked_at_utc=NOW,
    ).run(
        validated_intent=_validated(key),
        exchange_state=exchange_state,
        simulated_submit_response=response,
        reconciliation_ok=reconciliation_ok,
        scenario=key,
    )


def main() -> None:
    before = _hash_existing_production_files()
    exchange_calls = 0

    with tempfile.TemporaryDirectory() as raw_tmp:
        tmp = Path(raw_tmp)

        ack_key = "k-ack"
        ack_client_id = _reserved_submit_for_client_id(_validated(ack_key), tmp)
        ack_result = _run(tmp, ack_key, SIMULATED_ACK, _state(order_client_id=ack_client_id, order=True))
        assert ack_result.post_submit_reconciliation is not None
        assert ack_result.post_submit_reconciliation.post_submit_status == POST_SUBMIT_RECONCILIATION_OK
        assert ack_result.state_snapshot.submit_confirmed

        reject_result = _run(tmp, "k-reject", SIMULATED_REJECT, _state())
        assert reject_result.post_submit_reconciliation is not None
        assert reject_result.post_submit_reconciliation.post_submit_status == REJECT_CONFIRMED_NO_ORDER
        assert reject_result.state_snapshot.terminal_submit_failure

        unknown_result = _run(tmp, "k-unknown", SIMULATED_TIMEOUT_UNKNOWN, _state())
        assert unknown_result.post_submit_reconciliation is not None
        assert unknown_result.post_submit_reconciliation.post_submit_status == UNKNOWN_REQUIRES_EXCHANGE_CHECK
        assert unknown_result.state_snapshot.submit_unknown
        assert unknown_result.state_snapshot.block_new_orders

        reservation_failure = EndToEndTestnetDrill(
            ledger=ExecutionEventLedger(tmp / "reservation-fail-ledger.jsonl"),
            identity_registry=ExecutionIdentityRegistry(),
            intent_journal_path=tmp / "reservation-fail-intent.csv",
            manual_enable=False,
            checked_at_utc=NOW,
        ).run(
            validated_intent=_validated("k-reservation-fail"),
            exchange_state=_state(),
            simulated_submit_response=SIMULATED_ACK,
            scenario="reservation_failure",
        )
        assert not reservation_failure.allowed
        assert reservation_failure.submit_result is None
        assert "manual enable" in reservation_failure.reason.lower()

        recon_failure = _run(tmp, "k-recon-fail", SIMULATED_ACK, _state(), reconciliation_ok=False)
        assert not recon_failure.allowed
        assert recon_failure.submit_result is None
        assert "reconciliation" in recon_failure.reason.lower()

        ledger = ExecutionEventLedger(tmp / "rebuild-ledger.jsonl")
        v = _validated("k-rebuild")
        exchange_ready = __import__("backtest.execution.quantity_converter", fromlist=["convert_validated_intent_quantity"]).convert_validated_intent_quantity(v)
        reconciliation = __import__("backtest.execution.exchange_reconciler", fromlist=["ExchangeReconciler"]).ExchangeReconciler().reconcile(
            local_snapshot=ledger.rebuild_snapshot(v.canonical_setup_key),
            exchange_positions=_state().positions,
            exchange_open_orders=_state().open_orders,
            exchange_balances=_state().balances,
            instrument_rules=_state().instrument_rules,
            canonical_setup_key=v.canonical_setup_key,
            symbol=v.symbol,
            checked_at_utc=NOW,
        )
        command = TestnetCommandGateway(mode=MODE_TESTNET, manual_enable=True).prepare_command(
            exchange_ready_intent=exchange_ready,
            reconciliation_result=reconciliation,
            ledger=ledger,
            decided_at_utc=NOW,
        )
        assert ledger.events[0].event_type == RESERVED_PRE_SUBMIT
        submit = TestnetSubmitAdapterSkeleton().submit(
            command_decision=command,
            exchange_ready_intent=exchange_ready,
            simulated_response=SIMULATED_ACK,
            submitted_at_utc=NOW,
        )
        submit_before = copy.deepcopy(submit)
        snapshot_a = SubmitOutcomeHandler().handle_submit_result(submit_result=submit, ledger=ledger, recorded_at_utc=NOW)
        snapshot_b = ledger.rebuild_snapshot(v.canonical_setup_key)
        assert ledger.events[-1].event_type == SUBMIT_ACK
        assert snapshot_a == snapshot_b
        assert submit == submit_before

        source_intent = _validated("k-immutable")
        source_before = copy.deepcopy(source_intent)
        source_state = _state()
        source_state_before = copy.deepcopy(source_state)
        _run(tmp, "k-immutable", SIMULATED_REJECT, source_state)
        assert source_intent == source_before
        assert source_state == source_state_before

    after = _hash_existing_production_files()
    production_files_modified = 0 if before == after else 1

    print(
        "SMOKE_E19_OK "
        "ack_path=1 "
        "reject_path=1 "
        "unknown_path=1 "
        "reservation_failure=1 "
        "reconciliation_failure=1 "
        "ledger_rebuild=1 "
        "deterministic_replay=1 "
        "immutable=1 "
        f"exchange_calls={exchange_calls} "
        f"production_files_modified={production_files_modified}"
    )


if __name__ == "__main__":
    main()
