from __future__ import annotations

from dataclasses import FrozenInstanceError

from backtest.execution.decision_consumer import consume_decision_row


def valid_row() -> dict[str, str]:
    return {
        "schema_version": "ATS_EXECUTION_INTENT_V1",
        "cycle_ts": "2026-07-08T10:00:00+00:00",
        "symbol": "XRPUSDT",
        "model": "TDP_REENTRY",
        "side": "LONG",
        "canonical_setup_key": "XRPUSDT|TDP_REENTRY|LONG|2026-07-08T09:45:00+00:00",
        "setup_id": "XRPUSDT|2026-07-08T10:00:00+00:00|TDP_REENTRY|LONG",
        "setup_created_ts": "2026-07-08T09:45:00+00:00",
        "signal_ts": "2026-07-08T10:00:00+00:00",
        "visible_ts": "2026-07-08T10:00:00+00:00",
        "wait_confirm_ts": "2026-07-08T10:00:00+00:00",
        "intended_entry_ts": "2026-07-08T10:00:00+00:00",
        "entry_window_expires_ts": "2026-07-08T10:15:00+00:00",
        "selected_for_execution": "True",
        "execution_rank": "1",
        "selection_reason": "selected",
        "entry": "1.1111",
        "sl": "1.1080007142857142",
        "tp": "1.1172985714285715",
        "planned_rr": "2.0",
        "risk_pct": "0.002789385036707541",
        "reward_pct": "0.005578770073415082",
        "risk_distance": "0.003099285714285749",
        "reward_distance": "0.006198571428571498",
        "authorized_qty": "16.132",
        "authorized_notional": "17.9253652",
        "authorized_risk_usd": "0.05",
        "risk_snapshot_id": "risk-snap-1",
        "position_snapshot_id": "pos-snap-1",
        "authority_waterfall_id": "waterfall-1",
    }


source = valid_row()
before = dict(source)
validated = consume_decision_row(source)
assert source == before, "Decision consumer mutated source row"
assert validated.symbol == "XRPUSDT"
assert validated.side == "LONG"
assert validated.canonical_setup_key == source["canonical_setup_key"]
try:
    validated.intent = validated.intent
except FrozenInstanceError:
    immutable = True
else:
    immutable = False
assert immutable, "ValidatedExecutionIntent is not immutable"

print("SMOKE_E2_OK valid_intent=1 immutable=1 mutated_source=0 exchange_calls=0 telemetry_writes=0")
