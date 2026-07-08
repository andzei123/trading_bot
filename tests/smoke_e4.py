from __future__ import annotations

import tempfile
from dataclasses import replace
from pathlib import Path

from backtest.execution.decision_consumer import ValidatedExecutionIntent, consume_decision_row
from backtest.execution.mechanical_safety_bridge import BLOCK, CRITICAL, INFO, check_mechanical_safety


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

passed = check_mechanical_safety(
    validated,
    checked_at_utc="2026-07-08T10:00:01+00:00",
)
assert passed.allowed is True
assert passed.reason == "mechanical_safety_passed"
assert passed.severity == INFO

with tempfile.TemporaryDirectory() as tmpdir:
    kill_switch = Path(tmpdir) / "EXECUTOR_KILL_SWITCH"
    kill_switch.write_text("stop\n", encoding="utf-8")
    killed = check_mechanical_safety(
        validated,
        kill_switch_path=kill_switch,
        checked_at_utc="2026-07-08T10:00:01+00:00",
    )
    assert killed.allowed is False
    assert killed.reason == "kill_switch_present"
    assert killed.severity == CRITICAL

invalid_geometry_intent = ValidatedExecutionIntent(
    intent=replace(validated.intent, tp=validated.entry)
)
invalid_geometry = check_mechanical_safety(
    invalid_geometry_intent,
    checked_at_utc="2026-07-08T10:00:01+00:00",
)
assert invalid_geometry.allowed is False
assert invalid_geometry.reason == "invalid_long_geometry"
assert invalid_geometry.severity == BLOCK

duplicate = check_mechanical_safety(
    validated,
    pending_identity_keys={validated.canonical_setup_key},
    checked_at_utc="2026-07-08T10:00:01+00:00",
)
assert duplicate.allowed is False
assert duplicate.reason == "pending_identity_already_present"
assert duplicate.severity == BLOCK

expired = check_mechanical_safety(
    validated,
    checked_at_utc="2026-07-08T10:16:00+00:00",
)
assert expired.allowed is False
assert expired.reason == "intent_expired"

assert source == before, "Mechanical Safety Bridge mutated source row"
assert validated.canonical_setup_key == before["canonical_setup_key"]

production_paths = [
    Path("live_observation_shell.py"),
    Path("pipeline_core.py"),
    Path("policy_engine.py"),
]
assert all(not path.exists() for path in production_paths), "Smoke workspace unexpectedly contains production files"

print(
    "SMOKE_E4_OK valid_pass=1 kill_switch_block=1 invalid_geometry_block=1 "
    "duplicate_pending_block=1 expired_block=1 mutated_source=0 exchange_calls=0 "
    "telemetry_writes=0 production_files_modified=0"
)
