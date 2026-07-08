from __future__ import annotations

import csv
import tempfile
from pathlib import Path

from backtest.execution.decision_consumer import consume_decision_row
from backtest.execution.intent_journal import IntentJournal, load_intent_journal
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

with tempfile.TemporaryDirectory() as tmpdir:
    journal_path = Path(tmpdir) / "intent_journal.csv"
    journal = IntentJournal(journal_path)
    record = journal.append(
        validated,
        executor_accepted_ts="2026-07-08T10:00:01+00:00",
    )

    assert source == before, "Intent journal mutated source row"
    assert record.canonical_setup_key == validated.canonical_setup_key
    assert record.authorized_risk_usd == validated.authorized_risk_usd

    with journal_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    loaded = load_intent_journal(journal_path)
    assert len(rows) == 1, "Intent journal did not append exactly one record"
    assert len(loaded) == 1, "Intent journal replay load did not return one record"
    assert rows[0]["canonical_setup_key"] == source["canonical_setup_key"]
    assert rows[0]["symbol"] == "XRPUSDT"
    assert rows[0]["side"] == "LONG"
    assert rows[0]["model"] == "TDP_REENTRY"
    assert rows[0]["entry"] == source["entry"]
    assert rows[0]["stop"] == source["sl"]
    assert rows[0]["target"] == source["tp"]
    assert rows[0]["authorized_risk_usd"] == source["authorized_risk_usd"]

print("SMOKE_E3_OK journal_records=1 append_only=1 mutated_source=0 exchange_calls=0 telemetry_writes=0")
