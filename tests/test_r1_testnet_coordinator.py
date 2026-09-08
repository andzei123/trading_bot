from decimal import Decimal
from copy import copy
from backtest.execution.intent import ExecutionIntent
from backtest.execution.decision_consumer import consume_decision
from backtest.execution.r1_contracts import *
from backtest.execution.r1_permit_authority import _CapabilityHandle
from tests.r1_fixtures import make_product

def intent():
    row={"schema_version":"ATS_EXECUTION_INTENT_V1","cycle_ts":"2026-09-08T18:00:00Z","symbol":"BTCUSDT","model":"TDP_REENTRY","side":"LONG","canonical_setup_key":"k1","setup_id":"s","setup_created_ts":"2026-09-08T17:00:00Z","signal_ts":"2026-09-08T17:01:00Z","visible_ts":"2026-09-08T17:02:00Z","wait_confirm_ts":"2026-09-08T17:03:00Z","intended_entry_ts":"2026-09-08T18:00:00Z","entry_window_expires_ts":"2026-09-08T19:00:00Z","selected_for_execution":"1","execution_rank":"1","selection_reason":"x","entry":"50000","sl":"49000","tp":"52000","planned_rr":"2","risk_pct":"1","reward_pct":"2","risk_distance":"1000","reward_distance":"2000","authorized_qty":"0.01","authorized_notional":"500","authorized_risk_usd":"10","risk_snapshot_id":"r","position_snapshot_id":"p","authority_waterfall_id":"a"}
    return consume_decision(ExecutionIntent.from_row(row))

def test_create_one_call_and_no_second_reservation(tmp_path):
    p,c,r,t,m=make_product(tmp_path/'l.jsonl'); x=p.create(intent()); assert x.classification=='ACK_PENDING_QUERY'; assert len(t.create_calls)==1
    y=p.create(intent()); assert y.classification=='BLOCKED'; assert len(t.create_calls)==1; p.close()
def test_nonzero_is_unknown_and_no_retry(tmp_path):
    p,c,r,t,m=make_product(tmp_path/'l.jsonl'); t.create_response={"retCode":10001,"retMsg":"bad","result":{"orderId":"","orderLinkId":""},"retExtInfo":{},"time":1}; x=p.create(intent()); assert x.classification=='UNKNOWN'; assert len(t.create_calls)==1; p.close()
def test_cancel_closed_payload(tmp_path):
    p,c,r,t,m=make_product(tmp_path/'l.jsonl'); x=p.create(intent()); c._ledger._health='PERSISTENCE_HEALTHY'; c._ledger._r1=tuple(e for e in c._ledger._r1 if e.event_type not in {'CREATE_ACK_PENDING_QUERY'}); c._ledger._union=tuple(c._ledger._r1)
    # Separate clean coordinator needed because ACK correctly blocks mutations; prove cancel payload using fresh clean ledger/known target fixture.
    p.close(); p,c,r,t,m=make_product(tmp_path/'c.jsonl'); r.client='AT1_X'; y=p.cancel(R1CancelTarget('k','BTCUSDT','AT1_X','OID1')); assert y.classification=='ACK_PENDING_QUERY'; assert set(t.cancel_calls[0])=={'category','symbol','orderId','orderLinkId'}; p.close()
def test_cancel_stale_at_exact_five_seconds(tmp_path):
    p,c,r,t,m=make_product(tmp_path/'l.jsonl'); original=r.query
    obs=original(R1QueryRequest('BTCUSDT','AT1_X','OID1')); m.advance(5)
    r.query=lambda req: obs
    y=p.cancel(R1CancelTarget('k','BTCUSDT','AT1_X','OID1')); assert y.classification=='BLOCKED'; assert not t.cancel_calls; p.close()

def test_query_rejected_is_only_reject_confirmed_path(tmp_path):
    p,c,r,t,m=make_product(tmp_path/'l.jsonl'); t.create_response={"retCode":10001,"retMsg":"bad","result":{"orderId":"OID1","orderLinkId":""},"retExtInfo":{},"time":1}; x=p.create(intent()); assert x.classification=='UNKNOWN'
    r.state='Rejected'; r.client=x.client_order_id; obs=p.query(R1QueryRequest('BTCUSDT',x.client_order_id,'OID1')); assert any(e.event_type=='CREATE_REJECT_CONFIRMED' for e in c._ledger.events); assert not c._ledger.projection().block_mutations; p.close()

def test_exact_ten_second_reservation_expiry(tmp_path):
    p,c,r,t,m=make_product(tmp_path/'l.jsonl'); r.advance_on_reconcile=10.0; x=p.create(intent())
    assert x.classification=='BLOCKED'; assert not t.create_calls
    assert any(e.event_type=='CREATE_RESERVED' for e in c._ledger.events)
    assert any(e.event_type=='CREATE_RESERVATION_EXPIRED' for e in c._ledger.events)
    assert not any(e.event_type=='CREATE_DISPATCHING' for e in c._ledger.events)
    p.close()

def test_s1_1_second_gate_expiry_persistence_indeterminate_is_unknown(tmp_path):
    from backtest.execution.execution_event_ledger import PersistenceStateUnknownError
    p,c,r,t,m=make_product(tmp_path/'s1_1_unknown.jsonl'); r.advance_on_reconcile=10.0
    original=c._append
    def fail_expiry(event,*args,**kwargs):
        if event=='CREATE_RESERVATION_EXPIRED':
            c._ledger._health='PERSISTENCE_STATE_UNKNOWN'
            raise PersistenceStateUnknownError('simulated indeterminate expiry append')
        return original(event,*args,**kwargs)
    c._append=fail_expiry
    x=p.create(intent())
    assert x.classification=='PERSISTENCE_UNKNOWN'; assert x.query_required
    assert not t.create_calls
    assert not any(e.event_type=='CREATE_DISPATCHING' for e in c._ledger.events)
    assert c._ledger.persistence_health=='PERSISTENCE_STATE_UNKNOWN'
    p.close()
def test_just_before_ten_seconds_fresh(tmp_path):
    p,c,r,t,m=make_product(tmp_path/'l.jsonl'); r.advance_on_reconcile=9.999999; x=p.create(intent()); assert x.classification=='ACK_PENDING_QUERY'; assert len(t.create_calls)==1; p.close()
def test_cancel_ack_requires_query_confirmation(tmp_path):
    p,c,r,t,m=make_product(tmp_path/'l.jsonl'); r.client='AT1_X'; x=p.cancel(R1CancelTarget('k','BTCUSDT','AT1_X','OID1')); assert x.classification=='ACK_PENDING_QUERY'; assert not any(e.event_type=='CANCEL_CONFIRMED' for e in c._ledger.events); r.state='Cancelled'; p.query(R1QueryRequest('BTCUSDT','AT1_X','OID1')); assert any(e.event_type=='CANCEL_CONFIRMED' for e in c._ledger.events); p.close()

def test_arm_capability_copy_has_zero_authority(tmp_path):
    p,c,r,t,m=make_product(tmp_path/'l.jsonl'); forged=copy(c._arm); assert not c._arm_issuer.validate_arm(forged,c._ctx()); p.close()
def test_reconciliation_capability_one_use(tmp_path):
    p,c,r,t,m=make_product(tmp_path/'l.jsonl'); ctx={'operation_identity':'o','attempt_id':'a','symbol':'BTCUSDT','client_order_id':'c','client_identity_digest':'d','account_identity':'acct','environment':'TESTNET'}; h=c._rec_issuer.issue_for_create(expected_context=ctx,symbol='BTCUSDT',client_order_id='c',client_identity_digest='d'); assert c._rec_issuer.consume_reconciliation(h,operation='CREATE',expected_context=ctx); assert not c._rec_issuer.consume_reconciliation(h,operation='CREATE',expected_context=ctx); p.close()

def test_s1_dispatch_rebuild_uses_post_reload_fresh_rules_and_persisted_fingerprint(tmp_path):
    p,c,r,t,m=make_product(tmp_path/'s1.jsonl')
    calls={'n':0}; original=r.instrument_rules
    def rules(symbol):
        calls['n']+=1
        proof=original(symbol)
        if calls['n']>=2:
            # Change the actual request geometry after durable reservation; persisted fingerprint must veto dispatch.
            r.rules_variant=1
            proof=original(symbol)
        return proof
    r.instrument_rules=rules
    x=p.create(intent())
    assert x.classification=='BLOCKED'
    assert 'fingerprint' in x.reason
    assert len(t.create_calls)==0
    assert any(e.event_type=='CREATE_RESERVED' for e in c._ledger.events)
    assert not any(e.event_type=='CREATE_DISPATCHING' for e in c._ledger.events)
    p.close()

def _trigger_kill(c):
    import json
    from pathlib import Path
    q=Path(c._cfg.kill_artifact_path)
    obj=json.loads(q.read_text(encoding='utf-8')); obj['state']='TRIGGERED'
    q.write_text(json.dumps(obj,sort_keys=True),encoding='utf-8')

def test_s2_create_kill_artifact_changed_after_admission_blocks_before_dispatch(tmp_path):
    p,c,r,t,m=make_product(tmp_path/'s2c.jsonl')
    r.on_reconcile=lambda:_trigger_kill(c)
    x=p.create(intent())
    assert x.classification=='BLOCKED' and 'kill' in x.reason
    assert not t.create_calls
    assert not any(e.event_type=='CREATE_DISPATCHING' for e in c._ledger.events)
    p.close()

def test_s3_cancel_final_common_gate_revalidation_detects_kill_change(tmp_path):
    p,c,r,t,m=make_product(tmp_path/'s3.jsonl')
    r.client='AT1_X'; r.on_query=lambda:_trigger_kill(c)
    x=p.cancel(R1CancelTarget('k','BTCUSDT','AT1_X','OID1'))
    assert x.classification=='BLOCKED' and 'kill' in x.reason
    assert not t.cancel_calls
    assert not any(e.event_type=='CANCEL_DISPATCHING' for e in c._ledger.events)
    p.close()

def test_s4_reconciliation_issuer_owns_read_provenance(tmp_path):
    p,c,r,t,m=make_product(tmp_path/'s4.jsonl')
    assert c._rec_issuer._read is r
    assert not hasattr(c._rec_issuer,'issue')
    ctx={'operation_identity':'o','attempt_id':'a','symbol':'BTCUSDT','client_order_id':'c','client_identity_digest':'d','account_identity':'acct','environment':'TESTNET'}
    h=c._rec_issuer.issue_for_create(expected_context=ctx,symbol='BTCUSDT',client_order_id='c',client_identity_digest='d')
    assert c._rec_issuer.consume_reconciliation(h,operation='CREATE',expected_context=ctx)
    p.close()
