import json,pytest
from backtest.execution.execution_event_ledger import *
from backtest.execution.execution_event_ledger import _parse_union_records,_r2_canonical_digest

EVENTS=['R2_LOCAL_CLASSIFIED','R2_DISCOVERY_STARTED','R2_ACTIVATION_CONSUMPTION_STARTED','R2_DISCOVERY_COMPLETED','R2_RECONCILED_DISARMED','R2_RECONCILIATION_UNKNOWN','R2_STARTUP_ELIGIBILITY_REVOKED','R2_OPERATOR_ARMED']
def surface(sid): return dict(surface_id=sid,http_method='GET',endpoint_path='/x',canonical_parameters_digest='a'*64,coverage_start_utc=None,coverage_end_utc=None,first_request_offset_ms=0,last_response_offset_ms=1,request_count=1,page_count=1,record_count=0,ordered_page_manifest_digest='b'*64,normalized_content_digest='c'*64,cursor_termination='EMPTY_CURSOR',schema_contract_id='V1',complete=True)
def proof(epoch='e'):
    ids=['SERVER_TIME_S0','API_KEY_INFO','ORDER_HISTORY:0','EXECUTION_HISTORY:0','SERVER_TIME_S1','ORDER_HISTORY_OVERLAP','EXECUTION_HISTORY_OVERLAP','REALTIME_OPEN_ORDERS','POSITIONS']
    return dict(manifest_schema='ATS_R2_DISCOVERY_PROOF_MANIFEST_V1',startup_epoch_id=epoch,server_time_start_utc='2026-09-09T09:00:00Z',server_time_end_utc='2026-09-09T09:01:00Z',epoch_monotonic_duration_ms=100,current_barrier_duration_ms=50,account_proof=dict(origin='https://api-testnet.bybit.eu',account_uid=107087555,parent_uid=0,credential_fingerprint='c',account_mode='1',read_only=False,permissions_digest='d',request_digest='e',normalized_response_digest='f',observed_after_s0=True,observed_before_s1=True),registry_proof=dict(identity_version='AT1',activation_utc='2026-09-09T08:00:00.000000Z',entry_digest='1'*64,registry_root_digest='2'*64,chief_authority_reference='CHIEF'),retention_proof=dict(documented_horizon_seconds=86400,safety_margin_seconds=300,maximum_age_seconds=86100,age_at_s1_microseconds=1,oldest_request_send_offset_ms=1,oldest_response_receive_offset_ms=2,oldest_window_first=True,continuous_local_authority=False,retention_sufficient=True),surface_proofs=[{**surface(i), **({'coverage_start_utc':'2026-09-09T08:59:00.000000Z','coverage_end_utc':'2026-09-09T09:01:00Z'} if i in {'ORDER_HISTORY_OVERLAP','EXECUTION_HISTORY_OVERLAP'} else {}), 'first_request_offset_ms':j*10, 'last_response_offset_ms':j*10+1} for j,i in enumerate(ids)],rules_proofs=[])
def disposition(pd,proof_seq=1,result='RECONCILED_CLEAN'): return dict(manifest_schema='ATS_R2_RECONCILIATION_DISPOSITION_V1',proof_event_sequence=proof_seq,proof_manifest_digest=pd,local_projection_digest='3'*64,operation_dispositions=[],identity_class_counts={},order_state_counts={},execution_count=0,nonzero_position_count=0,open_order_count=0,unknown_count=0,blocking_reason_codes=[],result=result)
def base(seq=1,event='R2_LOCAL_CLASSIFIED',epoch='e'):
    d={k:None for k in R2_V1_FIELDS}; d.update(schema_version=R2_V1_SCHEMA_VERSION,sequence=seq,startup_epoch_id=epoch,event_type=event,recorded_at_utc='2026-09-09T09:00:00Z',origin='https://api-testnet.bybit.eu',account_uid=107087555,parent_uid=0,credential_fingerprint='c',category='linear',settle_coin='USDT',configuration_digest='f'*64,block_mutations=True,reason_code='X'); return d
def consumption(seq=1,pre_digest=None,epoch='e'):
    d=base(seq,'R2_ACTIVATION_CONSUMPTION_STARTED',epoch); d.update(reason_code='ACTIVATION_CONSUMED_BEFORE_DISCOVERY',process_session_identity_digest='1'*64,local_pre_tip_sequence=seq-1,local_pre_tip_digest=pre_digest or R2_EMPTY_LEDGER_SENTINEL_DIGEST,activation_entry_digest='3'*64,registry_root_digest='4'*64,chief_activation_authority_reference='CHIEF',r3_execution_authority_reference='R3'); return d
def discovery(seq,epoch='e'): d=base(seq,'R2_DISCOVERY_COMPLETED',epoch); m=proof(epoch); d.update(proof_manifest=m,proof_manifest_digest=_r2_canonical_digest(m)); return d
def reconciled(seq,proof_seq,epoch='e'): d=base(seq,'R2_RECONCILED_DISARMED',epoch); m=proof(epoch); pd=_r2_canonical_digest(m); dm=disposition(pd,proof_seq); d.update(reason_code='RECONCILED_CLEAN',block_mutations=True,proof_manifest=m,proof_manifest_digest=pd,disposition_manifest=dm,disposition_manifest_digest=_r2_canonical_digest(dm)); return d
def parse(*ds): return _parse_union_records((''.join(json.dumps(d,sort_keys=True)+'\n' for d in ds)).encode())

def test_01_every_recognized_event_has_same_31_key_family():
    for e in EVENTS: assert len(base(event=e))==31 and frozenset(base(event=e))==R2_V1_FIELDS
def test_02_missing_each_new_key_fails():
    for k in R2_CONSUMPTION_FIELDS:
        d=base(); d.pop(k)
        with pytest.raises(ExecutionEventLedgerError): parse(d)
def test_03_extra_approved_s0_or_arbitrary_key_fails():
    for k in ('approved_origin','S0','arbitrary'):
        d=base(); d[k]='x'
        with pytest.raises(ExecutionEventLedgerError): parse(d)
def test_04_duplicate_json_key_fails():
    d=base(); raw=json.dumps(d)[:-1]+',"reason_code":"Y"}\n'
    with pytest.raises(ExecutionEventLedgerError): _parse_union_records(raw.encode())
def test_05_nonconsumption_events_keep_seven_null_under_event_rules():
    # LOCAL_CLASSIFIED is the only pre-consumption R2 event admitted by Rev2.1.
    parse(base(event='R2_LOCAL_CLASSIFIED'))
    c=consumption(1)
    for e in ['R2_DISCOVERY_STARTED','R2_RECONCILIATION_UNKNOWN','R2_STARTUP_ELIGIBILITY_REVOKED','R2_OPERATOR_ARMED']:
        d=base(2,event=e); parse(c,d)
    d=discovery(2); parse(c,d)
    parse(c,d,reconciled(3,2))
def test_06_each_new_field_nonnull_on_nonconsumption_fails():
    for k in R2_CONSUMPTION_FIELDS:
        d=base(); d[k]=1 if k=='local_pre_tip_sequence' else '1'*64
        with pytest.raises(ExecutionEventLedgerError): parse(d)
def test_07_consumption_all_seven_valid_passes(): parse(consumption())
def test_08_each_consumption_field_null_empty_wrong_type_or_malformed_fails():
    for k in R2_CONSUMPTION_FIELDS:
        bad=(None,'',123,' bad ') if k in {'chief_activation_authority_reference','r3_execution_authority_reference'} else (None,'',123,'xyz')
        for v in bad:
            d=consumption(); d[k]=v
            with pytest.raises(ExecutionEventLedgerError): parse(d)
def test_09_consumption_manifests_nonnull_fail():
    for k in ('proof_manifest','disposition_manifest'):
        d=consumption(); d[k]={}; d[k+'_digest']='0'*64
        with pytest.raises(ExecutionEventLedgerError): parse(d)
def test_10_consumption_block_false_or_reason_wrong_fails():
    for k,v in [('block_mutations',False),('reason_code','WRONG')]:
        d=consumption(); d[k]=v
        with pytest.raises(ExecutionEventLedgerError): parse(d)
def test_11_context_mismatch_fails():
    for k,v in [('origin','https://api-testnet.bybit.com'),('account_uid',1),('parent_uid',2),('category','inverse'),('settle_coin','BTC')]:
        d=consumption(); d[k]=v
        with pytest.raises(ExecutionEventLedgerError): parse(d)
def test_12_pretip_sequence_must_equal_sequence_minus_one():
    d=consumption(); d['local_pre_tip_sequence']=1
    with pytest.raises(ExecutionEventLedgerError): parse(d)
def test_13_pretip_digest_and_empty_sentinel_mismatch_fail():
    d=consumption(); d['local_pre_tip_digest']='0'*64
    with pytest.raises(ExecutionEventLedgerError): parse(d)
def test_14_duplicate_consumption_same_epoch_fails():
    first=consumption(); raw=json.dumps(first,sort_keys=True)+'\n'; dig=__import__('hashlib').sha256(raw.encode()).hexdigest(); second=consumption(2,dig)
    with pytest.raises(ExecutionEventLedgerError): parse(first,second)
def test_15_consumption_after_discovery_or_success_or_arm_fails():
    for first in (discovery(1),base(1,'R2_OPERATOR_ARMED')):
        raw=json.dumps(first,sort_keys=True)+'\n'; dig=__import__('hashlib').sha256(raw.encode()).hexdigest(); c=consumption(2,dig)
        with pytest.raises(ExecutionEventLedgerError): parse(first,c)
def test_16_consumption_then_discovery_then_reconciled_exact_union_passes():
    c=consumption(1); c_raw=json.dumps(c,sort_keys=True)+'\n'; d=discovery(2); parse(c,d,reconciled(3,2))
def test_17_old_24_field_draft_never_default_expands():
    d=base(); [d.pop(k) for k in R2_CONSUMPTION_FIELDS]
    with pytest.raises(ExecutionEventLedgerError): parse(d)
def test_18_mixed_v0_r1_r2_global_sequence_preserves_projections():
    from tests.test_r1_ledger_schema import v0,v1
    u,l,r,r2=parse(v0(1),v1(2),base(3)); assert [x.sequence for x in u]==[1,2,3] and len(l)==len(r)==len(r2)==1
