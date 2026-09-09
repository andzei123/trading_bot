import json
from datetime import datetime,timezone
from pathlib import Path
import pytest
from backtest.execution.execution_event_ledger import R1AuthorityLedger
from backtest.execution.r2_identity_registry import *
from backtest.execution.r2_startup_authority import R2StartupEligibilityIssuer
from backtest.execution.r2_startup_reconciliation import *
from backtest.execution.r2_startup_reconciliation import _digest
from backtest.execution.r2_startup_reconciliation import _history_windows,_paged,_validate_account,_remote_execution_lifecycle_truth,_dedupe_executions,_reduce_order_snapshots,_operation_dispositions,R2_HISTORY_WINDOW_MS

class Read:
    def __init__(self): self.calls=[]; self.t=1757408400 # 2025-ish irrelevant relative? overridden fixture activation near server
    def _x(self,n,p=None): self.calls.append((n,p)); return {'retCode':0,'result':{'list':[]}}
    def server_time(self): self.calls.append(('server_time',None)); return {'retCode':0,'result':{'timeSecond':'1788944400'}} # 2026-09-09 09:00 UTC approx
    def api_key_info(self,p=None): self.calls.append(('api_key_info',p)); return {'retCode':0,'result':{'userID':'107087555','parentUid':0,'readOnly':0,'unifiedMarginStatus':1,'permissions':{'ContractTrade':['Order','Position']}}}
    def order_history(self,p): return self._x('order_history',p)
    def execution_history(self,p): return self._x('execution_history',p)
    def query(self,p): return self._x('query',p)
    def positions(self,p): return self._x('positions',p)
    def instrument_rules(self,p): return self._x('instrument_rules',p)

def authority():
    d=dict(registry_schema=REGISTRY_SCHEMA,identity_version='AT1',activation_utc='2026-09-09T08:00:00.000000Z',approved_origin='https://api-testnet.bybit.eu',approved_environment='TESTNET',approved_category='linear',approved_settle_coin='USDT',approved_account_uid=107087555,approved_parent_uid=0,predecessor_entry_digest='0'*64,chief_authority_reference='CHIEF_FIXTURE')
    return fixture_admitted_genesis_authority(d)

def test_consumption_is_durable_before_first_network_and_first_call_is_s0(tmp_path):
    l=R1AuthorityLedger(tmp_path/'l.jsonl'); rd=Read(); rr=R2StartupReconciler(ledger=l,read_authority=rd,eligibility_issuer=R2StartupEligibilityIssuer(),admitted_authority=authority(),configuration_digest='f'*64,credential_fingerprint='c'*64,r3_execution_authority_reference='R3_FIXTURE',utc_now=lambda:datetime(2026,9,9,9,0,tzinfo=timezone.utc))
    result=rr.reconcile(); assert rd.calls[0][0]=='server_time'; assert l.r2_events[0].event_type=='R2_LOCAL_CLASSIFIED'; assert result.classification=='RECONCILED_CLEAN'
    assert [x.event_type for x in l.r2_events][:3]==['R2_LOCAL_CLASSIFIED','R2_ACTIVATION_CONSUMPTION_STARTED','R2_DISCOVERY_STARTED']; l.close()

def test_consumption_append_failure_means_zero_network(tmp_path):
    class BadLedger(R1AuthorityLedger):
        def append_r2(self,**kw): raise RuntimeError('append fail')
    l=BadLedger(tmp_path/'l.jsonl'); rd=Read(); rr=R2StartupReconciler(ledger=l,read_authority=rd,eligibility_issuer=R2StartupEligibilityIssuer(),admitted_authority=authority(),configuration_digest='f'*64,credential_fingerprint='c'*64,r3_execution_authority_reference='R3_FIXTURE',utc_now=lambda:datetime(2026,9,9,9,0,tzinfo=timezone.utc))
    with pytest.raises(RuntimeError): rr.reconcile()
    assert rd.calls==[]; l.close()

def test_oldest_order_history_is_before_other_discovery_surfaces(tmp_path):
    l=R1AuthorityLedger(tmp_path/'l.jsonl'); rd=Read(); R2StartupReconciler(ledger=l,read_authority=rd,eligibility_issuer=R2StartupEligibilityIssuer(),admitted_authority=authority(),configuration_digest='f'*64,credential_fingerprint='c'*64,r3_execution_authority_reference='R3_FIXTURE',utc_now=lambda:datetime(2026,9,9,9,0,tzinfo=timezone.utc)).reconcile()
    names=[x[0] for x in rd.calls]; assert names.index('order_history')<names.index('execution_history'); assert [n for n,_ in rd.calls].count('server_time')==2; assert names.index('query')<names.index('positions'); l.close()

class ScriptedRead(Read):
    def __init__(self):
        super().__init__(); self.server_seconds=['1788944400','1788944400']; self.pages={}; self.account_override={}
    def server_time(self):
        self.calls.append(('server_time',None)); v=self.server_seconds[min(sum(1 for n,_ in self.calls if n=='server_time')-1,len(self.server_seconds)-1)]; return {'retCode':0,'result':{'timeSecond':v}}
    def api_key_info(self,p=None):
        raw=super().api_key_info(p); raw['result'].update(self.account_override); return raw
    def _page(self,name,p):
        self.calls.append((name,dict(p))); key=(name,p.get('startTime'),p.get('endTime'),p.get('cursor','')); return self.pages.get(key,{'retCode':0,'result':{'list':[],'nextPageCursor':''}})
    def order_history(self,p): return self._page('order_history',p)
    def execution_history(self,p): return self._page('execution_history',p)
    def query(self,p): return self._page('query',p)
    def positions(self,p): return self._page('positions',p)

def _rr(tmp_path,rd,auth=None,mono=None,utc=None):
    return R2StartupReconciler(ledger=R1AuthorityLedger(tmp_path/'x.jsonl'),read_authority=rd,eligibility_issuer=R2StartupEligibilityIssuer(),admitted_authority=auth or authority(),configuration_digest='f'*64,credential_fingerprint='c'*64,r3_execution_authority_reference='R3_FIXTURE',monotonic=mono or (lambda:100.0),utc_now=utc or (lambda:datetime(2026,9,9,9,0,tzinfo=timezone.utc)))

def test_history_window_helper_exact_7d_and_plus_1ms():
    assert _history_windows(0,R2_HISTORY_WINDOW_MS)==[(0,R2_HISTORY_WINDOW_MS)]
    assert _history_windows(0,R2_HISTORY_WINDOW_MS+1)==[(0,R2_HISTORY_WINDOW_MS),(R2_HISTORY_WINDOW_MS,R2_HISTORY_WINDOW_MS+1)]

def test_pagination_50_51_boundary_and_cursor_verbatim():
    rows=[_s5_order(order_id=str(i),status='Cancelled',qty='1',cum='0') for i in range(50)]; calls=[]
    def fn(p):
        calls.append(dict(p)); return {'retCode':0,'result':{'list':rows if 'cursor' not in p else [_s5_order(order_id='50',status='Cancelled',qty='1',cum='0')],'nextPageCursor':'NEXT' if 'cursor' not in p else ''}}
    out,proof=_paged(fn,{'category':'linear','limit':50},50,'ORDER_HISTORY:0','/v5/order/history',0)
    assert len(out)==51 and proof['page_count']==2 and calls[1]['cursor']=='NEXT'

def test_missing_cursor_on_full_page_is_ambiguous():
    def fn(p): return {'retCode':0,'result':{'list':[{'orderId':str(i)} for i in range(50)],'nextPageCursor':''}}
    with pytest.raises(ValueError,match='PAGINATION_AMBIGUOUS'): _paged(fn,{'limit':50},50,'ORDER_HISTORY:0','/v5/order/history',0)

def test_repeated_cursor_is_ambiguous():
    def fn(p): return {'retCode':0,'result':{'list':[{'orderId':str(len(p))}],'nextPageCursor':'X'}}
    with pytest.raises(ValueError,match='PAGINATION_AMBIGUOUS'): _paged(fn,{'limit':50},50,'ORDER_HISTORY:0','/v5/order/history',0)

def test_retention_equality_23h55m_is_blocked(tmp_path):
    entry=dict(registry_schema=REGISTRY_SCHEMA,identity_version='AT1',activation_utc='2026-09-08T09:05:00.000000Z',approved_origin='https://api-testnet.bybit.eu',approved_environment='TESTNET',approved_category='linear',approved_settle_coin='USDT',approved_account_uid=107087555,approved_parent_uid=0,predecessor_entry_digest='0'*64,chief_authority_reference='CHIEF_FIXTURE')
    rd=ScriptedRead(); result=_rr(tmp_path,rd,fixture_admitted_genesis_authority(entry)).reconcile(); assert result.classification=='RECONCILED_BLOCKED_RETENTION'

def test_retention_one_microsecond_inside_boundary_may_pass(tmp_path):
    entry=dict(registry_schema=REGISTRY_SCHEMA,identity_version='AT1',activation_utc='2026-09-08T09:05:00.000001Z',approved_origin='https://api-testnet.bybit.eu',approved_environment='TESTNET',approved_category='linear',approved_settle_coin='USDT',approved_account_uid=107087555,approved_parent_uid=0,predecessor_entry_digest='0'*64,chief_authority_reference='CHIEF_FIXTURE')
    rd=ScriptedRead(); result=_rr(tmp_path,rd,fixture_admitted_genesis_authority(entry)).reconcile(); assert result.classification=='RECONCILED_CLEAN'

def test_account_parent_mismatch_fails_closed(tmp_path):
    rd=ScriptedRead(); rd.account_override={'parentUid':99}; assert _rr(tmp_path,rd).reconcile().classification=='RECONCILED_BLOCKED_ACCOUNT_MISMATCH'

def test_server_skew_over_5_seconds_fails_closed(tmp_path):
    rd=ScriptedRead(); assert _rr(tmp_path,rd,utc=lambda:datetime(2026,9,9,9,0,6,tzinfo=timezone.utc)).reconcile().classification=='RECONCILIATION_UNKNOWN_FRESHNESS'

def test_non_ats_open_order_blocks_exposure(tmp_path):
    rd=ScriptedRead(); rd.pages[('query',None,None,'')]={'retCode':0,'result':{'list':[{'orderId':'O','orderLinkId':'manual','symbol':'ETHUSDT','side':'Buy','orderStatus':'New','qty':'1','cumExecQty':'0','createdTime':'1788940000000','updatedTime':'1788940000000'}],'nextPageCursor':''}}
    assert _rr(tmp_path,rd).reconcile().classification=='RECONCILED_BLOCKED_EXPOSURE'

def test_probable_at1_remote_orphan_blocks(tmp_path):
    rd=ScriptedRead(); rd.pages[('query',None,None,'')]={'retCode':0,'result':{'list':[{'orderId':'O','orderLinkId':'AT1_'+'a'*32,'symbol':'ETHUSDT','side':'Buy','orderStatus':'Cancelled','qty':'1','cumExecQty':'0','createdTime':'1788940000000','updatedTime':'1788940000000'}],'nextPageCursor':''}}
    assert _rr(tmp_path,rd).reconcile().classification=='RECONCILED_BLOCKED_REMOTE_ORPHAN'

def test_proof_manifest_surface_order_rejects_reordering():
    from tests.test_r2_ledger_schema import proof
    from backtest.execution.execution_event_ledger import _validate_r2_proof_manifest,ExecutionEventLedgerError
    p=proof(); p['surface_proofs'][0],p['surface_proofs'][1]=p['surface_proofs'][1],p['surface_proofs'][0]
    with pytest.raises(ExecutionEventLedgerError): _validate_r2_proof_manifest(p)

def _write_jsonl(path,*rows): path.write_text(''.join(json.dumps(r,sort_keys=True)+'\n' for r in rows),encoding='utf-8')

def test_local_matrix_missing_empty_temp_and_activation_unknown(tmp_path):
    p=tmp_path/'m.jsonl'; now=datetime(2026,9,9,9,0,tzinfo=timezone.utc)
    assert classify_local_snapshot(p,now_utc=now)=='MISSING'
    p.write_bytes(b''); assert classify_local_snapshot(p,now_utc=now)=='EMPTY'
    p.unlink(); Path(str(p)+'.tmp').write_text('x'); assert classify_local_snapshot(p,now_utc=now)=='MISSING_WITH_TEMP'
    Path(str(p)+'.tmp').unlink(); p.write_bytes(b''); assert classify_local_snapshot(p,now_utc=now,activation_authority_admitted=False)=='EMPTY'

def test_local_matrix_malformed_unsupported_duplicate_and_sequence_invalid(tmp_path):
    from tests.test_r1_ledger_schema import v0
    now=datetime(2026,9,9,9,0,tzinfo=timezone.utc); p=tmp_path/'m.jsonl'
    p.write_bytes(b'{bad\n'); assert classify_local_snapshot(p,now_utc=now)=='MALFORMED'
    d=v0(); d['extra']=1; _write_jsonl(p,d); assert classify_local_snapshot(p,now_utc=now)=='UNSUPPORTED_SCHEMA'
    a=v0(1); b=v0(1); _write_jsonl(p,a,b); assert classify_local_snapshot(p,now_utc=now)=='DUPLICATE_SEQUENCE'
    a=v0(1); b=v0(3); _write_jsonl(p,a,b); assert classify_local_snapshot(p,now_utc=now)=='SEQUENCE_INVALID'

def test_local_matrix_r1_reserved_dispatch_unknown_and_terminal_classes(tmp_path):
    from tests.test_r1_ledger_schema import v1
    now=datetime(2026,9,9,9,0,tzinfo=timezone.utc); p=tmp_path/'m.jsonl'
    def one(event,classification='X',recorded='2026-09-09T08:59:55Z'):
        d=v1(1); d.update(event_type=event,classification=classification,recorded_at_utc=recorded,operation_identity='op',operation='CREATE'); _write_jsonl(p,d); return classify_local_snapshot(p,now_utc=now)
    assert one('CREATE_RESERVED')=='R1_RESERVED_UNDISPATCHED'
    assert one('CREATE_RESERVED',recorded='2026-09-09T08:59:49Z')=='R1_RESERVED_EXPIRED'
    assert one('CREATE_DISPATCHING')=='R1_CREATE_DISPATCH_UNKNOWN'
    assert one('CREATE_UNKNOWN')=='R1_CREATE_UNKNOWN'
    assert one('CREATE_REJECT_CONFIRMED')=='R1_REJECTED'
    assert one('IDENTITY_CONFLICT')=='R1_IDENTITY_CONFLICT'
    assert one('CREATE_ACK_PENDING_QUERY')=='R1_ACK_OPEN'
    assert one('CREATE_ACK_PENDING_QUERY','PARTIALLY_FILLED')=='R1_ACK_PARTIAL'
    assert one('CREATE_ACK_PENDING_QUERY','FILLED')=='R1_ACK_FILLED'

def test_local_matrix_legacy_only_and_identity_epoch_unknown(tmp_path):
    from tests.test_r1_ledger_schema import v0
    now=datetime(2026,9,9,9,0,tzinfo=timezone.utc); p=tmp_path/'m.jsonl'; _write_jsonl(p,v0(1))
    assert classify_local_snapshot(p,now_utc=now)=='LEGACY_ONLY'
    assert classify_local_snapshot(p,now_utc=now,activation_authority_admitted=False)=='IDENTITY_EPOCH_UNKNOWN'


def test_s4_exact_success_trace_and_overlap_uses_s1(tmp_path):
    rd=ScriptedRead(); rd.server_seconds=['1788944400','1788944401']
    result=_rr(tmp_path,rd,utc=lambda:datetime(2026,9,9,9,0,tzinfo=timezone.utc)).reconcile(); assert result.classification=='RECONCILED_CLEAN'
    names=[n for n,_ in rd.calls]
    sidx=[i for i,n in enumerate(names) if n=='server_time']; assert len(sidx)==2
    # all historical reads precede S1; exact overlap follows S1; realtime and positions are last barrier surfaces.
    s1i=sidx[1]
    after=rd.calls[s1i+1:]
    assert [n for n,_ in after[:4]]==['order_history','execution_history','query','positions']
    c=1788944400*1000-60000; s1=1788944401*1000
    assert after[0][1]['startTime']==c and after[0][1]['endTime']==s1
    assert after[1][1]['startTime']==c and after[1][1]['endTime']==s1

def test_s4_s1_failure_makes_zero_overlap_realtime_positions_after_failure(tmp_path):
    class FailS1(ScriptedRead):
        def server_time(self):
            count=sum(1 for n,_ in self.calls if n=='server_time'); self.calls.append(('server_time',None))
            if count: return {'retCode':1,'result':{}}
            return {'retCode':0,'result':{'timeSecond':'1788944400'}}
    rd=FailS1(); result=_rr(tmp_path,rd).reconcile(); assert result.classification=='RECONCILIATION_UNKNOWN_ENDPOINT'
    second=[i for i,(n,_) in enumerate(rd.calls) if n=='server_time'][1]
    assert rd.calls[second+1:]==[]

def test_s3_exact_permission_contract_and_digest():
    valid={'retCode':0,'result':{'userID':'107087555','parentUid':0,'unifiedMarginStatus':1,'readOnly':0,'permissions':{'ContractTrade':['Position','Order'],'Wallet':[]}}}
    d1=_validate_account(valid)
    valid2=json.loads(json.dumps(valid)); valid2['result']['permissions']['ContractTrade']=['Order','Position']
    assert _validate_account(valid2)==d1
    valid3=json.loads(json.dumps(valid2)); valid3['result']['permissions']['Exchange']=[]
    assert _validate_account(valid3)!=d1
    bad=[]
    for mut in (
        lambda r:r['result'].pop('permissions'), lambda r:r['result'].__setitem__('permissions',None),
        lambda r:r['result'].__setitem__('permissions',[]), lambda r:r['result']['permissions'].pop('ContractTrade'),
        lambda r:r['result']['permissions'].__setitem__('ContractTrade',None), lambda r:r['result']['permissions'].__setitem__('ContractTrade',['Order']),
        lambda r:r['result']['permissions'].__setitem__('ContractTrade',['Order','position']), lambda r:r['result']['permissions'].__setitem__('ContractTrade',['Order','Order']),
        lambda r:r['result']['permissions'].__setitem__('ContractTrade',['Order','Position','X']), lambda r:r['result']['permissions'].__setitem__('Wallet',['Read']),
        lambda r:r['result'].__setitem__('readOnly',False), lambda r:r['result'].__setitem__('readOnly','0'),
        lambda r:r['result'].__setitem__('readOnly',None), lambda r:r['result'].pop('readOnly'), lambda r:r['result'].__setitem__('readOnly',1),
    ):
        r=json.loads(json.dumps(valid2)); mut(r); bad.append(r)
    for r in bad:
        with pytest.raises(R2AccountMismatch) as exc: _validate_account(r)
        assert exc.value.reason_code=='ACCOUNT_PERMISSION_AUTHORITY_INVALID'

def test_s3_missing_or_wrong_account_mode_fails():
    raw={'retCode':0,'result':{'userID':'107087555','parentUid':0,'readOnly':0,'permissions':{'ContractTrade':['Order','Position']}}}
    with pytest.raises(R2AccountMismatch): _validate_account(raw)
    raw['result']['unifiedMarginStatus']='1'
    with pytest.raises(R2AccountMismatch): _validate_account(raw)
    raw['result']['unifiedMarginStatus']=2
    with pytest.raises(R2AccountMismatch): _validate_account(raw)


def _r1row(seq,event,*,attempt='a',op='CREATE',client='AT1_test',cid='d'*64,exchange=''):
    from tests.test_r1_ledger_schema import v1
    d=v1(seq); d.update(event_type=event,attempt_id=attempt,operation=op,client_order_id=client,client_identity_digest=cid,exchange_order_id=exchange,operation_identity=attempt,canonical_setup_key='k'); return d

def test_s1_real_reconcile_path_durably_classifies_r1_dispatch_unknown_before_network(tmp_path):
    p=tmp_path/'x.jsonl'; _write_jsonl(p,_r1row(1,'CREATE_DISPATCHING'))
    l=R1AuthorityLedger(p); rd=ScriptedRead(); rr=R2StartupReconciler(ledger=l,read_authority=rd,eligibility_issuer=R2StartupEligibilityIssuer(),admitted_authority=authority(),configuration_digest='f'*64,credential_fingerprint='c'*64,r3_execution_authority_reference='R3_FIXTURE',utc_now=lambda:datetime(2026,9,9,9,0,tzinfo=timezone.utc))
    rr.reconcile(); assert l.r2_events[0].event_type=='R2_LOCAL_CLASSIFIED'; assert l.r2_events[0].local_classification=='R1_CREATE_DISPATCH_UNKNOWN'; assert rd.calls[0][0]=='server_time'

def test_s2_create_and_cancel_unresolved_each_get_exact_one_disposition():
    events=[]
    for d in (_r1row(1,'CREATE_DISPATCHING',attempt='a',op='CREATE',client='A',cid='a'*64),_r1row(2,'CREATE_UNKNOWN',attempt='a',op='CREATE',client='A',cid='a'*64),_r1row(3,'CANCEL_DISPATCHING',attempt='b',op='CANCEL',client='B',cid='b'*64,exchange='E2'),_r1row(4,'CANCEL_UNKNOWN',attempt='b',op='CANCEL',client='B',cid='b'*64,exchange='E2')):
        from backtest.execution.execution_event_ledger import R1AuthorityEvent
        events.append(R1AuthorityEvent(**d))
    from backtest.execution.r2_startup_reconciliation import _operation_dispositions
    ds,problem=_operation_dispositions(events,[{'orderId':'E2','orderLinkId':'B','symbol':'ETHUSDT','orderStatus':'Cancelled'}],[],[])
    assert problem is None and len(ds)==2 and [x['local_dispatch_sequence'] for x in ds]==[1,3]
    assert {x['operation_kind'] for x in ds}=={'CREATE','CANCEL'}


class S5Read(ScriptedRead):
    def __init__(self,*,orders=None,executions=None,realtime=None,positions=None):
        super().__init__(); self._orders=list(orders or []); self._executions=list(executions or [])
        self._realtime=list(realtime or []); self._positions=list(positions or [])
    def order_history(self,p):
        self.calls.append(('order_history',dict(p)))
        return {'retCode':0,'result':{'list':list(self._orders),'nextPageCursor':''}}
    def execution_history(self,p):
        self.calls.append(('execution_history',dict(p)))
        return {'retCode':0,'result':{'list':list(self._executions),'nextPageCursor':''}}
    def query(self,p):
        self.calls.append(('query',dict(p)))
        return {'retCode':0,'result':{'list':list(self._realtime),'nextPageCursor':''}}
    def positions(self,p):
        self.calls.append(('positions',dict(p)))
        return {'retCode':0,'result':{'list':list(self._positions),'nextPageCursor':''}}

def _s5_order(order_id='O1',client='manual',status='Filled',qty='1',cum='1',side='Buy',created='1788940000000',symbol='ETHUSDT',updated='__AUTO__'):
    return {'orderId':order_id,'orderLinkId':client,'symbol':symbol,'side':side,'orderStatus':status,'qty':qty,'cumExecQty':cum,'createdTime':created,'updatedTime':created if updated=='__AUTO__' else updated}

def _s5_execution(exec_id='EX1',order_id='O1',client='manual',qty='1',side='Buy',when='1788940000000',symbol='ETHUSDT',order_qty=None):
    row={'execId':exec_id,'orderId':order_id,'orderLinkId':client,'symbol':symbol,'side':side,'execQty':qty,'execTime':when}
    if order_qty is not None: row['orderQty']=order_qty
    return row

def test_s5_execution_with_exact_correlated_historical_order_can_remain_clean(tmp_path):
    rd=S5Read(orders=[_s5_order()],executions=[_s5_execution()])
    assert _rr(tmp_path,rd).reconcile().classification=='RECONCILED_CLEAN'

def test_s5_execution_without_authoritative_order_is_not_clean(tmp_path):
    rd=S5Read(executions=[_s5_execution()])
    assert _rr(tmp_path,rd).reconcile().classification=='RECONCILIATION_UNKNOWN_DISAGREEMENT'

def test_s5_order_absent_realtime_but_present_history_is_used(tmp_path):
    rd=S5Read(orders=[_s5_order()],executions=[_s5_execution()],realtime=[])
    assert _rr(tmp_path,rd).reconcile().classification=='RECONCILED_CLEAN'

def test_s5_conflicting_duplicate_execution_identity_is_not_clean(tmp_path):
    class ConflictExecutionRead(S5Read):
        def execution_history(self,p):
            self.calls.append(('execution_history',dict(p)))
            start=p.get('startTime')
            row=_s5_execution(client='manual' if start!=1788944340000 else 'different')
            return {'retCode':0,'result':{'list':[row],'nextPageCursor':''}}
    rd=ConflictExecutionRead(orders=[_s5_order()])
    assert _rr(tmp_path,rd).reconcile().classification=='RECONCILIATION_UNKNOWN_DISAGREEMENT'

def test_s5_execution_order_client_identity_contradiction_is_not_clean(tmp_path):
    rd=S5Read(orders=[_s5_order(client='manual-A')],executions=[_s5_execution(client='manual-B')])
    assert _rr(tmp_path,rd).reconcile().classification=='RECONCILIATION_UNKNOWN_DISAGREEMENT'

def test_s5_ats_like_historical_remote_identity_without_local_authority_is_never_clean(tmp_path):
    client='AT1_'+'a'*32
    rd=S5Read(orders=[_s5_order(client=client)],executions=[_s5_execution(client=client)],realtime=[])
    assert _rr(tmp_path,rd).reconcile().classification=='RECONCILED_BLOCKED_REMOTE_ORPHAN'

def test_s5_zero_positions_and_zero_realtime_do_not_hide_uncorrelated_execution(tmp_path):
    rd=S5Read(executions=[_s5_execution()],realtime=[],positions=[])
    result=_rr(tmp_path,rd).reconcile()
    assert result.classification!='RECONCILED_CLEAN'
    assert result.classification=='RECONCILIATION_UNKNOWN_DISAGREEMENT'


# V4 S5.1/S5.2 exact completion regressions.
def _s5_result(tmp_path,order,executions):
    import uuid
    case=tmp_path/uuid.uuid4().hex; case.mkdir()
    return _rr(case,S5Read(orders=[order] if order else [],executions=executions)).reconcile().classification

def test_v4_a_two_distinct_execids_same_order_are_preserved(tmp_path):
    o=_s5_order(qty='1',cum='1')
    xs=[_s5_execution('EX1',qty='0.4'),_s5_execution('EX2',qty='0.6')]
    assert _s5_result(tmp_path,o,xs)=='RECONCILED_CLEAN'

def test_v4_b_semantic_duplicate_execid_is_one_logical_execution(tmp_path):
    o=_s5_order(qty='1.000',cum='1.0')
    xs=[_s5_execution('EX1',qty='1'),_s5_execution('EX1',qty='1.000')]
    assert _s5_result(tmp_path,o,xs)=='RECONCILED_CLEAN'

def test_v4_c_conflicting_duplicate_execid_is_disagreement(tmp_path):
    o=_s5_order(qty='1',cum='1')
    xs=[_s5_execution('EX1',qty='1'),_s5_execution('EX1',qty='0.5')]
    assert _s5_result(tmp_path,o,xs)=='RECONCILIATION_UNKNOWN_DISAGREEMENT'

def test_v4_d_symbol_contradiction_not_clean(tmp_path):
    assert _s5_result(tmp_path,_s5_order(symbol='BTCUSDT'),[_s5_execution(symbol='ETHUSDT')])=='RECONCILIATION_UNKNOWN_DISAGREEMENT'

def test_v4_e_side_contradiction_not_clean(tmp_path):
    assert _s5_result(tmp_path,_s5_order(side='Buy'),[_s5_execution(side='Sell')])=='RECONCILIATION_UNKNOWN_DISAGREEMENT'

def test_v4_f_coherent_partial_fills(tmp_path):
    o=_s5_order(status='Filled',qty='1',cum='1.000')
    assert _s5_result(tmp_path,o,[_s5_execution('EX1',qty='0.4'),_s5_execution('EX2',qty='0.6')])=='RECONCILED_CLEAN'

def test_v4_g_quantity_contradiction_not_clean(tmp_path):
    o=_s5_order(status='Filled',qty='1',cum='0.6')
    assert _s5_result(tmp_path,o,[_s5_execution('EX1',qty='0.4'),_s5_execution('EX2',qty='0.2')])=='RECONCILIATION_UNKNOWN_DISAGREEMENT'

def test_v4_h_historical_order_correlation_still_works(tmp_path):
    assert _s5_result(tmp_path,_s5_order(),[_s5_execution()])=='RECONCILED_CLEAN'

def test_v4_i_uncorrelated_execution_stays_not_clean(tmp_path):
    assert _s5_result(tmp_path,None,[_s5_execution()])=='RECONCILIATION_UNKNOWN_DISAGREEMENT'

@pytest.mark.parametrize('qty,cum,execqty',[('1','1.0','1.000'),('1.000','1','1.0')])
def test_v4_exact_decimal_equivalence_without_float(tmp_path,qty,cum,execqty):
    assert _s5_result(tmp_path,_s5_order(qty=qty,cum=cum),[_s5_execution(qty=execqty)])=='RECONCILED_CLEAN'

@pytest.mark.parametrize('bad',['1e0','+1',' 1','1 ','NaN','Infinity','01','1.','.1','-1'])
def test_v4_malformed_decimal_strings_fail_endpoint(tmp_path,bad):
    assert _s5_result(tmp_path,_s5_order(qty=bad,cum='1'),[_s5_execution()])=='RECONCILIATION_UNKNOWN_ENDPOINT'

@pytest.mark.parametrize('bad',[1,1.0,True,None])
def test_v4_non_string_decimal_types_fail_endpoint(tmp_path,bad):
    assert _s5_result(tmp_path,_s5_order(qty=bad,cum='1'),[_s5_execution()])=='RECONCILIATION_UNKNOWN_ENDPOINT'

def test_v4_qty_zero_and_execqty_zero_fail_endpoint(tmp_path):
    assert _s5_result(tmp_path,_s5_order(qty='0',cum='0'),[])=='RECONCILIATION_UNKNOWN_ENDPOINT'
    assert _s5_result(tmp_path,_s5_order(status='New',qty='1',cum='0'),[_s5_execution(qty='0')])=='RECONCILIATION_UNKNOWN_ENDPOINT'

def test_v4_execution_sum_greater_than_qty_is_disagreement(tmp_path):
    assert _s5_result(tmp_path,_s5_order(qty='1',cum='1'),[_s5_execution(qty='1.1')])=='RECONCILIATION_UNKNOWN_DISAGREEMENT'

def test_v4_cumexecqty_must_equal_execution_sum(tmp_path):
    assert _s5_result(tmp_path,_s5_order(qty='1',cum='0.5'),[_s5_execution(qty='0.4')])=='RECONCILIATION_UNKNOWN_DISAGREEMENT'

@pytest.mark.parametrize('status,qty,cum,execs,expected',[
    ('New','1','0',[],'RECONCILED_BLOCKED_EXPOSURE'),
    ('Untriggered','1','0',[],'RECONCILED_BLOCKED_EXPOSURE'),
    ('Triggered','1','0',[],'RECONCILED_BLOCKED_EXPOSURE'),
    ('PartiallyFilled','1','0.4',['0.4'],'RECONCILED_BLOCKED_EXPOSURE'),
    ('Filled','1','1',['0.4','0.6'],'RECONCILED_CLEAN'),
    ('Cancelled','1','0',[],'RECONCILED_CLEAN'),
    ('Cancelled','1','0.4',['0.4'],'RECONCILED_CLEAN'),
    ('Rejected','1','0',[],'RECONCILED_CLEAN'),
    ('New','1','0.1',['0.1'],'RECONCILIATION_UNKNOWN_DISAGREEMENT'),
    ('Untriggered','1','0.1',['0.1'],'RECONCILIATION_UNKNOWN_DISAGREEMENT'),
    ('Triggered','1','0.1',['0.1'],'RECONCILIATION_UNKNOWN_DISAGREEMENT'),
    ('PartiallyFilled','1','0',[],'RECONCILIATION_UNKNOWN_DISAGREEMENT'),
    ('PartiallyFilled','1','1',['1'],'RECONCILIATION_UNKNOWN_DISAGREEMENT'),
    ('Filled','1','0.9',['0.9'],'RECONCILIATION_UNKNOWN_DISAGREEMENT'),
    ('Cancelled','1','1',['1'],'RECONCILIATION_UNKNOWN_DISAGREEMENT'),
    ('Rejected','1','0.1',['0.1'],'RECONCILIATION_UNKNOWN_DISAGREEMENT'),
])
def test_v4_status_matrix_boundaries(tmp_path,status,qty,cum,execs,expected):
    xs=[_s5_execution('EX'+str(i+1),qty=q) for i,q in enumerate(execs)]
    assert _s5_result(tmp_path,_s5_order(status=status,qty=qty,cum=cum),xs)==expected

@pytest.mark.parametrize('status',['filled','Deactivated','PartiallyFilledCanceled',''])
def test_v4_unsupported_or_case_variant_status_is_endpoint(tmp_path,status):
    assert _s5_result(tmp_path,_s5_order(status=status),[_s5_execution()])=='RECONCILIATION_UNKNOWN_ENDPOINT'

@pytest.mark.parametrize('which',['qty','cumExecQty','createdTime'])
def test_v4_missing_required_order_quantity_or_time_is_endpoint(tmp_path,which):
    o=_s5_order(); o.pop(which)
    assert _s5_result(tmp_path,o,[_s5_execution()])=='RECONCILIATION_UNKNOWN_ENDPOINT'

@pytest.mark.parametrize('which',['execQty','execTime'])
def test_v4_missing_required_execution_quantity_or_time_is_endpoint(tmp_path,which):
    x=_s5_execution(); x.pop(which)
    assert _s5_result(tmp_path,_s5_order(),[x])=='RECONCILIATION_UNKNOWN_ENDPOINT'

def test_v4_exec_time_before_created_is_disagreement_and_equal_passes(tmp_path):
    o=_s5_order(created='1000')
    assert _s5_result(tmp_path,o,[_s5_execution(when='999')])=='RECONCILIATION_UNKNOWN_DISAGREEMENT'
    assert _s5_result(tmp_path,o,[_s5_execution(when='1000')])=='RECONCILED_CLEAN'

def test_v4_equal_exec_time_distinct_execids_remain_distinct(tmp_path):
    o=_s5_order(qty='1',cum='1',created='1000')
    xs=[_s5_execution('EX1',qty='0.4',when='1000'),_s5_execution('EX2',qty='0.6',when='1000')]
    assert _s5_result(tmp_path,o,xs)=='RECONCILED_CLEAN'

def test_v4_no_max_execution_age_rule(tmp_path):
    o=_s5_order(created='1')
    assert _s5_result(tmp_path,o,[_s5_execution(when='9999999999999')])=='RECONCILED_CLEAN'

def test_v4_execution_orderqty_is_exact_corroboration(tmp_path):
    assert _s5_result(tmp_path,_s5_order(qty='1.0'),[_s5_execution(order_qty='1.000')])=='RECONCILED_CLEAN'
    assert _s5_result(tmp_path,_s5_order(qty='1'),[_s5_execution(order_qty='2')])=='RECONCILIATION_UNKNOWN_DISAGREEMENT'

# V5 S5.3 exact order-snapshot reduction regressions.
def _v5_reduce(rows):
    return _reduce_order_snapshots(rows)

def _v5_snap(*,updated='1788940000000',**kw):
    return _s5_order(updated=updated,**kw)

def test_v5_identical_and_metadata_only_observations_collapse():
    a=_v5_snap(); b=dict(a); b['irrelevantMetadata']='different'
    assert _v5_reduce([a,a])==_v5_reduce([a])
    assert _v5_reduce([a,b])==_v5_reduce([a])

def test_v5_whole_json_inequality_is_not_lifecycle_authority():
    a=_v5_snap(); b=dict(a); b['arbitrary']='x'
    assert _v5_reduce([b,a])[0]['orderId']=='O1'

def test_v5_immutable_conflicts_are_disagreement():
    base=_v5_snap(updated='2000',created='1000')
    changes=[('orderLinkId','other'),('symbol','BTCUSDT'),('side','Sell'),('qty','2'),('createdTime','999')]
    for field,value in changes:
        later=dict(base); later['updatedTime']='3000'; later[field]=value
        with pytest.raises(R2RemoteDisagreement): _v5_reduce([base,later])

def test_v5_decimal_lexical_equivalents_are_projection_equal():
    a=_v5_snap(qty='1',cum='1.0',updated='2000',created='1000')
    b=_v5_snap(qty='1.000',cum='1',updated='2000',created='1000')
    assert len(_v5_reduce([a,b]))==1

def test_v5_missing_or_malformed_updatedtime_is_endpoint():
    a=_v5_snap(); a.pop('updatedTime')
    with pytest.raises(ValueError): _v5_reduce([a])
    for bad in ['0','01','-1','1.0',' 1','1 ',1,None,True]:
        with pytest.raises(ValueError): _v5_reduce([_v5_snap(updated=bad)])

def test_v5_updatedtime_before_createdtime_is_disagreement():
    with pytest.raises(R2RemoteDisagreement): _v5_reduce([_v5_snap(created='2000',updated='1999')])

def test_v5_equal_time_safety_projection_conflicts_are_disagreement():
    a=_v5_snap(status='PartiallyFilled',qty='1',cum='0.4',created='1000',updated='2000')
    for field,value in [('orderStatus','Cancelled'),('cumExecQty','0.5')]:
        b=dict(a); b[field]=value
        with pytest.raises(R2RemoteDisagreement): _v5_reduce([a,b])

def test_v5_surface_and_row_permutation_do_not_change_reduction():
    rows=[
        _v5_snap(status='New',qty='1',cum='0',created='1000',updated='1100'),
        _v5_snap(status='PartiallyFilled',qty='1.0',cum='0.4',created='1000',updated='1200'),
        _v5_snap(status='Filled',qty='1.00',cum='1',created='1000',updated='1300'),
    ]
    assert _v5_reduce(rows)==_v5_reduce(list(reversed(rows)))==_v5_reduce([rows[1],rows[2],rows[0]])

def test_v5_stale_realtime_cannot_override_later_history_and_later_realtime_can_win():
    history=_v5_snap(status='Filled',qty='1',cum='1',created='1000',updated='3000')
    stale_rt=_v5_snap(status='PartiallyFilled',qty='1',cum='0.4',created='1000',updated='2000')
    assert _v5_reduce([history,stale_rt])[0]['orderStatus']=='Filled'
    old_hist=_v5_snap(status='PartiallyFilled',qty='1',cum='0.4',created='1000',updated='2000')
    later_rt=_v5_snap(status='Filled',qty='1',cum='1',created='1000',updated='3000')
    assert _v5_reduce([old_hist,later_rt])[0]['orderStatus']=='Filled'

def test_v5_valid_partial_progressions_and_decrease_rejected():
    p1=_v5_snap(status='PartiallyFilled',qty='1',cum='0.4',created='1000',updated='2000')
    p2=_v5_snap(status='PartiallyFilled',qty='1',cum='0.6',created='1000',updated='2500')
    filled=_v5_snap(status='Filled',qty='1',cum='1',created='1000',updated='3000')
    assert _v5_reduce([p1,filled])[0]['cumExecQty']=='1'
    assert _v5_reduce([p1,p2])[0]['cumExecQty']=='0.6'
    down=_v5_snap(status='PartiallyFilled',qty='1',cum='0.3',created='1000',updated='3000')
    with pytest.raises(R2RemoteDisagreement): _v5_reduce([p1,down])

def test_v5_status_regression_and_terminal_escape_are_disagreement():
    partial=_v5_snap(status='PartiallyFilled',qty='1',cum='0.4',created='1000',updated='2000')
    new=_v5_snap(status='New',qty='1',cum='0',created='1000',updated='3000')
    with pytest.raises(R2RemoteDisagreement): _v5_reduce([partial,new])
    for terminal,cum in [('Filled','1'),('Cancelled','0.4'),('Rejected','0')]:
        first=_v5_snap(status=terminal,qty='1',cum=cum,created='1000',updated='2000')
        later=_v5_snap(status='New',qty='1',cum='0',created='1000',updated='3000')
        with pytest.raises(R2RemoteDisagreement): _v5_reduce([first,later])

def test_v5_latest_validated_snapshot_alone_enters_s5_1(tmp_path):
    rows=[_v5_snap(status='PartiallyFilled',qty='1',cum='0.4',created='1788940000000',updated='1788940000100'),
          _v5_snap(status='Filled',qty='1',cum='1',created='1788940000000',updated='1788940000200')]
    rd=S5Read(orders=rows,executions=[_s5_execution('EX1',qty='1')])
    assert _rr(tmp_path,rd).reconcile().classification=='RECONCILED_CLEAN'

def test_v5_full_hist_overlap_realtime_path_uses_reducer(tmp_path):
    hist=[_v5_snap(status='PartiallyFilled',qty='1',cum='0.4',created='1788940000000',updated='1788940000100')]
    rt=[_v5_snap(status='Filled',qty='1',cum='1',created='1788940000000',updated='1788940000200')]
    rd=S5Read(orders=hist,executions=[_s5_execution('EX1',qty='1')],realtime=rt)
    assert _rr(tmp_path,rd).reconcile().classification=='RECONCILED_CLEAN'

def test_v5_overlap_realtime_open_order_merge_uses_reducer(tmp_path):
    # Same order appears as older overlap/history New and later realtime Cancelled.
    # Latest reducer projection is terminal Cancelled, so stale New cannot falsely block exposure.
    old=_v5_snap(status='New',qty='1',cum='0',created='1788940000000',updated='1788940000100')
    latest=_v5_snap(status='Cancelled',qty='1',cum='0',created='1788940000000',updated='1788940000200')
    rd=S5Read(orders=[old],executions=[],realtime=[latest])
    assert _rr(tmp_path,rd).reconcile().classification=='RECONCILED_CLEAN'


# V6 S5.4 operation-disposition authority alignment regressions.
def _v6_unresolved_create_events(client='AT1_X'):
    from backtest.execution.execution_event_ledger import R1AuthorityEvent
    return [
        R1AuthorityEvent(**_r1row(1,'CREATE_DISPATCHING',attempt='v6',op='CREATE',client=client,cid='e'*64)),
        R1AuthorityEvent(**_r1row(2,'CREATE_UNKNOWN',attempt='v6',op='CREATE',client=client,cid='e'*64)),
    ]

def test_v6_historical_cancelled_order_defeats_absent_in_operation_disposition():
    events=_v6_unresolved_create_events('AT1_X')
    historical=[_v5_snap(order_id='O1',client='AT1_X',status='Cancelled',qty='1',cum='0',created='1000',updated='2000')]
    authoritative=_reduce_order_snapshots(historical)
    ds,problem=_operation_dispositions(events,authoritative,[],[])
    assert problem is None
    assert len(ds)==1
    assert ds[0]['remote_disposition']=='TERMINAL_NO_OPEN_EXPOSURE'
    assert ds[0]['remote_disposition']!='ABSENT'

def test_v6_historical_rejected_order_defeats_absent_in_operation_disposition():
    events=_v6_unresolved_create_events('AT1_X')
    historical=[_v5_snap(order_id='O1',client='AT1_X',status='Rejected',qty='1',cum='0',created='1000',updated='2000')]
    authoritative=_reduce_order_snapshots(historical)
    ds,problem=_operation_dispositions(events,authoritative,[],[])
    assert problem is None
    assert len(ds)==1
    assert ds[0]['remote_disposition']=='TERMINAL_NO_OPEN_EXPOSURE'
    assert ds[0]['remote_disposition']!='ABSENT'

def test_v6_true_absent_remains_possible():
    events=_v6_unresolved_create_events('AT1_X')
    ds,problem=_operation_dispositions(events,[],[],[])
    assert problem is None
    assert len(ds)==1
    assert ds[0]['remote_disposition']=='ABSENT'

@pytest.mark.parametrize('status',['Cancelled','Rejected'])
def test_v6_historical_terminal_only_does_not_become_current_exposure(tmp_path,status):
    order=_v5_snap(order_id='O1',client='AT1_X',status=status,qty='1',cum='0',
                   created='1788940000000',updated='1788940000200')
    rd=S5Read(orders=[order],executions=[],realtime=[],positions=[])
    p=tmp_path/'x.jsonl'
    _write_jsonl(
        p,
        _r1row(1,'CREATE_DISPATCHING',attempt='v6',op='CREATE',client='AT1_X',cid='e'*64),
        _r1row(2,'CREATE_UNKNOWN',attempt='v6',op='CREATE',client='AT1_X',cid='e'*64),
    )
    ledger=R1AuthorityLedger(p)
    rr=R2StartupReconciler(ledger=ledger,read_authority=rd,eligibility_issuer=R2StartupEligibilityIssuer(),
        admitted_authority=authority(),configuration_digest='f'*64,credential_fingerprint='c'*64,
        r3_execution_authority_reference='R3_FIXTURE',monotonic=lambda:100.0,
        utc_now=lambda:datetime(2026,9,9,9,0,tzinfo=timezone.utc))
    result=rr.reconcile()
    assert result.classification!='RECONCILED_BLOCKED_EXPOSURE'
    event=ledger.r2_events[-1]
    disp=event.disposition_manifest['operation_dispositions']
    assert len(disp)==1
    assert disp[0]['remote_disposition']=='TERMINAL_NO_OPEN_EXPOSURE'

def test_v6_execution_disposition_behavior_preserved_with_authoritative_orders():
    events=_v6_unresolved_create_events('AT1_X')
    order=_v5_snap(order_id='O1',client='AT1_X',status='Filled',qty='1',cum='1',created='1000',updated='2000')
    execution=_s5_execution('EX1',order_id='O1',client='AT1_X',qty='1',when='2000')
    authoritative=_reduce_order_snapshots([order])
    executions=_dedupe_executions([execution])
    ds,problem=_operation_dispositions(events,authoritative,executions,[])
    assert problem is None
    assert ds[0]['remote_disposition']=='FILLED_OR_EXECUTED'


# V7 S5.5 joint operation-identifier correlation regressions.
def _v7_unresolved_events(*,client='AT1_X',exchange='O1'):
    from backtest.execution.execution_event_ledger import R1AuthorityEvent
    return [
        R1AuthorityEvent(**_r1row(1,'CREATE_DISPATCHING',attempt='v7',op='CREATE',client=client,exchange=exchange,cid='f'*64)),
        R1AuthorityEvent(**_r1row(2,'CREATE_UNKNOWN',attempt='v7',op='CREATE',client=client,exchange=exchange,cid='f'*64)),
    ]

def test_v7_client_match_orderid_contradiction_is_disagreement():
    events=_v7_unresolved_events(client='AT1_X',exchange='O1')
    remote=[_v5_snap(order_id='O2',client='AT1_X',status='Cancelled',qty='1',cum='0',created='1000',updated='2000')]
    ds,problem=_operation_dispositions(events,remote,[],[])
    assert ds==[]
    assert problem=='CONFLICTING_OPERATION_DISPOSITION'

def test_v7_orderid_match_client_contradiction_is_disagreement():
    events=_v7_unresolved_events(client='AT1_X',exchange='O1')
    remote=[_v5_snap(order_id='O1',client='AT1_Y',status='Cancelled',qty='1',cum='0',created='1000',updated='2000')]
    ds,problem=_operation_dispositions(events,remote,[],[])
    assert ds==[]
    assert problem=='CONFLICTING_OPERATION_DISPOSITION'

def test_v7_both_identifiers_match_uses_normal_existing_disposition():
    events=_v7_unresolved_events(client='AT1_X',exchange='O1')
    remote=[_v5_snap(order_id='O1',client='AT1_X',status='Cancelled',qty='1',cum='0',created='1000',updated='2000')]
    ds,problem=_operation_dispositions(events,remote,[],[])
    assert problem is None
    assert ds[0]['remote_disposition']=='TERMINAL_NO_OPEN_EXPOSURE'

def test_v7_execution_client_match_orderid_contradiction_is_disagreement():
    events=_v7_unresolved_events(client='AT1_X',exchange='O1')
    execution=_s5_execution('EX1',order_id='O2',client='AT1_X',qty='1',when='2000')
    ds,problem=_operation_dispositions(events,[],[execution],[])
    assert ds==[]
    assert problem=='CONFLICTING_OPERATION_DISPOSITION'

def test_v7_execution_orderid_match_client_contradiction_is_disagreement():
    events=_v7_unresolved_events(client='AT1_X',exchange='O1')
    execution=_s5_execution('EX1',order_id='O1',client='AT1_Y',qty='1',when='2000')
    ds,problem=_operation_dispositions(events,[],[execution],[])
    assert ds==[]
    assert problem=='CONFLICTING_OPERATION_DISPOSITION'

def test_v7_client_only_positive_preserved():
    events=_v7_unresolved_events(client='AT1_X',exchange='')
    remote=[_v5_snap(order_id='O2',client='AT1_X',status='Cancelled',qty='1',cum='0',created='1000',updated='2000')]
    ds,problem=_operation_dispositions(events,remote,[],[])
    assert problem is None
    assert ds[0]['remote_disposition']=='TERMINAL_NO_OPEN_EXPOSURE'

def test_v7_exchange_only_positive_preserved():
    events=_v7_unresolved_events(client='',exchange='O1')
    remote=[_v5_snap(order_id='O1',client='AT1_Y',status='Cancelled',qty='1',cum='0',created='1000',updated='2000')]
    ds,problem=_operation_dispositions(events,remote,[],[])
    assert problem is None
    assert ds[0]['remote_disposition']=='TERMINAL_NO_OPEN_EXPOSURE'

def test_v7_multiple_competing_identity_candidates_fail_closed():
    events=_v7_unresolved_events(client='AT1_X',exchange='O1')
    exact=_v5_snap(order_id='O1',client='AT1_X',status='Cancelled',qty='1',cum='0',created='1000',updated='2000')
    partial=_v5_snap(order_id='O2',client='AT1_X',status='Cancelled',qty='1',cum='0',created='1000',updated='2000')
    ds,problem=_operation_dispositions(events,[exact,partial],[],[])
    assert ds==[]
    assert problem=='CONFLICTING_OPERATION_DISPOSITION'

def test_v7_s5_4_historical_terminal_and_true_absent_regressions_still_hold():
    hist_events=_v7_unresolved_events(client='AT1_X',exchange='')
    cancelled=[_v5_snap(order_id='O1',client='AT1_X',status='Cancelled',qty='1',cum='0',created='1000',updated='2000')]
    rejected=[_v5_snap(order_id='O1',client='AT1_X',status='Rejected',qty='1',cum='0',created='1000',updated='2000')]
    for rows in (cancelled,rejected):
        ds,problem=_operation_dispositions(hist_events,rows,[],[])
        assert problem is None
        assert ds[0]['remote_disposition']=='TERMINAL_NO_OPEN_EXPOSURE'
    ds,problem=_operation_dispositions(hist_events,[],[],[])
    assert problem is None
    assert ds[0]['remote_disposition']=='ABSENT'


# V8 S5.6 attempt-level durable identifier reconstruction regressions.
def _v8_events(rows):
    from backtest.execution.execution_event_ledger import R1AuthorityEvent
    return [R1AuthorityEvent(**row) for row in rows]

def test_v8_later_ack_exchange_enrichment_detects_remote_contradiction():
    events=_v8_events([
        _r1row(1,'CREATE_DISPATCHING',attempt='v8a',op='CREATE',client='AT1_X',exchange='',cid='a'*64),
        _r1row(2,'CREATE_ACK_PENDING_QUERY',attempt='v8a',op='CREATE',client='AT1_X',exchange='O1',cid='a'*64),
    ])
    remote=[_v5_snap(order_id='O2',client='AT1_X',status='Cancelled',qty='1',cum='0',created='1000',updated='2000')]
    ds,problem=_operation_dispositions(events,remote,[],[])
    assert ds==[]
    assert problem=='CONFLICTING_OPERATION_DISPOSITION'

def test_v8_later_ack_exchange_enrichment_positive_and_proof_uses_aggregate():
    events=_v8_events([
        _r1row(1,'CREATE_DISPATCHING',attempt='v8b',op='CREATE',client='AT1_X',exchange='',cid='a'*64),
        _r1row(2,'CREATE_ACK_PENDING_QUERY',attempt='v8b',op='CREATE',client='AT1_X',exchange='O1',cid='a'*64),
    ])
    remote=[_v5_snap(order_id='O1',client='AT1_X',status='Cancelled',qty='1',cum='0',created='1000',updated='2000')]
    ds,problem=_operation_dispositions(events,remote,[],[])
    assert problem is None
    assert ds[0]['remote_disposition']=='TERMINAL_NO_OPEN_EXPOSURE'
    assert ds[0]['exchange_order_id_digest']==hashlib.sha256(b'O1').hexdigest()
    expected_proof={
        'local_dispatch_sequence':1,
        'operation_kind':'CREATE',
        'client_order_id':'AT1_X',
        'client_identity_digest':'a'*64,
        'exchange_order_id':'O1',
        'orders':remote,
        'executions':[],
        'positions':[],
    }
    assert ds[0]['correlation_proof_digest']==_digest(expected_proof)

def test_v8_exchange_id_local_conflict_fails_before_remote_selection():
    events=_v8_events([
        _r1row(1,'CREATE_DISPATCHING',attempt='v8c',op='CREATE',client='AT1_X',exchange='O1',cid='a'*64),
        _r1row(2,'CREATE_UNKNOWN',attempt='v8c',op='CREATE',client='AT1_X',exchange='O2',cid='a'*64),
    ])
    ds,problem=_operation_dispositions(events,[],[],[])
    assert ds==[]
    assert problem=='CONFLICTING_OPERATION_DISPOSITION'

def test_v8_client_id_local_conflict_fails_before_remote_selection():
    events=_v8_events([
        _r1row(1,'CREATE_DISPATCHING',attempt='v8d',op='CREATE',client='AT1_X',exchange='O1',cid='a'*64),
        _r1row(2,'CREATE_UNKNOWN',attempt='v8d',op='CREATE',client='AT1_Y',exchange='O1',cid='b'*64),
    ])
    ds,problem=_operation_dispositions(events,[],[],[])
    assert ds==[]
    assert problem=='CONFLICTING_OPERATION_DISPOSITION'

@pytest.mark.parametrize('later_event',['CREATE_ACK_PENDING_QUERY','CREATE_UNKNOWN'])
def test_v8_unknown_or_ack_event_can_enrich_exchange_identity(later_event):
    events=_v8_events([
        _r1row(1,'CREATE_DISPATCHING',attempt='v8e',op='CREATE',client='AT1_X',exchange='',cid='a'*64),
        _r1row(2,later_event,attempt='v8e',op='CREATE',client='AT1_X',exchange='O1',cid='a'*64),
    ])
    remote=[_v5_snap(order_id='O1',client='AT1_X',status='Cancelled',qty='1',cum='0',created='1000',updated='2000')]
    ds,problem=_operation_dispositions(events,remote,[],[])
    assert problem is None
    assert ds[0]['remote_disposition']=='TERMINAL_NO_OPEN_EXPOSURE'
    assert ds[0]['exchange_order_id_digest']==hashlib.sha256(b'O1').hexdigest()

def test_v8_cancel_attempt_enrichment_preserves_normal_correlation():
    events=_v8_events([
        _r1row(1,'CANCEL_DISPATCHING',attempt='v8f',op='CANCEL',client='AT1_X',exchange='',cid='a'*64),
        _r1row(2,'CANCEL_ACK_PENDING_QUERY',attempt='v8f',op='CANCEL',client='AT1_X',exchange='O1',cid='a'*64),
    ])
    remote=[_v5_snap(order_id='O1',client='AT1_X',status='Cancelled',qty='1',cum='0',created='1000',updated='2000')]
    ds,problem=_operation_dispositions(events,remote,[],[])
    assert problem is None
    assert ds[0]['operation_kind']=='CANCEL'
    assert ds[0]['remote_disposition']=='TERMINAL_NO_OPEN_EXPOSURE'
    assert ds[0]['exchange_order_id_digest']==hashlib.sha256(b'O1').hexdigest()

def test_v8_cancel_local_exchange_conflict_fails_closed():
    events=_v8_events([
        _r1row(1,'CANCEL_DISPATCHING',attempt='v8g',op='CANCEL',client='AT1_X',exchange='O1',cid='a'*64),
        _r1row(2,'CANCEL_UNKNOWN',attempt='v8g',op='CANCEL',client='AT1_X',exchange='O2',cid='a'*64),
    ])
    ds,problem=_operation_dispositions(events,[],[],[])
    assert ds==[]
    assert problem=='CONFLICTING_OPERATION_DISPOSITION'

def test_v8_dispatch_only_identifier_behavior_preserved():
    events=_v8_events([
        _r1row(1,'CREATE_DISPATCHING',attempt='v8h',op='CREATE',client='AT1_X',exchange='O1',cid='a'*64),
    ])
    remote=[_v5_snap(order_id='O1',client='AT1_X',status='Cancelled',qty='1',cum='0',created='1000',updated='2000')]
    ds,problem=_operation_dispositions(events,remote,[],[])
    assert problem is None
    assert ds[0]['remote_disposition']=='TERMINAL_NO_OPEN_EXPOSURE'

@pytest.mark.parametrize(
    'remote_client,remote_order,expected_problem',
    [
        ('AT1_X','O2','CONFLICTING_OPERATION_DISPOSITION'),
        ('AT1_Y','O1','CONFLICTING_OPERATION_DISPOSITION'),
        ('AT1_X','O1',None),
    ],
)
def test_v8_s5_5_joint_matcher_regression_with_attempt_aggregate(remote_client,remote_order,expected_problem):
    events=_v8_events([
        _r1row(1,'CREATE_DISPATCHING',attempt='v8i',op='CREATE',client='AT1_X',exchange='',cid='a'*64),
        _r1row(2,'CREATE_ACK_PENDING_QUERY',attempt='v8i',op='CREATE',client='AT1_X',exchange='O1',cid='a'*64),
    ])
    remote=[_v5_snap(order_id=remote_order,client=remote_client,status='Cancelled',qty='1',cum='0',created='1000',updated='2000')]
    ds,problem=_operation_dispositions(events,remote,[],[])
    assert problem==expected_problem
    if expected_problem is None:
        assert ds[0]['remote_disposition']=='TERMINAL_NO_OPEN_EXPOSURE'
    else:
        assert ds==[]


# V9 S5.7 attempt-level full client identity digest authority regressions.
def test_v9_same_display_id_different_full_digest_fails_before_remote_selection():
    events=_v8_events([
        _r1row(1,'CREATE_DISPATCHING',attempt='v9a',op='CREATE',client='AT1_X',exchange='',cid='a'*64),
        _r1row(2,'CREATE_ACK_PENDING_QUERY',attempt='v9a',op='CREATE',client='AT1_X',exchange='O1',cid='b'*64),
    ])
    # Remote evidence would otherwise be a positive match; local digest conflict must win first.
    remote=[_v5_snap(order_id='O1',client='AT1_X',status='Cancelled',qty='1',cum='0',created='1000',updated='2000')]
    ds,problem=_operation_dispositions(events,remote,[],[])
    assert ds==[]
    assert problem=='CONFLICTING_OPERATION_DISPOSITION'

def test_v9_same_full_digest_preserves_exchange_enrichment_and_proof_authority():
    digest='a'*64
    events=_v8_events([
        _r1row(1,'CREATE_DISPATCHING',attempt='v9b',op='CREATE',client='AT1_X',exchange='',cid=digest),
        _r1row(2,'CREATE_ACK_PENDING_QUERY',attempt='v9b',op='CREATE',client='AT1_X',exchange='O1',cid=digest),
    ])
    remote=[_v5_snap(order_id='O1',client='AT1_X',status='Cancelled',qty='1',cum='0',created='1000',updated='2000')]
    ds,problem=_operation_dispositions(events,remote,[],[])
    assert problem is None
    assert ds[0]['client_identity_digest']==digest
    assert ds[0]['exchange_order_id_digest']==hashlib.sha256(b'O1').hexdigest()
    expected_proof={
        'local_dispatch_sequence':1,
        'operation_kind':'CREATE',
        'client_order_id':'AT1_X',
        'client_identity_digest':digest,
        'exchange_order_id':'O1',
        'orders':remote,
        'executions':[],
        'positions':[],
    }
    assert ds[0]['correlation_proof_digest']==_digest(expected_proof)

def test_v9_unknown_lifecycle_full_digest_conflict_fails_closed():
    events=_v8_events([
        _r1row(1,'CREATE_DISPATCHING',attempt='v9c',op='CREATE',client='AT1_X',exchange='',cid='a'*64),
        _r1row(2,'CREATE_UNKNOWN',attempt='v9c',op='CREATE',client='AT1_X',exchange='O1',cid='b'*64),
    ])
    ds,problem=_operation_dispositions(events,[],[],[])
    assert ds==[]
    assert problem=='CONFLICTING_OPERATION_DISPOSITION'

def test_v9_cancel_full_digest_conflict_fails_closed():
    events=_v8_events([
        _r1row(1,'CANCEL_DISPATCHING',attempt='v9d',op='CANCEL',client='AT1_X',exchange='O1',cid='a'*64),
        _r1row(2,'CANCEL_UNKNOWN',attempt='v9d',op='CANCEL',client='AT1_X',exchange='O1',cid='b'*64),
    ])
    ds,problem=_operation_dispositions(events,[],[],[])
    assert ds==[]
    assert problem=='CONFLICTING_OPERATION_DISPOSITION'

def test_v9_empty_digest_does_not_override_nonempty_attempt_authority():
    digest='a'*64
    events=_v8_events([
        _r1row(1,'CREATE_DISPATCHING',attempt='v9e',op='CREATE',client='AT1_X',exchange='',cid=''),
        _r1row(2,'CREATE_ACK_PENDING_QUERY',attempt='v9e',op='CREATE',client='AT1_X',exchange='O1',cid=digest),
    ])
    remote=[_v5_snap(order_id='O1',client='AT1_X',status='Cancelled',qty='1',cum='0',created='1000',updated='2000')]
    ds,problem=_operation_dispositions(events,remote,[],[])
    assert problem is None
    assert ds[0]['client_identity_digest']==digest
    assert ds[0]['exchange_order_id_digest']==hashlib.sha256(b'O1').hexdigest()


# V10 S5.8 exact attempt-lifecycle grouping/cardinality regressions.
def test_v10_valid_create_dispatch_exactly_one_disposition():
    ds,problem=_operation_dispositions(
        _v8_events([_r1row(1,'CREATE_DISPATCHING',attempt='v10a',op='CREATE')]),[],[],[])
    assert problem is None and len(ds)==1 and ds[0]['operation_kind']=='CREATE'

def test_v10_valid_cancel_dispatch_exactly_one_disposition():
    ds,problem=_operation_dispositions(
        _v8_events([_r1row(1,'CANCEL_DISPATCHING',attempt='v10b',op='CANCEL')]),[],[],[])
    assert problem is None and len(ds)==1 and ds[0]['operation_kind']=='CANCEL'

def test_v10_distinct_create_cancel_attempts_two_dispositions_in_dispatch_order():
    events=_v8_events([
        _r1row(2,'CANCEL_DISPATCHING',attempt='v10c2',op='CANCEL'),
        _r1row(1,'CREATE_DISPATCHING',attempt='v10c1',op='CREATE'),
    ])
    ds,problem=_operation_dispositions(events,[],[],[])
    assert problem is None
    assert [(d['local_dispatch_sequence'],d['operation_kind']) for d in ds]==[(1,'CREATE'),(2,'CANCEL')]

def test_v10_mixed_create_cancel_same_attempt_fails_closed():
    events=_v8_events([
        _r1row(1,'CREATE_DISPATCHING',attempt='v10d',op='CREATE'),
        _r1row(2,'CANCEL_DISPATCHING',attempt='v10d',op='CANCEL'),
    ])
    assert _operation_dispositions(events,[],[],[])==([], 'CONFLICTING_OPERATION_DISPOSITION')

def test_v10_create_dispatch_plus_cancel_unknown_same_attempt_fails_closed():
    events=_v8_events([
        _r1row(1,'CREATE_DISPATCHING',attempt='v10e',op='CREATE'),
        _r1row(2,'CANCEL_UNKNOWN',attempt='v10e',op='CANCEL'),
    ])
    assert _operation_dispositions(events,[],[],[])==([], 'CONFLICTING_OPERATION_DISPOSITION')

def test_v10_cancel_dispatch_plus_create_unknown_same_attempt_fails_closed():
    events=_v8_events([
        _r1row(1,'CANCEL_DISPATCHING',attempt='v10f',op='CANCEL'),
        _r1row(2,'CREATE_UNKNOWN',attempt='v10f',op='CREATE'),
    ])
    assert _operation_dispositions(events,[],[],[])==([], 'CONFLICTING_OPERATION_DISPOSITION')

def test_v10_two_create_dispatch_anchors_same_attempt_fail_closed():
    events=_v8_events([
        _r1row(1,'CREATE_DISPATCHING',attempt='v10g',op='CREATE'),
        _r1row(2,'CREATE_DISPATCHING',attempt='v10g',op='CREATE'),
    ])
    assert _operation_dispositions(events,[],[],[])==([], 'CONFLICTING_OPERATION_DISPOSITION')

def test_v10_two_cancel_dispatch_anchors_same_attempt_fail_closed():
    events=_v8_events([
        _r1row(1,'CANCEL_DISPATCHING',attempt='v10h',op='CANCEL'),
        _r1row(2,'CANCEL_DISPATCHING',attempt='v10h',op='CANCEL'),
    ])
    assert _operation_dispositions(events,[],[],[])==([], 'CONFLICTING_OPERATION_DISPOSITION')

def test_v10_create_cancel_dispatch_anchors_same_attempt_fail_closed():
    events=_v8_events([
        _r1row(1,'CREATE_DISPATCHING',attempt='v10i',op='CREATE'),
        _r1row(2,'CANCEL_DISPATCHING',attempt='v10i',op='CANCEL'),
    ])
    assert _operation_dispositions(events,[],[],[])==([], 'CONFLICTING_OPERATION_DISPOSITION')

def test_v10_operation_field_event_family_mismatch_fails_closed():
    events=_v8_events([_r1row(1,'CREATE_DISPATCHING',attempt='v10j',op='CANCEL')])
    assert _operation_dispositions(events,[],[],[])==([], 'CONFLICTING_OPERATION_DISPOSITION')

def test_v10_conflicting_operation_identity_fails_closed():
    rows=[
        _r1row(1,'CREATE_DISPATCHING',attempt='v10k',op='CREATE'),
        _r1row(2,'CREATE_UNKNOWN',attempt='v10k',op='CREATE'),
    ]
    rows[0]['operation_identity']='OP_A'
    rows[1]['operation_identity']='OP_B'
    assert _operation_dispositions(_v8_events(rows),[],[],[])==([], 'CONFLICTING_OPERATION_DISPOSITION')

def test_v10_cross_family_terminal_cannot_suppress_unresolved_create():
    events=_v8_events([
        _r1row(1,'CREATE_DISPATCHING',attempt='v10l',op='CREATE'),
        _r1row(2,'CANCEL_CONFIRMED',attempt='v10l',op='CANCEL'),
    ])
    assert _operation_dispositions(events,[],[],[])==([], 'CONFLICTING_OPERATION_DISPOSITION')

@pytest.mark.parametrize(
    'op,dispatch,terminal',
    [
        ('CREATE','CREATE_DISPATCHING','CREATE_REJECT_CONFIRMED'),
        ('CANCEL','CANCEL_DISPATCHING','CANCEL_CONFIRMED'),
        ('CANCEL','CANCEL_DISPATCHING','CANCEL_NOT_EFFECTIVE_TERMINAL'),
        ('CANCEL','CANCEL_DISPATCHING','CANCEL_NOT_CONFIRMED'),
    ],
)
def test_v10_valid_same_family_terminal_closes_only_own_lifecycle(op,dispatch,terminal):
    events=_v8_events([
        _r1row(1,dispatch,attempt='v10m',op=op),
        _r1row(2,terminal,attempt='v10m',op=op),
    ])
    assert _operation_dispositions(events,[],[],[])==([],None)

@pytest.mark.parametrize(
    'op,dispatch',
    [('CREATE','CREATE_DISPATCHING'),('CANCEL','CANCEL_DISPATCHING')],
)
def test_v10_dispatch_only_create_cancel_behavior_preserved(op,dispatch):
    ds,problem=_operation_dispositions(
        _v8_events([_r1row(1,dispatch,attempt='v10n',op=op)]),[],[],[])
    assert problem is None and len(ds)==1 and ds[0]['remote_disposition']=='ABSENT'

@pytest.mark.parametrize('later', ['CREATE_ACK_PENDING_QUERY','CREATE_UNKNOWN'])
def test_v10_valid_ack_unknown_enrichment_preserves_s5_5_s5_7(later):
    digest='a'*64
    events=_v8_events([
        _r1row(1,'CREATE_DISPATCHING',attempt='v10o',op='CREATE',client='AT1_X',exchange='',cid=digest),
        _r1row(2,later,attempt='v10o',op='CREATE',client='AT1_X',exchange='O1',cid=digest),
    ])
    remote=[_v5_snap(order_id='O1',client='AT1_X',status='Cancelled',qty='1',cum='0',created='1000',updated='2000')]
    ds,problem=_operation_dispositions(events,remote,[],[])
    assert problem is None
    assert ds[0]['client_identity_digest']==digest
    assert ds[0]['exchange_order_id_digest']==hashlib.sha256(b'O1').hexdigest()
    assert ds[0]['remote_disposition']=='TERMINAL_NO_OPEN_EXPOSURE'

@pytest.mark.parametrize('event_type',['CREATE_RESERVED','CREATE_RESERVATION_EXPIRED'])
def test_v10_zero_dispatch_create_predispatch_lifecycle_emits_no_remote_disposition(event_type):
    events=_v8_events([_r1row(1,event_type,attempt='v10p',op='CREATE')])
    assert _operation_dispositions(events,[],[],[])==([],None)

@pytest.mark.parametrize(
    'op,event_type',
    [
        ('CREATE','CREATE_ACK_PENDING_QUERY'),
        ('CREATE','CREATE_UNKNOWN'),
        ('CREATE','CREATE_REJECT_CONFIRMED'),
        ('CANCEL','CANCEL_ACK_PENDING_QUERY'),
        ('CANCEL','CANCEL_UNKNOWN'),
        ('CANCEL','CANCEL_CONFIRMED'),
    ],
)
def test_v10_postdispatch_outcome_without_matching_dispatch_fails_closed(op,event_type):
    events=_v8_events([_r1row(1,event_type,attempt='v10q',op=op)])
    assert _operation_dispositions(events,[],[],[])==([], 'CONFLICTING_OPERATION_DISPOSITION')

@pytest.mark.parametrize('event_type',['QUERY_OBSERVED','AUTHORITY_BLOCKED','IDENTITY_CONFLICT'])
def test_v10_nonmutation_event_inside_mutation_attempt_fails_closed(event_type):
    events=_v8_events([
        _r1row(1,'CREATE_DISPATCHING',attempt='v10r',op='CREATE'),
        _r1row(2,event_type,attempt='v10r',op='CREATE'),
    ])
    assert _operation_dispositions(events,[],[],[])==([], 'CONFLICTING_OPERATION_DISPOSITION')

def test_v10_exact_v9_failure_shape_does_not_repair_into_subdispositions():
    events=_v8_events([
        _r1row(1,'CREATE_DISPATCHING',attempt='v10s',op='CREATE'),
        _r1row(2,'CREATE_UNKNOWN',attempt='v10s',op='CREATE'),
        _r1row(3,'CANCEL_DISPATCHING',attempt='v10s',op='CANCEL'),
        _r1row(4,'CANCEL_UNKNOWN',attempt='v10s',op='CANCEL'),
    ])
    assert _operation_dispositions(events,[],[],[])==([], 'CONFLICTING_OPERATION_DISPOSITION')

def test_v10_validation_precedes_cross_operation_terminal_suppression():
    events=_v8_events([
        _r1row(1,'CREATE_DISPATCHING',attempt='v10t',op='CREATE'),
        _r1row(2,'CREATE_UNKNOWN',attempt='v10t',op='CREATE'),
        _r1row(3,'CANCEL_CONFIRMED',attempt='v10t',op='CANCEL'),
    ])
    assert _operation_dispositions(events,[],[],[])==([], 'CONFLICTING_OPERATION_DISPOSITION')

def test_v10_empty_operation_is_not_repaired_by_family():
    row=_r1row(1,'CREATE_DISPATCHING',attempt='v10u',op='CREATE')
    row['operation']=''
    assert _operation_dispositions(_v8_events([row]),[],[],[])==([], 'CONFLICTING_OPERATION_DISPOSITION')

def test_v10_missing_nonempty_operation_identity_fails_closed():
    row=_r1row(1,'CREATE_DISPATCHING',attempt='v10v',op='CREATE')
    row['operation_identity']=''
    assert _operation_dispositions(_v8_events([row]),[],[],[])==([], 'CONFLICTING_OPERATION_DISPOSITION')
