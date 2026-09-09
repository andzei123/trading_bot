from __future__ import annotations
import hashlib,json,time,uuid,re
from decimal import Decimal, localcontext
from dataclasses import dataclass
from datetime import datetime,timezone,timedelta
from typing import Any,Mapping,Protocol,Callable
from .execution_event_ledger import R1AuthorityLedger,R2_EMPTY_LEDGER_SENTINEL_DIGEST
from .r1_contracts import R1_TESTNET_ORIGIN,parse_utc,utc_z
from .r2_identity_registry import AdmittedAT1GenesisAuthorityV1,match_admitted_context,validate_admitted_genesis,classify_exchange_client_identity
from .r2_startup_authority import R2StartupEligibilityIssuer

BOOT_DISARMED='BOOT_DISARMED'; LOCAL_CLASSIFIED='LOCAL_CLASSIFIED'; DISCOVERY_IN_PROGRESS='DISCOVERY_IN_PROGRESS'; RECONCILIATION_UNKNOWN_OR_BLOCKED='RECONCILIATION_UNKNOWN_OR_BLOCKED'; RECONCILED_DISARMED='RECONCILED_DISARMED'; OPERATOR_ARMED='OPERATOR_ARMED'
AT1_AUTHORITY_NOT_ADMITTED='AT1_AUTHORITY_NOT_ADMITTED'
class R2AccountMismatch(ValueError):
    def __init__(self,message,reason_code='ACCOUNT_AUTHORITY_INVALID'):
        super().__init__(message); self.reason_code=reason_code
class R2FreshnessError(ValueError): pass
class R2RemoteDisagreement(ValueError): pass
R2_MAX_RETENTION_AGE_US=86100*1_000_000; R2_HISTORY_WINDOW_MS=7*24*60*60*1000

class R2ReadAuthority(Protocol):
    def server_time(self)->Mapping[str,Any]: ...
    def api_key_info(self,params:Mapping[str,Any]|None=None)->Mapping[str,Any]: ...
    def order_history(self,params:Mapping[str,Any])->Mapping[str,Any]: ...
    def execution_history(self,params:Mapping[str,Any])->Mapping[str,Any]: ...
    def query(self,params:Mapping[str,Any])->Mapping[str,Any]: ...
    def positions(self,params:Mapping[str,Any])->Mapping[str,Any]: ...
    def instrument_rules(self,params:Mapping[str,Any])->Mapping[str,Any]: ...

@dataclass(frozen=True,slots=True)
class R2StartupResult:
    startup_state:str; classification:str; startup_epoch_id:str; eligibility:object=None; proof_manifest_digest:str=''; disposition_manifest_digest:str=''

class R2StartupReconciler:
    def __init__(self,*,ledger:R1AuthorityLedger,read_authority:R2ReadAuthority,eligibility_issuer:R2StartupEligibilityIssuer,admitted_authority:AdmittedAT1GenesisAuthorityV1,configuration_digest:str,credential_fingerprint:str,r3_execution_authority_reference:str,monotonic=time.monotonic,utc_now:Callable[[],datetime]|None=None):
        self._ledger=ledger; self._read=read_authority; self._issuer=eligibility_issuer; self._authority=admitted_authority; self._cfg=configuration_digest; self._cred=credential_fingerprint; self._r3=r3_execution_authority_reference; self._mono=monotonic; self._utc_now=utc_now or (lambda:datetime.now(timezone.utc))
        self._entry=match_admitted_context(admitted_authority,origin=R1_TESTNET_ORIGIN,environment='TESTNET',category='linear',settle_coin='USDT',account_uid=107087555,parent_uid=0)
    def reconcile(self)->R2StartupResult:
        epoch=uuid.uuid4().hex; process_digest=hashlib.sha256((epoch+'\0'+str(id(self))).encode()).hexdigest(); entry=validate_admitted_genesis(self._authority)
        # S1: strict physical replay/classification is authoritative before activation consumption or network.
        try:
            _,_,_,pre_r2=self._ledger.exact_reread()
            local_class=classify_local_snapshot(self._ledger._path,now_utc=self._utc_now(),activation_authority_admitted=True)
        except Exception:
            return R2StartupResult(RECONCILIATION_UNKNOWN_OR_BLOCKED,'RECONCILIATION_UNKNOWN_LOCAL',epoch)
        pre_seq,pre_digest=self._ledger.physical_tip()
        if pre_seq==0 and pre_digest!=R2_EMPTY_LEDGER_SENTINEL_DIGEST: return R2StartupResult(RECONCILIATION_UNKNOWN_OR_BLOCKED,'RECONCILIATION_UNKNOWN_LOCAL',epoch)
        local_event=self._ledger.append_r2(**_event(epoch,'R2_LOCAL_CLASSIFIED',self._cfg,self._cred,'LOCAL_CLASSIFIED',True,recorded_at=utc_z(self._utc_now()),local_snapshot_digest=_digest({'pre_tip_sequence':pre_seq,'pre_tip_digest':pre_digest}),local_classification=local_class))
        local_rr=self._ledger.exact_reread()[3]
        if len([x for x in local_rr if x.sequence==local_event.sequence and x.startup_epoch_id==epoch and x.event_type=='R2_LOCAL_CLASSIFIED' and x.local_classification==local_class])!=1:
            return R2StartupResult(RECONCILIATION_UNKNOWN_OR_BLOCKED,'RECONCILIATION_UNKNOWN_LOCAL',epoch)
        pre_seq,pre_digest=self._ledger.physical_tip()
        consumption=self._ledger.append_r2(**_event(epoch,'R2_ACTIVATION_CONSUMPTION_STARTED',self._cfg,self._cred,'ACTIVATION_CONSUMED_BEFORE_DISCOVERY',True,recorded_at=utc_z(self._utc_now()),process_session_identity_digest=process_digest,local_pre_tip_sequence=pre_seq,local_pre_tip_digest=pre_digest,activation_entry_digest=self._authority.expected_entry_sha256,registry_root_digest=self._authority.expected_registry_root_sha256,chief_activation_authority_reference=entry.chief_authority_reference,r3_execution_authority_reference=self._r3))
        rr=self._ledger.exact_reread()[3]
        if len([x for x in rr if x.sequence==consumption.sequence and x.startup_epoch_id==epoch and x.event_type=='R2_ACTIVATION_CONSUMPTION_STARTED'])!=1: return R2StartupResult(RECONCILIATION_UNKNOWN_OR_BLOCKED,'RECONCILIATION_UNKNOWN_LOCAL',epoch)
        epoch_start=self._mono(); surface=[]
        try:
            s0raw=self._read.server_time(); s0=_server_time(s0raw); _skew(self._utc_now(),s0); surface.append(_single_surface('SERVER_TIME_S0','/v5/market/time',{},s0raw,0,0))
            self._ledger.append_r2(**_event(epoch,'R2_DISCOVERY_STARTED',self._cfg,self._cred,'DISCOVERY_STARTED',True,recorded_at=utc_z(self._utc_now())))
            account_raw=self._read.api_key_info({}); permissions_digest=_validate_account(account_raw); surface.append(_single_surface('API_KEY_INFO','/v5/user/query-api',{},account_raw,1,2))
            activation=parse_utc(entry.activation_utc); cutoff=s0-timedelta(seconds=60)
            windows=_history_windows(_ms(activation),_ms(cutoff))
            hist=[]; exe=[]; oldest_send=None; oldest_receive=None; off=3
            for i,(start_ms,end_ms) in enumerate(windows):
                if i==0: oldest_send=self._mono()
                rows,pf=_paged(self._read.order_history,{'category':'linear','limit':50,'startTime':start_ms,'endTime':end_ms},50,f'ORDER_HISTORY:{i}','/v5/order/history',off,coverage=(start_ms,end_ms)); hist.extend(rows); surface.append(pf); off+=pf['request_count']
                if i==0: oldest_receive=self._mono()
            for i,(start_ms,end_ms) in enumerate(windows):
                rows,pf=_paged(self._read.execution_history,{'category':'linear','limit':100,'startTime':start_ms,'endTime':end_ms},100,f'EXECUTION_HISTORY:{i}','/v5/execution/list',off,coverage=(start_ms,end_ms)); exe.extend(rows); surface.append(pf); off+=pf['request_count']
            # S4 Rev2.3: S1 closes historical retention, then exact [C,S1] overlaps, then realtime/positions.
            barrier_start=self._mono()
            s1raw=self._read.server_time(); s1=_server_time(s1raw); _skew(self._utc_now(),s1); surface.append(_single_surface('SERVER_TIME_S1','/v5/market/time',{},s1raw,off,off)); off+=1
            oh_ov,ohproof=_paged(self._read.order_history,{'category':'linear','limit':50,'startTime':_ms(cutoff),'endTime':_ms(s1)},50,'ORDER_HISTORY_OVERLAP','/v5/order/history',off,coverage=(_ms(cutoff),_ms(s1))); surface.append(ohproof); off+=ohproof['request_count']
            ex_ov,exproof=_paged(self._read.execution_history,{'category':'linear','limit':100,'startTime':_ms(cutoff),'endTime':_ms(s1)},100,'EXECUTION_HISTORY_OVERLAP','/v5/execution/list',off,coverage=(_ms(cutoff),_ms(s1))); surface.append(exproof); off+=exproof['request_count']
            rt,rtproof=_paged(self._read.query,{'category':'linear','settleCoin':'USDT','openOnly':0,'limit':50},50,'REALTIME_OPEN_ORDERS','/v5/order/realtime',off); surface.append(rtproof); off+=rtproof['request_count']
            pos,posproof=_paged(self._read.positions,{'category':'linear','settleCoin':'USDT','limit':200},200,'POSITIONS','/v5/position/list',off); surface.append(posproof); off+=posproof['request_count']
            # Instrument rules are non-retention-sensitive and are sampled only after the closed discovery/barrier order.
            symbols=sorted({str(x.get('symbol','')) for x in (hist+exe+rt+pos+oh_ov+ex_ov) if str(x.get('symbol',''))})
            rules_proofs=[]
            for symbol in symbols:
                rules_raw=self._read.instrument_rules({'category':'linear','symbol':symbol}); _envelope(rules_raw); rules_proofs.append(_rules_proof(symbol,rules_raw,s1))
        except R2AccountMismatch as exc: return self._unknown(epoch,'RECONCILED_BLOCKED_ACCOUNT_MISMATCH',exc.reason_code)
        except R2FreshnessError as exc: return self._unknown(epoch,'RECONCILIATION_UNKNOWN_FRESHNESS',str(exc))
        except R2RemoteDisagreement as exc: return self._unknown(epoch,'RECONCILIATION_UNKNOWN_DISAGREEMENT',str(exc))
        except Exception as exc: return self._unknown(epoch,'RECONCILIATION_UNKNOWN_ENDPOINT',type(exc).__name__)
        epoch_ms=int((self._mono()-epoch_start)*1000); barrier_ms=int((self._mono()-barrier_start)*1000)
        if s1<s0 or epoch_ms>120000 or barrier_ms>30000: return self._unknown(epoch,'RECONCILIATION_UNKNOWN_FRESHNESS','TIME_BOUND')
        age_us=int((s1-parse_utc(entry.activation_utc)).total_seconds()*1_000_000)
        retention_ok=(age_us>=0 and age_us<R2_MAX_RETENTION_AGE_US)
        account_result=account_raw['result']; ap={'origin':R1_TESTNET_ORIGIN,'account_uid':107087555,'parent_uid':0,'credential_fingerprint':self._cred,'account_mode':str(account_result['unifiedMarginStatus']),'read_only':False,'permissions_digest':permissions_digest,'request_digest':_digest({}),'normalized_response_digest':_digest(account_raw),'observed_after_s0':True,'observed_before_s1':True}
        reg={'identity_version':'AT1','activation_utc':entry.activation_utc,'entry_digest':self._authority.expected_entry_sha256,'registry_root_digest':self._authority.expected_registry_root_sha256,'chief_authority_reference':entry.chief_authority_reference}
        retention={'documented_horizon_seconds':86400,'safety_margin_seconds':300,'maximum_age_seconds':86100,'age_at_s1_microseconds':age_us,'oldest_request_send_offset_ms':int(((oldest_send or epoch_start)-epoch_start)*1000),'oldest_response_receive_offset_ms':int(((oldest_receive or epoch_start)-epoch_start)*1000),'oldest_window_first':True,'continuous_local_authority':False,'retention_sufficient':retention_ok}
        proof={'manifest_schema':'ATS_R2_DISCOVERY_PROOF_MANIFEST_V1','startup_epoch_id':epoch,'server_time_start_utc':utc_z(s0),'server_time_end_utc':utc_z(s1),'epoch_monotonic_duration_ms':epoch_ms,'current_barrier_duration_ms':barrier_ms,'account_proof':ap,'registry_proof':reg,'retention_proof':retention,'surface_proofs':surface,'rules_proofs':rules_proofs}
        pd=_digest(proof); discovery_digest=_digest({'proof_manifest_digest':pd,'cutoff':utc_z(cutoff)})
        proof_event=self._ledger.append_r2(**_event(epoch,'R2_DISCOVERY_COMPLETED',self._cfg,self._cred,'DISCOVERY_COMPLETED',True,recorded_at=utc_z(self._utc_now()),proof_manifest=proof,proof_manifest_digest=pd,discovery_bundle_digest=discovery_digest,discovery_cutoff_utc=utc_z(s1),reconciliation_classification='DISCOVERY_COMPLETE'))
        if not retention_ok:
            disposition={'manifest_schema':'ATS_R2_RECONCILIATION_DISPOSITION_V1','proof_event_sequence':proof_event.sequence,'proof_manifest_digest':pd,'local_projection_digest':_digest({'r1_events':[e.sequence for e in self._ledger.events]}),'operation_dispositions':_operation_dispositions(self._ledger.events,[],[],[])[0],'identity_class_counts':{},'order_state_counts':{},'execution_count':0,'nonzero_position_count':0,'open_order_count':0,'unknown_count':0,'blocking_reason_codes':['INSUFFICIENT_RETENTION'],'result':'RECONCILED_BLOCKED_RETENTION'}
            dd=_digest(disposition); self._ledger.append_r2(**_event(epoch,'R2_RECONCILIATION_UNKNOWN',self._cfg,self._cred,'RECONCILED_BLOCKED_RETENTION',True,recorded_at=utc_z(self._utc_now()),proof_manifest=proof,proof_manifest_digest=pd,disposition_manifest=disposition,disposition_manifest_digest=dd,discovery_bundle_digest=discovery_digest,discovery_cutoff_utc=utc_z(s1),reconciliation_classification='RECONCILED_BLOCKED_RETENTION',reconstructed_state_digest=_digest(disposition)))
            return R2StartupResult(RECONCILIATION_UNKNOWN_OR_BLOCKED,'RECONCILED_BLOCKED_RETENTION',epoch,None,pd,dd)
        try:
            open_orders=_reduce_order_snapshots(oh_ov+rt); positions=_dedupe(pos); executions=_dedupe_executions(exe+ex_ov)
        except R2RemoteDisagreement as exc:
            return self._unknown(epoch,'RECONCILIATION_UNKNOWN_DISAGREEMENT',str(exc))
        except ValueError as exc:
            return self._unknown(epoch,'RECONCILIATION_UNKNOWN_ENDPOINT',str(exc))
        nonzero=[x for x in positions if _nonzero(x.get('size'))]; active=[x for x in open_orders if str(x.get('orderStatus','')) in {'New','PartiallyFilled','Untriggered','Triggered'}]
        local_clients={e.client_order_id:e.client_identity_digest for e in self._ledger.events if getattr(e,'client_order_id','')}
        # S5: CLEAN requires every deduplicated execution to belong to one authoritative
        # remote order lifecycle reconstructed from complete accepted order evidence.
        authoritative_orders=_reduce_order_snapshots(hist+oh_ov+rt)
        identity_classes,remote_problem=_remote_execution_lifecycle_truth(authoritative_orders,executions,local_clients)
        orphan=any(x in {'ATS_PROBABLE_UNVERIFIED','UNRECOGNIZED_VERSION','IDENTITY_CONFLICT'} for x in identity_classes)
        operation_dispositions, disposition_problem=_operation_dispositions(self._ledger.events,authoritative_orders,executions,positions)
        local_unknown=disposition_problem is not None
        result='RECONCILED_CLEAN'
        if remote_problem is not None: result=remote_problem
        elif active or nonzero: result='RECONCILED_BLOCKED_EXPOSURE'
        elif orphan: result='RECONCILED_BLOCKED_REMOTE_ORPHAN'
        elif local_unknown: result='RECONCILED_BLOCKED_LOCAL_UNKNOWN'
        disposition={'manifest_schema':'ATS_R2_RECONCILIATION_DISPOSITION_V1','proof_event_sequence':proof_event.sequence,'proof_manifest_digest':pd,'local_projection_digest':_digest({'r1_events':[e.sequence for e in self._ledger.events]}),'operation_dispositions':operation_dispositions,'identity_class_counts':_value_counts(identity_classes),'order_state_counts':_counts(open_orders,'orderStatus'),'execution_count':len(executions),'nonzero_position_count':len(nonzero),'open_order_count':len(active),'unknown_count':0,'blocking_reason_codes':[] if result=='RECONCILED_CLEAN' else [result],'result':result}
        dd=_digest(disposition)
        if result!='RECONCILED_CLEAN':
            self._ledger.append_r2(**_event(epoch,'R2_RECONCILIATION_UNKNOWN',self._cfg,self._cred,result,True,recorded_at=utc_z(self._utc_now()),proof_manifest=proof,proof_manifest_digest=pd,disposition_manifest=disposition,disposition_manifest_digest=dd,discovery_bundle_digest=discovery_digest,discovery_cutoff_utc=utc_z(s1),reconciliation_classification=result,reconstructed_state_digest=_digest(disposition)))
            return R2StartupResult(RECONCILIATION_UNKNOWN_OR_BLOCKED,result,epoch,None,pd,dd)
        rec=self._ledger.append_r2(**_event(epoch,'R2_RECONCILED_DISARMED',self._cfg,self._cred,'RECONCILED_CLEAN',True,recorded_at=utc_z(self._utc_now()),proof_manifest=proof,proof_manifest_digest=pd,disposition_manifest=disposition,disposition_manifest_digest=dd,discovery_bundle_digest=discovery_digest,discovery_cutoff_utc=utc_z(s1),reconciliation_classification='RECONCILED_CLEAN',reconstructed_state_digest=_digest(disposition)))
        reread=self._ledger.exact_reread()[3]
        if not any(x.sequence==rec.sequence and x.event_type=='R2_RECONCILED_DISARMED' and x.proof_manifest_digest==pd and x.disposition_manifest_digest==dd for x in reread): return R2StartupResult(RECONCILIATION_UNKNOWN_OR_BLOCKED,'RECONCILIATION_UNKNOWN_LOCAL',epoch)
        tip_sequence,tip_digest=self._ledger.physical_tip(); ctx={'startup_epoch_id':epoch,'proof_manifest_digest':pd,'disposition_manifest_digest':dd,'configuration_digest':self._cfg,'origin':R1_TESTNET_ORIGIN,'account_uid':107087555,'ledger_tip_sequence':tip_sequence,'ledger_tip_digest':tip_digest}; eligibility=self._issuer.issue(startup_epoch_id=epoch,context=ctx)
        return R2StartupResult(RECONCILED_DISARMED,'RECONCILED_CLEAN',epoch,eligibility,pd,dd)
    def _unknown(self,epoch,reason,detail=''):
        self._ledger.append_r2(**_event(epoch,'R2_RECONCILIATION_UNKNOWN',self._cfg,self._cred,reason,True,recorded_at=utc_z(self._utc_now()),reconciliation_classification=reason)); return R2StartupResult(RECONCILIATION_UNKNOWN_OR_BLOCKED,reason,epoch)
    def _blocked(self,epoch,reason): return R2StartupResult(RECONCILIATION_UNKNOWN_OR_BLOCKED,reason,epoch)

def _event(epoch,event,cfg,cred,reason,block,*,recorded_at,proof_manifest=None,proof_manifest_digest=None,disposition_manifest=None,disposition_manifest_digest=None,**kw):
    d=dict(startup_epoch_id=epoch,event_type=event,recorded_at_utc=recorded_at,origin=R1_TESTNET_ORIGIN,account_uid=107087555,parent_uid=0,credential_fingerprint=cred,category='linear',settle_coin='USDT',configuration_digest=cfg,local_snapshot_digest=kw.pop('local_snapshot_digest',None),local_classification=kw.pop('local_classification',None),discovery_bundle_digest=kw.pop('discovery_bundle_digest',None),discovery_cutoff_utc=kw.pop('discovery_cutoff_utc',None),reconciliation_classification=kw.pop('reconciliation_classification',None),reconstructed_state_digest=kw.pop('reconstructed_state_digest',None),block_mutations=block,reason_code=reason,proof_manifest=proof_manifest,proof_manifest_digest=proof_manifest_digest,disposition_manifest=disposition_manifest,disposition_manifest_digest=disposition_manifest_digest,process_session_identity_digest=None,local_pre_tip_sequence=None,local_pre_tip_digest=None,activation_entry_digest=None,registry_root_digest=None,chief_activation_authority_reference=None,r3_execution_authority_reference=None); d.update(kw); return d

def _server_time(raw):
    _envelope(raw); sec=raw['result'].get('timeSecond');
    if sec is None: raise ValueError('server time missing')
    return datetime.fromtimestamp(float(sec),timezone.utc)
def _validate_account(raw):
    _envelope(raw); r=raw['result']
    if str(r.get('userID'))!='107087555' or type(r.get('parentUid')) is not int or r['parentUid']!=0:
        raise R2AccountMismatch('account mismatch')
    if 'unifiedMarginStatus' not in r or type(r['unifiedMarginStatus']) is not int or r['unifiedMarginStatus']!=1:
        raise R2AccountMismatch('account mode invalid')
    if 'readOnly' not in r or type(r['readOnly']) is not int or r['readOnly']!=0:
        raise R2AccountMismatch('readOnly invalid','ACCOUNT_PERMISSION_AUTHORITY_INVALID')
    if 'permissions' not in r or type(r['permissions']) is not dict:
        raise R2AccountMismatch('permissions invalid','ACCOUNT_PERMISSION_AUTHORITY_INVALID')
    permissions=r['permissions']
    if 'ContractTrade' not in permissions or type(permissions['ContractTrade']) is not list:
        raise R2AccountMismatch('ContractTrade invalid','ACCOUNT_PERMISSION_AUTHORITY_INVALID')
    ct=permissions['ContractTrade']
    if any(type(x) is not str for x in ct) or len(ct)!=2 or len(set(ct))!=2 or set(ct)!={'Order','Position'}:
        raise R2AccountMismatch('ContractTrade values invalid','ACCOUNT_PERMISSION_AUTHORITY_INVALID')
    for key,value in permissions.items():
        if type(key) is not str or type(value) is not list or (key!='ContractTrade' and value!=[]):
            raise R2AccountMismatch('excess permission authority','ACCOUNT_PERMISSION_AUTHORITY_INVALID')
    normalized={k:sorted(v,key=lambda x:x.encode('utf-8')) for k,v in sorted(permissions.items(),key=lambda kv:kv[0].encode('utf-8'))}
    raw_bytes=json.dumps(normalized,separators=(',',':'),ensure_ascii=True).encode('utf-8')
    return hashlib.sha256(raw_bytes).hexdigest()

def _joint_operation_identity_candidates(rows,client,exchange):
    """Correlate one unresolved local operation using every known local identifier."""
    # Preserve the existing zero-identifier behavior: no fallback identity is invented.
    if not client and not exchange:
        return [],None
    matched=[]
    for row in rows:
        remote_client=str(row.get('orderLinkId',''))
        remote_exchange=str(row.get('orderId',''))
        client_match=bool(client) and remote_client==client
        exchange_match=bool(exchange) and remote_exchange==exchange
        any_known_match=client_match or exchange_match
        if client and exchange:
            if any_known_match and not (client_match and exchange_match):
                return [],'CONFLICTING_OPERATION_DISPOSITION'
            if client_match and exchange_match:
                matched.append(row)
        elif client:
            if client_match:
                matched.append(row)
        elif exchange:
            if exchange_match:
                matched.append(row)
    return matched,None

def _attempt_operation_identity_authority(hist,dispatch_event):
    """Reconstruct durable local correlation identity across one attempt lifecycle."""
    clients={e.client_order_id for e in hist if getattr(e,'client_order_id','')}
    exchanges={e.exchange_order_id for e in hist if getattr(e,'exchange_order_id','')}
    client_digests={e.client_identity_digest for e in hist if getattr(e,'client_identity_digest','')}
    if len(clients)>1 or len(exchanges)>1 or len(client_digests)>1:
        return '','','','CONFLICTING_OPERATION_DISPOSITION'
    client=next(iter(clients)) if clients else ''
    exchange=next(iter(exchanges)) if exchanges else ''
    client_digest=next(iter(client_digests)) if client_digests else ''
    return client,exchange,client_digest,None

def _operation_dispositions(events,orders,executions,positions):
    unresolved=[]
    by_attempt={}
    for e in events:
        if e.attempt_id: by_attempt.setdefault(e.attempt_id,[]).append(e)

    create_family={
        'CREATE_RESERVED','CREATE_RESERVATION_EXPIRED','CREATE_DISPATCHING',
        'CREATE_ACK_PENDING_QUERY','CREATE_REJECT_CONFIRMED','CREATE_UNKNOWN',
    }
    cancel_family={
        'CANCEL_DISPATCHING','CANCEL_ACK_PENDING_QUERY','CANCEL_CONFIRMED',
        'CANCEL_NOT_EFFECTIVE_TERMINAL','CANCEL_NOT_CONFIRMED','CANCEL_UNKNOWN',
    }
    family_by_operation={'CREATE':create_family,'CANCEL':cancel_family}
    dispatch_by_operation={'CREATE':'CREATE_DISPATCHING','CANCEL':'CANCEL_DISPATCHING'}
    terminal_by_operation={
        'CREATE':{'CREATE_REJECT_CONFIRMED'},
        'CANCEL':{'CANCEL_CONFIRMED','CANCEL_NOT_EFFECTIVE_TERMINAL','CANCEL_NOT_CONFIRMED'},
    }
    predispatch_create={'CREATE_RESERVED','CREATE_RESERVATION_EXPIRED'}

    for hist in by_attempt.values():
        operations={e.operation for e in hist if getattr(e,'operation','')}
        if len(operations)!=1:
            return [],'CONFLICTING_OPERATION_DISPOSITION'
        operation=next(iter(operations))
        if operation not in family_by_operation:
            return [],'CONFLICTING_OPERATION_DISPOSITION'
        if any(getattr(e,'operation','')!=operation for e in hist):
            return [],'CONFLICTING_OPERATION_DISPOSITION'

        family=family_by_operation[operation]
        if any(e.event_type not in family for e in hist):
            return [],'CONFLICTING_OPERATION_DISPOSITION'

        operation_identities={
            e.operation_identity for e in hist if getattr(e,'operation_identity','')
        }
        if len(operation_identities)!=1:
            return [],'CONFLICTING_OPERATION_DISPOSITION'

        dispatch_type=dispatch_by_operation[operation]
        dispatches=[e for e in hist if e.event_type==dispatch_type]
        if len(dispatches)>1:
            return [],'CONFLICTING_OPERATION_DISPOSITION'
        if not dispatches:
            types={e.event_type for e in hist}
            if operation!='CREATE' or not types.issubset(predispatch_create):
                return [],'CONFLICTING_OPERATION_DISPOSITION'
            continue

        dispatch=dispatches[0]
        if any(e.event_type in terminal_by_operation[operation] for e in hist):
            continue

        unresolved.append((dispatch,tuple(hist)))

    out=[]
    for e,hist in sorted(unresolved,key=lambda x:x[0].sequence):
        client,exchange,client_identity_digest,local_identity_problem=_attempt_operation_identity_authority(hist,e)
        if local_identity_problem is not None: return [],local_identity_problem
        om,order_identity_problem=_joint_operation_identity_candidates(orders,client,exchange)
        if order_identity_problem is not None: return [],order_identity_problem
        xm,execution_identity_problem=_joint_operation_identity_candidates(executions,client,exchange)
        if execution_identity_problem is not None: return [],execution_identity_problem
        symbols={str(x.get('symbol','')) for x in om+xm if x.get('symbol')}
        pm=[x for x in positions if str(x.get('symbol','')) in symbols and _nonzero(x.get('size'))]
        correlated_order_ids={str(x.get('orderId','')) for x in om+xm if x.get('orderId')}
        if len(correlated_order_ids)>1: return [],'CONFLICTING_OPERATION_DISPOSITION'
        status={str(x.get('orderStatus','')) for x in om if x.get('orderStatus')}
        if len(status)>1: return [],'CONFLICTING_OPERATION_DISPOSITION'
        if not om and not xm:
            remote='ABSENT'
        elif pm or any(x in {'New','PartiallyFilled','Untriggered','Triggered'} for x in status):
            remote='OPEN_OR_EXPOSED'
        elif xm or status & {'Filled'}:
            remote='FILLED_OR_EXECUTED'
        elif status & {'Cancelled','Rejected','Deactivated'}:
            remote='TERMINAL_NO_OPEN_EXPOSURE'
        else:
            return [],'INCOMPLETE_REMOTE_LIFECYCLE'
        proof={'local_dispatch_sequence':e.sequence,'operation_kind':e.operation,'client_order_id':client,'client_identity_digest':client_identity_digest,'exchange_order_id':exchange,'orders':om,'executions':xm,'positions':pm}
        out.append({'local_dispatch_sequence':e.sequence,'operation_kind':e.operation,'client_identity_digest':client_identity_digest,'exchange_order_id_digest':hashlib.sha256(exchange.encode()).hexdigest() if exchange else hashlib.sha256(b'').hexdigest(),'remote_disposition':remote,'correlation_proof_digest':_digest(proof)})
    return out,None

def _envelope(raw):
    if not isinstance(raw,Mapping) or raw.get('retCode')!=0 or not isinstance(raw.get('result'),Mapping): raise ValueError('Bybit envelope invalid')
def _paged(fn,params,limit,surface_id,path,offset,coverage=None):
    immutable=dict(params); cursor=''; seen=set(); pages=[]; rows=[]
    for n in range(1000):
        q=dict(immutable)
        if cursor: q['cursor']=cursor
        raw=fn(q); _envelope(raw); result=raw['result']; page=result.get('list'); out=str(result.get('nextPageCursor','') or '')
        if not isinstance(page,list): raise ValueError('page list invalid')
        page_digest=_digest(raw); pages.append({'request':_digest(q),'in_cursor':_digest(cursor),'out_cursor':_digest(out),'response':page_digest,'record_count':len(page),'offset':offset+n}); rows.extend(page)
        if not out:
            if len(page)>=limit: raise ValueError('PAGINATION_AMBIGUOUS')
            break
        if out in seen: raise ValueError('PAGINATION_AMBIGUOUS')
        seen.add(out); cursor=out
    else: raise ValueError('PAGINATION_LIMIT_EXCEEDED')
    execution_surface=surface_id.startswith('EXECUTION_HISTORY')
    order_surface=surface_id.startswith('ORDER_HISTORY') or surface_id=='REALTIME_OPEN_ORDERS'
    proof=_surface(surface_id,path,immutable,rows,pages,offset,offset+len(pages),cursor_termination='EMPTY_CURSOR',coverage=coverage,execution_surface=execution_surface,order_surface=order_surface)
    normalized=_dedupe_executions(rows) if execution_surface else (_reduce_order_snapshots(rows) if order_surface else _dedupe(rows))
    return normalized,proof
def _single_surface(sid,path,params,raw,a,b): return _surface(sid,path,params,[],[{'response':_digest(raw)}],a,b,'NO_CURSOR')
def _surface(sid,path,params,rows,pages,a,b,cursor_termination,coverage=None,execution_surface=False,order_surface=False):
    cov_start,cov_end=(coverage or (None,None)); normalized=_dedupe_executions(rows) if execution_surface else (_reduce_order_snapshots(rows) if order_surface else _dedupe(rows))
    return {'surface_id':sid,'http_method':'GET','endpoint_path':path,'canonical_parameters_digest':_digest(params),'coverage_start_utc':_utc_from_ms(cov_start) if cov_start is not None else None,'coverage_end_utc':_utc_from_ms(cov_end) if cov_end is not None else None,'first_request_offset_ms':a,'last_response_offset_ms':b,'request_count':len(pages),'page_count':len(pages),'record_count':len(rows),'ordered_page_manifest_digest':_digest(pages),'normalized_content_digest':_digest(normalized),'cursor_termination':cursor_termination,'schema_contract_id':'BYBIT_V5_R2_PHASE_A_V1','complete':True}
def _rules_proof(symbol,raw,observed): return {'symbol':symbol,'rules_schema_version':'R1_INSTRUMENT_RULES_V1','provenance_digest':_digest({'origin':R1_TESTNET_ORIGIN,'path':'/v5/market/instruments-info'}),'normalized_rules_digest':_digest(raw),'observed_at_utc':utc_z(observed),'expires_at_utc':utc_z(observed+timedelta(seconds=120))}
def _dedupe(rows):
    out={}
    for r in rows:
        if not isinstance(r,Mapping): raise ValueError('record object required')
        key=str(r.get('orderId') or r.get('execId') or (str(r.get('symbol',''))+'|'+str(r.get('positionIdx',''))+'|'+str(r.get('side',''))))
        raw=json.dumps(dict(r),sort_keys=True,separators=(',',':'))
        if key in out and out[key][0]!=raw: raise ValueError('cross-surface identity conflict')
        out[key]=(raw,dict(r))
    return [out[k][1] for k in sorted(out)]

_ORDER_STATUSES={'New','Untriggered','Triggered','PartiallyFilled','Filled','Cancelled','Rejected'}
_ORDER_TRANSITIONS={
    'Untriggered':{'Untriggered','Triggered','New','PartiallyFilled','Filled','Cancelled','Rejected'},
    'Triggered':{'Triggered','New','PartiallyFilled','Filled','Cancelled','Rejected'},
    'New':{'New','PartiallyFilled','Filled','Cancelled'},
    'PartiallyFilled':{'PartiallyFilled','Filled','Cancelled'},
    'Filled':{'Filled'}, 'Cancelled':{'Cancelled'}, 'Rejected':{'Rejected'},
}

def _order_snapshot_projection(row):
    if not isinstance(row,Mapping): raise ValueError('order record object required')
    order_id=row.get('orderId'); link=row.get('orderLinkId'); symbol=row.get('symbol'); side=row.get('side'); status=row.get('orderStatus')
    if type(order_id) is not str or not order_id: raise ValueError('orderId malformed')
    if type(link) is not str: raise ValueError('orderLinkId malformed')
    if type(symbol) is not str or not symbol: raise ValueError('symbol malformed')
    if type(side) is not str or not side: raise ValueError('side malformed')
    qty=_exact_decimal(row.get('qty'),'qty',positive=True); created=_exact_time(row.get('createdTime'),'createdTime')
    updated=_exact_time(row.get('updatedTime'),'updatedTime'); cum=_exact_decimal(row.get('cumExecQty'),'cumExecQty')
    if type(status) is not str or status not in _ORDER_STATUSES: raise ValueError('orderStatus malformed')
    if updated<created: raise R2RemoteDisagreement('updatedTime before createdTime')
    if cum>qty: raise R2RemoteDisagreement('cumExecQty exceeds qty')
    valid=(status in {'New','Untriggered','Triggered','Rejected'} and cum==0) or (status=='PartiallyFilled' and Decimal(0)<cum<qty) or (status=='Filled' and cum==qty) or (status=='Cancelled' and Decimal(0)<=cum<qty)
    if not valid: raise R2RemoteDisagreement('snapshot status quantity contradiction')
    # Exact nine-field safety projection only. Decimal/timestamp lexical variants normalize semantically.
    normalized=(order_id,link,symbol,side,qty,created,updated,status,cum)
    projected={'orderId':order_id,'orderLinkId':link,'symbol':symbol,'side':side,'qty':row['qty'],'createdTime':row['createdTime'],'updatedTime':row['updatedTime'],'orderStatus':status,'cumExecQty':row['cumExecQty']}
    return normalized,projected

def _reduce_order_snapshots(rows):
    groups={}
    for row in rows:
        normalized,projected=_order_snapshot_projection(row)
        groups.setdefault(normalized[0],[]).append((normalized,projected))
    reduced=[]
    for order_id in sorted(groups):
        observations=groups[order_id]
        immutable=None; by_time={}
        for normalized,projected in observations:
            current_immutable=normalized[:6]
            if immutable is None: immutable=current_immutable
            elif current_immutable!=immutable: raise R2RemoteDisagreement('immutable order snapshot conflict')
            updated=normalized[6]
            prior=by_time.get(updated)
            if prior is not None:
                if prior[0]!=normalized: raise R2RemoteDisagreement('equal-time order snapshot conflict')
                continue
            by_time[updated]=(normalized,projected)
        ordered=[by_time[t] for t in sorted(by_time)]
        for (earlier,_),(later,_) in zip(ordered,ordered[1:]):
            if later[8]<earlier[8]: raise R2RemoteDisagreement('cumExecQty regression')
            if later[7] not in _ORDER_TRANSITIONS[earlier[7]]: raise R2RemoteDisagreement('order status regression')
            if earlier[7] in {'Filled','Cancelled','Rejected'} and later[8]!=earlier[8]: raise R2RemoteDisagreement('terminal cumExecQty changed')
        if ordered: reduced.append(ordered[-1][1])
    return reduced

_DECIMAL_RE=re.compile(r'^(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$')
_TIME_RE=re.compile(r'^(?:0|[1-9][0-9]*)$')

def _exact_decimal(value,field,*,positive=False):
    if type(value) is not str or _DECIMAL_RE.fullmatch(value) is None: raise ValueError(field+' malformed')
    with localcontext() as ctx:
        ctx.prec=max(64,len(value.replace('.',''))+8)
        parsed=Decimal(value)
    if positive and parsed<=0: raise ValueError(field+' domain')
    if not positive and parsed<0: raise ValueError(field+' domain')
    return parsed

def _exact_time(value,field):
    if type(value) is not str or _TIME_RE.fullmatch(value) is None: raise ValueError(field+' malformed')
    parsed=int(value)
    if parsed<=0: raise ValueError(field+' domain')
    return parsed

def _execution_semantic_record(row):
    if not isinstance(row,Mapping): raise ValueError('execution record object required')
    if type(row.get('execId')) is not str or not row['execId']: raise ValueError('execId missing')
    normalized=dict(row)
    d=_exact_decimal(normalized.get('execQty'),'execQty',positive=True)
    normalized['execQty']=('DECIMAL_RATIO',d.as_integer_ratio())
    if 'orderQty' in normalized:
        oq=_exact_decimal(normalized['orderQty'],'orderQty')
        normalized['orderQty']=('DECIMAL_RATIO',oq.as_integer_ratio())
    normalized['execTime']=('TIME',_exact_time(normalized.get('execTime'),'execTime'))
    return normalized

def _dedupe_executions(rows):
    out={}
    for r in rows:
        normalized=_execution_semantic_record(r)
        key=r['execId']
        raw=json.dumps(normalized,sort_keys=True,separators=(',',':'),default=str)
        if key in out and out[key][0]!=raw: raise R2RemoteDisagreement('conflicting duplicate execId')
        out[key]=(raw,dict(r))
    return [out[k][1] for k in sorted(out)]

def _remote_execution_lifecycle_truth(order_rows,executions,local_clients):
    """Validate exact S5.1/S5.2 remote lifecycle truth before CLEAN."""
    orders_by_id={}; identity_classes=[]
    try:
        for row in order_rows:
            if not isinstance(row,Mapping): return identity_classes,'RECONCILIATION_UNKNOWN_ENDPOINT'
            order_id=row.get('orderId')
            if type(order_id) is not str or not order_id: return identity_classes,'RECONCILIATION_UNKNOWN_ENDPOINT'
            if order_id in orders_by_id: return identity_classes,'RECONCILIATION_UNKNOWN_DISAGREEMENT'
            orders_by_id[order_id]=dict(row)
            client=row.get('orderLinkId','')
            if client not in ('',None) and type(client) is not str: return identity_classes,'RECONCILIATION_UNKNOWN_ENDPOINT'
            if client:
                identity_classes.append(classify_exchange_client_identity(client,local_full_identity_digest=local_clients.get(client)))

        grouped={oid:[] for oid in orders_by_id}
        for execution in executions:
            if not isinstance(execution,Mapping): return identity_classes,'RECONCILIATION_UNKNOWN_ENDPOINT'
            execution_id=execution.get('execId'); order_id=execution.get('orderId')
            if type(execution_id) is not str or not execution_id or type(order_id) is not str or not order_id:
                return identity_classes,'RECONCILIATION_UNKNOWN_ENDPOINT'
            order=orders_by_id.get(order_id)
            if order is None: return identity_classes,'RECONCILIATION_UNKNOWN_DISAGREEMENT'
            for field in ('symbol','side'):
                ov=order.get(field); ev=execution.get(field)
                if ov is not None or ev is not None:
                    if type(ov) is not str or type(ev) is not str or not ov or not ev: return identity_classes,'RECONCILIATION_UNKNOWN_ENDPOINT'
                    if ov!=ev: return identity_classes,'RECONCILIATION_UNKNOWN_DISAGREEMENT'
            oc=order.get('orderLinkId',''); ec=execution.get('orderLinkId','')
            if oc not in ('',None) and type(oc) is not str: return identity_classes,'RECONCILIATION_UNKNOWN_ENDPOINT'
            if ec not in ('',None) and type(ec) is not str: return identity_classes,'RECONCILIATION_UNKNOWN_ENDPOINT'
            if oc and ec and oc!=ec: return identity_classes,'RECONCILIATION_UNKNOWN_DISAGREEMENT'
            if ec:
                execution_class=classify_exchange_client_identity(ec,local_full_identity_digest=local_clients.get(ec)); identity_classes.append(execution_class)
                if oc:
                    order_class=classify_exchange_client_identity(oc,local_full_identity_digest=local_clients.get(oc))
                    if execution_class!=order_class: return identity_classes,'RECONCILIATION_UNKNOWN_DISAGREEMENT'
            grouped[order_id].append(execution)

        for order_id,order in orders_by_id.items():
            qty=_exact_decimal(order.get('qty'),'qty',positive=True)
            cum=_exact_decimal(order.get('cumExecQty'),'cumExecQty')
            created=_exact_time(order.get('createdTime'),'createdTime')
            status=order.get('orderStatus')
            if type(status) is not str or status not in {'New','Untriggered','Triggered','PartiallyFilled','Filled','Cancelled','Rejected'}:
                return identity_classes,'RECONCILIATION_UNKNOWN_ENDPOINT'
            values=[]
            for execution in grouped[order_id]:
                eq=_exact_decimal(execution.get('execQty'),'execQty',positive=True); values.append(eq)
                if 'orderQty' in execution and _exact_decimal(execution['orderQty'],'orderQty')!=qty:
                    return identity_classes,'RECONCILIATION_UNKNOWN_DISAGREEMENT'
                if _exact_time(execution.get('execTime'),'execTime')<created:
                    return identity_classes,'RECONCILIATION_UNKNOWN_DISAGREEMENT'
            # Decimal addition must not inherit a process-global precision that can round the sum.
            with localcontext() as ctx:
                ctx.prec=max([64,len(qty.as_tuple().digits),len(cum.as_tuple().digits)]+[len(v.as_tuple().digits) for v in values])+len(values)+8
                execution_sum=sum(values,Decimal(0))
            if execution_sum>qty or cum>qty or cum!=execution_sum:
                return identity_classes,'RECONCILIATION_UNKNOWN_DISAGREEMENT'
            valid=(status in {'New','Untriggered','Triggered','Rejected'} and execution_sum==0) or (status=='PartiallyFilled' and Decimal(0)<execution_sum<qty) or (status=='Filled' and execution_sum==qty) or (status=='Cancelled' and Decimal(0)<=execution_sum<qty)
            if not valid: return identity_classes,'RECONCILIATION_UNKNOWN_DISAGREEMENT'
    except ValueError:
        return identity_classes,'RECONCILIATION_UNKNOWN_ENDPOINT'
    return identity_classes,None

def _value_counts(values):
    d={}
    for x in values: d[x]=d.get(x,0)+1
    return d
def _counts(rows,key):
    d={}
    for x in rows: d[str(x.get(key,''))]=d.get(str(x.get(key,'')),0)+1
    return d
def _nonzero(x):
    try: return float(x or 0)!=0
    except Exception: return True
def _skew(local,server):
    if abs((local-server).total_seconds())>5: raise R2FreshnessError('server/local clock skew')
def _ms(dt): return int(dt.timestamp()*1000)
def _digest(x): return hashlib.sha256(json.dumps(x,sort_keys=True,separators=(',',':'),ensure_ascii=True,default=str).encode()).hexdigest()

def _history_windows(start_ms:int,end_ms:int):
    if end_ms < start_ms: raise ValueError('history cutoff precedes activation')
    if end_ms == start_ms: return [(start_ms,end_ms)]
    out=[]; cur=start_ms
    while cur < end_ms:
        nxt=min(cur+R2_HISTORY_WINDOW_MS,end_ms); out.append((cur,nxt)); cur=nxt
    return out

def _utc_from_ms(ms:int)->str:
    return utc_z(datetime.fromtimestamp(ms/1000,timezone.utc))

def classify_local_snapshot(path, *, now_utc:datetime, activation_authority_admitted:bool=True)->str:
    """Deterministic pre-network local-byte classification; never repairs or promotes temp bytes."""
    from pathlib import Path
    from .execution_event_ledger import _parse_union_records,ExecutionEventLedgerError,_persistence_temp_path
    p=Path(path)
    tmp=_persistence_temp_path(p)
    if not p.exists(): return 'MISSING_WITH_TEMP' if tmp.exists() else 'MISSING'
    raw=p.read_bytes()
    try:
        if p.stat().st_mode & 0o222 == 0: return 'READ_ONLY_LEDGER'
    except OSError: return 'READ_ONLY_LEDGER'
    if raw==b'': return 'TEMP_RESIDUE' if tmp.exists() else 'EMPTY'
    try: union,legacy,r1,r2=_parse_union_records(raw)
    except ExecutionEventLedgerError as exc:
        m=str(exc).lower()
        if 'sequence gap/conflict' in m:
            try:
                seqs=[json.loads(line).get('sequence') for line in raw.decode('utf-8').splitlines() if line.strip()]
                return 'DUPLICATE_SEQUENCE' if len(seqs)!=len(set(seqs)) else 'SEQUENCE_INVALID'
            except Exception: return 'SEQUENCE_INVALID'
        if 'duplicate json key' in m: return 'UNSUPPORTED_SCHEMA'
        if 'malformed' in m or 'truncated' in m or 'blank' in m: return 'MALFORMED'
        if 'schema' in m or 'field' in m: return 'UNSUPPORTED_SCHEMA'
        return 'UNSUPPORTED_SCHEMA'
    if tmp.exists(): return 'TEMP_RESIDUE'
    if not activation_authority_admitted: return 'IDENTITY_EPOCH_UNKNOWN'
    if r2:
        last=r2[-1].event_type
        if last=='R2_OPERATOR_ARMED': return 'PRIOR_ARMED'
        if last=='R2_DISCOVERY_COMPLETED': return 'UNCOMMITTED_DISCOVERY'
        if last=='R2_DISCOVERY_STARTED': return 'INTERRUPTED_DISCOVERY'
        if last=='R2_RECONCILED_DISARMED': return 'PRIOR_R2_SUCCESS'
        return 'VALID_UNION'
    if r1:
        types=[e.event_type for e in r1]
        if 'IDENTITY_CONFLICT' in types: return 'R1_IDENTITY_CONFLICT'
        if 'CREATE_UNKNOWN' in types: return 'R1_CREATE_UNKNOWN'
        if 'CANCEL_UNKNOWN' in types: return 'R1_CANCEL_UNKNOWN'
        if 'CREATE_DISPATCHING' in types and not any(x in types for x in ('CREATE_ACK_PENDING_QUERY','CREATE_REJECT_CONFIRMED','CREATE_UNKNOWN')): return 'R1_CREATE_DISPATCH_UNKNOWN'
        if 'CANCEL_DISPATCHING' in types and not any(x in types for x in ('CANCEL_ACK_PENDING_QUERY','CANCEL_CONFIRMED','CANCEL_NOT_EFFECTIVE_TERMINAL','CANCEL_NOT_CONFIRMED','CANCEL_UNKNOWN')): return 'R1_CANCEL_DISPATCH_UNKNOWN'
        if 'CREATE_RESERVED' in types and not any(x in types for x in ('CREATE_DISPATCHING','CREATE_RESERVATION_EXPIRED')):
            ev=next(e for e in reversed(r1) if e.event_type=='CREATE_RESERVED'); return 'R1_RESERVED_EXPIRED' if now_utc>=parse_utc(ev.recorded_at_utc)+timedelta(seconds=10) else 'R1_RESERVED_UNDISPATCHED'
        if 'CREATE_REJECT_CONFIRMED' in types: return 'R1_REJECTED'
        if 'CANCEL_CONFIRMED' in types: return 'R1_CANCEL_CONFIRMED'
        if 'CREATE_ACK_PENDING_QUERY' in types:
            classes={str(e.classification).upper() for e in r1}
            if any('PARTIAL' in x for x in classes): return 'R1_ACK_PARTIAL'
            if any('FILLED' in x for x in classes): return 'R1_ACK_FILLED'
            return 'R1_ACK_OPEN'
        return 'R1_MIXED_COMPLETE' if len({e.operation_identity for e in r1 if e.operation_identity})>1 else 'R1_NO_MUTATION'
    if legacy: return 'LEGACY_ONLY'
    return 'VALID_UNION'
