from __future__ import annotations
import hashlib, json, uuid
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal, ROUND_FLOOR
from typing import Any, Mapping, Protocol, Callable
from .decision_consumer import ValidatedExecutionIntent
from .execution_event_ledger import R1AuthorityLedger, PersistenceStateUnknownError
from .r1_contracts import *
from .r1_permit_authority import R1AuthorityClock, R1MutationArmIssuer, R1ReconciliationPermitIssuer, read_kill_artifact

class R1ReadAuthority(Protocol):
    def instrument_rules(self,symbol:str)->R1InstrumentRulesProof: ...
    def reconciliation_for_create(self,*,symbol:str,client_order_id:str,client_identity_digest:str)->tuple[str,str]: ...
    def query(self,request:R1QueryRequest)->R1QueryObservation: ...
class R1MutationTransport(Protocol):
    def create(self,payload:Mapping[str,Any])->dict[str,Any]: ...
    def cancel(self,payload:Mapping[str,Any])->dict[str,Any]: ...

@dataclass(frozen=True)
class R1CoordinatorConfig:
    environment:str; origin:str; account_identity:str; category:str; configuration_digest:str; session_challenge:str
    emergency_max_notional:Decimal; kill_artifact_path:str; kill_source_digest:str; account_proof_expires_at_utc:str; cap_expires_at_utc:str; kill_expires_at_utc:str

class R1TestnetCoordinator:
    def __init__(self,*,config:R1CoordinatorConfig,clock:R1AuthorityClock,ledger:R1AuthorityLedger,read_authority:R1ReadAuthority,transport:R1MutationTransport,arm_issuer:R1MutationArmIssuer,reconciliation_issuer:R1ReconciliationPermitIssuer,startup_gate_validator:Callable[[],bool]):
        exact_testnet_origin(config.origin)
        if config.environment!="TESTNET" or not config.account_identity or not config.configuration_digest or not config.session_challenge: raise ValueError("invalid R1 TESTNET config")
        if not config.emergency_max_notional.is_finite() or config.emergency_max_notional<=0: raise ValueError("positive finite cap required")
        if not config.kill_artifact_path or not config.kill_source_digest: raise ValueError("mandatory kill artifact authority required")
        for expiry in (config.account_proof_expires_at_utc,config.cap_expires_at_utc,config.kill_expires_at_utc):
            if clock.now()>=parse_utc(expiry): raise ValueError("fresh account/cap/kill authority required")
        digest,expiry=read_kill_artifact(config.kill_artifact_path,expected_account_identity=config.account_identity,clock=clock)
        if digest!=config.kill_source_digest or expiry!=config.kill_expires_at_utc: raise ValueError("kill artifact admission identity mismatch")
        self._cfg=config; self._clock=clock; self._ledger=ledger; self._read=read_authority; self._transport=transport; self._arm_issuer=arm_issuer; self._rec_issuer=reconciliation_issuer; self._startup_gate_validator=startup_gate_validator; self._arm=None; self._startup_arm_committed=False; self._closed=False
    def stage_testnet_mutation_arm(self,request:OperatorArmRequestV1):
        if self._ledger.projection().block_mutations: return None,ArmStatus(False,"LEDGER_BLOCKED")
        return self._arm_issuer.issue(request)
    def publish_staged_arm(self,handle)->bool:
        if handle is None or not self._arm_issuer.validate_arm(handle,self._ctx()): return False
        self._arm=handle; self._startup_arm_committed=True; return True
    def invalidate_staged_arm(self): self._arm_issuer.invalidate_all(); self._arm=None; self._startup_arm_committed=False
    def request_testnet_mutation_arm(self,request:OperatorArmRequestV1)->ArmStatus:
        if not self._startup_gate_validator(): return ArmStatus(False,"AT1_AUTHORITY_NOT_ADMITTED")
        handle,status=self.stage_testnet_mutation_arm(request)
        if not status.armed: return status
        if not self.publish_staged_arm(handle): self.invalidate_staged_arm(); return ArmStatus(False,"AT1_AUTHORITY_NOT_ADMITTED")
        return status
    def disarm(self): self._arm_issuer.invalidate_all(); self._rec_issuer.invalidate_all(); self._arm=None; self._startup_arm_committed=False
    def close(self): self.disarm(); self._closed=True; self._ledger.close()
    def describe_disarmed_session(self): return {"environment":"TESTNET","armed":False,"query_available":True}
    def _ctx(self): return {"account_identity":self._cfg.account_identity,"configuration_digest":self._cfg.configuration_digest,"session_challenge":self._cfg.session_challenge}
    def _require_arm(self):
        if not self._startup_arm_committed or not self._startup_gate_validator(): raise ValueError("AT1_AUTHORITY_NOT_ADMITTED")
        if self._closed or self._arm is None or not self._arm_issuer.validate_arm(self._arm,self._ctx()): raise ValueError("mutation DISARMED")
        if any(self._clock.now()>=parse_utc(x) for x in (self._cfg.account_proof_expires_at_utc,self._cfg.cap_expires_at_utc,self._cfg.kill_expires_at_utc)): raise ValueError("account/cap/kill authority expired")
        digest,expiry=read_kill_artifact(self._cfg.kill_artifact_path,expected_account_identity=self._cfg.account_identity,clock=self._clock)
        if digest!=self._cfg.kill_source_digest or expiry!=self._cfg.kill_expires_at_utc: raise ValueError("kill artifact changed since admission")
        if self._ledger.projection().block_mutations: raise ValueError("ledger blocks mutation")
    def _select_durable_create_reservation(self, *, operation_id:str, client_id:str, identity:str)->tuple[CreateReservationV1,Any]:
        candidates=[e for e in self._ledger.events if e.operation_identity==operation_id and e.event_type=="CREATE_RESERVED"]
        if len(candidates)!=1: raise ValueError("unique committed CREATE reservation required")
        event=candidates[0]
        if any(e.operation_identity==operation_id and e.event_type!="CREATE_RESERVED" for e in self._ledger.events): raise ValueError("CREATE reservation conflicts with later durable state")
        if event.environment!="TESTNET" or event.origin!=self._cfg.origin or event.account_identity!=self._cfg.account_identity or event.client_order_id!=client_id or event.client_identity_digest!=identity or event.operation!="CREATE" or not event.reservation_id or not event.attempt_id or not event.request_fingerprint or not event.proof_bundle_digest: raise ValueError("persisted CREATE reservation authority mismatch")
        created=parse_utc(event.recorded_at_utc); expires=created+timedelta(microseconds=CREATE_RESERVATION_TTL_US)
        reservation=CreateReservationV1(event.reservation_id,event.attempt_id,event.operation_identity,event.canonical_setup_key,event.client_order_id,event.client_identity_digest,event.request_fingerprint,event.proof_bundle_digest,event.symbol,event.recorded_at_utc,utc_z(expires))
        return reservation,event
    def _build_create_payload(self, intent:ValidatedExecutionIntent, *, client_id:str, rules:R1InstrumentRulesProof)->tuple[dict[str,Any],Decimal]:
        qty=_floor(intent.authorized_qty,rules.quantity_step); price=_floor(intent.entry,rules.price_tick); notional=qty*price
        if qty<=0 or qty<rules.min_quantity or qty>rules.max_quantity or notional<rules.min_notional or notional>rules.max_notional or notional>self._cfg.emergency_max_notional: raise ValueError("quantity/notional gate")
        return {"category":self._cfg.category,"symbol":intent.symbol,"side":"Buy" if intent.side=="LONG" else "Sell","orderType":"Limit","qty":canonical_decimal(qty),"price":canonical_decimal(price),"timeInForce":"GTC","orderLinkId":client_id,"reduceOnly":False},notional
    def query(self,request:R1QueryRequest)->R1QueryObservation:
        obs=self._read.query(request)
        # QUERY is always read-authority first and never arms mutation. If persistence is healthy,
        # it may conservatively resolve a previously dispatched identity-bound operation.
        matches=[e for e in self._ledger.events if e.client_order_id==obs.client_order_id and (not obs.exchange_order_id or not e.exchange_order_id or e.exchange_order_id==obs.exchange_order_id)]
        if matches and self._ledger.persistence_health=="PERSISTENCE_HEALTHY":
            base=matches[-1]; common={k:getattr(base,k) for k in ("operation_identity","canonical_setup_key","environment","origin","account_identity","client_order_id","client_identity_digest","request_fingerprint","request_fingerprint_version","reservation_id","attempt_id","symbol","operation","exchange_order_id","proof_bundle_digest")}
            if obs.classification=="FOUND_CONSISTENT" and 0 <= (self._clock.now()-parse_utc(obs.observed_at_utc)).total_seconds() < 5 and obs.environment=="TESTNET" and obs.origin==self._cfg.origin and obs.account_identity==self._cfg.account_identity:
                if base.operation=="CREATE" and obs.order_state=="Rejected" and obs.exchange_order_id and obs.client_order_id==base.client_order_id:
                    common["exchange_order_id"]=obs.exchange_order_id; self._append("CREATE_REJECT_CONFIRMED",common,"REJECT_CONFIRMED",False,False,"fresh QUERY observed Rejected")
                elif base.operation=="CANCEL" and obs.order_state in {"Cancelled","PartiallyFilledCanceled"}: self._append("CANCEL_CONFIRMED",common,"CANCEL_CONFIRMED",False,False,"fresh QUERY confirmed cancellation")
                elif base.operation=="CANCEL" and obs.order_state in {"Filled","Rejected","Deactivated"}: self._append("CANCEL_NOT_EFFECTIVE_TERMINAL",common,"CANCEL_NOT_EFFECTIVE_TERMINAL",False,False,"fresh QUERY observed terminal non-cancel state")
                elif base.operation=="CANCEL" and obs.order_state in {"New","PartiallyFilled"}: self._append("CANCEL_NOT_CONFIRMED",common,"CANCEL_NOT_CONFIRMED",False,False,"fresh QUERY observed active order")
        return obs
    def create(self,intent:ValidatedExecutionIntent)->R1OperationResult:
        self._require_arm()
        if not isinstance(intent,ValidatedExecutionIntent): return R1OperationResult("BLOCKED","validated admitted intent required")
        identity=_identity(intent,self._cfg); client_id="AT1_"+identity[:32]; operation_id="CREATE:"+identity
        collision=[e for e in self._ledger.events if e.client_order_id==client_id and e.client_identity_digest and e.client_identity_digest!=identity]
        if collision:
            base=_common(self._cfg,operation_id,intent.canonical_setup_key,client_id,identity,"","","",intent.symbol,"CREATE",""); self._append("IDENTITY_CONFLICT",base,"UNKNOWN",True,True,"AT1 prefix collision with different full digest"); return R1OperationResult("UNKNOWN","client identity collision",client_id,query_required=True)
        if any(e.operation_identity==operation_id and e.event_type in {"CREATE_RESERVED","CREATE_RESERVATION_EXPIRED","CREATE_DISPATCHING","CREATE_ACK_PENDING_QUERY","CREATE_REJECT_CONFIRMED","CREATE_UNKNOWN"} for e in self._ledger.events): return R1OperationResult("BLOCKED","R1 CREATE identity already reserved",client_id)
        rules=self._read.instrument_rules(intent.symbol); created=self._clock.now(); _validate_rules(rules,self._cfg,intent.symbol,created)
        try: payload,_=self._build_create_payload(intent,client_id=client_id,rules=rules)
        except ValueError as exc: return R1OperationResult("BLOCKED",str(exc),client_id)
        fp=sha256_hex(canonical_json_bytes(payload)); proof=digest_mapping({"rules":rules.provenance_digest,"kill":self._cfg.kill_source_digest,"config":self._cfg.configuration_digest})
        rid=str(uuid.uuid4()); aid=str(uuid.uuid4()); common=_common(self._cfg,operation_id,intent.canonical_setup_key,client_id,identity,fp,rid,aid,intent.symbol,"CREATE",proof)
        self._append("CREATE_RESERVED",common,"RESERVED",False,False,"reservation committed",recorded_at=utc_z(created))
        # Authority cut: discard pre-reload request material. The durable CREATE_RESERVED record is re-opened and selected as the source of request identity/fingerprint.
        path=self._ledger._path; self._ledger.close(); self._ledger=R1AuthorityLedger(path)
        try: reservation,reserved_event=self._select_durable_create_reservation(operation_id=operation_id,client_id=client_id,identity=identity)
        except ValueError as exc: return R1OperationResult("BLOCKED",str(exc),client_id)
        if self._clock.now()>=parse_utc(reservation.expires_at_utc):
            durable_common=_common_from_event(reserved_event); self._append("CREATE_RESERVATION_EXPIRED",durable_common,"EXPIRED",False,False,"reservation expired before dispatch"); return R1OperationResult("BLOCKED","reservation expired",client_id)
        rec_ctx={"operation_identity":reservation.operation_identity,"attempt_id":reservation.attempt_id,"symbol":reservation.symbol,"client_order_id":reservation.client_order_id,"client_identity_digest":reservation.client_identity_digest,"account_identity":self._cfg.account_identity,"environment":"TESTNET"}
        try: permit=self._rec_issuer.issue_for_create(expected_context=rec_ctx,symbol=reservation.symbol,client_order_id=reservation.client_order_id,client_identity_digest=reservation.client_identity_digest)
        except ValueError as exc: return R1OperationResult("BLOCKED",str(exc),client_id)
        # Fresh rules + deterministic rebuild after reload. Pre-reload payload is not reused.
        dispatch_rules=self._read.instrument_rules(reservation.symbol); _validate_rules(dispatch_rules,self._cfg,reservation.symbol,self._clock.now())
        if intent.canonical_setup_key!=reservation.canonical_setup_key or intent.symbol!=reservation.symbol: return R1OperationResult("BLOCKED","intent/reservation binding mismatch",client_id)
        try: rebuilt,_=self._build_create_payload(intent,client_id=reservation.client_order_id,rules=dispatch_rules)
        except ValueError as exc: return R1OperationResult("BLOCKED",str(exc),client_id)
        actual_fp=sha256_hex(canonical_json_bytes(rebuilt))
        if actual_fp!=reservation.request_fingerprint:
            return R1OperationResult("BLOCKED","pre-dispatch durable reservation fingerprint invalid",client_id)
        if self._clock.now()>=parse_utc(reservation.expires_at_utc):
            durable_common=_common_from_event(reserved_event)
            try:
                self._append("CREATE_RESERVATION_EXPIRED",durable_common,"EXPIRED",False,False,"reservation expired at final pre-dispatch freshness gate")
            except PersistenceStateUnknownError:
                return R1OperationResult("PERSISTENCE_UNKNOWN","reservation expiry terminalization persistence unknown",client_id,query_required=True)
            return R1OperationResult("BLOCKED","reservation expired",client_id)
        # Final common-gate revalidation is immediately before one-use permit consume and durable dispatch commit.
        try: self._require_arm()
        except ValueError as exc: return R1OperationResult("BLOCKED",str(exc),client_id)
        if not self._rec_issuer.consume_reconciliation(permit,operation="CREATE",expected_context=rec_ctx): return R1OperationResult("BLOCKED","reconciliation capability invalid",client_id)
        durable_common=_common_from_event(reserved_event)
        self._append("CREATE_DISPATCHING",durable_common,"DISPATCHING",True,True,"attempt consumed before network")
        try: response=self._transport.create(rebuilt)
        except Exception as exc:
            self._append("CREATE_UNKNOWN",durable_common,"UNKNOWN",True,True,type(exc).__name__); return R1OperationResult("UNKNOWN",type(exc).__name__,client_id,query_required=True)
        cls,order_id,ret,retmsg=_normalize_response(response,client_id)
        if cls=="ACK_PENDING_QUERY": self._append("CREATE_ACK_PENDING_QUERY",{**durable_common,"exchange_order_id":order_id},cls,False,True,"query required",ret,retmsg); return R1OperationResult(cls,"query required",client_id,order_id,True)
        self._append("CREATE_UNKNOWN",durable_common,"UNKNOWN",True,True,"direct response cannot prove rejection",ret,retmsg); return R1OperationResult("UNKNOWN","query required",client_id,order_id,True)
    def cancel(self,target:R1CancelTarget)->R1OperationResult:
        self._require_arm()
        opid="CANCEL:"+target.client_order_id+":"+target.exchange_order_id; aid=str(uuid.uuid4())
        base_ctx={"operation_identity":opid,"attempt_id":aid,"symbol":target.symbol,"client_order_id":target.client_order_id,"exchange_order_id":target.exchange_order_id,"account_identity":self._cfg.account_identity,"environment":"TESTNET","origin":self._cfg.origin,"category":self._cfg.category}
        try: permit,obs,rec_ctx=self._rec_issuer.issue_for_cancel(expected_context=base_ctx,request=R1QueryRequest(target.symbol,target.client_order_id,target.exchange_order_id))
        except ValueError as exc: return R1OperationResult("BLOCKED",str(exc),target.client_order_id,target.exchange_order_id)
        if obs.origin!=self._cfg.origin or obs.category!=self._cfg.category: return R1OperationResult("BLOCKED","cancel target domain/category mismatch")
        proof=obs.observation_digest
        # Final arm/account/cap/physical-kill revalidation after the fresh QUERY and immediately before consume/dispatch.
        try: self._require_arm()
        except ValueError as exc: return R1OperationResult("BLOCKED",str(exc),target.client_order_id,target.exchange_order_id)
        if not self._rec_issuer.consume_reconciliation(permit,operation="CANCEL",expected_context=rec_ctx): return R1OperationResult("BLOCKED","cancel permit invalid")
        common=_common(self._cfg,opid,target.canonical_setup_key,target.client_order_id,"", "", "",aid,target.symbol,"CANCEL",proof); common["exchange_order_id"]=target.exchange_order_id
        self._append("CANCEL_DISPATCHING",common,"DISPATCHING",True,True,"cancel attempt consumed before network")
        payload={"category":self._cfg.category,"symbol":target.symbol,"orderId":target.exchange_order_id,"orderLinkId":target.client_order_id}
        try: response=self._transport.cancel(payload)
        except Exception as exc: self._append("CANCEL_UNKNOWN",common,"UNKNOWN",True,True,type(exc).__name__); return R1OperationResult("UNKNOWN",type(exc).__name__,target.client_order_id,target.exchange_order_id,True)
        cls,oid,ret,msg=_normalize_response(response,target.client_order_id,expected_order_id=target.exchange_order_id)
        if cls=="ACK_PENDING_QUERY": self._append("CANCEL_ACK_PENDING_QUERY",common,"ACK_PENDING_QUERY",True,True,"query required",ret,msg); return R1OperationResult("ACK_PENDING_QUERY","query required",target.client_order_id,target.exchange_order_id,True)
        self._append("CANCEL_UNKNOWN",common,"UNKNOWN",True,True,"direct response cannot prove cancel",ret,msg); return R1OperationResult("UNKNOWN","query required",target.client_order_id,target.exchange_order_id,True)
    def _append(self,event,common,classification,block,query,reason,ret="",msg="",recorded_at=None): self._ledger.append(**{**common,"event_type":event,"recorded_at_utc":recorded_at or self._clock.now_utc(),"classification":classification,"block_mutations":block,"query_required":query,"exchange_ret_code":str(ret),"exchange_ret_message":str(msg)[:256],"reason":reason})

def _identity(intent,cfg): return hashlib.sha256(("ATS_R1_IDENTITY_V1\0"+cfg.account_identity+"\0"+cfg.origin+"\0"+intent.canonical_setup_key).encode()).hexdigest()
def _floor(v,step): return (v/step).to_integral_value(rounding=ROUND_FLOOR)*step
def _validate_rules(r,cfg,symbol,now):
    if r.environment!="TESTNET" or r.origin!=cfg.origin or r.symbol!=symbol or now>=parse_utc(r.expires_at_utc): raise ValueError("instrument rules invalid/stale")
    for v in (r.quantity_step,r.min_quantity,r.max_quantity,r.price_tick,r.min_notional,r.max_notional):
        if not v.is_finite() or v<=0: raise ValueError("instrument rules must be positive finite")
def _common(cfg,opid,key,cid,cid_digest,fp,rid,aid,symbol,operation,proof): return {"operation_identity":opid,"canonical_setup_key":key,"environment":"TESTNET","origin":cfg.origin,"account_identity":cfg.account_identity,"client_order_id":cid,"client_identity_digest":cid_digest,"request_fingerprint":fp,"request_fingerprint_version":"R1_SHA256_CANONICAL_JSON_V1","reservation_id":rid,"attempt_id":aid,"symbol":symbol,"operation":operation,"exchange_order_id":"","proof_bundle_digest":proof}

def _common_from_event(e):
    return {k:getattr(e,k) for k in ("operation_identity","canonical_setup_key","environment","origin","account_identity","client_order_id","client_identity_digest","request_fingerprint","request_fingerprint_version","reservation_id","attempt_id","symbol","operation","exchange_order_id","proof_bundle_digest")}
def _normalize_response(response,expected_client,expected_order_id=""):
    if not isinstance(response,dict) or set(response)!={"retCode","retMsg","result","retExtInfo","time"} or type(response.get("retCode")) is not int or not isinstance(response.get("retMsg"),str) or not isinstance(response.get("result"),dict) or not isinstance(response.get("retExtInfo"),dict) or type(response.get("time")) is not int: return "UNKNOWN","","MALFORMED","malformed"
    result=response["result"]
    if set(result)!={"orderId","orderLinkId"} or not all(isinstance(result.get(k),str) for k in ("orderId","orderLinkId")): return "UNKNOWN","",response["retCode"],response["retMsg"]
    oid=result["orderId"]
    if response["retCode"]==0 and oid and result["orderLinkId"]==expected_client and (not expected_order_id or oid==expected_order_id): return "ACK_PENDING_QUERY",oid,0,response["retMsg"]
    return "UNKNOWN",oid,response["retCode"],response["retMsg"]
