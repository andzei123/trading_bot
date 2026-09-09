from __future__ import annotations
import hashlib,json,threading
from decimal import Decimal
from typing import Mapping
from .execution_event_ledger import R1AuthorityLedger
from .r1_bybit_transport import _BybitTestnetTransport,_Credentials,_R2ReadOnlyTransportView
from .r1_contracts import OperatorArmRequestV1,R1CancelTarget,R1QueryRequest,R1_TESTNET_ORIGIN,digest_mapping,ArmStatus
from .r1_permit_authority import R1AuthorityClock,R1MutationArmIssuer,R1ReconciliationPermitIssuer,read_kill_artifact
from .r1_testnet_coordinator import R1CoordinatorConfig,R1TestnetCoordinator
from .r2_identity_registry import PRODUCTION_AT1_AUTHORITY,StagedNoAT1AuthorityV1
from .r2_startup_authority import R2CommittedArmGate,R2StartupEligibilityIssuer
from .r2_startup_reconciliation import R2StartupResult,BOOT_DISARMED,AT1_AUTHORITY_NOT_ADMITTED

class R1TestnetProduct:
    __slots__=("__coordinator","__r2","__eligibility_issuer","__r2_result","__arm_commit_gate","__committed_gate","__ledger","__configuration_digest","__r2_read_view")
    def __init__(self, coordinator:R1TestnetCoordinator, *, r2_reconciler=None, eligibility_issuer=None, committed_gate=None, ledger=None, configuration_digest="", _initial_r2_result=None, _r2_read_view=None):
        self.__coordinator=coordinator; self.__r2=r2_reconciler; self.__eligibility_issuer=eligibility_issuer; self.__r2_result=_initial_r2_result; self.__arm_commit_gate=threading.Lock(); self.__committed_gate=committed_gate; self.__ledger=ledger; self.__configuration_digest=configuration_digest; self.__r2_read_view=_r2_read_view
    def describe_disarmed_session(self):
        d=self.__coordinator.describe_disarmed_session(); d["startup_state"]=self.startup_reconciliation_status().startup_state; d["startup_classification"]=self.startup_reconciliation_status().classification; return d
    def startup_reconciliation_status(self):
        if self.__r2_result is not None: return self.__r2_result
        return R2StartupResult(BOOT_DISARMED,AT1_AUTHORITY_NOT_ADMITTED,"")
    def run_startup_reconciliation(self):
        if self.__r2 is None: return self.startup_reconciliation_status()
        with self.__arm_commit_gate:
            self.__coordinator.disarm(); self.__committed_gate.clear(); self.__r2_result=self.__r2.reconcile(); return self.__r2_result
    def request_testnet_mutation_arm(self,request:OperatorArmRequestV1):
        with self.__arm_commit_gate:
            r=self.__r2_result
            if r is None or r.classification!="RECONCILED_CLEAN" or r.eligibility is None or self.__eligibility_issuer is None or self.__ledger is None: return ArmStatus(False,AT1_AUTHORITY_NOT_ADMITTED)
            pre_seq,pre_digest=self.__ledger.physical_tip()
            ctx={'startup_epoch_id':r.startup_epoch_id,'proof_manifest_digest':r.proof_manifest_digest,'disposition_manifest_digest':r.disposition_manifest_digest,'configuration_digest':self.__configuration_digest,'origin':R1_TESTNET_ORIGIN,'account_uid':107087555,'ledger_tip_sequence':pre_seq,'ledger_tip_digest':pre_digest}
            if not self.__eligibility_issuer.consume(r.eligibility,startup_epoch_id=r.startup_epoch_id,context=ctx): self.__r2_result=None; return ArmStatus(False,"R2_ELIGIBILITY_INVALID")
            staged,status=self.__coordinator.stage_testnet_mutation_arm(request)
            if not status.armed: self.__coordinator.invalidate_staged_arm(); self.__r2_result=None; return status
            try:
                ev=self.__ledger.append_r2(startup_epoch_id=r.startup_epoch_id,event_type='R2_OPERATOR_ARMED',recorded_at_utc=self.__coordinator._clock.now_utc(),origin=R1_TESTNET_ORIGIN,account_uid=107087555,parent_uid=0,credential_fingerprint='process-bound',category='linear',settle_coin='USDT',configuration_digest=self.__configuration_digest,local_snapshot_digest=None,local_classification=None,discovery_bundle_digest=r.proof_manifest_digest,discovery_cutoff_utc=None,reconciliation_classification='RECONCILED_CLEAN',reconstructed_state_digest=r.disposition_manifest_digest,block_mutations=False,reason_code='OPERATOR_ARMED',proof_manifest=None,proof_manifest_digest=None,disposition_manifest=None,disposition_manifest_digest=None,process_session_identity_digest=None,local_pre_tip_sequence=None,local_pre_tip_digest=None,activation_entry_digest=None,registry_root_digest=None,chief_activation_authority_reference=None,r3_execution_authority_reference=None)
                rr=self.__ledger.exact_reread()[3]
                match=[x for x in rr if x.sequence==ev.sequence and x.event_type=='R2_OPERATOR_ARMED' and x.startup_epoch_id==r.startup_epoch_id]
                if len(match)!=1 or self.__ledger.physical_tip()[0]!=ev.sequence: raise ValueError('R2 arm exact reread/tip mismatch')
                ed=hashlib.sha256(json.dumps(match[0].__dict__ if hasattr(match[0],'__dict__') else {k:getattr(match[0],k) for k in match[0].__dataclass_fields__},sort_keys=True,default=str).encode()).hexdigest()
                if not self.__coordinator.publish_staged_arm(staged): raise ValueError('R1 staged arm invalid')
                self.__committed_gate.publish(event_sequence=ev.sequence,event_digest=ed,startup_epoch_id=r.startup_epoch_id)
                return status
            except Exception:
                self.__coordinator.invalidate_staged_arm(); self.__committed_gate.clear(); self.__r2_result=None; return ArmStatus(False,"R2_ARM_COMMIT_FAILED")
    def disarm(self):
        with self.__arm_commit_gate: self.__committed_gate.clear(); return self.__coordinator.disarm()
    def create(self,admitted_intent):
        with self.__arm_commit_gate: return self.__coordinator.create(admitted_intent)
    def query(self,query_request:R1QueryRequest): return self.__coordinator.query(query_request)
    def cancel(self,cancel_target:R1CancelTarget):
        with self.__arm_commit_gate: return self.__coordinator.cancel(cancel_target)
    def close(self):
        with self.__arm_commit_gate: self.__committed_gate.clear(); return self.__coordinator.close()

class _ProductionReadAuthority:
    def __init__(self, transport:_BybitTestnetTransport, *, account_identity:str, category:str, clock:R1AuthorityClock): self._t=transport; self._account=account_identity; self._category=category; self._clock=clock
    def instrument_rules(self,symbol):
        from .r1_contracts import R1InstrumentRulesProof,utc_z,digest_mapping
        from datetime import timedelta
        raw=self._t.instrument_rules({"category":self._category,"symbol":symbol}); rows=raw.get("result",{}).get("list",[]) if isinstance(raw,dict) else []
        if len(rows)!=1 or rows[0].get("symbol")!=symbol: raise ValueError("exact TESTNET instrument rules unavailable")
        row=rows[0]; lot=row.get("lotSizeFilter",{}); price=row.get("priceFilter",{}); now=self._clock.now()
        return R1InstrumentRulesProof("TESTNET",R1_TESTNET_ORIGIN,symbol,Decimal(str(lot["qtyStep"])),Decimal(str(lot["minOrderQty"])),Decimal(str(lot["maxOrderQty"])),Decimal(str(price["tickSize"])),Decimal(str(lot.get("minNotionalValue","0"))),Decimal(str(lot.get("maxOrderAmt","999999999"))),utc_z(now),utc_z(now+timedelta(seconds=5)),digest_mapping({"raw":raw,"symbol":symbol}))
    def reconciliation_for_create(self,*,symbol,client_order_id,client_identity_digest):
        from .r1_contracts import utc_z,digest_mapping
        raw=self._t.query({"category":self._category,"symbol":symbol,"orderLinkId":client_order_id}); positions=self._t.positions({"category":self._category,"symbol":symbol}); rows=raw.get("result",{}).get("list",[]) if isinstance(raw,dict) else []; pos=positions.get("result",{}).get("list",[]) if isinstance(positions,dict) else []
        if rows: raise ValueError("CREATE identity already exists or conflicts")
        if any(Decimal(str(x.get("size","0"))) != 0 for x in pos): raise ValueError("CREATE_CLEAR denied by existing position observation")
        return digest_mapping({"orders":raw,"positions":positions,"symbol":symbol,"client":client_order_id}),self._clock.now_utc()
    def query(self,request):
        from .r1_contracts import R1QueryObservation,digest_mapping
        params={"category":self._category,"symbol":request.symbol}
        if request.client_order_id: params["orderLinkId"]=request.client_order_id
        if request.exchange_order_id: params["orderId"]=request.exchange_order_id
        raw=self._t.query(params); rows=raw.get("result",{}).get("list",[]) if isinstance(raw,dict) else []; now=self._clock.now_utc()
        if len(rows)!=1: return R1QueryObservation("NOT_FOUND" if not rows else "CONFLICT","TESTNET",R1_TESTNET_ORIGIN,self._account,self._category,request.symbol,request.client_order_id,request.exchange_order_id,"",None,now,digest_mapping({"raw":raw}))
        r=rows[0]; leaves=Decimal(str(r.get("leavesQty","0"))) if r.get("leavesQty") not in (None,"") else None
        return R1QueryObservation("FOUND_CONSISTENT","TESTNET",R1_TESTNET_ORIGIN,self._account,self._category,request.symbol,str(r.get("orderLinkId","")),str(r.get("orderId","")),str(r.get("orderStatus","")),leaves,now,digest_mapping({"raw":raw}))

def open_r1_testnet_product(config_source, credential_source, ledger_path) -> R1TestnetProduct:
    """Sole production composition root. Phase A binds structurally absent AT1 authority and performs no startup discovery."""
    if not isinstance(PRODUCTION_AT1_AUTHORITY,StagedNoAT1AuthorityV1): raise ValueError('active production AT1 authority forbidden in Phase A')
    cfg=dict(config_source.load_r1_testnet_config()); creds=dict(credential_source.load_bybit_testnet_credentials())
    if cfg.get("environment")!="TESTNET" or cfg.get("origin")!=R1_TESTNET_ORIGIN: raise ValueError("exact EU TESTNET config required")
    if any(k in cfg for k in ('activation_utc','entry_digest','registry_root','registry_root_digest','at1_authority')): raise ValueError('production activation override forbidden')
    account=str(cfg.get("account_identity","")).strip(); api_key=str(creds.get("api_key","")).strip(); api_secret=str(creds.get("api_secret","")).strip()
    if account!="107087555" or not api_key or not api_secret: raise ValueError("exact admitted TESTNET account and credentials required")
    clock=R1AuthorityClock(server_utc_at_receipt=str(cfg["server_utc_at_receipt"]),local_utc_at_receipt=str(cfg["local_utc_at_receipt"]))
    configuration_digest=digest_mapping({k:str(v) for k,v in cfg.items() if k not in {"server_utc_at_receipt","local_utc_at_receipt"}})
    kill_path=str(cfg["kill_artifact_path"]); kill_digest,kill_expiry=read_kill_artifact(kill_path,expected_account_identity=account,clock=clock)
    cc=R1CoordinatorConfig("TESTNET",R1_TESTNET_ORIGIN,account,str(cfg.get("category","linear")),configuration_digest,str(cfg["session_challenge"]),Decimal(str(cfg["emergency_max_notional"])),kill_path,kill_digest,str(cfg["account_proof_expires_at_utc"]),str(cfg["cap_expires_at_utc"]),kill_expiry)
    ledger=R1AuthorityLedger(ledger_path); transport=_BybitTestnetTransport(credentials=_Credentials(api_key,api_secret)); r2_read=_R2ReadOnlyTransportView(transport); read=_ProductionReadAuthority(transport,account_identity=account,category=cc.category,clock=clock); committed=R2CommittedArmGate()
    ctx={"account_identity":account,"configuration_digest":configuration_digest,"session_challenge":cc.session_challenge}
    coordinator=R1TestnetCoordinator(config=cc,clock=clock,ledger=ledger,read_authority=read,transport=transport,arm_issuer=R1MutationArmIssuer(clock=clock,expected_context=ctx),reconciliation_issuer=R1ReconciliationPermitIssuer(clock=clock,read_authority=read),startup_gate_validator=committed.is_committed)
    # No R2 reconciler/issuer is constructed in production Phase A because active AT1 authority is absent.
    return R1TestnetProduct(coordinator,r2_reconciler=None,eligibility_issuer=None,committed_gate=committed,ledger=ledger,configuration_digest=configuration_digest,_r2_read_view=r2_read)
