from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping
from .execution_event_ledger import R1AuthorityLedger
from .r1_bybit_transport import _BybitTestnetTransport,_Credentials
from .r1_contracts import OperatorArmRequestV1,R1CancelTarget,R1QueryRequest,R1_TESTNET_ORIGIN,digest_mapping
from .r1_permit_authority import R1AuthorityClock,R1MutationArmIssuer,R1ReconciliationPermitIssuer,read_kill_artifact
from .r1_testnet_coordinator import R1CoordinatorConfig,R1TestnetCoordinator

class R1TestnetProduct:
    __slots__=("__coordinator",)
    def __init__(self, coordinator:R1TestnetCoordinator): self.__coordinator=coordinator
    def describe_disarmed_session(self): return self.__coordinator.describe_disarmed_session()
    def request_testnet_mutation_arm(self,request:OperatorArmRequestV1): return self.__coordinator.request_testnet_mutation_arm(request)
    def disarm(self): return self.__coordinator.disarm()
    def create(self,admitted_intent): return self.__coordinator.create(admitted_intent)
    def query(self,query_request:R1QueryRequest): return self.__coordinator.query(query_request)
    def cancel(self,cancel_target:R1CancelTarget): return self.__coordinator.cancel(cancel_target)
    def close(self): return self.__coordinator.close()

class _ProductionReadAuthority:
    def __init__(self, transport:_BybitTestnetTransport, *, account_identity:str, category:str, clock:R1AuthorityClock): self._t=transport; self._account=account_identity; self._category=category; self._clock=clock
    def instrument_rules(self,symbol):
        from .r1_contracts import R1InstrumentRulesProof,utc_z,digest_mapping
        from datetime import timedelta
        from decimal import Decimal
        raw=self._t.instrument_rules({"category":self._category,"symbol":symbol})
        rows=raw.get("result",{}).get("list",[]) if isinstance(raw,dict) else []
        if len(rows)!=1 or rows[0].get("symbol")!=symbol: raise ValueError("exact TESTNET instrument rules unavailable")
        row=rows[0]; lot=row.get("lotSizeFilter",{}); price=row.get("priceFilter",{}); now=self._clock.now()
        return R1InstrumentRulesProof("TESTNET",R1_TESTNET_ORIGIN,symbol,Decimal(str(lot["qtyStep"])),Decimal(str(lot["minOrderQty"])),Decimal(str(lot["maxOrderQty"])),Decimal(str(price["tickSize"])),Decimal(str(lot.get("minNotionalValue","0"))),Decimal(str(lot.get("maxOrderAmt","999999999"))),utc_z(now),utc_z(now+timedelta(seconds=5)),digest_mapping({"raw":raw,"symbol":symbol}))
    def reconciliation_for_create(self,*,symbol,client_order_id,client_identity_digest):
        from .r1_contracts import utc_z,digest_mapping
        raw=self._t.query({"category":self._category,"symbol":symbol,"orderLinkId":client_order_id})
        positions=self._t.positions({"category":self._category,"symbol":symbol})
        rows=raw.get("result",{}).get("list",[]) if isinstance(raw,dict) else []
        pos=positions.get("result",{}).get("list",[]) if isinstance(positions,dict) else []
        if rows: raise ValueError("CREATE identity already exists or conflicts")
        if any(Decimal(str(x.get("size","0"))) != 0 for x in pos): raise ValueError("CREATE_CLEAR denied by existing position observation")
        now=self._clock.now(); return digest_mapping({"orders":raw,"positions":positions,"symbol":symbol,"client":client_order_id}),utc_z(now)
    def query(self,request):
        from .r1_contracts import R1QueryObservation,utc_z,digest_mapping
        from decimal import Decimal
        params={"category":self._category,"symbol":request.symbol}
        if request.client_order_id: params["orderLinkId"]=request.client_order_id
        if request.exchange_order_id: params["orderId"]=request.exchange_order_id
        raw=self._t.query(params); rows=raw.get("result",{}).get("list",[]) if isinstance(raw,dict) else []
        now=utc_z(self._clock.now())
        if len(rows)!=1: return R1QueryObservation("NOT_FOUND" if not rows else "CONFLICT","TESTNET",R1_TESTNET_ORIGIN,self._account,self._category,request.symbol,request.client_order_id,request.exchange_order_id,"",None,now,digest_mapping({"raw":raw}))
        r=rows[0]; leaves=Decimal(str(r.get("leavesQty","0"))) if r.get("leavesQty") not in (None,"") else None
        return R1QueryObservation("FOUND_CONSISTENT","TESTNET",R1_TESTNET_ORIGIN,self._account,self._category,request.symbol,str(r.get("orderLinkId","")),str(r.get("orderId","")),str(r.get("orderStatus","")),leaves,now,digest_mapping({"raw":raw}))

def open_r1_testnet_product(config_source, credential_source, ledger_path) -> R1TestnetProduct:
    """Only production composition root. Construction performs no network mutation."""
    cfg=dict(config_source.load_r1_testnet_config()); creds=dict(credential_source.load_bybit_testnet_credentials())
    if cfg.get("environment")!="TESTNET" or cfg.get("origin")!=R1_TESTNET_ORIGIN: raise ValueError("exact TESTNET config required")
    account=str(cfg.get("account_identity","")).strip(); api_key=str(creds.get("api_key","")).strip(); api_secret=str(creds.get("api_secret","")).strip()
    if not account or not api_key or not api_secret: raise ValueError("account and TESTNET credentials required")
    clock=R1AuthorityClock(server_utc_at_receipt=str(cfg["server_utc_at_receipt"]),local_utc_at_receipt=str(cfg["local_utc_at_receipt"]))
    configuration_digest=digest_mapping({k:str(v) for k,v in cfg.items() if k not in {"server_utc_at_receipt","local_utc_at_receipt"}})
    kill_path=str(cfg["kill_artifact_path"]); kill_digest,kill_expiry=read_kill_artifact(kill_path,expected_account_identity=account,clock=clock)
    cc=R1CoordinatorConfig(environment="TESTNET",origin=R1_TESTNET_ORIGIN,account_identity=account,category=str(cfg.get("category","linear")),configuration_digest=configuration_digest,session_challenge=str(cfg["session_challenge"]),emergency_max_notional=Decimal(str(cfg["emergency_max_notional"])),kill_artifact_path=kill_path,kill_source_digest=kill_digest,account_proof_expires_at_utc=str(cfg["account_proof_expires_at_utc"]),cap_expires_at_utc=str(cfg["cap_expires_at_utc"]),kill_expires_at_utc=kill_expiry)
    ledger=R1AuthorityLedger(ledger_path); transport=_BybitTestnetTransport(credentials=_Credentials(api_key,api_secret)); read=_ProductionReadAuthority(transport,account_identity=account,category=cc.category,clock=clock)
    ctx={"account_identity":account,"configuration_digest":configuration_digest,"session_challenge":cc.session_challenge}
    coordinator=R1TestnetCoordinator(config=cc,clock=clock,ledger=ledger,read_authority=read,transport=transport,arm_issuer=R1MutationArmIssuer(clock=clock,expected_context=ctx),reconciliation_issuer=R1ReconciliationPermitIssuer(clock=clock,read_authority=read))
    return R1TestnetProduct(coordinator)
