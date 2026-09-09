from __future__ import annotations
from datetime import datetime,timezone,timedelta
from decimal import Decimal
from pathlib import Path
from backtest.execution.r1_contracts import *
from backtest.execution.r1_permit_authority import *
from backtest.execution.r1_testnet_coordinator import *
from backtest.execution.r1_testnet_product import R1TestnetProduct
from backtest.execution.r2_startup_authority import R2CommittedArmGate,R2StartupEligibilityIssuer
from backtest.execution.r2_startup_reconciliation import R2StartupResult
from backtest.execution.execution_event_ledger import R1AuthorityLedger

class FakeMono:
    def __init__(self): self.v=100.0
    def __call__(self): return self.v
    def advance(self,s): self.v+=s
class FakeTransport:
    def __init__(self): self.create_calls=[]; self.cancel_calls=[]; self.create_response={"retCode":0,"retMsg":"OK","result":{"orderId":"OID1","orderLinkId":""},"retExtInfo":{},"time":1}; self.cancel_response={"retCode":0,"retMsg":"OK","result":{"orderId":"OID1","orderLinkId":""},"retExtInfo":{},"time":1}
    def create(self,payload): self.create_calls.append(dict(payload)); r={**self.create_response,"result":dict(self.create_response["result"])}; r["result"]["orderLinkId"]=payload["orderLinkId"]; return r
    def cancel(self,payload): self.cancel_calls.append(dict(payload)); r={**self.cancel_response,"result":dict(self.cancel_response["result"])}; r["result"]["orderLinkId"]=payload["orderLinkId"]; r["result"]["orderId"]=payload["orderId"]; return r
class FakeRead:
    def __init__(self,clock): self.clock=clock; self.state="New"; self.client=""; self.oid="OID1"; self.advance_on_reconcile=0; self.on_reconcile=None; self.on_query=None; self.rules_variant=0
    def instrument_rules(self,symbol):
        now=self.clock.now(); tick=Decimal("0.1") if self.rules_variant==0 else Decimal("7")
        return R1InstrumentRulesProof("TESTNET",R1_TESTNET_ORIGIN,symbol,Decimal("0.001"),Decimal("0.001"),Decimal("100"),tick,Decimal("1"),Decimal("1000000"),utc_z(now),utc_z(now+timedelta(seconds=30)),"rules"+str(self.rules_variant))
    def reconciliation_for_create(self,*,symbol,client_order_id,client_identity_digest):
        self.client=client_order_id; self.clock._mono.v += self.advance_on_reconcile
        if self.on_reconcile: self.on_reconcile()
        observed=self.clock.now_utc(); return "obs",observed
    def query(self,request):
        if self.on_query: self.on_query()
        cid=request.client_order_id or self.client; oid=request.exchange_order_id or self.oid
        return R1QueryObservation("FOUND_CONSISTENT","TESTNET",R1_TESTNET_ORIGIN,"acct","linear",request.symbol,cid,oid,self.state,Decimal("1"),self.clock.now_utc(),"q")
def make_product(path):
    mono=FakeMono(); base=datetime(2026,9,8,18,0,tzinfo=timezone.utc); clock=R1AuthorityClock(server_utc_at_receipt=utc_z(base),local_utc_at_receipt=utc_z(base),monotonic_at_receipt=mono(),monotonic=mono)
    future=utc_z(clock.now()+timedelta(hours=1)); kill_path=Path(path).with_suffix('.kill.json')
    kill_path.write_text(__import__('json').dumps({"schema_version":"ATS_TESTNET_KILL_V1","environment":"TESTNET","account_identity":"acct","state":"NOT_TRIGGERED","issued_at_utc":clock.now_utc(),"expires_at_utc":future},sort_keys=True),encoding='utf-8')
    kill_digest=sha256_hex(kill_path.read_bytes())
    cfg=R1CoordinatorConfig("TESTNET",R1_TESTNET_ORIGIN,"acct","linear","cfg","challenge",Decimal("100000"),str(kill_path),kill_digest,future,future,future); ledger=R1AuthorityLedger(path); read=FakeRead(clock); transport=FakeTransport(); ctx={"account_identity":"acct","configuration_digest":"cfg","session_challenge":"challenge"}
    gate=R2CommittedArmGate(); issuer=R2StartupEligibilityIssuer(); c=R1TestnetCoordinator(config=cfg,clock=clock,ledger=ledger,read_authority=read,transport=transport,arm_issuer=R1MutationArmIssuer(clock=clock,expected_context=ctx),reconciliation_issuer=R1ReconciliationPermitIssuer(clock=clock,read_authority=read),startup_gate_validator=gate.is_committed)
    # Predecessor R1 behavior fixtures now carry the minimum admitted R2 causal predecessor required by Rev2.1/2.2.
    seq,dig=ledger.physical_tip(); epoch="fixture_epoch"
    ledger.append_r2(startup_epoch_id=epoch,event_type='R2_ACTIVATION_CONSUMPTION_STARTED',recorded_at_utc=clock.now_utc(),origin=R1_TESTNET_ORIGIN,account_uid=107087555,parent_uid=0,credential_fingerprint='c'*64,category='linear',settle_coin='USDT',configuration_digest='cfg',local_snapshot_digest=None,local_classification=None,discovery_bundle_digest=None,discovery_cutoff_utc=None,reconciliation_classification=None,reconstructed_state_digest=None,block_mutations=True,reason_code='ACTIVATION_CONSUMED_BEFORE_DISCOVERY',proof_manifest=None,proof_manifest_digest=None,disposition_manifest=None,disposition_manifest_digest=None,process_session_identity_digest='a'*64,local_pre_tip_sequence=seq,local_pre_tip_digest=dig,activation_entry_digest='b'*64,registry_root_digest='c'*64,chief_activation_authority_reference='CHIEF_FIXTURE',r3_execution_authority_reference='R3_FIXTURE')
    seq,dig=ledger.physical_tip(); proof="p"*64; disp="d"*64; r2ctx={"startup_epoch_id":epoch,"proof_manifest_digest":proof,"disposition_manifest_digest":disp,"configuration_digest":"cfg","origin":R1_TESTNET_ORIGIN,"account_uid":107087555,"ledger_tip_sequence":seq,"ledger_tip_digest":dig}; elig=issuer.issue(startup_epoch_id=epoch,context=r2ctx); result=R2StartupResult("RECONCILED_DISARMED","RECONCILED_CLEAN",epoch,elig,proof,disp)
    p=R1TestnetProduct(c,eligibility_issuer=issuer,committed_gate=gate,ledger=ledger,configuration_digest="cfg",_initial_r2_result=result); p.request_testnet_mutation_arm(OperatorArmRequestV1("TESTNET","I ACK TESTNET","challenge","acct","cfg")); return p,c,read,transport,mono
