import json
import pytest
from backtest.execution.execution_event_ledger import _parse_union_records,ExecutionEventLedgerError,R1_V1_FIELDS,R1_V1_SCHEMA_VERSION

def v0(seq=1): return {"sequence":seq,"canonical_setup_key":"k","event_type":"INTENT_ACCEPTED","recorded_at_utc":"2026-09-08T18:00:00+00:00","symbol":"BTCUSDT","side":"LONG","client_order_id":"","status":"","block_new_orders":False,"requires_manual_review":False,"reason":""}
def v1(seq=2):
    d={k:"" for k in R1_V1_FIELDS}; d.update(schema_version=R1_V1_SCHEMA_VERSION,sequence=seq,operation_identity="o",canonical_setup_key="k",event_type="AUTHORITY_BLOCKED",recorded_at_utc="2026-09-08T18:00:00Z",environment="TESTNET",origin="https://api-testnet.bybit.eu",account_identity="a",client_order_id="c",client_identity_digest="d",request_fingerprint="f",request_fingerprint_version="v",reservation_id="r",attempt_id="a",symbol="BTCUSDT",operation="CREATE",exchange_order_id="",proof_bundle_digest="p",classification="BLOCKED",block_mutations=False,query_required=False,exchange_ret_code="",exchange_ret_message="",reason="x"); return d
def test_mixed_global_sequence():
    raw=(json.dumps(v0())+'\n'+json.dumps(v1())+'\n').encode(); u,l,r,r2=_parse_union_records(raw); assert len(u)==2 and len(l)==1 and len(r)==1 and len(r2)==0
def test_duplicate_json_key_rejected():
    raw=b'{"sequence":1,"sequence":1}\n'
    with pytest.raises(ExecutionEventLedgerError): _parse_union_records(raw)
def test_extra_field_rejected():
    d=v0(); d['extra']=1
    with pytest.raises(ExecutionEventLedgerError): _parse_union_records((json.dumps(d)+'\n').encode())
def test_unknown_v1_version_rejected():
    d=v1(1); d['schema_version']='X'
    with pytest.raises(ExecutionEventLedgerError): _parse_union_records((json.dumps(d)+'\n').encode())
