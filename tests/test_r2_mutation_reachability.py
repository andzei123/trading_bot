import ast,inspect,json
from datetime import datetime,timezone,timedelta
from decimal import Decimal
from pathlib import Path
import pytest
import socket as _socket

def _r2_phase_a_external_network_denied(*args,**kwargs):
    raise AssertionError("external network access forbidden in deterministic R2 Phase-A suite")

# Session-wide fail-fast guard: collection imports this authorized module before test execution.
_socket.create_connection=_r2_phase_a_external_network_denied
import backtest.execution.r2_identity_registry as registry
from backtest.execution.r1_contracts import R1_TESTNET_ORIGIN,OperatorArmRequestV1
from backtest.execution.r1_testnet_product import open_r1_testnet_product,R1TestnetProduct
from backtest.execution.r1_bybit_transport import _BybitTestnetTransport,_Credentials,R2_FIXED_GET_PATHS

class Cfg:
    def __init__(self,p): self.p=p
    def load_r1_testnet_config(self):
        now=datetime.now(timezone.utc); f=(now+timedelta(hours=1)).isoformat().replace('+00:00','Z')
        return {'environment':'TESTNET','origin':R1_TESTNET_ORIGIN,'account_identity':'107087555','category':'linear','server_utc_at_receipt':now.isoformat().replace('+00:00','Z'),'local_utc_at_receipt':now.isoformat().replace('+00:00','Z'),'session_challenge':'x','emergency_max_notional':'1000','kill_artifact_path':str(self.p),'account_proof_expires_at_utc':f,'cap_expires_at_utc':f}
class Cred:
    def load_bybit_testnet_credentials(self): return {'api_key':'fixture-key','api_secret':'fixture-secret'}

def kill(p):
    now=datetime.now(timezone.utc); p.write_text(json.dumps({'schema_version':'ATS_TESTNET_KILL_V1','environment':'TESTNET','account_identity':'107087555','state':'NOT_TRIGGERED','issued_at_utc':now.isoformat().replace('+00:00','Z'),'expires_at_utc':(now+timedelta(hours=1)).isoformat().replace('+00:00','Z')},sort_keys=True))

def test_production_staged_graph_cannot_arm_and_exposes_no_active_genesis(tmp_path):
    kp=tmp_path/'k.json'; kill(kp); p=open_r1_testnet_product(Cfg(kp),Cred(),tmp_path/'l.jsonl')
    assert p.startup_reconciliation_status().classification=='AT1_AUTHORITY_NOT_ADMITTED'; assert p.run_startup_reconciliation().classification=='AT1_AUTHORITY_NOT_ADMITTED'
    assert not p.request_testnet_mutation_arm(OperatorArmRequestV1('TESTNET','ACK','x','acct','bad')).armed
    for n in ('PRODUCTION_AT1_GENESIS_ENTRY','PRODUCTION_AT1_CANONICAL_ENTRY_BYTES','EXPECTED_AT1_REGISTRY_ROOT_SHA256'): assert not hasattr(registry,n)
    p.close()

def test_exact_eu_origin_and_no_com_authority():
    assert R1_TESTNET_ORIGIN=='https://api-testnet.bybit.eu'; assert all(x.startswith('/v5/') for x in R2_FIXED_GET_PATHS)
    src=Path('backtest/execution/r1_bybit_transport.py').read_text(); assert 'api-testnet.bybit.com' not in src

def test_r2_modules_have_no_mutation_endpoint_literals_or_post_calls():
    for f in ('backtest/execution/r2_identity_registry.py','backtest/execution/r2_startup_authority.py','backtest/execution/r2_startup_reconciliation.py'):
        s=Path(f).read_text(); assert ('/v5/order/'+'create') not in s and ('/v5/order/'+'cancel') not in s and 'urlopen' not in s

def test_production_factory_signature_has_no_activation_override():
    sig=inspect.signature(open_r1_testnet_product); assert tuple(sig.parameters)==('config_source','credential_source','ledger_path')

def test_network_deny_guard_staged_production_performs_zero_io(tmp_path,monkeypatch):
    import urllib.request,socket
    def denied(*a,**k): raise AssertionError('external network access forbidden in deterministic R2 Phase-A proof')
    monkeypatch.setattr(urllib.request,'urlopen',denied)
    monkeypatch.setattr(socket,'create_connection',denied)
    kp=tmp_path/'k.json'; kill(kp); p=open_r1_testnet_product(Cfg(kp),Cred(),tmp_path/'l.jsonl')
    assert p.run_startup_reconciliation().classification=='AT1_AUTHORITY_NOT_ADMITTED'
    assert not p.request_testnet_mutation_arm(OperatorArmRequestV1('TESTNET','ACK','x','107087555','bad')).armed
    p.disarm(); p.close()

def test_retired_e17_callables_fail_closed_and_are_not_imported_by_active_r1_r2_graph():
    from backtest.execution.testnet_real_submit import RealTestnetSubmitExecutor,UrllibBybitTestnetSubmitTransport
    for fn in (lambda:RealTestnetSubmitExecutor().submit_once_result(),lambda:UrllibBybitTestnetSubmitTransport().post_order(request=None,credentials=None)):
        with pytest.raises(RuntimeError,match='retired by R1'): fn()
    active='\n'.join(Path(f).read_text() for f in ('backtest/execution/r1_testnet_product.py','backtest/execution/r1_testnet_coordinator.py','backtest/execution/r1_bybit_transport.py','backtest/execution/r2_identity_registry.py','backtest/execution/r2_startup_reconciliation.py','backtest/execution/r2_startup_authority.py'))
    assert 'testnet_real_submit' not in active

def test_exact_origin_rejects_variants():
    from backtest.execution.r1_contracts import exact_testnet_origin,R1ContractError
    assert exact_testnet_origin('https://api-testnet.bybit.eu')=='https://api-testnet.bybit.eu'
    for x in ('https://api-testnet.bybit.com','https://API-TESTNET.BYBIT.EU','https://api-testnet.bybit.eu:443','https://u:p@api-testnet.bybit.eu','https://api-testnet.bybit.eu/','https://api-testnet.bybit.eu?x=1','https://api-testnet.bybit.eu#x'):
        with pytest.raises((R1ContractError,ValueError)): exact_testnet_origin(x)

def test_staged_production_create_cancel_are_blocked_before_network(tmp_path,monkeypatch):
    import urllib.request,socket
    from tests.test_r1_testnet_coordinator import intent
    from backtest.execution.r1_contracts import R1CancelTarget
    def denied(*a,**k): raise AssertionError('network reached from staged mutation path')
    monkeypatch.setattr(urllib.request,'urlopen',denied); monkeypatch.setattr(socket,'create_connection',denied)
    kp=tmp_path/'k.json'; kill(kp); p=open_r1_testnet_product(Cfg(kp),Cred(),tmp_path/'l.jsonl')
    with pytest.raises(ValueError,match='AT1_AUTHORITY_NOT_ADMITTED'): p.create(intent())
    with pytest.raises(ValueError,match='AT1_AUTHORITY_NOT_ADMITTED'): p.cancel(R1CancelTarget('k','BTCUSDT','AT1_'+'0'*32,'OID'))
    p.close()
