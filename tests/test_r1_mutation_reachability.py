import ast, pathlib, inspect
from backtest.execution.r1_testnet_product import R1TestnetProduct
ROOT=pathlib.Path(__file__).resolve().parents[1]
def test_mutation_endpoint_literals_confined():
    hits=[]
    for p in ROOT.rglob('*.py'):
        if '__pycache__' in p.parts: continue
        text=p.read_text(encoding='utf-8')
        if ('/v5/order/'+'create') in text or ('/v5/order/'+'cancel') in text: hits.append(p.relative_to(ROOT).as_posix())
    assert hits==['backtest/execution/r1_bybit_transport.py']
def test_facade_has_closed_surface():
    public={n for n,v in inspect.getmembers(R1TestnetProduct,inspect.isfunction) if not n.startswith('_')}
    assert public=={'describe_disarmed_session','startup_reconciliation_status','run_startup_reconciliation','request_testnet_mutation_arm','disarm','create','query','cancel','close'}
    for n in ('create','cancel'):
        params=set(inspect.signature(getattr(R1TestnetProduct,n)).parameters)
        assert not params & {'transport','permit','reconciliation_result','endpoint','credentials','ledger_writer','request_builder'}
def test_no_live_mutation_origin_in_r1_transport():
    text=(ROOT/'backtest/execution/r1_bybit_transport.py').read_text(); assert 'https://api.bybit.com' not in text
