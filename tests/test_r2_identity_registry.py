import copy,pytest
import backtest.execution.r2_identity_registry as r

def entry():
    return dict(registry_schema=r.REGISTRY_SCHEMA,identity_version='AT1',activation_utc='2026-09-09T09:00:00.000000Z',approved_origin='https://api-testnet.bybit.eu',approved_environment='TESTNET',approved_category='linear',approved_settle_coin='USDT',approved_account_uid=107087555,approved_parent_uid=0,predecessor_entry_digest='0'*64,chief_authority_reference='ATS_CHIEF_TEST_FIXTURE')

def test_production_is_structurally_staged_and_active_symbols_absent():
    assert isinstance(r.PRODUCTION_AT1_AUTHORITY,r.StagedNoAT1AuthorityV1)
    assert r.PRODUCTION_AT1_AUTHORITY.__slots__==()
    for name in ('PRODUCTION_AT1_GENESIS_ENTRY','PRODUCTION_AT1_CANONICAL_ENTRY_BYTES','EXPECTED_AT1_REGISTRY_ROOT_SHA256'):
        assert not hasattr(r,name)

def test_fixture_admission_is_exact_canonical_and_root_pinned():
    a=r.fixture_admitted_genesis_authority(entry()); e=r.validate_admitted_genesis(a)
    assert e.approved_origin=='https://api-testnet.bybit.eu'
    assert r.entry_sha256(a.canonical_entry_bytes)==a.expected_entry_sha256
    assert r.registry_root_digest([a.expected_entry_sha256])==a.expected_registry_root_sha256

def test_dynamic_placeholder_and_foreign_forms_fail():
    for k,v in [('activation_utc',''),('approved_origin','https://api-testnet.bybit.com'),('approved_account_uid',1),('predecessor_entry_digest','1'*64)]:
        d=entry(); d[k]=v
        with pytest.raises(ValueError): r.fixture_admitted_genesis_authority(d)

def test_noncanonical_or_duplicate_registry_bytes_fail():
    a=r.fixture_admitted_genesis_authority(entry())
    with pytest.raises(ValueError): r.AdmittedAT1GenesisAuthorityV1(a.canonical_entry_bytes+b'\n',a.expected_entry_sha256,a.expected_registry_root_sha256)
