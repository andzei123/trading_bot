from __future__ import annotations
import hashlib, json, re
from dataclasses import dataclass, asdict
from typing import Any, Mapping
from .r1_contracts import R1_TESTNET_ORIGIN, parse_utc

REGISTRY_SCHEMA="ATS_IDENTITY_EPOCH_REGISTRY_V1"
IDENTITY_VERSION="AT1"
GENESIS_PREDECESSOR_SENTINEL="0"*64
_ENTRY_FIELDS=frozenset({"registry_schema","identity_version","activation_utc","approved_origin","approved_environment","approved_category","approved_settle_coin","approved_account_uid","approved_parent_uid","predecessor_entry_digest","chief_authority_reference"})
_HEX64=re.compile(r"^[0-9a-f]{64}$")
_CANONICAL_UTC=re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$")

@dataclass(frozen=True, slots=True)
class StagedNoAT1AuthorityV1:
    pass

@dataclass(frozen=True, slots=True)
class AT1IdentityEpochEntryV1:
    registry_schema:str
    identity_version:str
    activation_utc:str
    approved_origin:str
    approved_environment:str
    approved_category:str
    approved_settle_coin:str
    approved_account_uid:int
    approved_parent_uid:int
    predecessor_entry_digest:str
    chief_authority_reference:str

@dataclass(frozen=True, slots=True)
class AdmittedAT1GenesisAuthorityV1:
    canonical_entry_bytes:bytes
    expected_entry_sha256:str
    expected_registry_root_sha256:str
    def __post_init__(self):
        if not isinstance(self.canonical_entry_bytes,(bytes,bytearray)) or not self.canonical_entry_bytes: raise ValueError("canonical entry bytes required")
        if not _HEX64.fullmatch(self.expected_entry_sha256 or "") or not _HEX64.fullmatch(self.expected_registry_root_sha256 or ""): raise ValueError("exact admitted SHA-256 values required")
        validate_admitted_genesis(self)

PRODUCTION_AT1_AUTHORITY = StagedNoAT1AuthorityV1()

def _pairs(pairs):
    out={}
    for k,v in pairs:
        if k in out: raise ValueError(f"duplicate registry key: {k}")
        out[k]=v
    return out

def canonical_entry_bytes(entry:AT1IdentityEpochEntryV1|Mapping[str,Any])->bytes:
    d=asdict(entry) if isinstance(entry,AT1IdentityEpochEntryV1) else dict(entry)
    if frozenset(d)!=_ENTRY_FIELDS: raise ValueError("registry entry exact field set required")
    _validate_entry_mapping(d)
    return json.dumps(d,sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False).encode("utf-8")

def entry_sha256(raw:bytes)->str: return hashlib.sha256(raw).hexdigest()

def registry_root_digest(entry_digests:list[str]|tuple[str,...])->str:
    for d in entry_digests:
        if not _HEX64.fullmatch(d): raise ValueError("registry entry digest malformed")
    material=(REGISTRY_SCHEMA+"\n"+"\n".join(entry_digests)).encode("ascii")
    return hashlib.sha256(material).hexdigest()

def parse_canonical_entry(raw:bytes)->AT1IdentityEpochEntryV1:
    if not isinstance(raw,(bytes,bytearray)) or not raw: raise ValueError("registry bytes required")
    try: obj=json.loads(bytes(raw).decode("utf-8","strict"),object_pairs_hook=_pairs)
    except Exception as exc: raise ValueError("registry canonical bytes malformed") from exc
    if not isinstance(obj,dict) or frozenset(obj)!=_ENTRY_FIELDS: raise ValueError("registry exact field set mismatch")
    _validate_entry_mapping(obj)
    canonical=json.dumps(obj,sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False).encode("utf-8")
    if canonical!=bytes(raw): raise ValueError("registry bytes are not canonical")
    return AT1IdentityEpochEntryV1(**obj)

def _validate_entry_mapping(d:Mapping[str,Any])->None:
    if d.get("registry_schema")!=REGISTRY_SCHEMA or d.get("identity_version")!=IDENTITY_VERSION: raise ValueError("registry schema/version mismatch")
    if not isinstance(d.get("activation_utc"),str) or not _CANONICAL_UTC.fullmatch(d["activation_utc"]): raise ValueError("activation UTC must have six fractional digits")
    parse_utc(d["activation_utc"])
    if d.get("approved_origin")!=R1_TESTNET_ORIGIN or d.get("approved_environment")!="TESTNET" or d.get("approved_category")!="linear" or d.get("approved_settle_coin")!="USDT": raise ValueError("registry context mismatch")
    if type(d.get("approved_account_uid")) is not int or type(d.get("approved_parent_uid")) is not int: raise ValueError("registry UID types invalid")
    if d["approved_account_uid"]!=107087555 or d["approved_parent_uid"]!=0: raise ValueError("registry account mismatch")
    if not _HEX64.fullmatch(str(d.get("predecessor_entry_digest",""))): raise ValueError("predecessor digest malformed")
    if not isinstance(d.get("chief_authority_reference"),str) or not d["chief_authority_reference"] or d["chief_authority_reference"].strip()!=d["chief_authority_reference"]: raise ValueError("Chief authority reference invalid")

def validate_admitted_genesis(authority:AdmittedAT1GenesisAuthorityV1)->AT1IdentityEpochEntryV1:
    raw=bytes(authority.canonical_entry_bytes); entry=parse_canonical_entry(raw)
    if entry.predecessor_entry_digest!=GENESIS_PREDECESSOR_SENTINEL: raise ValueError("genesis predecessor sentinel mismatch")
    actual=entry_sha256(raw)
    if actual!=authority.expected_entry_sha256: raise ValueError("admitted entry digest mismatch")
    if registry_root_digest([actual])!=authority.expected_registry_root_sha256: raise ValueError("admitted registry root mismatch")
    return entry

def fixture_admitted_genesis_authority(entry:Mapping[str,Any])->AdmittedAT1GenesisAuthorityV1:
    """Test-facing constructor only; production root never imports or calls this helper."""
    e=AT1IdentityEpochEntryV1(**dict(entry)); raw=canonical_entry_bytes(e); digest=entry_sha256(raw); root=registry_root_digest([digest])
    return AdmittedAT1GenesisAuthorityV1(raw,digest,root)

def match_admitted_context(authority:AdmittedAT1GenesisAuthorityV1, *, origin:str, environment:str, category:str, settle_coin:str, account_uid:int, parent_uid:int)->AT1IdentityEpochEntryV1:
    e=validate_admitted_genesis(authority)
    if (origin,environment,category,settle_coin,account_uid,parent_uid)!=(e.approved_origin,e.approved_environment,e.approved_category,e.approved_settle_coin,e.approved_account_uid,e.approved_parent_uid): raise ValueError("REGISTRY_FOREIGN_CONTEXT")
    return e

_AT1_CLIENT_ID=re.compile(r"^AT1_[0-9a-f]{32}$")
_ATS_VERSIONED=re.compile(r"^AT[0-9]+_")
def classify_exchange_client_identity(order_link_id:str, *, local_full_identity_digest:str|None=None)->str:
    if not isinstance(order_link_id,str): return "NON_ATS"
    if _AT1_CLIENT_ID.fullmatch(order_link_id):
        if local_full_identity_digest is None: return "ATS_PROBABLE_UNVERIFIED"
        if not _HEX64.fullmatch(local_full_identity_digest): return "IDENTITY_CONFLICT"
        return "ATS_PROVEN" if order_link_id=="AT1_"+local_full_identity_digest[:32] else "IDENTITY_CONFLICT"
    if _ATS_VERSIONED.match(order_link_id): return "UNRECOGNIZED_VERSION"
    return "NON_ATS"
