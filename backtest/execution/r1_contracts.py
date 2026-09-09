from __future__ import annotations

import hashlib, json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping
from urllib.parse import urlsplit

R1_SCHEMA_VERSION = "ATS_R1_AUTHORITY_EVENT_V1"
R1_TESTNET_ORIGIN = "https://api-testnet.bybit.eu"
CREATE_RESERVATION_TTL_US = 10_000_000
QUERY_FRESHNESS_US = 5_000_000
ARM_TTL_US = 15 * 60 * 1_000_000
R1_DIRECT_REJECT_CONFIRMED_RETCODES_V1: frozenset[int] = frozenset()

class R1ContractError(ValueError): pass

def parse_utc(value: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise R1ContractError("timestamp must use canonical UTC Z form")
    try: dt=datetime.fromisoformat(value[:-1]+"+00:00")
    except ValueError as exc: raise R1ContractError("invalid UTC timestamp") from exc
    if dt.tzinfo is None or dt.utcoffset().total_seconds()!=0: raise R1ContractError("timestamp must be UTC")
    return dt

def utc_z(dt: datetime) -> str:
    if dt.tzinfo is None: raise R1ContractError("timezone-aware datetime required")
    return dt.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00","Z")

def exact_testnet_origin(origin: str) -> str:
    p=urlsplit(origin)
    if p.scheme!="https" or p.hostname!="api-testnet.bybit.eu" or p.port is not None or p.username or p.password or p.path not in ("",) or p.query or p.fragment:
        raise R1ContractError("origin is not exact Bybit TESTNET origin")
    if origin != R1_TESTNET_ORIGIN: raise R1ContractError("origin is not canonical TESTNET origin")
    return origin

def canonical_decimal(v: Decimal) -> str:
    if not isinstance(v, Decimal) or not v.is_finite(): raise R1ContractError("finite Decimal required")
    s=format(v,"f")
    if "." in s: s=s.rstrip("0").rstrip(".")
    return s or "0"

def canonical_json_bytes(value: Mapping[str,Any]) -> bytes:
    return json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False).encode("utf-8")

def sha256_hex(data: bytes) -> str: return hashlib.sha256(data).hexdigest()

def digest_mapping(value: Mapping[str,Any]) -> str: return sha256_hex(canonical_json_bytes(value))

@dataclass(frozen=True)
class OperatorArmRequestV1:
    mode: str; acknowledgement: str; session_challenge: str; expected_account_id: str; configuration_digest: str

@dataclass(frozen=True)
class ArmStatus:
    armed: bool; reason: str; expires_at_utc: str = ""

@dataclass(frozen=True)
class R1InstrumentRulesProof:
    environment: str; origin: str; symbol: str; quantity_step: Decimal; min_quantity: Decimal; max_quantity: Decimal
    price_tick: Decimal; min_notional: Decimal; max_notional: Decimal; observed_at_utc: str; expires_at_utc: str; provenance_digest: str

@dataclass(frozen=True)
class R1QueryRequest:
    symbol: str; client_order_id: str=""; exchange_order_id: str=""

@dataclass(frozen=True)
class R1QueryObservation:
    classification: str; environment: str; origin: str; account_identity: str; category: str; symbol: str
    client_order_id: str; exchange_order_id: str; order_state: str; leaves_qty: Decimal|None; observed_at_utc: str; observation_digest: str

@dataclass(frozen=True)
class R1CancelTarget:
    canonical_setup_key: str; symbol: str; client_order_id: str; exchange_order_id: str

@dataclass(frozen=True)
class R1OperationResult:
    classification: str; reason: str; client_order_id: str=""; exchange_order_id: str=""; query_required: bool=False

@dataclass(frozen=True)
class CreateReservationV1:
    reservation_id: str; attempt_id: str; operation_identity: str; canonical_setup_key: str; client_order_id: str
    client_identity_digest: str; request_fingerprint: str; proof_bundle_digest: str; symbol: str; created_at_utc: str; expires_at_utc: str
