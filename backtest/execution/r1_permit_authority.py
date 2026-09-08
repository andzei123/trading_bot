from __future__ import annotations
import hmac, os, secrets, time
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Callable, Mapping, Any
from .r1_contracts import ARM_TTL_US, QUERY_FRESHNESS_US, ArmStatus, OperatorArmRequestV1, R1QueryRequest, parse_utc, utc_z, sha256_hex

@dataclass(frozen=True, eq=False)
class _CapabilityHandle:
    token: bytes
    diagnostic_identity: str
    def __copy__(self): return type(self)(self.token,self.diagnostic_identity)
    def __deepcopy__(self,memo): return type(self)(bytes(self.token),self.diagnostic_identity)

def _reject_duplicate_pairs(pairs):
    out={}
    for k,v in pairs:
        if k in out: raise ValueError(f"duplicate kill artifact key: {k}")
        out[k]=v
    return out

def read_kill_artifact(path: str|Path, *, expected_account_identity: str, clock) -> tuple[str,str]:
    import json
    q=Path(path)
    if not q.is_file(): raise ValueError("mandatory kill artifact unavailable or non-regular")
    try: raw=q.read_bytes(); text=raw.decode("utf-8","strict")
    except (OSError,UnicodeDecodeError) as exc: raise ValueError("mandatory kill artifact unreadable/invalid UTF-8") from exc
    try: obj=json.loads(text,object_pairs_hook=_reject_duplicate_pairs)
    except (json.JSONDecodeError,ValueError) as exc: raise ValueError("mandatory kill artifact malformed") from exc
    fields={"schema_version","environment","account_identity","state","issued_at_utc","expires_at_utc"}
    if not isinstance(obj,dict) or set(obj)!=fields: raise ValueError("mandatory kill artifact schema mismatch")
    if obj["schema_version"]!="ATS_TESTNET_KILL_V1" or obj["environment"]!="TESTNET" or obj["account_identity"]!=expected_account_identity: raise ValueError("mandatory kill artifact binding mismatch")
    if obj["state"]!="NOT_TRIGGERED": raise ValueError("kill artifact triggered")
    parse_utc(obj["issued_at_utc"]); expiry=parse_utc(obj["expires_at_utc"])
    if clock.now()>=expiry: raise ValueError("kill artifact expired")
    return sha256_hex(raw),obj["expires_at_utc"]

class R1AuthorityClock:
    def __init__(self, *, server_utc_at_receipt: str, local_utc_at_receipt: str, monotonic_at_receipt: float|None=None, monotonic: Callable[[],float]=time.monotonic):
        server=parse_utc(server_utc_at_receipt); local=parse_utc(local_utc_at_receipt)
        if abs((server-local).total_seconds())>5: raise ValueError("TESTNET server/local clock skew exceeds five seconds")
        self._server=server; self._mono=monotonic; self._start=monotonic() if monotonic_at_receipt is None else monotonic_at_receipt
    def now(self): return self._server+timedelta(seconds=max(0.0,self._mono()-self._start))
    def now_utc(self)->str: return utc_z(self.now())

class R1MutationArmIssuer:
    def __init__(self, *, clock:R1AuthorityClock, expected_context:Mapping[str,str]):
        self._clock=clock; self._ctx=dict(expected_context); self._registry={}; self._nonce=secrets.token_hex(16); self._pid=os.getpid()
    def issue(self, request:OperatorArmRequestV1)->tuple[_CapabilityHandle,ArmStatus]:
        if request.mode!="TESTNET" or request.expected_account_id!=self._ctx.get("account_identity") or request.configuration_digest!=self._ctx.get("configuration_digest"):
            raise ValueError("arm request context mismatch")
        if not request.acknowledgement or request.session_challenge!=self._ctx.get("session_challenge"): raise ValueError("arm acknowledgement/challenge invalid")
        token=secrets.token_bytes(32); h=_CapabilityHandle(token,secrets.token_hex(12)); exp=self._clock.now()+timedelta(microseconds=ARM_TTL_US)
        self._registry[token]=(h,dict(self._ctx),exp,self._pid,self._nonce)
        return h,ArmStatus(True,"ARMED",utc_z(exp))
    def validate_arm(self, handle:_CapabilityHandle, expected_context:Mapping[str,str])->bool:
        rec=self._registry.get(getattr(handle,"token",b""));
        if not rec: return False
        registered,ctx,exp,pid,nonce=rec
        return handle is registered and hmac.compare_digest(handle.token,registered.token) and pid==os.getpid() and nonce==self._nonce and ctx==dict(expected_context) and self._clock.now()<exp
    def invalidate_all(self): self._registry.clear()

class R1ReconciliationPermitIssuer:
    def __init__(self, *, clock:R1AuthorityClock, read_authority:Any):
        self._clock=clock; self._read=read_authority; self._registry={}; self._nonce=secrets.token_hex(16); self._pid=os.getpid()
    def _register(self, *, operation:str, expected_context:Mapping[str,str], observation_digest:str, observed_at_utc:str)->_CapabilityHandle:
        age=(self._clock.now()-parse_utc(observed_at_utc)).total_seconds()
        if age<0 or age>=QUERY_FRESHNESS_US/1_000_000: raise ValueError("reconciliation observation stale")
        if not observation_digest: raise ValueError("reconciliation observation digest required")
        token=secrets.token_bytes(32); h=_CapabilityHandle(token,secrets.token_hex(12)); exp=self._clock.now()+timedelta(microseconds=QUERY_FRESHNESS_US)
        self._registry[token]=(h,operation,dict(expected_context),observation_digest,exp,self._nonce,self._pid)
        return h
    def issue_for_create(self, *, expected_context:Mapping[str,str], symbol:str, client_order_id:str, client_identity_digest:str)->_CapabilityHandle:
        observation_digest,observed_at_utc=self._read.reconciliation_for_create(symbol=symbol,client_order_id=client_order_id,client_identity_digest=client_identity_digest)
        return self._register(operation="CREATE",expected_context=expected_context,observation_digest=observation_digest,observed_at_utc=observed_at_utc)
    def issue_for_cancel(self, *, expected_context:Mapping[str,str], request:R1QueryRequest):
        obs=self._read.query(request)
        age=(self._clock.now()-parse_utc(obs.observed_at_utc)).total_seconds()
        if obs.classification!="FOUND_CONSISTENT" or age<0 or age>=QUERY_FRESHNESS_US/1_000_000: raise ValueError("cancel observation unavailable/stale")
        if obs.environment!="TESTNET" or obs.origin!=expected_context.get("origin") or obs.category!=expected_context.get("category") or obs.account_identity!=expected_context.get("account_identity") or obs.symbol!=expected_context.get("symbol") or obs.client_order_id!=expected_context.get("client_order_id") or obs.exchange_order_id!=expected_context.get("exchange_order_id"): raise ValueError("cancel observation provenance mismatch")
        if obs.order_state=="New": conclusion="New"
        elif obs.order_state=="PartiallyFilled" and obs.leaves_qty is not None and obs.leaves_qty>0: conclusion="PartiallyFilled"
        else: raise ValueError("cancel state not allowed")
        ctx={**dict(expected_context),"cancel_conclusion":conclusion}
        return self._register(operation="CANCEL",expected_context=ctx,observation_digest=obs.observation_digest,observed_at_utc=obs.observed_at_utc),obs,ctx
    def consume_reconciliation(self, handle:_CapabilityHandle, *, operation:str, expected_context:Mapping[str,str])->bool:
        rec=self._registry.pop(getattr(handle,"token",b""),None)
        if not rec: return False
        registered,op,ctx,digest,exp,nonce,pid=rec
        return handle is registered and hmac.compare_digest(handle.token,registered.token) and op==operation and ctx==dict(expected_context) and nonce==self._nonce and pid==os.getpid() and self._clock.now()<exp
    def invalidate_all(self): self._registry.clear()
