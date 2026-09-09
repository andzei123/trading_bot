from __future__ import annotations
import hashlib,hmac,json,os,secrets,threading,time
from dataclasses import dataclass
from typing import Any,Mapping,Callable

@dataclass(frozen=True,slots=True,eq=False)
class R2StartupEligibilityHandle:
    token:bytes
    startup_epoch_id:str
    diagnostic_identity:str
    def __copy__(self): return type(self)(bytes(self.token),self.startup_epoch_id,self.diagnostic_identity)
    def __deepcopy__(self,memo): return self.__copy__()

class R2StartupEligibilityIssuer:
    """Private process-local, one-use R2 eligibility capability issuer."""
    def __init__(self,*,clock:Callable[[],float]=time.monotonic,ttl_seconds:float=60.0):
        if ttl_seconds<=0: raise ValueError('positive R2 eligibility TTL required')
        self._clock=clock; self._ttl=float(ttl_seconds); self._lock=threading.Lock(); self._records={}; self._secret=secrets.token_bytes(32); self._process_nonce=secrets.token_bytes(32); self._pid=os.getpid()
    def issue(self,*,startup_epoch_id:str,context:Mapping[str,Any])->R2StartupEligibilityHandle:
        if not startup_epoch_id: raise ValueError('startup epoch required')
        token=secrets.token_bytes(32); issued=self._clock(); expires=issued+self._ttl; nonce=secrets.token_bytes(32); h=R2StartupEligibilityHandle(token,startup_epoch_id,secrets.token_hex(12)); ctx=dict(context)
        auth=self._auth(token,startup_epoch_id,ctx,issued,expires,nonce)
        with self._lock: self._records[token]=(h,ctx,issued,expires,nonce,auth,self._pid,self._process_nonce)
        return h
    def consume(self,h:R2StartupEligibilityHandle,*,startup_epoch_id:str,context:Mapping[str,Any])->bool:
        token=getattr(h,'token',b'')
        with self._lock: rec=self._records.pop(token,None)
        if not rec: return False
        registered,ctx,issued,expires,nonce,auth,pid,process_nonce=rec
        expected=self._auth(token,startup_epoch_id,dict(context),issued,expires,nonce)
        return bool(registered is h and startup_epoch_id==registered.startup_epoch_id and pid==os.getpid() and process_nonce==self._process_nonce and self._clock()<expires and ctx==dict(context) and hmac.compare_digest(auth,expected) and hmac.compare_digest(token,registered.token))
    def invalidate_all(self):
        with self._lock: self._records.clear()
    def _auth(self,token,epoch,context,issued,expires,nonce):
        material=json.dumps({'epoch':epoch,'context':dict(context),'issued':issued,'expires':expires,'pid':self._pid},sort_keys=True,separators=(',',':'),default=str).encode()+token+nonce+self._process_nonce
        return hmac.new(self._secret,material,hashlib.sha256).digest()

class R2CommittedArmGate:
    """Facade-owned publication latch; coordinator can only observe committed state."""
    def __init__(self): self._event_sequence=None; self._event_digest=None; self._epoch=None
    def is_committed(self): return self._event_sequence is not None and self._event_digest is not None and self._epoch is not None
    def publish(self,*,event_sequence:int,event_digest:str,startup_epoch_id:str):
        if event_sequence<1 or not event_digest or not startup_epoch_id: raise ValueError('committed R2 arm binding invalid')
        self._event_sequence=event_sequence; self._event_digest=event_digest; self._epoch=startup_epoch_id
    def clear(self): self._event_sequence=self._event_digest=self._epoch=None
    @property
    def binding(self): return (self._event_sequence,self._event_digest,self._epoch)
