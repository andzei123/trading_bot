from __future__ import annotations
import hashlib,hmac,json,time,urllib.request,urllib.parse
from dataclasses import dataclass
from typing import Any, Mapping
from .r1_contracts import R1_TESTNET_ORIGIN, exact_testnet_origin

_CREATE_PATH="/v5/order/create"; _CANCEL_PATH="/v5/order/cancel"; _QUERY_PATH="/v5/order/realtime"; _POSITION_PATH="/v5/position/list"; _RULES_PATH="/v5/market/instruments-info"
@dataclass(frozen=True)
class _Credentials: api_key:str; api_secret:str
def _strict_json(raw:bytes):
    def hook(pairs):
        out={}
        for k,v in pairs:
            if k in out: raise ValueError("duplicate JSON key")
            out[k]=v
        return out
    return json.loads(raw.decode("utf-8"),object_pairs_hook=hook)

class _BybitTestnetTransport:
    def __init__(self, *, credentials:_Credentials, origin:str=R1_TESTNET_ORIGIN, timeout:float=10): self._c=credentials; self._origin=exact_testnet_origin(origin); self._timeout=timeout
    def _signed_headers(self,payload:str)->dict[str,str]:
        ts=str(int(time.time()*1000)); rw="5000"; sig=hmac.new(self._c.api_secret.encode(),(ts+self._c.api_key+rw+payload).encode(),hashlib.sha256).hexdigest()
        return {"X-BAPI-API-KEY":self._c.api_key,"X-BAPI-TIMESTAMP":ts,"X-BAPI-RECV-WINDOW":rw,"X-BAPI-SIGN":sig,"Content-Type":"application/json"}
    def _post(self,path:str,payload:Mapping[str,Any])->dict[str,Any]:
        body=json.dumps(dict(payload),sort_keys=True,separators=(",",":")); req=urllib.request.Request(self._origin+path,data=body.encode(),headers=self._signed_headers(body),method="POST")
        with urllib.request.urlopen(req,timeout=self._timeout) as r: return _strict_json(r.read())
    def create(self,payload:Mapping[str,Any])->dict[str,Any]: return self._post(_CREATE_PATH,payload)
    def cancel(self,payload:Mapping[str,Any])->dict[str,Any]: return self._post(_CANCEL_PATH,payload)
    def query(self,params:Mapping[str,str])->dict[str,Any]:
        q=urllib.parse.urlencode(sorted(params.items())); headers=self._signed_headers(q); req=urllib.request.Request(self._origin+_QUERY_PATH+"?"+q,headers=headers,method="GET")
        with urllib.request.urlopen(req,timeout=self._timeout) as r: return _strict_json(r.read())
    def positions(self,params:Mapping[str,str])->dict[str,Any]:
        q=urllib.parse.urlencode(sorted(params.items())); headers=self._signed_headers(q); req=urllib.request.Request(self._origin+_POSITION_PATH+"?"+q,headers=headers,method="GET")
        with urllib.request.urlopen(req,timeout=self._timeout) as r: return _strict_json(r.read())
    def instrument_rules(self,params:Mapping[str,str])->dict[str,Any]:
        q=urllib.parse.urlencode(sorted(params.items())); req=urllib.request.Request(self._origin+_RULES_PATH+"?"+q,method="GET")
        with urllib.request.urlopen(req,timeout=self._timeout) as r: return _strict_json(r.read())
