from __future__ import annotations
import hashlib,hmac,json,time,urllib.request,urllib.parse
from dataclasses import dataclass
from typing import Any, Mapping
from .r1_contracts import R1_TESTNET_ORIGIN, exact_testnet_origin

_SERVER_TIME_PATH="/v5/market/time"
_API_KEY_PATH="/v5/user/query-api"
_QUERY_PATH="/v5/order/realtime"
_ORDER_HISTORY_PATH="/v5/order/history"
_EXECUTION_HISTORY_PATH="/v5/execution/list"
_POSITION_PATH="/v5/position/list"
_RULES_PATH="/v5/market/instruments-info"
_CREATE_PATH="/v5/order/create"
_CANCEL_PATH="/v5/order/cancel"
R2_FIXED_GET_PATHS=frozenset({_SERVER_TIME_PATH,_API_KEY_PATH,_QUERY_PATH,_ORDER_HISTORY_PATH,_EXECUTION_HISTORY_PATH,_POSITION_PATH,_RULES_PATH})
R1_MUTATION_PATHS=frozenset({_CREATE_PATH,_CANCEL_PATH})

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
    """Single exact-EU TESTNET credential/origin transport. No caller-selectable origin."""
    def __init__(self, *, credentials:_Credentials, timeout:float=10):
        self._c=credentials; self._origin=exact_testnet_origin(R1_TESTNET_ORIGIN); self._timeout=timeout
    @property
    def origin(self)->str: return self._origin
    def _signed_headers(self,payload:str)->dict[str,str]:
        ts=str(int(time.time()*1000)); rw="5000"; sig=hmac.new(self._c.api_secret.encode(),(ts+self._c.api_key+rw+payload).encode(),hashlib.sha256).hexdigest()
        return {"X-BAPI-API-KEY":self._c.api_key,"X-BAPI-TIMESTAMP":ts,"X-BAPI-RECV-WINDOW":rw,"X-BAPI-SIGN":sig,"Content-Type":"application/json"}
    def _get(self,path:str,params:Mapping[str,Any]|None=None,*,signed:bool=True)->dict[str,Any]:
        if path not in R2_FIXED_GET_PATHS: raise ValueError("GET path outside exact R1/R2 allowlist")
        params={} if params is None else dict(params)
        q=urllib.parse.urlencode(sorted((str(k),str(v)) for k,v in params.items()))
        headers=self._signed_headers(q) if signed else {}
        url=self._origin+path+("?"+q if q else "")
        req=urllib.request.Request(url,headers=headers,method="GET")
        with urllib.request.urlopen(req,timeout=self._timeout) as r: return _strict_json(r.read())
    def _post(self,path:str,payload:Mapping[str,Any])->dict[str,Any]:
        if path not in R1_MUTATION_PATHS: raise ValueError("POST path outside exact R1 mutation allowlist")
        body=json.dumps(dict(payload),sort_keys=True,separators=(",",":")); req=urllib.request.Request(self._origin+path,data=body.encode(),headers=self._signed_headers(body),method="POST")
        with urllib.request.urlopen(req,timeout=self._timeout) as r: return _strict_json(r.read())
    def create(self,payload:Mapping[str,Any])->dict[str,Any]: return self._post(_CREATE_PATH,payload)
    def cancel(self,payload:Mapping[str,Any])->dict[str,Any]: return self._post(_CANCEL_PATH,payload)
    def server_time(self)->dict[str,Any]: return self._get(_SERVER_TIME_PATH,{},signed=False)
    def api_key_info(self)->dict[str,Any]: return self._get(_API_KEY_PATH,{})
    def query(self,params:Mapping[str,Any])->dict[str,Any]: return self._get(_QUERY_PATH,params)
    def order_history(self,params:Mapping[str,Any])->dict[str,Any]: return self._get(_ORDER_HISTORY_PATH,params)
    def execution_history(self,params:Mapping[str,Any])->dict[str,Any]: return self._get(_EXECUTION_HISTORY_PATH,params)
    def positions(self,params:Mapping[str,Any])->dict[str,Any]: return self._get(_POSITION_PATH,params)
    def instrument_rules(self,params:Mapping[str,Any])->dict[str,Any]: return self._get(_RULES_PATH,params,signed=False)

class _R2ReadOnlyTransportView:
    __slots__=("__transport",)
    def __init__(self,transport:_BybitTestnetTransport): self.__transport=transport
    @property
    def origin(self): return self.__transport.origin
    def server_time(self): return self.__transport.server_time()
    def api_key_info(self,params=None): return self.__transport.api_key_info()
    def realtime_orders(self,params): return self.__transport.query(params)
    def query(self,params): return self.__transport.query(params)
    def order_history(self,params): return self.__transport.order_history(params)
    def execution_history(self,params): return self.__transport.execution_history(params)
    def positions(self,params): return self.__transport.positions(params)
    def instrument_rules(self,params): return self.__transport.instrument_rules(params)
