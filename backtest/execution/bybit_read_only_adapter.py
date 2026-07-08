from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import hashlib
import hmac
import json
import os
import time
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

from backtest.execution.exchange_read_adapter import (
    ExchangeBalanceSnapshot,
    ExchangeInstrumentRules,
    ExchangeOpenOrderSnapshot,
    ExchangePositionSnapshot,
    ExchangeReadResult,
    require_credentials_configured,
)


READ_ONLY_MODE = "READ_ONLY"
DEFAULT_BYBIT_BASE_URL = "https://api.bybit.com"
DEFAULT_RECV_WINDOW_MS = 5000


@dataclass(frozen=True)
class BybitReadOnlyConfig:
    """External configuration for the E12 real read-only Bybit adapter.

    Secrets are supplied externally. This config must never hardcode credentials
    and the adapter must fail closed when private-read credentials are missing.
    """

    api_key: str | None = None
    api_secret: str | None = None
    base_url: str = DEFAULT_BYBIT_BASE_URL
    account_type: str = "UNIFIED"
    category: str = "linear"
    settle_coin: str = "USDT"
    symbol: str | None = None
    recv_window_ms: int = DEFAULT_RECV_WINDOW_MS

    @classmethod
    def from_env(cls) -> "BybitReadOnlyConfig":
        """Build config from environment variables without requiring them.

        Supported variables:
        BYBIT_API_KEY, BYBIT_API_SECRET, BYBIT_BASE_URL, BYBIT_ACCOUNT_TYPE,
        BYBIT_CATEGORY, BYBIT_SETTLE_COIN, BYBIT_SYMBOL, BYBIT_RECV_WINDOW_MS.
        """

        recv_window_raw = os.getenv("BYBIT_RECV_WINDOW_MS")
        recv_window_ms = DEFAULT_RECV_WINDOW_MS
        if recv_window_raw:
            try:
                recv_window_ms = int(recv_window_raw)
            except ValueError:
                recv_window_ms = DEFAULT_RECV_WINDOW_MS

        return cls(
            api_key=os.getenv("BYBIT_API_KEY"),
            api_secret=os.getenv("BYBIT_API_SECRET"),
            base_url=os.getenv("BYBIT_BASE_URL", DEFAULT_BYBIT_BASE_URL).rstrip("/"),
            account_type=os.getenv("BYBIT_ACCOUNT_TYPE", "UNIFIED"),
            category=os.getenv("BYBIT_CATEGORY", "linear"),
            settle_coin=os.getenv("BYBIT_SETTLE_COIN", "USDT"),
            symbol=os.getenv("BYBIT_SYMBOL") or None,
            recv_window_ms=recv_window_ms,
        )


class BybitReadOnlyAdapter:
    """E12 real read-only Bybit adapter.

    This adapter only performs read-only HTTP GET requests. It does not expose
    order submission, cancellation, amendment, TP/SL, or position-closing
    methods. It normalizes Bybit responses into executor-local immutable models.
    """

    mode = READ_ONLY_MODE

    def __init__(self, config: BybitReadOnlyConfig) -> None:
        self.config = config

    def read_server_time(self) -> ExchangeReadResult[str]:
        observed_at = _utc_now_iso()
        response = self._public_get("/v5/market/time", {})
        if not response.ok or response.result is None:
            return ExchangeReadResult.failure(response.error, observed_at_utc=observed_at)

        result = response.result.get("result", {})
        seconds = result.get("timeSecond")
        nanos = result.get("timeNano")
        if seconds is not None:
            try:
                return ExchangeReadResult.success(
                    _epoch_seconds_to_iso(Decimal(str(seconds))), observed_at_utc=observed_at
                )
            except Exception:
                pass
        if nanos is not None:
            try:
                return ExchangeReadResult.success(
                    _epoch_seconds_to_iso(Decimal(str(nanos)) / Decimal("1000000000")),
                    observed_at_utc=observed_at,
                )
            except Exception:
                pass
        return ExchangeReadResult.failure("Bybit server time response missing time", observed_at_utc=observed_at)

    def read_balance(self) -> ExchangeReadResult[tuple[ExchangeBalanceSnapshot, ...]]:
        observed_at = _utc_now_iso()
        credentials = require_credentials_configured(
            api_key=self.config.api_key,
            api_secret=self.config.api_secret,
            observed_at_utc=observed_at,
        )
        if not credentials.ok:
            return ExchangeReadResult.failure(credentials.error, observed_at_utc=observed_at)

        response = self._private_get(
            "/v5/account/wallet-balance",
            {"accountType": self.config.account_type, "coin": self.config.settle_coin},
        )
        if not response.ok or response.result is None:
            return ExchangeReadResult.failure(response.error, observed_at_utc=observed_at)

        balances: list[ExchangeBalanceSnapshot] = []
        for account in response.result.get("result", {}).get("list", []):
            account_type = str(account.get("accountType", self.config.account_type))
            for coin in account.get("coin", []):
                balances.append(
                    ExchangeBalanceSnapshot(
                        exchange="BYBIT",
                        account_type=account_type,
                        asset=str(coin.get("coin", "")),
                        wallet_balance=_decimal(coin.get("walletBalance")),
                        available_balance=_decimal(
                            coin.get("availableToWithdraw")
                            or coin.get("walletBalance")
                            or coin.get("equity")
                        ),
                        equity=_decimal(coin.get("equity")),
                        observed_at_utc=observed_at,
                    )
                )
        return ExchangeReadResult.success(tuple(balances), observed_at_utc=observed_at)

    def read_open_positions(self) -> ExchangeReadResult[tuple[ExchangePositionSnapshot, ...]]:
        observed_at = _utc_now_iso()
        credentials = require_credentials_configured(
            api_key=self.config.api_key,
            api_secret=self.config.api_secret,
            observed_at_utc=observed_at,
        )
        if not credentials.ok:
            return ExchangeReadResult.failure(credentials.error, observed_at_utc=observed_at)

        params: dict[str, str] = {"category": self.config.category}
        if self.config.symbol:
            params["symbol"] = self.config.symbol
        else:
            params["settleCoin"] = self.config.settle_coin

        response = self._private_get("/v5/position/list", params)
        if not response.ok or response.result is None:
            return ExchangeReadResult.failure(response.error, observed_at_utc=observed_at)

        positions: list[ExchangePositionSnapshot] = []
        for row in response.result.get("result", {}).get("list", []):
            qty = _decimal(row.get("size"))
            if qty == Decimal("0"):
                continue
            positions.append(
                ExchangePositionSnapshot(
                    exchange="BYBIT",
                    symbol=str(row.get("symbol", "")),
                    side=str(row.get("side", "")),
                    quantity=qty,
                    entry_price=_optional_decimal(row.get("avgPrice")),
                    mark_price=_optional_decimal(row.get("markPrice")),
                    unrealized_pnl=_optional_decimal(row.get("unrealisedPnl")),
                    observed_at_utc=observed_at,
                )
            )
        return ExchangeReadResult.success(tuple(positions), observed_at_utc=observed_at)

    def read_open_orders(self) -> ExchangeReadResult[tuple[ExchangeOpenOrderSnapshot, ...]]:
        observed_at = _utc_now_iso()
        credentials = require_credentials_configured(
            api_key=self.config.api_key,
            api_secret=self.config.api_secret,
            observed_at_utc=observed_at,
        )
        if not credentials.ok:
            return ExchangeReadResult.failure(credentials.error, observed_at_utc=observed_at)

        params: dict[str, str] = {"category": self.config.category, "openOnly": "0"}
        if self.config.symbol:
            params["symbol"] = self.config.symbol
        else:
            params["settleCoin"] = self.config.settle_coin

        response = self._private_get("/v5/order/realtime", params)
        if not response.ok or response.result is None:
            return ExchangeReadResult.failure(response.error, observed_at_utc=observed_at)

        orders: list[ExchangeOpenOrderSnapshot] = []
        for row in response.result.get("result", {}).get("list", []):
            qty = _decimal(row.get("qty"))
            leaves_qty = _decimal(row.get("leavesQty", row.get("qty")))
            orders.append(
                ExchangeOpenOrderSnapshot(
                    exchange="BYBIT",
                    symbol=str(row.get("symbol", "")),
                    side=str(row.get("side", "")),
                    order_id=str(row.get("orderId", "")),
                    client_order_id=str(row.get("orderLinkId", "")),
                    order_type=str(row.get("orderType", "")),
                    price=_optional_decimal(row.get("price")),
                    quantity=qty,
                    remaining_quantity=leaves_qty,
                    status=str(row.get("orderStatus", "")),
                    observed_at_utc=observed_at,
                )
            )
        return ExchangeReadResult.success(tuple(orders), observed_at_utc=observed_at)

    def read_instrument_rules(self) -> ExchangeReadResult[tuple[ExchangeInstrumentRules, ...]]:
        observed_at = _utc_now_iso()
        params: dict[str, str] = {"category": self.config.category}
        if self.config.symbol:
            params["symbol"] = self.config.symbol
        response = self._public_get("/v5/market/instruments-info", params)
        if not response.ok or response.result is None:
            return ExchangeReadResult.failure(response.error, observed_at_utc=observed_at)

        rules: list[ExchangeInstrumentRules] = []
        for row in response.result.get("result", {}).get("list", []):
            lot_filter = row.get("lotSizeFilter", {})
            price_filter = row.get("priceFilter", {})
            qty_step = _decimal(lot_filter.get("qtyStep"))
            tick_size = _decimal(price_filter.get("tickSize"))
            rules.append(
                ExchangeInstrumentRules(
                    exchange="BYBIT",
                    symbol=str(row.get("symbol", "")),
                    quantity_step=qty_step,
                    min_quantity=_decimal(lot_filter.get("minOrderQty")),
                    quantity_precision=_precision_from_step(qty_step),
                    price_tick=tick_size,
                    price_precision=_precision_from_step(tick_size),
                    min_notional=_optional_decimal(lot_filter.get("minNotionalValue")),
                    observed_at_utc=observed_at,
                )
            )
        return ExchangeReadResult.success(tuple(rules), observed_at_utc=observed_at)

    def _public_get(self, path: str, params: dict[str, str]) -> ExchangeReadResult[dict[str, Any]]:
        return self._http_get(path, params, signed=False)

    def _private_get(self, path: str, params: dict[str, str]) -> ExchangeReadResult[dict[str, Any]]:
        return self._http_get(path, params, signed=True)

    def _http_get(
        self, path: str, params: dict[str, str], *, signed: bool
    ) -> ExchangeReadResult[dict[str, Any]]:
        observed_at = _utc_now_iso()
        query_string = urlencode(sorted((k, v) for k, v in params.items() if v is not None))
        url = f"{self.config.base_url}{path}"
        if query_string:
            url = f"{url}?{query_string}"

        headers = {"Accept": "application/json"}
        if signed:
            if not self.config.api_key or not self.config.api_secret:
                return ExchangeReadResult.failure(
                    "missing read-only exchange credentials", observed_at_utc=observed_at
                )
            timestamp_ms = str(int(time.time() * 1000))
            recv_window = str(self.config.recv_window_ms)
            payload = timestamp_ms + self.config.api_key + recv_window + query_string
            signature = hmac.new(
                self.config.api_secret.encode("utf-8"),
                payload.encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()
            headers.update(
                {
                    "X-BAPI-API-KEY": self.config.api_key,
                    "X-BAPI-TIMESTAMP": timestamp_ms,
                    "X-BAPI-RECV-WINDOW": recv_window,
                    "X-BAPI-SIGN": signature,
                    "X-BAPI-SIGN-TYPE": "2",
                }
            )

        request = Request(url, headers=headers, method="GET")
        try:
            with urlopen(request, timeout=10) as response:  # noqa: S310 - read-only explicit URL
                raw = response.read().decode("utf-8")
        except HTTPError as exc:
            return ExchangeReadResult.failure(f"Bybit read failed: HTTP {exc.code}", observed_at_utc=observed_at)
        except URLError as exc:
            return ExchangeReadResult.failure(f"Bybit read failed: {exc.reason}", observed_at_utc=observed_at)
        except Exception as exc:  # fail closed for optional validation
            return ExchangeReadResult.failure(f"Bybit read failed: {exc}", observed_at_utc=observed_at)

        try:
            payload_json = json.loads(raw)
        except json.JSONDecodeError as exc:
            return ExchangeReadResult.failure(f"Bybit read returned invalid JSON: {exc}", observed_at_utc=observed_at)

        ret_code = payload_json.get("retCode")
        if ret_code not in (0, "0", None):
            ret_msg = payload_json.get("retMsg", "unknown Bybit error")
            return ExchangeReadResult.failure(f"Bybit read returned retCode={ret_code}: {ret_msg}", observed_at_utc=observed_at)

        return ExchangeReadResult.success(payload_json, observed_at_utc=observed_at)


def _decimal(value: object) -> Decimal:
    parsed = _optional_decimal(value)
    return parsed if parsed is not None else Decimal("0")


def _optional_decimal(value: object) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _precision_from_step(step: Decimal) -> int:
    if step <= 0:
        return 0
    return max(0, -step.normalize().as_tuple().exponent)


def _utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime())


def _epoch_seconds_to_iso(seconds: Decimal) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(float(seconds)))
