from __future__ import annotations

import time
from typing import Optional, Dict, Any, List

import pandas as pd
import requests


# Bybit v5 public endpoint (market kline)
_BASE = "https://api.bybit.com"


class BybitRateLimitError(RuntimeError):
    """Raised when Bybit retCode indicates rate limit (10006)."""


def _bybit_get_kline(category: str, symbol: str, interval: str, limit: int) -> List[Dict[str, Any]]:
    """
    Fetch recent klines from Bybit v5.

    Returns raw 'list' items: [startTime, open, high, low, close, volume, turnover]
    as strings.
    """
    url = f"{_BASE}/v5/market/kline"
    params = {
        "category": category,
        "symbol": symbol,
        "interval": str(interval),
        "limit": int(limit),
    }
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    j = r.json()
    ret = int(j.get("retCode", 0) or 0)
    if ret == 10006:
        raise BybitRateLimitError(j.get("retMsg", "Rate limit"))
    if ret != 0:
        raise RuntimeError(f"Bybit retCode={ret} retMsg={j.get('retMsg')}")
    result = j.get("result", {}) or {}
    return result.get("list", []) or []


def load_bybit_latest(
    category: str,
    symbol: str,
    interval: str,
    candles: int,
    *,
    max_retries: int = 5,
    backoff_s: float = 2.0,
) -> pd.DataFrame:
    """
    Load latest candles from Bybit and return standardized OHLCV dataframe:
    columns: timestamp, open, high, low, close, volume

    Notes:
    - timestamp is UTC-aware pd.Timestamp
    - sorted ascending by timestamp
    - duplicates dropped by timestamp
    """
    last_err: Optional[Exception] = None
    sleep_s = float(backoff_s)

    for _ in range(max(1, int(max_retries))):
        try:
            raw = _bybit_get_kline(category, symbol, interval, candles)
            if not raw:
                return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

            # Bybit returns newest-first; each item is list[str]
            rows = []
            for item in raw:
                if not item or len(item) < 6:
                    continue
                start_ms = int(float(item[0]))
                rows.append(
                    {
                        "timestamp": pd.to_datetime(start_ms, unit="ms", utc=True),
                        "open": float(item[1]),
                        "high": float(item[2]),
                        "low": float(item[3]),
                        "close": float(item[4]),
                        "volume": float(item[5]),
                    }
                )

            df = pd.DataFrame(rows)
            if df.empty:
                return df

            df = df.sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)
            return df

        except BybitRateLimitError as e:
            last_err = e
            time.sleep(min(300.0, sleep_s))
            sleep_s = min(300.0, sleep_s * 2.0)
            continue
        except Exception as e:
            last_err = e
            time.sleep(min(10.0, sleep_s))
            sleep_s = min(10.0, sleep_s * 1.5)
            continue

    if last_err:
        raise last_err
    return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
