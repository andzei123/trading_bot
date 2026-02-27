from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional
import json
import pandas as pd


@dataclass
class PortfolioConfig:
    # real/primary field
    max_signals_per_cycle: int = 1

    # ✅ backward-compatible alias (older code may pass this)
    max_open_positions: Optional[int] = None

    # fields used by filter_signals_portfolio (required)
    per_symbol_cooldown_candles: int = 6
    max_1_signal_per_candle_per_symbol: bool = True

    # optional extras (future-proof / common variants)
    max_signals_per_symbol: int = 1

    def __post_init__(self):
        # map alias -> primary
        if self.max_open_positions is not None:
            try:
                self.max_signals_per_cycle = int(self.max_open_positions)
            except Exception:
                pass


class PortfolioState:
    """Minimal state (JSON):
    - last_signal_ts[symbol] = last accepted signal timestamp (UTC ISO)
    - last_signal_candle_ts[symbol] = candle timestamp (UTC ISO) to enforce 1 per candle
    """

    def __init__(self, path: str):
        self.path = Path(path)
        self.last_signal_ts: Dict[str, str] = {}
        self.last_signal_candle_ts: Dict[str, str] = {}

    def load(self) -> None:
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            self.last_signal_ts = dict(data.get("last_signal_ts", {}))
            self.last_signal_candle_ts = dict(data.get("last_signal_candle_ts", {}))
        except Exception:
            self.last_signal_ts = {}
            self.last_signal_candle_ts = {}

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "last_signal_ts": self.last_signal_ts,
            "last_signal_candle_ts": self.last_signal_candle_ts,
        }
        self.path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _to_utc_ts(x) -> Optional[pd.Timestamp]:
    ts = pd.to_datetime(x, errors="coerce", utc=True)
    if pd.isna(ts):
        return None
    return ts


def _cooldown_ok(last_ts: Optional[pd.Timestamp], cur_ts: pd.Timestamp, bybit_interval_min: int, cooldown_candles: int) -> bool:
    if last_ts is None:
        return True
    cooldown_seconds = int(bybit_interval_min) * 60 * int(cooldown_candles)
    return (cur_ts - last_ts).total_seconds() >= cooldown_seconds


def filter_signals_portfolio(
    signals_df: pd.DataFrame,
    cfg: PortfolioConfig,
    state: PortfolioState,
    bybit_interval_min: int,
) -> pd.DataFrame:
    """Portfolio-level risk filter.

    Input df must have columns:
      - timestamp (signal timestamp, utc ok)
      - symbol (string)

    Returns filtered df (subset), updates state in-memory (caller should state.save()).
    """
    if signals_df is None or signals_df.empty:
        return signals_df

    df = signals_df.copy()

    # normalize
    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df = df.dropna(subset=["timestamp"])
    if df.empty:
        return df

    # newest first (prioritize freshest signals)
    df = df.sort_values("timestamp", ascending=False).reset_index(drop=True)

    out_rows: List[int] = []
    accepted = 0

    for idx, row in df.iterrows():
        if accepted >= int(cfg.max_signals_per_cycle):
            break

        sym = str(row["symbol"])
        ts = row["timestamp"]
        if not isinstance(ts, pd.Timestamp):
            continue

        last_ts_str = state.last_signal_ts.get(sym)
        last_candle_str = state.last_signal_candle_ts.get(sym)

        last_ts = _to_utc_ts(last_ts_str) if last_ts_str else None
        last_candle_ts = _to_utc_ts(last_candle_str) if last_candle_str else None

        # ------------------------------------------------------------------
        # Backfill / historical test safety:
        # If we are processing older timestamps than what's stored in state
        # (common when testing on history), do NOT apply cooldown against a
        # "future" last_ts. Otherwise everything gets rejected.
        #
        # In live mode this situation should not happen because runner feeds
        # newest candles only.
        # ------------------------------------------------------------------
        if last_ts is not None and ts <= last_ts:
            last_ts = None
            last_candle_ts = None

        # 1) cooldown per symbol
        if not _cooldown_ok(last_ts, ts, bybit_interval_min, int(cfg.per_symbol_cooldown_candles)):
            continue

        # 2) max 1 signal per candle per symbol
        if bool(cfg.max_1_signal_per_candle_per_symbol) and last_candle_ts is not None:
            if ts == last_candle_ts:
                continue

        out_rows.append(idx)
        accepted += 1

        state.last_signal_ts[sym] = ts.isoformat()
        state.last_signal_candle_ts[sym] = ts.isoformat()

    if not out_rows:
        return df.iloc[0:0].copy()

    return df.iloc[out_rows].sort_values("timestamp").reset_index(drop=True)
