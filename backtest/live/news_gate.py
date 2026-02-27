
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import pandas as pd
from datetime import datetime, timezone

@dataclass
class GateDecision:
    allow_trade: bool
    risk_multiplier: float
    reason: str


def _safe_read_csv(p: str | Path) -> pd.DataFrame:
    p = Path(p)
    if not p.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(p)
    except Exception:
        return pd.DataFrame()


def news_gate_decision(
    news_events_csv: str | Path | pd.DataFrame,
    *,
    now_utc: pd.Timestamp | None = None,
    blackout_minutes: int = 60,
    default_allow_trade: bool = True,
    base_risk_multiplier: float = 1.0,
    blackout_risk_multiplier: float = 0.25,
) -> GateDecision:
    """
    News gate.
    IMPORTANT CHANGE (B2/B3):
      - Do NOT hard-block trades by default.
      - During blackout window, reduce risk (risk_multiplier < 1.0).
    Expected CSV columns (aliases supported):
      - timestamp OR timestamp_utc OR start_utc (UTC)
      - importance / impact (optional)
    """
    # Load data (path or preloaded DataFrame)
    if isinstance(news_events_csv, pd.DataFrame):
        df = news_events_csv.copy()
    else:
        df = _safe_read_csv(news_events_csv)

    if df.empty:
        return GateDecision(bool(default_allow_trade), float(base_risk_multiplier), "NEWS_GATE: base (no events)")

    # Normalize timestamp column aliases
    if "timestamp" not in df.columns:
        if "timestamp_utc" in df.columns:
            df = df.rename(columns={"timestamp_utc": "timestamp"})
        elif "start_utc" in df.columns:
            df = df.rename(columns={"start_utc": "timestamp"})

    if now_utc is None:
        now_utc = pd.Timestamp.utcnow().tz_localize("UTC")

    # Validate timestamp
    if "timestamp" not in df.columns:
        return GateDecision(True, float(base_risk_multiplier), "NEWS_GATE: missing timestamp column (no block)")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp")

    # choose relevant window
    win = pd.Timedelta(minutes=int(blackout_minutes))
    in_blackout = ((df["timestamp"] - now_utc).abs() <= win).any()

    if in_blackout:
        return GateDecision(
            allow_trade=True,
            risk_multiplier=float(blackout_risk_multiplier),
            reason=f"NEWS_GATE: BLACKOUT +/-{int(blackout_minutes)}m -> RISKx{float(blackout_risk_multiplier)} (no block)",
        )

    return GateDecision(True, float(base_risk_multiplier), "NEWS_GATE: base")


# Backwards-compatible helpers

def load_news_events_csv(path):
    return _safe_read_csv(path)


def news_gate(news_events_csv: str | Path | pd.DataFrame, now_utc=None, **kwargs):
    # now_utc gali ateiti kaip str arba datetime
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    else:
        try:
            now_utc = pd.to_datetime(now_utc, utc=True)
        except Exception:
            pass

    return news_gate_decision(
        news_events_csv=news_events_csv,
        now_utc=now_utc,
        **kwargs,
    )
