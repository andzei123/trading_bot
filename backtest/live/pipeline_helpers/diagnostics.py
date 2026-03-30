from pathlib import Path
from typing import Any

import pandas as pd


def _write_candidate_pressure_row(*, entries: list[Any], symbol: str, ts: Any, out_csv: str | None) -> None:
    """Validation-only Stage 2.6 artifact written before cluster filter.

    Read-only / fail-open: counts raw candidates and cluster-group pressure
    without changing strategy, scoring, filters, wait-entry, risk or execution.
    """
    if not out_csv:
        return
    try:
        from backtest.filters.signal_cluster_filter import _extract_symbol, default_group_for_symbol

        counts: dict[str, int] = {}
        for e in list(entries or []):
            base = _extract_symbol(e)
            grp = default_group_for_symbol(base)
            counts[grp] = int(counts.get(grp, 0)) + 1

        row = {
            "timestamp": pd.to_datetime(ts, utc=True, errors="coerce"),
            "symbol": str(symbol),
            "raw_candidate_count": int(len(entries or [])),
            "cluster_group_count": int(len(counts)),
            "groups_gt1": int(sum(1 for v in counts.values() if int(v) > 1)),
            "groups_gt2": int(sum(1 for v in counts.values() if int(v) > 2)),
            "groups_gt3": int(sum(1 for v in counts.values() if int(v) > 3)),
        }

        p = Path(str(out_csv))
        p.parent.mkdir(parents=True, exist_ok=True)
        write_header = (not p.exists()) or p.stat().st_size == 0
        pd.DataFrame([row]).to_csv(p, mode="a", header=write_header, index=False)
    except Exception:
        return