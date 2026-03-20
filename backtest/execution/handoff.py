from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd


def handoff_signals(
    *,
    df_e: pd.DataFrame,
    paper: bool,
    bybit_symbol: str,
    latest_ts: Any,
    rr: float | None,
    once: bool,
    diag_log: Callable[..., None],
    now_utc_str: Callable[[], str],
) -> None:
    # ============================================================
    # ============================================================
    # PAPER mode: maintain signals_live.csv + paper_trades.csv
    # ============================================================
    if paper:
        try:
            diag_log(
                "POST_EMIT_CALL_EXECUTION",
                symbol=str(bybit_symbol),
                count=int(len(df_e)),
                mode="paper",
            )
            from backtest.journal.paper_executor import run_paper_executor  # type: ignore

            signals_path = Path("backtest/journal/exports_live/signals_live.csv")
            signals_path.parent.mkdir(parents=True, exist_ok=True)

            # Stable schema for paper bridge (signals -> paper_trades)
            SIGNALS_COLS = [
                "timestamp", "signal_ts", "symbol", "model", "side",
                "entry", "sl", "tp", "rr",
                "ctx_sub_label", "phase", "regime", "trend_dir",
                "status",
            ]

            df_sig = df_e.copy()

            # Ensure mandatory columns exist
            if "signal_ts" not in df_sig.columns:
                df_sig["signal_ts"] = pd.to_datetime(latest_ts, utc=True, errors="coerce")
            else:
                df_sig["signal_ts"] = pd.to_datetime(df_sig["signal_ts"], utc=True, errors="coerce").fillna(
                    pd.to_datetime(latest_ts, utc=True, errors="coerce")
                )

            df_sig["symbol"] = str(bybit_symbol)

            if "rr" not in df_sig.columns:
                df_sig["rr"] = float(rr) if rr is not None else np.nan

            if "status" not in df_sig.columns:
                df_sig["status"] = "NEW"
            else:
                df_sig["status"] = df_sig["status"].replace("", "NEW").fillna("NEW")

            for c in SIGNALS_COLS:
                if c not in df_sig.columns:
                    df_sig[c] = np.nan

            df_sig = df_sig.reindex(columns=SIGNALS_COLS)

            # Overwrite snapshot (paper executor reads latest rows)
            if df_sig is None or df_sig.empty:
                # Keep deterministic files, but don't spam-run executor with empty signals.
                if not Path(signals_path).exists():
                    pd.DataFrame(columns=SIGNALS_COLS).to_csv(signals_path, index=False)
            else:
                df_sig.to_csv(signals_path, index=False)
                print(f"[{now_utc_str()}] Wrote {len(df_sig)} entries -> {signals_path}")

                _paper_trades_path = Path("backtest/journal/exports_live/paper_trades.csv")
                _paper_before_n = 0
                try:
                    if _paper_trades_path.exists() and _paper_trades_path.stat().st_size > 0:
                        _df_before = pd.read_csv(_paper_trades_path, engine="python", on_bad_lines="skip")
                        _paper_before_n = len(_df_before)
                except Exception:
                    _paper_before_n = 0

                run_paper_executor(
                    in_csv=str(signals_path),
                    out_csv=str(_paper_trades_path),
                )

                try:
                    if _paper_trades_path.exists() and _paper_trades_path.stat().st_size > 0:
                        _df_after = pd.read_csv(_paper_trades_path, engine="python", on_bad_lines="skip")
                        if len(_df_after) > _paper_before_n:
                            _df_new = _df_after.iloc[_paper_before_n:].copy()
                            if "status" in _df_new.columns:
                                _df_new_open = _df_new[_df_new["status"].astype(str).str.upper() == "OPEN"]
                                if not _df_new_open.empty:
                                    _r = _df_new_open.iloc[-1]
                                    diag_log(
                                        "TRADE_OPENED",
                                        symbol=_r.get("symbol"),
                                        model=_r.get("model"),
                                        entry=_r.get("entry"),
                                        tp=_r.get("tp"),
                                        sl=_r.get("sl"),
                                        mode="paper",
                                    )
                except Exception:
                    pass
        except Exception as _e:
            print(f"[{now_utc_str()}] [PAPER][WARN] {repr(_e)}")
    else:
        try:
            diag_log(
                "POST_EMIT_SKIPPED",
                symbol=str(bybit_symbol),
                reason="paper_false_no_post_emit_execution_path",
                count=int(len(df_e)),
                once=bool(once),
            )
        except Exception:
            pass
