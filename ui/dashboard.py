import json
import time
from pathlib import Path

import pandas as pd
import streamlit as st
import plotly.graph_objects as go


# ----------------------------
# Config
# ----------------------------
STATUS_DEFAULT = Path("backtest/journal/live_status.json")
CONTROLS_DEFAULT = Path("backtest/journal/live_controls.json")
ENTRIES_DEFAULT = Path("backtest/journal/live_entries.csv")
AUDIT_DEFAULT = Path("backtest/journal/_audit.csv")


# ----------------------------
# Helpers
# ----------------------------
@st.cache_data(show_spinner=False)
def read_json(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def write_json(path: str, obj: dict) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


@st.cache_data(show_spinner=False)
def read_csv(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(p)
    except Exception:
        return pd.DataFrame()
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    return df


def candles_df(candles_last) -> pd.DataFrame:
    if candles_last is None:
        return pd.DataFrame()
    if isinstance(candles_last, dict) and "candles_last" in candles_last:
        candles_last = candles_last["candles_last"]
    if not isinstance(candles_last, list):
        return pd.DataFrame()
    df = pd.DataFrame(candles_last)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    return df


def ensure_symbol_table(status: dict) -> pd.DataFrame:
    per_symbol = status.get("per_symbol") or {}
    if isinstance(per_symbol, list):
        per_symbol = {p.get("symbol"): p for p in per_symbol if isinstance(p, dict) and p.get("symbol")}

    rows = []
    if isinstance(per_symbol, dict):
        for sym, payload in per_symbol.items():
            if not isinstance(payload, dict):
                continue
            ctx_last = payload.get("ctx_last") or {}
            rows.append(
                {
                    "symbol": sym,
                    "latest_ts": payload.get("latest_ts") or payload.get("last_ts"),
                    "event": payload.get("event"),
                    "regime": ctx_last.get("regime"),
                    "phase": ctx_last.get("last_phase"),
                    "sub": ctx_last.get("last_ctx_sub_label"),
                }
            )
    df = pd.DataFrame(rows)

    if df.empty:
        syms = status.get("symbols") or []
        df = pd.DataFrame({"symbol": syms})
        for c in ["latest_ts", "event", "regime", "phase", "sub"]:
            df[c] = None
    return df


def build_candlestick(df_c: pd.DataFrame, title: str):
    fig = go.Figure()
    if df_c.empty:
        return fig
    fig.add_trace(
        go.Candlestick(
            x=df_c["timestamp"],
            open=df_c.get("open"),
            high=df_c.get("high"),
            low=df_c.get("low"),
            close=df_c.get("close"),
            name="price",
        )
    )
    fig.update_layout(
        title=title,
        xaxis_title="time (UTC)",
        yaxis_title="price",
        height=520,
        margin=dict(l=10, r=10, t=35, b=10),
        xaxis_rangeslider_visible=False,
    )
    return fig


def overlay_ctx_markers(fig: go.Figure, df_c: pd.DataFrame):
    if df_c.empty or "ctx_sub_label" not in df_c.columns:
        return
    m = df_c["ctx_sub_label"].astype(str).str.startswith(("TDP_", "TTS_"), na=False)
    if not m.any():
        return
    dd = df_c[m].copy()
    fig.add_trace(
        go.Scatter(
            x=dd["timestamp"],
            y=dd["close"],
            mode="markers",
            name="ctx",
            text=dd["ctx_sub_label"],
            hovertemplate="%{x}<br>%{text}<br>close=%{y}<extra></extra>",
        )
    )


def overlay_live_entries(fig: go.Figure, df_c: pd.DataFrame, entries: pd.DataFrame, symbol: str, show_blocked: bool):
    if df_c.empty or entries.empty:
        return pd.DataFrame()

    if "symbol" not in entries.columns or "timestamp" not in entries.columns:
        return pd.DataFrame()

    df_e = entries[entries["symbol"].astype(str) == str(symbol)].copy()
    if df_e.empty:
        return df_e

    df_e["timestamp"] = pd.to_datetime(df_e["timestamp"], utc=True, errors="coerce")
    df_e = df_e.dropna(subset=["timestamp"]).copy()

    if "context_allow" in df_e.columns:
        allow = df_e["context_allow"].astype(str).str.lower().isin(["true", "1", "yes"])
        df_e["context_allow"] = allow
        if not show_blocked:
            df_e = df_e[df_e["context_allow"] == True].copy()

    if df_e.empty:
        return df_e

    t0, t1 = df_c["timestamp"].min(), df_c["timestamp"].max()
    df_e = df_e[(df_e["timestamp"] >= t0) & (df_e["timestamp"] <= t1)].copy()
    if df_e.empty:
        return df_e

    hover_cols = [c for c in ["timestamp", "model", "side", "entry", "sl", "tp", "rr", "phase", "ctx_sub_label", "block_reason"] if c in df_e.columns]
    hover_text = df_e[hover_cols].astype(str).agg(" | ".join, axis=1)

    def add_side(dd: pd.DataFrame, name: str, marker_symbol: str):
        if dd.empty:
            return
        y = dd["entry"] if "entry" in dd.columns else dd.get("close", pd.Series(index=dd.index, dtype=float))
        fig.add_trace(
            go.Scatter(
                x=dd["timestamp"],
                y=y,
                mode="markers",
                name=name,
                marker=dict(symbol=marker_symbol, size=11),
                text=hover_text.loc[dd.index],
                hovertemplate="%{text}<extra></extra>",
            )
        )

    if "side" in df_e.columns:
        df_long = df_e[df_e["side"].astype(str).str.upper() == "LONG"].copy()
        df_short = df_e[df_e["side"].astype(str).str.upper() == "SHORT"].copy()
        add_side(df_long, "LIVE LONG", "triangle-up")
        add_side(df_short, "LIVE SHORT", "triangle-down")
    else:
        add_side(df_e, "LIVE entries", "diamond")

    return df_e


def overlay_audit_entries(fig: go.Figure, df_c: pd.DataFrame, df_audit: pd.DataFrame, symbol: str):
    # hollow markers
    if df_c.empty or df_audit is None or df_audit.empty:
        return

    if "symbol" not in df_audit.columns:
        return

    df = df_audit[df_audit["symbol"].astype(str) == str(symbol)].copy()
    if df.empty:
        return

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        df = df.dropna(subset=["timestamp"])
    if df.empty:
        return

    t0, t1 = df_c["timestamp"].min(), df_c["timestamp"].max()
    df = df[(df["timestamp"] >= t0) & (df["timestamp"] <= t1)].copy()
    if df.empty:
        return

    if "entry" in df.columns:
        df["_y"] = pd.to_numeric(df["entry"], errors="coerce")
    else:
        df["_y"] = pd.to_numeric(df.get("close"), errors="coerce")
    df = df.dropna(subset=["_y"])
    if df.empty:
        return

    hover_cols = [c for c in ["timestamp", "model", "side", "entry", "sl", "tp", "rr", "ctx_sub_label", "block_reason"] if c in df.columns]
    hover_text = df[hover_cols].astype(str).agg(" | ".join, axis=1)

    fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["_y"],
            mode="markers",
            name="AUDIT entries",
            marker=dict(symbol="circle-open", size=10, line=dict(width=2)),
            text=hover_text,
            hovertemplate="%{text}<extra></extra>",
        )
    )


def as_float(x):
    try:
        return float(x)
    except Exception:
        return None


# ----------------------------
# UI
# ----------------------------
st.set_page_config(page_title="Live Bot Dashboard", layout="wide")
st.title("📈 Live Bot Dashboard")

with st.sidebar:
    st.header("Controls")

    status_path = st.text_input("Status path", value=str(STATUS_DEFAULT))
    controls_path = st.text_input("Controls path", value=str(CONTROLS_DEFAULT))
    entries_path = st.text_input("Live entries CSV path", value=str(ENTRIES_DEFAULT))

    st.markdown("### B1 — Live vs Audit compare")

    audit_entries_path = st.text_input("Audit entries CSV path", value=str(AUDIT_DEFAULT))
    show_audit_overlay = st.toggle("Show AUDIT overlay", value=False)

    st.divider()
    show_entries = st.toggle("Show LIVE entries overlay", value=True)
    show_ctx = st.toggle("Show ctx markers (TDP/TTS)", value=True)
    show_blocked_entries = st.toggle("Show blocked entries", value=True)

    st.divider()
    show_sl_tp = st.toggle("Show SL/TP lines (selected entry)", value=True)
    risk_multiplier = st.slider("Risk multiplier", 0.0, 3.0, 1.0, 0.05)
    freeze = st.toggle("Freeze new signals", value=False)
    notes = st.text_area("Notes", value="", height=100)

    if st.button("Save controls", type="primary"):
        write_json(
            controls_path,
            {
                "updated_at_utc": pd.Timestamp.utcnow().isoformat(),
                "risk_multiplier": float(risk_multiplier),
                "freeze_new_signals": bool(freeze),
                "notes": str(notes),
            },
        )
        st.success("Saved controls ✅")

    st.divider()
    auto = st.toggle("Auto refresh", value=False)
    refresh_sec = st.slider("Refresh seconds", 10, 300, 15, 5)
    if st.button("Refresh now"):
        st.rerun()

# Lightweight auto refresh
if auto:
    now = time.time()
    last = st.session_state.get("_last_rerun_ts", 0.0)
    if now - last >= float(refresh_sec):
        st.session_state["_last_rerun_ts"] = now
        st.rerun()

status = read_json(status_path) or {}
df_symbols = ensure_symbol_table(status)

top_cols = st.columns([2, 3])
with top_cols[0]:
    st.caption(f"Status file: `{status_path}`")
with top_cols[1]:
    st.caption(f"Last update: {status.get('updated_at_utc')} | Mode: {status.get('mode')}")

st.subheader("Symbols state")
st.dataframe(df_symbols, width="stretch", hide_index=True)

pick_options = df_symbols["symbol"].dropna().astype(str).tolist()
if not pick_options:
    st.warning("No symbols in status yet. Start live runner first.")
    st.stop()

pick = st.selectbox("Select symbol", pick_options, index=0)

per_symbol = status.get("per_symbol") or {}
if isinstance(per_symbol, list):
    per_symbol = {p.get("symbol"): p for p in per_symbol if isinstance(p, dict) and p.get("symbol")}
sym_state = per_symbol.get(pick, {}) if isinstance(per_symbol, dict) else {}

df_c = candles_df(sym_state.get("candles_last"))

col_chart, col_side = st.columns([2.2, 1.0], gap="large")

with col_chart:
    st.subheader(f"Chart: {pick}")
    if df_c.empty:
        st.info("No candles_last in status yet. Run live_signal_runner with --status_candles_n > 0.")
    else:
        fig = build_candlestick(df_c, title="")
        if show_ctx:
            overlay_ctx_markers(fig, df_c)

        df_entries = read_csv(entries_path)
        df_audit = read_csv(audit_entries_path) if show_audit_overlay else pd.DataFrame()

        df_e_used = pd.DataFrame()
        if show_entries:
            df_e_used = overlay_live_entries(fig, df_c, df_entries, pick, show_blocked_entries)

        if show_audit_overlay:
            overlay_audit_entries(fig, df_c, df_audit, pick)

        # A3-ish: select entry and draw SL/TP lines
        selected_entry = None
        if show_sl_tp and isinstance(df_e_used, pd.DataFrame) and not df_e_used.empty and "timestamp" in df_e_used.columns:
            dsel = df_e_used.copy()
            dsel = dsel.sort_values("timestamp")
            dsel["_label"] = (
                dsel["timestamp"].dt.strftime("%Y-%m-%d %H:%M")
                + " | "
                + dsel.get("side", "").astype(str)
                + " | "
                + dsel.get("model", "").astype(str)
            )
            labels = dsel["_label"].tolist()
            chosen = st.selectbox("Select entry to inspect (SL/TP lines)", labels, index=len(labels) - 1)
            selected_entry = dsel[dsel["_label"] == chosen].iloc[-1].to_dict()

        if show_sl_tp and isinstance(selected_entry, dict):
            y_entry = as_float(selected_entry.get("entry"))
            y_sl = as_float(selected_entry.get("sl"))
            y_tp = as_float(selected_entry.get("tp"))
            if y_entry is not None:
                fig.add_hline(y=y_entry, line_dash="dot", annotation_text="ENTRY", annotation_position="top left")
            if y_sl is not None:
                fig.add_hline(y=y_sl, line_dash="dash", annotation_text="SL", annotation_position="top left")
            if y_tp is not None:
                fig.add_hline(y=y_tp, line_dash="dash", annotation_text="TP", annotation_position="top left")

        if show_audit_overlay:
            try:
                live_n = int((df_entries["symbol"].astype(str) == str(pick)).sum()) if (not df_entries.empty and "symbol" in df_entries.columns) else 0
                audit_n = int((df_audit["symbol"].astype(str) == str(pick)).sum()) if (not df_audit.empty and "symbol" in df_audit.columns) else 0
                st.caption(f"B1 compare for {pick}: LIVE entries={live_n} | AUDIT entries={audit_n}")
            except Exception:
                pass

        # =========================
        # B2 — Why entry was blocked
        # =========================
        st.subheader("B2 — Why entry was blocked")


        def _safe_bool(v):
            if v is None:
                return None
            if isinstance(v, bool):
                return v
            s = str(v).strip().lower()
            if s in ("true", "1", "yes", "y"):
                return True
            if s in ("false", "0", "no", "n"):
                return False
            return None


        def _load_entries_csv(path: str):
            import pandas as pd
            from pathlib import Path

            if not path:
                return None
            p = Path(path)
            if not p.exists():
                return None
            try:
                df = pd.read_csv(p)
            except Exception:
                return None
            if df is None or df.empty:
                return df

            # normalize
            if "symbol" in df.columns:
                df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()

            # parse timestamps if present
            ts_col = None
            for c in ("timestamp", "ts", "time", "created_at", "entry_ts"):
                if c in df.columns:
                    ts_col = c
                    break
            if ts_col:
                try:
                    df[ts_col] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
                except Exception:
                    pass

            return df


        # map to your dashboard variables
        audit_csv_path = audit_entries_path
        live_csv_path = entries_path

        audit_df = _load_entries_csv(audit_csv_path)
        live_df = _load_entries_csv(live_csv_path)

        # filter by selected symbol
        audit_sym = audit_df[audit_df["symbol"] == pick].copy() if (
                    audit_df is not None and not audit_df.empty and "symbol" in audit_df.columns) else None
        live_sym = live_df[live_df["symbol"] == pick].copy() if (
                    live_df is not None and not live_df.empty and "symbol" in live_df.columns) else None


        def _blocked_view(df):
            """Return blocked subset + reason counts.
            Blocked definition:
              - block_reason exists and is not empty
              - OR context_allow exists and is False
            """
            import pandas as pd
            if df is None or df.empty:
                return None, None

            d = df.copy()

            # block_reason normalization
            if "block_reason" in d.columns:
                d["block_reason"] = d["block_reason"].astype(str).replace({"nan": "", "None": ""}).str.strip()
            else:
                d["block_reason"] = ""

            # context_allow normalization if exists
            if "context_allow" in d.columns:
                d["_context_allow_bool"] = d["context_allow"].apply(_safe_bool)
            else:
                d["_context_allow_bool"] = None

            blocked_mask = (d["block_reason"] != "") | (d["_context_allow_bool"] == False)  # noqa: E712
            blocked = d[blocked_mask].copy()
            if blocked.empty:
                return blocked, pd.Series(dtype=int)

            # pick a "reason" field
            blocked["_reason"] = blocked["block_reason"]
            # if no block_reason but context_allow False -> label it
            blocked.loc[(blocked["_reason"] == "") & (
                        blocked["_context_allow_bool"] == False), "_reason"] = "context_allow=False"

            counts = blocked["_reason"].value_counts(dropna=False)
            return blocked, counts


        audit_blocked, audit_counts = _blocked_view(audit_sym)
        live_blocked, live_counts = _blocked_view(live_sym)

        # metrics
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("AUDIT entries", 0 if audit_sym is None else len(audit_sym))
        col2.metric("AUDIT blocked", 0 if audit_blocked is None else len(audit_blocked))
        col3.metric("LIVE entries", 0 if live_sym is None else len(live_sym))
        col4.metric("LIVE blocked", 0 if live_blocked is None else len(live_blocked))

        # show top reasons
        with st.expander("Top block reasons (AUDIT / LIVE)", expanded=True):
            c1, c2 = st.columns(2)

            with c1:
                st.caption("AUDIT reasons")
                if audit_counts is None or len(audit_counts) == 0:
                    st.write("No blocked entries found in AUDIT for this symbol.")
                else:
                    st.dataframe(audit_counts.reset_index().rename(
                        columns={"index": "reason", 0: "count", "count": "count"}), width="stretch")

            with c2:
                st.caption("LIVE reasons")
                if live_counts is None or len(live_counts) == 0:
                    st.write("No blocked entries found in LIVE for this symbol (or no columns).")
                else:
                    st.dataframe(live_counts.reset_index().rename(
                        columns={"index": "reason", 0: "count", "count": "count"}), width="stretch")

        # show blocked tables (last N)
        show_n = st.slider("Show last N blocked rows", 10, 500, 50, 10, key="b2_last_n")


        def _show_blocked_table(title, blocked_df):
            st.caption(title)
            if blocked_df is None or blocked_df.empty:
                st.write("—")
                return

            cols_pref = [
                "timestamp", "ts", "time",
                "model", "side", "entry", "sl", "tp", "rr",
                "ctx_sub_label", "phase", "regime",
                "context_allow", "macro_allow", "news_allow", "liq_allow",
                "macro_reason", "news_reason",
                "liq_bias", "liq_risk_multiplier",
                "block_reason",
                "_reason",
            ]
            cols = [c for c in cols_pref if c in blocked_df.columns]
            view = blocked_df[cols].tail(show_n).copy() if cols else blocked_df.tail(show_n).copy()
            st.dataframe(view, width="stretch")


        c1, c2 = st.columns(2)
        with c1:
            _show_blocked_table("AUDIT blocked (last N)", audit_blocked)
        with c2:
            _show_blocked_table("LIVE blocked (last N)", live_blocked)


        st.plotly_chart(fig, width="stretch")

with col_side:
    st.subheader("Live trace")
    st.json(sym_state.get("trace", {}), expanded=False)

    st.subheader("Context raw (ctx_last)")
    st.json(sym_state.get("ctx_last", {}), expanded=False)

    with st.expander("Raw symbol payload", expanded=False):
        st.json(sym_state, expanded=False)

with st.expander("Raw status JSON", expanded=False):
    st.json(status, expanded=False)