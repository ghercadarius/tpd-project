"""Streamlit dashboard for brand crisis monitoring."""
from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
import requests
import streamlit as st

API = os.environ.get("DASHBOARD_API", "http://localhost:8000")

st.set_page_config(page_title="Brand Crisis Monitor", layout="wide")
st.title("Reddit Brand Crisis Monitor")


@st.cache_data(ttl=15)
def _brands() -> list[str]:
    try:
        return requests.get(f"{API}/brands", timeout=5).json() or []
    except Exception:
        return []


@st.cache_data(ttl=15)
def _aggregates(brand: str, minutes: int) -> pd.DataFrame:
    r = requests.get(f"{API}/aggregates", params={"brand": brand, "minutes": minutes}, timeout=10)
    r.raise_for_status()
    df = pd.DataFrame(r.json())
    if not df.empty:
        df["window_start"] = pd.to_datetime(df["window_start"])
    return df


@st.cache_data(ttl=10)
def _alerts(active: bool) -> pd.DataFrame:
    r = requests.get(f"{API}/alerts", params={"active": str(active).lower(), "limit": 50}, timeout=10)
    r.raise_for_status()
    df = pd.DataFrame(r.json())
    if not df.empty:
        df["triggered_at"] = pd.to_datetime(df["triggered_at"])
    return df


# --- Sidebar -------------------------------------------------------------- #
st.sidebar.header("Controls")
all_brands = _brands()
brand = st.sidebar.selectbox("Brand", all_brands or ["(none)"])
window = st.sidebar.slider("Lookback (minutes)", 15, 24 * 60, 120, step=15)
auto_refresh = st.sidebar.checkbox("Auto-refresh (every 15s)", value=True)
if auto_refresh:
    st.sidebar.caption("Page reruns automatically.")
    st.experimental_set_query_params(t=int(datetime.utcnow().timestamp()) // 15)

# --- Active alerts -------------------------------------------------------- #
st.subheader("Active alerts (last hour)")
adf = _alerts(active=True)
if adf.empty:
    st.info("No active alerts.")
else:
    severity_color = {"low": "🟡", "medium": "🟠", "high": "🔴", "critical": "🚨"}
    for _, row in adf.head(10).iterrows():
        emoji = severity_color.get(row["severity"], "⚠️")
        st.markdown(
            f"{emoji} **{row['brand']}** — z={row['z_score']:.2f}, "
            f"neg_ratio={row['neg_ratio']:.2f}, volume={row['volume']} "
            f"@ {row['triggered_at']:%Y-%m-%d %H:%M:%S}"
        )
        if row.get("sample_text"):
            st.caption(row["sample_text"])

# --- Trends --------------------------------------------------------------- #
st.subheader(f"Trend for `{brand}`")
if not all_brands:
    st.warning("No brand data yet. Start the producers and Flink job.")
else:
    df = _aggregates(brand, window)
    if df.empty:
        st.info("No aggregates yet for this brand and window.")
    else:
        c1, c2 = st.columns(2)
        with c1:
            st.line_chart(df.set_index("window_start")[["volume", "neg_count"]])
        with c2:
            st.line_chart(df.set_index("window_start")[["neg_ratio", "avg_neg_prob"]])
        st.caption(f"{len(df)} aggregate windows over the last {window} min.")

# --- Recent alerts table -------------------------------------------------- #
st.subheader("Recent alerts (all brands)")
recent = _alerts(active=False)
if not recent.empty:
    st.dataframe(recent, use_container_width=True, hide_index=True)
