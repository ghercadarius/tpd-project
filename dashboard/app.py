"""Streamlit dashboard — Brand Crisis Monitor."""
from __future__ import annotations

import os
import time

import pandas as pd
import requests
import streamlit as st

API = os.environ.get("DASHBOARD_API", "http://localhost:8000")

st.set_page_config(page_title="Brand Crisis Monitor", layout="wide")
st.title("Reddit Brand Crisis Monitor")


# ---------------------------------------------------------------------------
# Data fetchers (cached per TTL)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=15)
def _brands() -> list[str]:
    try:
        return requests.get(f"{API}/brands", timeout=5).json() or []
    except Exception:
        return []


@st.cache_data(ttl=15)
def _aggregates(brand: str, minutes: int) -> pd.DataFrame:
    try:
        r = requests.get(f"{API}/aggregates", params={"brand": brand, "minutes": minutes}, timeout=10)
        r.raise_for_status()
        df = pd.DataFrame(r.json())
        if not df.empty:
            df["window_start"] = pd.to_datetime(df["window_start"], utc=True)
        return df
    except Exception as exc:
        st.error(f"Could not load aggregates: {exc}")
        return pd.DataFrame()


@st.cache_data(ttl=10)
def _alerts(active: bool) -> pd.DataFrame:
    try:
        r = requests.get(f"{API}/alerts", params={"active": str(active).lower(), "limit": 50}, timeout=10)
        r.raise_for_status()
        df = pd.DataFrame(r.json())
        if not df.empty:
            df["triggered_at"] = pd.to_datetime(df["triggered_at"], utc=True)
        return df
    except Exception as exc:
        st.error(f"Could not load alerts: {exc}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

st.sidebar.header("Controls")
all_brands = _brands()
brand = st.sidebar.selectbox("Brand", all_brands if all_brands else ["(none)"])
window = st.sidebar.slider("Lookback (minutes)", min_value=15, max_value=1440, value=120, step=15)
auto_refresh = st.sidebar.toggle("Auto-refresh (15 s)", value=True)

if auto_refresh:
    time.sleep(0)  # yield to Streamlit; rerun triggered by st.rerun below

# ---------------------------------------------------------------------------
# Active alerts
# ---------------------------------------------------------------------------

st.subheader("Active alerts (last hour)")
adf = _alerts(active=True)
if adf.empty:
    st.info("No active alerts.")
else:
    _SEVERITY_ICON = {"low": "🟡", "medium": "🟠", "high": "🔴", "critical": "🚨"}
    for _, row in adf.head(10).iterrows():
        icon = _SEVERITY_ICON.get(row["severity"], "⚠️")
        st.markdown(
            f"{icon} **{row['brand']}** — z={row['z_score']:.2f}, "
            f"neg_ratio={row['neg_ratio']:.2f}, volume={row['volume']} "
            f"@ {row['triggered_at']}"
        )
        if row.get("sample_text"):
            st.caption(row["sample_text"])

# ---------------------------------------------------------------------------
# Brand trend charts
# ---------------------------------------------------------------------------

st.subheader(f"Trend for `{brand}`")
if not all_brands:
    st.warning("No data yet — start the producer and Flink job first.")
else:
    df = _aggregates(brand, window)
    if df.empty:
        st.info("No aggregates for this brand/window yet.")
    else:
        col1, col2 = st.columns(2)
        with col1:
            st.line_chart(df.set_index("window_start")[["volume", "neg_count"]],
                          use_container_width=True)
        with col2:
            st.line_chart(df.set_index("window_start")[["neg_ratio", "avg_neg_prob"]],
                          use_container_width=True)
        st.caption(f"{len(df)} windows over the last {window} minutes.")

# ---------------------------------------------------------------------------
# All-brand alert table
# ---------------------------------------------------------------------------

st.subheader("Recent alerts (all brands)")
recent = _alerts(active=False)
if not recent.empty:
    st.dataframe(
        recent[["triggered_at", "brand", "severity", "z_score", "neg_ratio", "volume", "sample_text"]],
        use_container_width=True,
        hide_index=True,
    )

# Auto-refresh: trigger a rerun after 15 s.
if auto_refresh:
    time.sleep(15)
    st.rerun()
