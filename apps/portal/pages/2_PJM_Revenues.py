# apps/portal/pages/2_PJM_Revenues.py
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd
import streamlit as st

# --- make repo importable ----------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from derdata.markets.client import PJMClient

# ---------------------------- helpers ----------------------------------------

def _interval_hours(df: pd.DataFrame) -> pd.Series:
    if not {"interval_start_utc", "interval_end_utc"} <= set(df.columns):
        return pd.Series([], dtype=float)
    start = pd.to_datetime(df["interval_start_utc"], utc=True)
    end = pd.to_datetime(df["interval_end_utc"], utc=True)
    return (end - start).dt.total_seconds() / 3600.0


def _constant_mw_to_mwh(df: pd.DataFrame, mw: float) -> pd.Series:
    return _interval_hours(df) * float(mw)


def _preview_table(df: pd.DataFrame) -> pd.DataFrame:
    cols = [c for c in (
        "start_local", "interval_start_utc", "interval_end_utc",
        "location", "location_short_name", "location_type",
        "lmp"
    ) if c in df.columns]
    return df[cols].head(48)


def _csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def _uniq_sorted(series: pd.Series) -> list[str]:
    return sorted([str(x) for x in series.dropna().unique() if str(x).strip() != ""])


def _safe_selectbox(label: str, options: Sequence[str], key: str, disabled: bool = False) -> Optional[str]:
    """
    Streamlit selectbox helper:
    - If options is empty, show a disabled placeholder and return None.
    - Otherwise return the selected string (first by default).
    """
    if not options:
        st.sidebar.selectbox(label, ["(no options)"], index=0, key=key, disabled=True)
        return None
    return st.sidebar.selectbox(label, options, index=0, key=key)


# ------------------------------- UI ------------------------------------------

st.title("PJM Market Revenues (GridStatus Datasets)")
st.caption(
    "Powered by GridStatus.io datasets. We query hourly LMP datasets and compute "
    "simple energy revenues for constant MW participation."
)

# API key (optional if set in env)
api_key_input = st.sidebar.text_input("GridStatus API key (optional if set in env)", type="password")
api_key: Optional[str] = api_key_input or os.getenv("GRIDSTATUS_API_KEY") or None

# Date range
start_date = st.sidebar.date_input("Start date (market time)", pd.to_datetime("2025-06-01"))
end_date   = st.sidebar.date_input("End date (market time)",   pd.to_datetime("2025-06-02"))
start_str = pd.to_datetime(start_date).strftime("%Y-%m-%d")
end_str   = pd.to_datetime(end_date).strftime("%Y-%m-%d")

# Pricing node catalog (data/raw/pjm/pnode.csv)
pnode_file = PROJECT_ROOT / "data" / "raw" / "pjm" / "pnode.csv"
pnode_df = pd.read_csv(pnode_file)

# Dependent selectors: type -> subtype -> node
type_opts = _uniq_sorted(pnode_df["pnode_type"])
node_type = _safe_selectbox("Select Pricing Node Type", type_opts, key="sel_type")

subtype_opts: list[str] = []
if node_type:
    subtype_opts = _uniq_sorted(pnode_df.loc[pnode_df["pnode_type"] == node_type, "pnode_subtype"])

subtype = _safe_selectbox("Select Pnode Subtype", subtype_opts, key="sel_subtype")

node_opts: list[str] = []
if node_type and subtype:
    mask = (pnode_df["pnode_type"] == node_type) & (pnode_df["pnode_subtype"] == subtype)
    node_opts = _uniq_sorted(pnode_df.loc[mask, "pnode_name"])

node = _safe_selectbox("Select Pricing Node", node_opts, key="sel_node")

# Participation assumptions
st.sidebar.markdown("### Participation assumptions")
da_mw = st.sidebar.number_input("DA energy MW (export + / import -)", value=1.0, step=0.1)

# Client
pjm = PJMClient(api_key=api_key)

# ------------------------- Day-Ahead Energy ----------------------------------
with st.expander("Day-Ahead Energy (Hourly LMP)", expanded=False):
    if st.button("Fetch DA LMP & Compute"):
        # Guard: require a valid node selection
        if not node or pd.isna(node):
            st.warning("Please select a valid Pricing Node (type → subtype → node) and try again.")
            st.stop()

        try:
            # fetch
            da_df = pjm.lmp_hourly(
                market="DA",
                start=start_str,
                end=end_str,
                location_value=node,  # string guaranteed by guard above
                timezone="market",
            )

            if da_df.empty:
                st.warning("No rows returned. Try a different node or date range.")
            else:
                da_df = da_df.copy()
                da_df["hours"] = _interval_hours(da_df)
                da_df["mwh_at_const_mw"] = _constant_mw_to_mwh(da_df, da_mw)
                da_df["revenue_$"] = da_df["lmp"].astype(float) * da_df["mwh_at_const_mw"].astype(float)

                total_rev = float(da_df["revenue_$"].sum())
                total_mwh = float(da_df["mwh_at_const_mw"].sum())
                st.metric("DA Energy Revenue ($)", f"{total_rev:,.2f}")
                st.metric("Total MWh (assumed)", f"{total_mwh:,.3f}")

                st.subheader("Preview")
                st.dataframe(_preview_table(da_df), use_container_width=True)

                st.subheader("Full result")
                st.dataframe(da_df, use_container_width=True)

                c1, c2 = st.columns(2)
                with c1:
                    st.download_button(
                        "Download DA result (CSV)",
                        data=_csv_bytes(da_df),
                        file_name=f"pjm_da_lmp_{node}_{start_str}_to_{end_str}.csv",
                        mime="text/csv",
                    )
                with c2:
                    out = da_df.loc[:, ["interval_start_utc", "interval_end_utc", "mwh_at_const_mw"]]
                    out = out.rename(columns={"mwh_at_const_mw": "mwh"})
                    st.download_button(
                        "Download DA MWh profile (CSV)",
                        data=_csv_bytes(out),
                        file_name=f"pjm_da_mwh_{node}_{start_str}_to_{end_str}.csv",
                        mime="text/csv",
                    )

        except Exception as e:
            st.error(f"DA fetch failed: {e}")
