# --- make project root importable (so "derdata" is found) ---
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]  # .../flex-value-db
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# apps/dsire_browser/app.py
from typing import List, Tuple
import re

import pandas as pd
import streamlit as st

from derdata.dsire.parse import build_tables

PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "dsire"


@st.cache_data(show_spinner=False)
def list_versions() -> List[str]:
    if not RAW_DIR.exists():
        return []
    return sorted([p.name for p in RAW_DIR.iterdir() if p.is_dir()])


@st.cache_data(show_spinner=True)
def load_processed_or_build(version_tag: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    progs_pq = PROCESSED_DIR / f"programs_{version_tag}.parquet"
    params_pq = PROCESSED_DIR / f"parameters_{version_tag}.parquet"
    progs_csv = PROCESSED_DIR / f"programs_{version_tag}.csv"
    params_csv = PROCESSED_DIR / f"parameters_{version_tag}.csv"

    if progs_pq.exists() and params_pq.exists():
        return pd.read_parquet(progs_pq), pd.read_parquet(params_pq)
    if progs_csv.exists() and params_csv.exists():
        return pd.read_csv(progs_csv), pd.read_csv(params_csv)

    programs_df, parameters_df = build_tables(version_tag, PROJECT_ROOT)
    return programs_df, parameters_df


st.set_page_config(page_title="DSIRE Program Browser", layout="wide")
st.title("DSIRE Program Browser")

versions = list_versions()
if not versions:
    st.info("No DSIRE data found. Expected files under data/raw/dsire/<version_tag>.")
    st.stop()

version = st.sidebar.selectbox("Snapshot version", options=versions, index=len(versions) - 1)
st.sidebar.caption("Pick the folder under data/raw/dsire containing dsire_programs_*.json.gz")

programs_df, parameters_df = load_processed_or_build(version)
df = programs_df.copy()

if df.empty:
    st.warning("No records loaded for this version.")
    st.stop()

st.markdown(f"**Loaded {len(df):,} records from**  `{version}`")

# -------------------- filters --------------------
states = sorted([s for s in df["state"].dropna().unique()]) if "state" in df.columns else []
state_sel = st.sidebar.multiselect("Filter states", states)

types_ = sorted([t for t in df["type_name"].dropna().unique()]) if "type_name" in df.columns else []
type_sel = st.sidebar.multiselect("Filter program type", types_)

tech_tokens: List[str] = []
if "technologies" in df.columns:
    tokens = (
        df["technologies"]
        .dropna()
        .astype(str)
        .str.split(r"\s*;\s*")
        .explode()
        .dropna()
        .unique()
        .tolist()
    )
    tech_tokens = sorted(tokens)
tech_sel = st.sidebar.multiselect("Filter technology (token contains)", tech_tokens)

q = st.sidebar.text_input("Search name/admin/url/incentive text")

fdf = df.copy()

if state_sel and "state" in fdf.columns:
    fdf = fdf[fdf["state"].isin(state_sel)]

if type_sel and "type_name" in fdf.columns:
    fdf = fdf[fdf["type_name"].isin(type_sel)]

if tech_sel and "technologies" in fdf.columns:
    mask = pd.Series(False, index=fdf.index)
    for token in tech_sel:
        mask |= fdf["technologies"].fillna("").str.contains(fr"\b{re.escape(token)}\b", case=False, regex=True)
    fdf = fdf[mask]

if q:
    search_cols = [
        c
        for c in [
            "program_name",
            "administrator",
            "website_url",
            "incentive_text",
            "max_incentive_text",
            "equipment_requirements",
            "installation_requirements",
            "eligibility_text",
            "rec_ownership_text",
        ]
        if c in fdf.columns
    ]
    if search_cols:
        mask = pd.Series(False, index=fdf.index)
        for c in search_cols:
            mask |= fdf[c].fillna("").str.contains(q, case=False, na=False)
        fdf = fdf[mask]

# -------------------- table --------------------
show_cols = [
    "state",
    "program_name",
    "category_name",
    "type_name",
    "administrator",
    "technologies",
    "technology_categories",
    "sectors",
    "utilities",
    "utilities_eia_ids",
    "start_date",
    "end_date",
    "last_updated",
    "website_url",
]
show_cols = [c for c in show_cols if c in fdf.columns]

st.subheader("Programs")
st.caption("Use sidebar filters and search. Download your filtered view below.")
st.dataframe(fdf[show_cols], use_container_width=True, hide_index=True)

# -------------------- details + parameters --------------------
st.subheader("Record details")

if len(fdf) > 0:
    idx = st.number_input("Row index", min_value=0, max_value=len(fdf) - 1, value=0, step=1)
    idx = int(idx)
    rec = fdf.iloc[idx]

    st.caption(f"Selected: program_id={rec.get('program_id')}, name={rec.get('program_name')}, state={rec.get('state')}")

    detail_cols = [
        "program_id",
        "program_code",
        "program_name",
        "state",
        "administrator",
        "implementing_sector_name",
        "category_name",
        "type_name",
        "website_url",
        "funding_source",
        "budget_text",
        "start_date",
        "end_date",
        "last_updated",
        "technologies",
        "technology_categories",
        "sectors",
        "utilities",
        "utilities_eia_ids",
        "incentive_text",
        "max_incentive_text",
        "equipment_requirements",
        "installation_requirements",
        "eligibility_text",
        "rec_ownership_text",
    ]
    detail_cols = [c for c in detail_cols if c in rec.index]

    with st.expander("Selected record (program fields)", expanded=True):
        st.write(rec[detail_cols])

    pid = rec.get("program_id")
    st.markdown("**Parameters (numeric, machine-usable)**")
    p = parameters_df[parameters_df["program_id"] == pid]
    if len(p) == 0:
        st.info("No parameter rows found for this program.")
    else:
        ordered = [c for c in ["source", "tech", "sector", "qualifier", "amount", "units", "notes"] if c in p.columns]
        st.dataframe(p[ordered], use_container_width=True, hide_index=True)

    def df_to_csv_bytes(xdf: pd.DataFrame) -> bytes:
        return xdf.to_csv(index=False).encode("utf-8")

    st.download_button(
        "Download filtered programs as CSV",
        data=df_to_csv_bytes(fdf[[c for c in fdf.columns if c != "_raw"]]),
        file_name=f"programs_{version}_filtered.csv",
        mime="text/csv",
    )

    if len(p) > 0:
        st.download_button(
            "Download parameters for selected program as CSV",
            data=df_to_csv_bytes(p),
            file_name=f"parameters_{str(pid)}_{version}.csv",
            mime="text/csv",
        )
else:
    st.info("No rows in the filtered view.")

with st.expander("Data completeness (null %)"):
    null_pct = (fdf.isna().mean().sort_values(ascending=False) * 100).round(1)
    st.dataframe(null_pct.to_frame("null_percent"))
