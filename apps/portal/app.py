# apps/portal/app.py
from __future__ import annotations

import sys
from pathlib import Path
import streamlit as st

# make repo importable
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

st.set_page_config(page_title="Flex Value Stack Portal", layout="wide")

st.title("Flex Value Stack Portal")
st.caption("One app, multiple pages: DSIRE program browser and PJM market revenues.")

st.markdown(
    """
### What’s inside

- **DSIRE Programs** — browse incentives nationwide, filter, and export.
- **PJM Revenues** — sandbox to estimate revenues for energy, regulation, reserves, and capacity.

Use the left sidebar to navigate between pages.
"""
)
