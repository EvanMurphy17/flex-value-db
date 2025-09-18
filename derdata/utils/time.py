# derdata/utils/time.py
from __future__ import annotations

from typing import cast
import pandas as pd


def ensure_utc_dtindex(idx: pd.Index) -> pd.DatetimeIndex:
    """
    Coerce any pandas Index into a tz-aware UTC DatetimeIndex.
    Using 'cast' keeps static type checkers (Pylance) happy.
    """
    di = cast(pd.DatetimeIndex, pd.DatetimeIndex(idx))
    if di.tz is None:
        di = di.tz_localize("UTC")
    else:
        di = di.tz_convert("UTC")
    return di
