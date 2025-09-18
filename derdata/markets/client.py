# derdata/markets/pjm/client.py

from typing import Optional
import os

import pandas as pd
import requests
from dotenv import load_dotenv
from gridstatusio import GridStatusClient

load_dotenv()

_GLOBAL_API_KEY = os.getenv("GRIDSTATUS_API_KEY")
_gs_client = GridStatusClient(api_key=_GLOBAL_API_KEY)


class PJMClient:
    _MARKET_DATASET = {
        "DA": "pjm_lmp_day_ahead_hourly",
        # "RT": "pjm_lmp_real_time_hourly",  # add when needed
    }

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or _GLOBAL_API_KEY
        if not self.api_key:
            raise ValueError("API key is required (set GRIDSTATUS_API_KEY or pass api_key).")

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "der-value-db"})
        self.client = GridStatusClient(api_key=self.api_key)

    def lmp_hourly(
        self,
        market: str,
        start: str,
        end: str,
        location_value: str,
        timezone: str = "market",
    ) -> pd.DataFrame:
        m = market.upper()
        dataset_id = self._MARKET_DATASET.get(m)
        if not dataset_id:
            raise ValueError(f"Unsupported market '{market}'. Known: {list(self._MARKET_DATASET)}")

        df: pd.DataFrame = self.client.get_dataset(
            dataset_id,
            start=f"{start}T00:00:00Z",
            end=f"{end}T23:59:59Z",
            filter_column="location",
            filter_value=location_value,
            timezone=timezone,
        )

        for col in ("interval_start_utc", "interval_end_utc"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], utc=True)

        if "lmp" in df.columns:
            df["lmp"] = pd.to_numeric(df["lmp"], errors="coerce")

        return df
