from __future__ import annotations
import time
from datetime import datetime
from typing import Any, Dict

import requests

DSIRE_BASE = "http://programs.dsireusa.org/api/v1"

class DsireClient:
    """
    Thin client for the DSIRE Programs API

    Endpoints documented on DSIRE Data and Tools page
    GET {base}/getprograms/json
    GET {base}/getprogramsbydate/{YYYYMMDD}/{YYYYMMDD}/json
    """
    def __init__(self, base_url: str = DSIRE_BASE, timeout: int = 60, max_retries: int = 3, backoff_sec: float = 2.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_sec = backoff_sec
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "der-value-stack/0.1"})

    def _request(self, url: str) -> Dict[str, Any] | list:
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, timeout=self.timeout)
                resp.raise_for_status()
                return resp.json()
            except Exception:
                if attempt == self.max_retries:
                    raise
                time.sleep(self.backoff_sec * attempt)

    def get_programs_all(self) -> Dict[str, Any] | list:
        url = f"{self.base_url}/getprograms/json"
        return self._request(url)

    def get_programs_by_date(self, start_yyyymmdd: str, end_yyyymmdd: str) -> Dict[str, Any] | list:
        datetime.strptime(start_yyyymmdd, "%Y%m%d")
        datetime.strptime(end_yyyymmdd, "%Y%m%d")
        url = f"{self.base_url}/getprogramsbydate/{start_yyyymmdd}/{end_yyyymmdd}/json"
        return self._request(url)
