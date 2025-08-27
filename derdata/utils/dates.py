from __future__ import annotations
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Iterable, Tuple

DATE_FMT = "%Y%m%d"

def yyyymmdd(dt: datetime) -> str:
    return dt.strftime(DATE_FMT)

def month_chunks(start: datetime, end: datetime) -> Iterable[Tuple[datetime, datetime]]:
    cur = datetime(start.year, start.month, 1)
    stop = datetime(end.year, end.month, 1)
    while cur <= stop:
        nxt = cur + relativedelta(months=1) - timedelta(days=1)
        yield cur, min(nxt, end)
        cur = cur + relativedelta(months=1)
