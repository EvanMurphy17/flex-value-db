# derdata/utils/dates.py
from datetime import datetime, timedelta
from typing import Iterator, Tuple

from dateutil.relativedelta import relativedelta

DATE_FMT = "%Y%m%d"


def yyyymmdd(dt: datetime) -> str:
    return dt.strftime(DATE_FMT)


def month_chunks(start: datetime, end: datetime) -> Iterator[Tuple[datetime, datetime]]:
    """
    Yield (month_start, month_end) pairs from start..end inclusive.
    """
    cur = datetime(start.year, start.month, 1)
    stop = datetime(end.year, end.month, 1)
    while cur <= stop:
        nxt = cur + relativedelta(months=1) - timedelta(days=1)
        yield cur, min(nxt, end)
        cur = cur + relativedelta(months=1)
