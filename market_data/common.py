import os
from abc import ABCMeta
from datetime import date, timedelta
from typing import List, Tuple

import pandas as pd
from ratelimit import limits, sleep_and_retry

import settings
from common.helper import request
from logger import logger

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)


class RateLimitedSingletonMeta(ABCMeta):
    instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls.instances:
            cls.instances[cls] = super().__call__(*args, **kwargs)
        return cls.instances[cls]

    def __init__(cls, cls_name, bases, namespace, **kwds):
        if not isinstance(cls.MAX_CALLS, property) and not isinstance(
            cls.PERIOD, property
        ):
            setattr(
                cls,
                "request",
                sleep_and_retry(
                    limits(calls=cls.MAX_CALLS, period=cls.PERIOD)(request)
                ),
            )
        super().__init__(cls_name, bases, namespace, **kwds)


def get_periods(
    download_frame: timedelta,
    start: pd.Timestamp,
    end: pd.Timestamp = pd.Timestamp.now(tz="UTC"),
) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    start_date = start.date()
    date_today = date.today()
    end_date = min(end.date(), date_today)
    nb_days = (end_date - start_date).days + 1
    periods = [
        (
            start_date + timedelta(days=i),
            start_date + timedelta(days=i - 1) + download_frame,
        )
        for i in range(0, nb_days, download_frame.days,)
    ]
    if len(periods) > 0:
        periods[-1] = (periods[-1][0], end_date)
    return periods
