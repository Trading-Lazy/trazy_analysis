import os
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_UP
from typing import List, Tuple

import numpy as np
import pytz

import trazy_analysis.settings
from trazy_analysis.common.utils import timestamp_to_utc
from trazy_analysis.logger import logger

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
)


def get_intraday_periods(
    download_frame: timedelta,
    start: datetime,
    end: datetime = datetime.now(pytz.UTC),
):
    now = datetime.now(pytz.UTC)
    end = min(end, now)
    nb_seconds = (end - start).total_seconds() + 1
    nb_periods_decimal = Decimal.from_float(nb_seconds) / Decimal.from_float(
        download_frame.total_seconds()
    )
    nb_periods = int(nb_periods_decimal.quantize(exp=1, rounding=ROUND_UP))
    periods = np.array(
        [
            (
                start + i * download_frame,
                start + (i + 1) * download_frame - timedelta(seconds=1),
            )
            for i in range(0, nb_periods)
        ]
    )
    if len(periods) > 0:
        periods[-1] = (periods[-1][0], end)
    return periods


def get_daily_periods(
    download_frame: timedelta,
    start: datetime,
    end: datetime = datetime.now(pytz.UTC),
):
    start_date = start.date()
    date_today = date.today()
    end_date = min(end.date(), date_today)
    nb_days = (end_date - start_date).days + 1
    nb_periods_decimal = Decimal.from_float(nb_days) / Decimal.from_float(
        download_frame.days
    )
    nb_periods = int(nb_periods_decimal.quantize(exp=1, rounding=ROUND_UP))
    periods = np.array(
        [
            (
                start_date + i * download_frame,
                start_date + (i + 1) * download_frame - timedelta(days=1),
            )
            for i in range(0, nb_periods)
        ]
    )
    if len(periods) > 0:
        periods[-1] = (periods[-1][0], end_date)
    return periods


def get_periods(
    download_frame: timedelta,
    start: datetime,
    end: datetime = datetime.now(pytz.UTC),
) -> list[Tuple[datetime, datetime]]:
    if download_frame >= timedelta(days=1):
        download_frame = timedelta(days=download_frame.days)
        return get_daily_periods(download_frame, start, end)
    else:
        return get_intraday_periods(download_frame, start, end)


def datetime_from_epoch(epoch_ms: int) -> datetime:
    epoch = epoch_ms / 1000
    return timestamp_to_utc(datetime.utcfromtimestamp(epoch))
