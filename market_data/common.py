import os
from datetime import date, datetime, timedelta, timezone
from typing import List, Tuple

import numpy as np

import settings
from logger import logger

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)


def get_periods(
    download_frame: timedelta,
    start: datetime,
    end: datetime = datetime.now(timezone.utc),
) -> List[Tuple[datetime, datetime]]:
    start_date = start.date()
    date_today = date.today()
    end_date = min(end.date(), date_today)
    nb_days = (end_date - start_date).days + 1
    periods = np.array(
        [
            (
                start_date + timedelta(days=i),
                start_date + timedelta(days=i - 1) + download_frame,
            )
            for i in range(
                0,
                nb_days,
                download_frame.days,
            )
        ]
    )
    if len(periods) > 0:
        periods[-1] = (periods[-1][0], end_date)
    return periods
