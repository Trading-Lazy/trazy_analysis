import os
from abc import ABCMeta

from pathlib import Path
from ratelimit import limits, sleep_and_retry

import settings
from logger import logger

NONE_DIR = "NONE"
ERROR_DIR = "ERROR"
DONE_DIR = "DONE"
TICKERS_DIR = "tickers"
ENCODING = "utf8"
DATE_DIR_FORMAT = "%Y%m%d"
LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)
PATH_SEPARATOR = "/"
DATASETS_DIR = "datasets"


def concat_path(base_path: str, complement: str) -> str:
    if not base_path or not complement:
        return base_path + complement

    path = Path(base_path) / complement
    return str(path)


class RateLimitedSingletonMeta(ABCMeta):
    instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls.instances:
            cls.instances[cls] = super().__call__(*args, **kwargs)
            cls.request = sleep_and_retry(
                limits(calls=cls.MAX_CALLS, period=cls.PERIOD)(cls.request)
            )
        return cls.instances[cls]
