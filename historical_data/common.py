import os
import settings
from logger import logger
from pathlib import Path

NONE_DIR = "NONE"
ERROR_DIR = "ERROR"
DONE_DIR = "DONE"
TICKERS_DIR = "tickers"
ENCODING = "utf8"
STATUS_CODE_OK = 200
DATE_DIR_FORMAT = "%Y%m%d"
LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)


def concat_path(base_path: str, date_str) -> str:
    path = Path(base_path) / date_str
    return str(path)
