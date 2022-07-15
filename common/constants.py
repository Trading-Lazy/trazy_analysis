from datetime import datetime

from pandas_market_calendars.exchange_calendar_eurex import EUREXExchangeCalendar
from pandas_market_calendars.exchange_calendar_iex import IEXExchangeCalendar

from trazy_analysis.common.crypto_exchange_calendar import CryptoExchangeCalendar
from trazy_analysis.common.utils import timestamp_to_utc

ENCODING = "utf8"
DATE_DIR_FORMAT = "%Y%m%d"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
DEGIRO_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
CONNECTION_ERROR_MESSAGE = (
    "Connection error, the exception is: %s. The traceback is: %s"
)
MAX_TIMESTAMP = timestamp_to_utc(datetime.max)
NONE_API_KEYS = {
    "key": None,
    "secret": None,
    "password": None,
}

MARKET_CAL = {
    "binance": CryptoExchangeCalendar(),
    "euronext": EUREXExchangeCalendar(),
    "iex": IEXExchangeCalendar(),
}
