import os
from datetime import date, timedelta
from typing import Tuple

import settings
from logger import logger
from market_data.historical.historical_data_handler import HistoricalDataHandler
from market_data.tiingo_crypto_data_handler import TiingoCryptoDataHandler

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)


class TiingoHistoricalCryptoDataHandler(TiingoCryptoDataHandler, HistoricalDataHandler):
    BASE_URL_HISTORICAL_TICKER_DATA = (
        TiingoCryptoDataHandler.BASE_URL_API + "/prices?tickers={}&"
        "startDate={}&"
        "endDate={}&"
        "resampleFreq=1min&"
        "token={}"
    )
    EARLIEST_AVAILABLE_DATE_FOR_DOWNLOAD = date(2017, 12, 13)
    MAX_DOWNLOAD_FRAME = timedelta(days=3)

    @classmethod
    def generate_ticker_data_url(cls, ticker: str, period: Tuple[date, date]) -> str:
        ticker_url = cls.BASE_URL_HISTORICAL_TICKER_DATA.format(
            ticker, period[0], period[1] + timedelta(days=1), cls.API_TOKEN
        )
        return ticker_url
