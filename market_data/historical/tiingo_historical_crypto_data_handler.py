import os
from datetime import date, timedelta
from typing import Tuple

import trazy_analysis.settings
from trazy_analysis.logger import logger
from trazy_analysis.market_data.historical.historical_data_handler import (
    HistoricalDataHandler,
)
from trazy_analysis.market_data.tiingo_crypto_data_handler import (
    TiingoCryptoDataHandler,
)
from trazy_analysis.models.asset import Asset

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
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
    def generate_ticker_data_url(cls, ticker: Asset, period: Tuple[date, date]) -> str:
        ticker_url = cls.BASE_URL_HISTORICAL_TICKER_DATA.format(
            ticker.symbol.replace("/", ""),
            period[0],
            period[1] + timedelta(days=1),
            cls.API_TOKEN,
        )
        return ticker_url
