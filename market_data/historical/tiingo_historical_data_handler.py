from datetime import date, timedelta
from typing import Tuple

from trazy_analysis.market_data.historical.historical_data_handler import (
    HistoricalDataHandler,
)
from trazy_analysis.market_data.tiingo_data_handler import TiingoDataHandler
from trazy_analysis.models.asset import Asset


class TiingoHistoricalDataHandler(TiingoDataHandler, HistoricalDataHandler):
    BASE_URL_HISTORICAL_TICKER_DATA = (
        TiingoDataHandler.BASE_URL_API + "/{}/prices?"
        "startDate={}&"
        "endDate={}&"
        "format=csv&columns=open,high,low,close,volume&"
        "resampleFreq=1min&"
        "token={}"
    )
    EARLIEST_AVAILABLE_DATE_FOR_DOWNLOAD = date(2017, 3, 7)
    MAX_DOWNLOAD_FRAME = timedelta(days=30)

    @classmethod
    def generate_ticker_data_url(cls, ticker: Asset, period: Tuple[date, date]) -> str:
        ticker_url = cls.BASE_URL_HISTORICAL_TICKER_DATA.format(
            ticker.symbol, period[0], period[1], cls.API_TOKEN
        )
        return ticker_url
