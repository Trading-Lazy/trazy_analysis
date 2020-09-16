from datetime import date, timedelta
from typing import Tuple

from market_data.historical.historical_data_handler import HistoricalDataHandler
from market_data.tiingo_data_handler import TiingoDataHandler


class TiingoHistoricalDataHandler(TiingoDataHandler, HistoricalDataHandler):
    BASE_URL_HISTORICAL_TICKER_DATA = (
        TiingoDataHandler.BASE_URL_API + "/{}/prices?"
        "startDate={}&"
        "endDate={}&"
        "format=csv&columns=open,high,low,close,volume&"
        "resampleFreq=1min&"
        "token={}"
    )
    EARLIEST_AVAILABLE_DATE_FOR_DOWNLOAD = date(2018, 7, 1)
    MAX_DOWNLOAD_FRAME = timedelta(days=30)

    @classmethod
    def generate_ticker_data_url(cls, ticker: str, period: Tuple[date, date]) -> str:
        ticker_url = cls.BASE_URL_HISTORICAL_TICKER_DATA.format(
            ticker, period[0], period[1], cls.API_TOKEN
        )
        return ticker_url
