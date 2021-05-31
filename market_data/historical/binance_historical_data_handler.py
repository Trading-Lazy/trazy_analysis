from datetime import date, timedelta, datetime
from typing import Tuple

from market_data.historical.historical_data_handler import HistoricalDataHandler
from market_data.binance_data_handler import BinanceDataHandler


class BinanceHistoricalDataHandler(BinanceDataHandler, HistoricalDataHandler):
    BASE_URL_HISTORICAL_TICKER_DATA = (
        BinanceDataHandler.BASE_URL_API + "/klines?"
        "symbol={}&"
        "interval=1m&"
        "startTime={}&"
        "endTime={}&"
        "limit=1000"
    )
    EARLIEST_AVAILABLE_DATE_FOR_DOWNLOAD = date(2017, 1, 1)
    MAX_DOWNLOAD_FRAME = timedelta(hours=16, minutes=40)

    @classmethod
    def generate_ticker_data_url(
        cls, ticker: str, period: Tuple[datetime, datetime]
    ) -> str:
        start_epoch = int(period[0].timestamp()) * 1000
        end_epoch = int(period[1].timestamp()) * 1000
        ticker_url = cls.BASE_URL_HISTORICAL_TICKER_DATA.format(
            ticker, start_epoch, end_epoch, cls.API_TOKEN
        )
        return ticker_url
