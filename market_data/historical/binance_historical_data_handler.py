from datetime import date, datetime, timedelta
from typing import Tuple

from trazy_analysis.market_data.binance_data_handler import BinanceDataHandler
from trazy_analysis.market_data.historical.historical_data_handler import (
    HistoricalDataHandler,
)
from trazy_analysis.models.asset import Asset


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
        cls, ticker: Asset, period: Tuple[datetime, datetime]
    ) -> str:
        start_epoch = int(period[0].timestamp()) * 1000
        end_epoch = int(period[1].timestamp()) * 1000
        ticker_url = cls.BASE_URL_HISTORICAL_TICKER_DATA.format(
            ticker.symbol, start_epoch, end_epoch, cls.API_TOKEN
        )
        return ticker_url
