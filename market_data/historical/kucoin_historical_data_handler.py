from datetime import date, datetime, timedelta, timezone
from typing import Tuple

from common.helper import map_ticker_to_kucoin_symbol
from market_data.historical.historical_data_handler import HistoricalDataHandler
from market_data.kucoin_data_handler import KucoinDataHandler
from models.asset import Asset


class KucoinHistoricalDataHandler(KucoinDataHandler, HistoricalDataHandler):
    BASE_URL_HISTORICAL_TICKER_DATA = (
        KucoinDataHandler.BASE_URL_API + "/market/candles?"
        "symbol={}&"
        "type=1min&"
        "startAt={}&"
        "endAt={}"
    )
    EARLIEST_AVAILABLE_DATE_FOR_DOWNLOAD = date(2017, 10, 25)
    MAX_DOWNLOAD_FRAME = timedelta(days=1)

    @classmethod
    def generate_ticker_data_url(
        cls, ticker: Asset, period: Tuple[datetime, datetime]
    ) -> str:
        start_date = period[0]
        start_datetime = datetime(
            year=start_date.year,
            month=start_date.month,
            day=start_date.day,
            hour=0,
            minute=0,
            second=0,
            tzinfo=timezone.utc,
        )
        end_date = period[1]
        end_datetime = datetime(
            year=end_date.year,
            month=end_date.month,
            day=end_date.day,
            hour=23,
            minute=59,
            second=59,
            tzinfo=timezone.utc,
        )
        start_epoch = int(start_datetime.timestamp())
        end_epoch = int(end_datetime.timestamp())
        kucoin_ticker = map_ticker_to_kucoin_symbol(ticker.symbol)
        ticker_url = cls.BASE_URL_HISTORICAL_TICKER_DATA.format(
            kucoin_ticker, start_epoch, end_epoch, cls.API_TOKEN
        )
        return ticker_url
