from datetime import datetime, timedelta, timezone
from typing import List

from trazy_analysis.market_data.live.live_data_handler import LiveDataHandler
from trazy_analysis.market_data.tiingo_crypto_data_handler import (
    TiingoCryptoDataHandler,
)
from trazy_analysis.models.candle import Candle


class TiingoLiveCryptoDataHandler(TiingoCryptoDataHandler, LiveDataHandler):
    BASE_URL_TICKER_LATEST_DATA = (
        TiingoCryptoDataHandler.BASE_URL_API + "/prices?tickers={}&"
        "startDate={}&"
        "endDate={}&"
        "resampleFreq=1min&"
        "token={}"
    )

    @classmethod
    def parse_ticker_latest_data(cls, symbol: str, data: str) -> list[Candle]:
        candle_df = cls.ticker_data_to_dataframe(symbol, data)
        return candle_df.to_candles()

    @classmethod
    def generate_ticker_latest_data_url(cls, ticker: str) -> str:
        date_today = datetime.now(timezone.utc).date()
        date_tomorrow = date_today + timedelta(days=1)
        return cls.BASE_URL_TICKER_LATEST_DATA.format(
            ticker, date_today, date_tomorrow, cls.API_TOKEN
        )
