from datetime import datetime, timedelta, timezone
from typing import List

from market_data.live.live_data_handler import LiveDataHandler
from market_data.tiingo_crypto_data_handler import TiingoCryptoDataHandler
from models.candle import Candle


class TiingoLiveCryptoDataHandler(TiingoCryptoDataHandler, LiveDataHandler):
    BASE_URL_TICKER_LATEST_DATA = (
        TiingoCryptoDataHandler.BASE_URL_API + "/prices?tickers={}&"
        "startDate={}&"
        "endDate={}&"
        "resampleFreq=1min&"
        "token={}"
    )

    @classmethod
    def parse_ticker_latest_data(cls, symbol: str, data: str) -> List[Candle]:
        candle_df = cls.ticker_data_to_dataframe(symbol, data)
        return candle_df.to_candles()

    @classmethod
    def generate_ticker_latest_data_url(cls, ticker: str) -> str:
        date_today = datetime.now(timezone.utc).date()
        date_tomorrow = date_today + timedelta(days=1)
        return cls.BASE_URL_TICKER_LATEST_DATA.format(
            ticker, date_today, date_tomorrow, cls.API_TOKEN
        )
