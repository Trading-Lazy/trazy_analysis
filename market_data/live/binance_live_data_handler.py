from datetime import datetime, timezone
from typing import List

from trazy_analysis.market_data.binance_data_handler import BinanceDataHandler
from trazy_analysis.market_data.live.live_data_handler import LiveDataHandler
from trazy_analysis.models.candle import Candle


class BinanceLiveDataHandler(BinanceDataHandler, LiveDataHandler):
    BASE_URL_TICKER_LATEST_DATA = (
        BinanceDataHandler.BASE_URL_API + "/klines?"
        "symbol={}&"
        "interval=1m&"
        "startTime={}&"
        "endTime={}&"
        "limit=1000"
    )

    @classmethod
    def parse_ticker_latest_data(cls, symbol: str, data: str) -> list[Candle]:
        candle_df = cls.ticker_data_to_dataframe(symbol, data)
        return candle_df.to_candles()

    @classmethod
    def generate_ticker_latest_data_url(cls, ticker: str) -> str:
        now = datetime.now(timezone.utc)
        now_epoch = int(now.timestamp()) * 1000
        THIRTY_MINUTES_IN_MILLISECONDS = 30 * 60 * 1000
        ticker_url = cls.BASE_URL_TICKER_LATEST_DATA.format(
            ticker.replace("/", ""),
            now_epoch - THIRTY_MINUTES_IN_MILLISECONDS,
            now_epoch,
            cls.API_TOKEN,
        )
        return ticker_url
