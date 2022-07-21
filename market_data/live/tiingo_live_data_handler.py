from datetime import date
from typing import List

from trazy_analysis.market_data.live.live_data_handler import LiveDataHandler
from trazy_analysis.market_data.tiingo_data_handler import TiingoDataHandler
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle


class TiingoLiveDataHandler(TiingoDataHandler, LiveDataHandler):
    BASE_URL_TICKER_LATEST_DATA = (
        TiingoDataHandler.BASE_URL_API + "/{}/prices?"
        "startDate={}&"
        "endDate={}&"
        "format=csv&columns=open,high,low,close,volume&"
        "resampleFreq=1min&"
        "token={}"
    )

    @classmethod
    def parse_ticker_latest_data(cls, asset: Asset, data: str) -> list[Candle]:
        candle_df = cls.ticker_data_to_dataframe(asset, data)
        return candle_df.to_candles()

    @classmethod
    def generate_ticker_latest_data_url(cls, ticker: Asset) -> str:
        date_today = date.today()
        return cls.BASE_URL_TICKER_LATEST_DATA.format(
            ticker.symbol, date_today, date_today, cls.API_TOKEN
        )
