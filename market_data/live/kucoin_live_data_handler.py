from datetime import datetime, timezone
from typing import List

from trazy_analysis.common.helper import map_ticker_to_kucoin_symbol
from trazy_analysis.market_data.kucoin_data_handler import KucoinDataHandler
from trazy_analysis.market_data.live.live_data_handler import LiveDataHandler
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle


class KucoinLiveDataHandler(KucoinDataHandler, LiveDataHandler):
    BASE_URL_TICKER_LATEST_DATA = (
        KucoinDataHandler.BASE_URL_API + "/market/candles?"
        "symbol={}&"
        "type=1min&"
        "startAt={}&"
        "endAt={}"
    )

    @classmethod
    def parse_ticker_latest_data(cls, asset: Asset, data: str) -> List[Candle]:
        candle_df = cls.ticker_data_to_dataframe(asset, data)
        return candle_df.to_candles()

    @classmethod
    def generate_ticker_latest_data_url(cls, ticker: Asset) -> str:
        now = datetime.now(timezone.utc)
        now_epoch = int(now.timestamp())
        THIRTY_MINUTES_IN_SECONDS = 30 * 60
        kucoin_ticker = map_ticker_to_kucoin_symbol(ticker.symbol)
        ticker_url = cls.BASE_URL_TICKER_LATEST_DATA.format(
            kucoin_ticker,
            now_epoch - THIRTY_MINUTES_IN_SECONDS,
            now_epoch,
            cls.API_TOKEN,
        )
        return ticker_url
