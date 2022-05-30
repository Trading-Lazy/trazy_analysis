import json
from datetime import timedelta
from typing import List

import numpy as np

from trazy_analysis.common.crypto_exchange_calendar import CryptoExchangeCalendar
from trazy_analysis.common.helper import fill_missing_datetimes
from trazy_analysis.common.types import CandleDataFrame
from trazy_analysis.market_data.common import datetime_from_epoch
from trazy_analysis.market_data.data_handler import DataHandler
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle


class KucoinDataHandler(DataHandler):
    BASE_URL_API = "https://api.kucoin.com/api/v1"
    BASE_URL_GET_TICKERS_LIST = BASE_URL_API + "/symbols"
    TICKER_DATA_RESPONSE_USED_COLUMNS = [
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    TICKER_DATA_RESPONSE_USED_COLUMNS_DTYPE = {
        "open": str,
        "high": str,
        "low": str,
        "close": str,
        "volume": int,
    }
    API_TOKEN = "NO-TOKEN-FOR-PUBLIC-DATA"
    MAX_CALLS = 1800
    PERIOD = 60
    TICKER_DATA_TIMEZONE = "UTC"
    MARKET_CAL = CryptoExchangeCalendar()

    @classmethod
    def ticker_data_is_none(cls, data: str) -> bool:
        raw_candles_dict = json.loads(data)
        if "data" not in raw_candles_dict:
            return True
        raw_candles = raw_candles_dict["data"]
        return len(raw_candles) == 0

    @classmethod
    def parse_get_tickers_response(cls, tickers_response: str) -> List[str]:
        symbols_dict = json.loads(tickers_response)
        symbols_info = symbols_dict["data"]
        symbols_with_hyphen = [symbol_info["symbol"] for symbol_info in symbols_info]
        symbols = [symbol.replace("-", "") for symbol in symbols_with_hyphen]
        return symbols

    @classmethod
    def ticker_data_to_dataframe(cls, asset: Asset, data: str) -> CandleDataFrame:
        raw_candles_dict = json.loads(data)
        raw_candles = raw_candles_dict["data"]
        candles = np.array(
            [
                Candle(asset=asset, open=float(raw_candle[1]), high=float(raw_candle[3]), low=float(raw_candle[4]),
                       close=float(raw_candle[2]), volume=float(raw_candle[6]),
                       timestamp=datetime_from_epoch(int(raw_candle[0]) * 1000))
                for raw_candle in raw_candles
            ]
        )
        candle_df = CandleDataFrame.from_candle_list(asset=asset, candles=candles)
        candle_df = fill_missing_datetimes(df=candle_df, time_unit=timedelta(minutes=1))
        candle_df = CandleDataFrame.from_dataframe(candle_df, asset)
        return candle_df
