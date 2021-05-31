import json
from datetime import timedelta
from typing import List

import numpy as np

from common.crypto_stock_exchange_calendar import CryptoStockExchangeCalendar
from common.helper import fill_missing_datetimes
from common.types import CandleDataFrame
from market_data.common import datetime_from_epoch
from market_data.data_handler import DataHandler
from models.asset import Asset
from models.candle import Candle
from settings import BINANCE_API_KEY


class BinanceDataHandler(DataHandler):
    BASE_URL_API = "https://api.binance.com/api/v3"
    BASE_URL_GET_TICKERS_LIST = BASE_URL_API + "/exchangeInfo"
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
    API_TOKEN = BINANCE_API_KEY
    MAX_CALLS = 1200
    PERIOD = 60
    TICKER_DATA_TIMEZONE = "UTC"
    MARKET_CAL = CryptoStockExchangeCalendar()

    @classmethod
    def ticker_data_is_none(cls, data: str) -> bool:
        return data == "[]"

    @classmethod
    def parse_get_tickers_response(cls, tickers_response: str) -> List[str]:
        binance_exchange_info = json.loads(tickers_response)
        symbols_dicts = binance_exchange_info["symbols"]
        symbols = [symbol_dict["symbol"] for symbol_dict in symbols_dicts]
        return symbols

    @classmethod
    def ticker_data_to_dataframe(cls, asset: Asset, data: str) -> CandleDataFrame:
        raw_candles = json.loads(data)
        candles = np.array(
            [
                Candle(
                    asset=asset,
                    open=float(raw_candle[1]),
                    high=float(raw_candle[2]),
                    low=float(raw_candle[3]),
                    close=float(raw_candle[4]),
                    volume=float(raw_candle[5]),
                    timestamp=datetime_from_epoch(raw_candle[0]),
                )
                for raw_candle in raw_candles
            ]
        )
        candle_df = CandleDataFrame.from_candle_list(asset=asset, candles=candles)
        candle_df = fill_missing_datetimes(df=candle_df, time_unit=timedelta(minutes=1))
        candle_df = CandleDataFrame.from_dataframe(candle_df, asset)
        return candle_df
