import json
from typing import List

import pandas as pd

from trazy_analysis.common.types import CandleDataFrame
from trazy_analysis.market_data.data_handler import DataHandler
from trazy_analysis.settings import TIINGO_API_TOKEN


class TiingoCryptoDataHandler(DataHandler):
    BASE_URL_API = "https://api.tiingo.com/tiingo/crypto"
    # Tiingo doesn't have an url to get the list of all crypto pairs so we use the one of binance
    BASE_URL_GET_TICKERS_LIST = "https://api.binance.com/api/v3/exchangeInfo"
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
        "volume": float,
    }
    API_TOKEN = TIINGO_API_TOKEN
    MAX_CALLS = 500
    PERIOD = 3600
    TICKER_DATA_TIMEZONE = "UTC"

    @classmethod
    def ticker_data_is_none(cls, data: str) -> bool:
        ticker_price_data_list = json.loads(data)
        if not ticker_price_data_list:
            return True
        ticker_price_data_dict = ticker_price_data_list[0]
        ticker_price_data = ticker_price_data_dict["priceData"]
        return not ticker_price_data

    @classmethod
    def parse_get_tickers_response(cls, tickers_response: str) -> list[str]:
        binance_exchange_info = json.loads(tickers_response)
        symbols_dicts = binance_exchange_info["symbols"]
        symbols = [symbol_dict["symbol"] for symbol_dict in symbols_dicts]
        return symbols

    @classmethod
    def ticker_data_to_dataframe(cls, symbol: str, data: str) -> CandleDataFrame:
        ticker_price_data_list = json.loads(data)
        ticker_price_data_dict = ticker_price_data_list[0]
        ticker_price_data = ticker_price_data_dict["priceData"]
        df = pd.DataFrame(
            data=ticker_price_data,
            columns=cls.TICKER_DATA_RESPONSE_USED_COLUMNS,
        )
        df = df.astype(
            dtype=cls.TICKER_DATA_RESPONSE_USED_COLUMNS_DTYPE,
        )
        df = df.set_index("date")
        df.index = pd.to_datetime(df.index, utc=True)
        candle_df = CandleDataFrame.from_dataframe(df, symbol)
        return candle_df
