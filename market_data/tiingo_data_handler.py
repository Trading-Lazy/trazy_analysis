import io
import json
from typing import List

import pandas as pd

from common.types import CandleDataFrame
from common.utils import lists_equal
from market_data.data_handler import DataHandler
from settings import TIINGO_API_TOKEN


class TiingoDataHandler(DataHandler):
    BASE_URL_API = "https://api.tiingo.com/iex"
    BASE_URL_GET_TICKERS_LIST = BASE_URL_API + "?token={}"
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
    API_TOKEN = TIINGO_API_TOKEN
    MAX_CALLS = 500
    PERIOD = 3600
    TICKER_DATA_TIMEZONE = "UTC"

    @classmethod
    def ticker_data_is_none(cls, data: str) -> bool:
        columns_line = data.partition("\n")[0]
        columns = list(columns_line.split(","))
        return not lists_equal(columns, cls.TICKER_DATA_RESPONSE_USED_COLUMNS)

    @classmethod
    def parse_get_tickers_response(cls, tickers_response: str) -> List[str]:
        raw_tickers = json.loads(tickers_response)
        tickers = [raw_ticker["ticker"] for raw_ticker in raw_tickers]
        return tickers

    @classmethod
    def ticker_data_to_dataframe(cls, symbol: str, data: str) -> CandleDataFrame:
        df = pd.read_csv(
            io.StringIO(data.strip()),
            sep=",",
            parse_dates=True,
            usecols=cls.TICKER_DATA_RESPONSE_USED_COLUMNS,
            dtype=cls.TICKER_DATA_RESPONSE_USED_COLUMNS_DTYPE,
            index_col="date",
        )
        df.index = pd.to_datetime(df.index, utc=True)
        candle_df = CandleDataFrame.from_dataframe(df, symbol)
        return candle_df
