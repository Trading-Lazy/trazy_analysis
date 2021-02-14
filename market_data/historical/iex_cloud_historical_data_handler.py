import io
import json
import sys
from datetime import date, datetime, timedelta
from typing import Tuple

import numpy as np
import pandas as pd

from common.types import CandleDataFrame
from market_data.historical.historical_data_handler import HistoricalDataHandler
from settings import IEX_CLOUD_API_TOKEN


class IexCloudHistoricalDataHandler(HistoricalDataHandler):
    BASE_URL_API = "https://cloud.iexapis.com/stable/"
    BASE_URL_GET_TICKERS_LIST = BASE_URL_API + "ref-data/iex/symbols?token={}"
    BASE_URL_HISTORICAL_TICKER_DATA = (
        BASE_URL_API + "stock/{}/chart/date/{}?format=csv&token={" "}&chartIEXOnly=true"
    )
    EARLIEST_AVAILABLE_DATE_FOR_DOWNLOAD = datetime.today().date() - timedelta(days=30)
    MAX_DOWNLOAD_FRAME = timedelta(days=1)
    TICKER_DATA_RESPONSE_USED_COLUMNS = [
        "date",
        "minute",
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
    API_TOKEN = IEX_CLOUD_API_TOKEN
    MAX_CALLS = sys.maxsize
    PERIOD = sys.maxsize
    TICKER_DATA_TIMEZONE = "US/Eastern"

    @classmethod
    def generate_ticker_data_url(cls, ticker: str, period: Tuple[date, date]) -> str:
        date_str = period[0].strftime("%Y%m%d")
        ticker_url = cls.BASE_URL_HISTORICAL_TICKER_DATA.format(
            ticker, date_str, cls.API_TOKEN
        )
        return ticker_url

    @classmethod
    def parse_get_tickers_response(cls, tickers_response: str) -> np.array:  # [str]
        raw_tickers = json.loads(tickers_response)
        tickers = np.array(
            [raw_ticker["symbol"] for raw_ticker in raw_tickers], dtype="U6"
        )
        return tickers

    @classmethod
    def ticker_data_is_none(cls, data: str) -> bool:
        return data == ""

    @classmethod
    def ticker_data_to_dataframe(cls, symbol: str, data: str) -> CandleDataFrame:
        dateparser = lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M")
        df = pd.read_csv(
            io.StringIO(data),
            sep=",",
            parse_dates={"Date": ["date", "minute"]},
            usecols=cls.TICKER_DATA_RESPONSE_USED_COLUMNS,
            dtype=cls.TICKER_DATA_RESPONSE_USED_COLUMNS_DTYPE,
            date_parser=dateparser,
            index_col=0,
        )
        df.index = df.index.tz_localize(cls.TICKER_DATA_TIMEZONE)
        df.index.names = ["timestamp"]
        candle_df = CandleDataFrame.from_dataframe(df, symbol)
        return candle_df
