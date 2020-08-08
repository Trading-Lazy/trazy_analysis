import io
import json
from datetime import date, timedelta
from typing import List, Tuple

import pandas as pd
from pandas.core.frame import DataFrame

from common.utils import lists_equal
from historical_data.historical_data_api_access import HistoricalDataApiAccess
from settings import TIINGO_API_TOKEN


class TiingoApiAccess(HistoricalDataApiAccess):
    BASE_URL_GET_TICKERS = "https://api.tiingo.com/iex?token={}"
    BASE_URL_TICKER = (
        "https://api.tiingo.com/iex/{}/prices?"
        "startDate={}&"
        "endDate={}&"
        "format=csv&columns=open,high,low,close,volume&"
        "resampleFreq=1min&"
        "token={}"
    )
    EARLIEST_AVAILABLE_DATE_FOR_DOWNLOAD = date(2018, 7, 1)
    MAX_DOWNLOAD_FRAME = timedelta(days=30)
    TICKER_DATA_RESPONSE_USED_COLUMNS = [
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    API_TOKEN = TIINGO_API_TOKEN
    MAX_CALLS = 500
    PERIOD = 3600

    @classmethod
    def generate_ticker_url(cls, ticker: str, period: Tuple[date, date]) -> str:
        ticker_url = cls.BASE_URL_TICKER.format(
            ticker, period[0], period[1], cls.API_TOKEN
        )
        return ticker_url

    @classmethod
    def parse_get_tickers_response(cls, tickers_response: str) -> List[str]:
        raw_tickers = json.loads(tickers_response)
        tickers = [raw_ticker["ticker"] for raw_ticker in raw_tickers]
        return tickers

    @classmethod
    def ticker_data_is_none(cls, data: str) -> bool:
        columns_line = data.partition("\n")[0]
        columns = list(filter(None, columns_line.split(",")))
        return not lists_equal(columns, cls.TICKER_DATA_RESPONSE_USED_COLUMNS)

    @classmethod
    def ticker_data_to_dataframe(cls, data: str) -> DataFrame:
        df = pd.read_csv(
            io.StringIO(data.strip()),
            sep=",",
            parse_dates=True,
            usecols=cls.TICKER_DATA_RESPONSE_USED_COLUMNS,
            index_col=0,
        )
        df.index = df.index.tz_convert("UTC")
        return df
