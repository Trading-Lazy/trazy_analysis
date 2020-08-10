import io
import json
import sys
from datetime import date, datetime, timedelta
from typing import List, Tuple

import pandas as pd
from pandas.core.frame import DataFrame

from historical_data.historical_data_api_access import HistoricalDataApiAccess
from settings import IEX_CLOUD_API_TOKEN


class IexCloudApiAccess(HistoricalDataApiAccess):
    BASE_URL_GET_TICKERS = (
        "https://cloud.iexapis.com/stable/ref-data/iex/symbols?token={}"
    )
    BASE_URL_TICKER = "https://cloud.iexapis.com/stable/stock/{}/chart/date/{}?format=csv&token={}&chartIEXOnly=true"
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
    API_TOKEN = IEX_CLOUD_API_TOKEN
    MAX_CALLS = sys.maxsize
    PERIOD = sys.maxsize

    @classmethod
    def generate_ticker_url(cls, ticker: str, period: Tuple[date, date]) -> str:
        date_str = period[0].strftime("%Y%m%d")
        ticker_url = cls.BASE_URL_TICKER.format(ticker, date_str, cls.API_TOKEN)
        return ticker_url

    @classmethod
    def parse_get_tickers_response(cls, tickers_response: str) -> List[str]:
        raw_tickers = json.loads(tickers_response)
        tickers = [raw_ticker["symbol"] for raw_ticker in raw_tickers]
        return tickers

    @classmethod
    def ticker_data_is_none(cls, data: str) -> bool:
        return data == ""

    @classmethod
    def ticker_data_to_dataframe(cls, data: str) -> DataFrame:
        dateparser = lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M")
        df = pd.read_csv(
            io.StringIO(data),
            sep=",",
            parse_dates={"Date": ["date", "minute"]},
            usecols=cls.TICKER_DATA_RESPONSE_USED_COLUMNS,
            date_parser=dateparser,
            index_col=0,
        )
        df.index.names = ["date"]
        df.index = df.index.tz_localize("US/Eastern").tz_convert("UTC")
        return df
