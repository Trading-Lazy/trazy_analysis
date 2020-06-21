import io
import json
import pandas as pd
from datetime import date, timedelta, datetime
from historical_data.common import DATE_DIR_FORMAT
from historical_data.historical_data_api_access import HistoricalDataApiAccess
from pandas.core.groupby import DataFrameGroupBy
from settings import TIINGO_API_TOKEN
from typing import List, Tuple


class TiingoApiAccess(HistoricalDataApiAccess):
    base_url_get_tickers = (
        "https://api.tiingo.com/iex?" "token=c1215f14491ad1c57f4421bfcc7030c9d26d5f28"
    )
    base_url_ticker = (
        "https://api.tiingo.com/iex/{}/prices?"
        "startDate={}&"
        "endDate={}&"
        "format=csv&columns=open,high,low,close,volume&"
        "resampleFreq=1min&"
        "token={}"
    )
    earliest_available_date_for_download = date(2020, 6, 17)
    max_download_frame = timedelta(days=30)

    @classmethod
    def generate_url(cls, ticker: str, period: Tuple[date, date]) -> str:
        return cls.base_url_ticker.format(
            ticker, period[0], period[1], TIINGO_API_TOKEN
        )

    @classmethod
    def parse_tickers_response(cls, tickers_response: str) -> List[str]:
        raw_tickers = json.loads(tickers_response)
        tickers = [raw_ticker["ticker"] for raw_ticker in raw_tickers]
        return tickers

    def extract_date(self, raw_date: str) -> str:
        date = datetime.strptime(raw_date, "%Y-%m-%d")
        return date.strftime(DATE_DIR_FORMAT)

    def process_row(self, df, ind):
        candle_date = df["date"].loc[ind]
        return self.extract_date(candle_date[0:10])

    def parse_ticker_data(self, data: str) -> DataFrameGroupBy:
        df = pd.read_csv(
            io.StringIO(data),
            sep=",",
            parse_dates=True,
            usecols=["date", "open", "high", "low", "close", "volume"],
        )
        return df.groupby(lambda ind: self.process_row(df, ind))
