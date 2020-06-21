import abc
import requests
from datetime import date, timedelta
from historical_data.common import ENCODING, STATUS_CODE_OK
from pandas.core.groupby import DataFrameGroupBy
from typing import List, Tuple


class HistoricalDataApiAccess:
    base_url_get_tickers = ""
    base_url_ticker = ""
    earliest_available_date_for_download = date(1970, 1, 1)
    max_download_frame = timedelta(days=1)

    @classmethod
    @abc.abstractmethod
    def generate_url(cls, ticker: str, period: Tuple[date, date]) -> str:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def parse_tickers_response(cls, tickers_response: str) -> List[str]:
        raise NotImplementedError

    @abc.abstractmethod
    def parse_ticker_data(self, data: str) -> DataFrameGroupBy:
        raise NotImplementedError

    @classmethod
    def get_tickers(cls) -> List[str]:
        with requests.Session() as session:
            response = session.get(cls.base_url_get_tickers)
            if response.status_code == STATUS_CODE_OK:
                tickers_response = response.content.decode(ENCODING)
                return cls.parse_tickers_response(tickers_response)
            else:
                return []
