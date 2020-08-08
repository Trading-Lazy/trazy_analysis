import abc
from datetime import date, timedelta
from typing import List, Tuple

import pandas as pd
import pytz
import requests
from pandas import DataFrame
from pandas.core.groupby.generic import DataFrameGroupBy
from requests.models import Response

from common.utils import validate_dataframe_columns
from historical_data.common import ENCODING, LOG, RateLimitedSingletonMeta


class HistoricalDataApiAccess(metaclass=RateLimitedSingletonMeta):
    expected_columns = ["open", "high", "low", "close", "volume"]

    # properties
    @property
    @classmethod
    @abc.abstractmethod
    def BASE_URL_GET_TICKERS(cls) -> str:
        pass

    @property
    @classmethod
    @abc.abstractmethod
    def BASE_URL_TICKER(cls) -> str:
        pass

    @property
    @classmethod
    @abc.abstractmethod
    def EARLIEST_AVAILABLE_DATE_FOR_DOWNLOAD(cls) -> date:
        pass

    @property
    @classmethod
    @abc.abstractmethod
    def MAX_DOWNLOAD_FRAME(cls) -> timedelta:
        pass

    @property
    @classmethod
    @abc.abstractmethod
    def TICKER_DATA_RESPONSE_USED_COLUMNS(cls) -> List[str]:
        pass

    @property
    @classmethod
    @abc.abstractmethod
    def API_TOKEN(cls) -> str:
        pass

    @property
    @classmethod
    @abc.abstractmethod
    def MAX_CALLS(cls) -> int:
        pass

    @property
    @classmethod
    @abc.abstractmethod
    def PERIOD(cls) -> int:
        pass

    # methods
    @classmethod
    @abc.abstractmethod
    def generate_ticker_url(cls, ticker: str, period: Tuple[date, date]) -> str:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def parse_get_tickers_response(cls, tickers_response: str) -> List[str]:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def ticker_data_is_none(cls, data: str) -> bool:
        raise NotImplementedError

    @classmethod
    def process_row(cls, df: DataFrame, candle_date: pd.Timestamp) -> str:
        return candle_date.strftime("%Y%m%d")

    @classmethod
    @abc.abstractmethod
    def ticker_data_to_dataframe(cls, data: str) -> DataFrame:
        raise NotImplementedError

    @classmethod
    def parse_ticker_data(cls, data: str) -> DataFrameGroupBy:
        df = cls.ticker_data_to_dataframe(data)
        validate_dataframe_columns(df, cls.expected_columns)
        assert df.index.names == ["date"]
        assert df.index.tz == pytz.UTC
        return df.groupby(lambda ind: cls.process_row(df, ind))

    @classmethod
    def get_tickers(cls) -> List[str]:
        response = cls.request(cls.BASE_URL_GET_TICKERS.format(cls.API_TOKEN))
        if response:
            tickers_response = response.content.decode(ENCODING)
            return cls.parse_get_tickers_response(tickers_response)
        else:
            return []

    @classmethod
    def request(cls, url: str) -> Response:
        with requests.Session() as session:
            response = session.get(url)
            return response

    @classmethod
    def request_ticker_data(cls, ticker: str, period: Tuple[date, date]) -> Response:
        ticker_url = cls.generate_ticker_url(ticker, period)
        LOG.info("Url for {}: {}".format(ticker, ticker_url))
        return cls.request(ticker_url)
