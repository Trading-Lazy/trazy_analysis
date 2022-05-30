import abc
import os
import traceback
from datetime import date, datetime, timedelta
from typing import Dict, List, Set, Tuple

import numpy as np
import pytz
from pandas.core.groupby.generic import DataFrameGroupBy
from requests.models import Response

import trazy_analysis.settings
from trazy_analysis.common.constants import CONNECTION_ERROR_MESSAGE, ENCODING
from trazy_analysis.common.helper import fill_missing_datetimes, request
from trazy_analysis.common.meta import RateLimitedSingletonMeta
from trazy_analysis.common.types import CandleDataFrame
from trazy_analysis.common.utils import timestamp_to_utc
from trazy_analysis.db_storage.db_storage import DbStorage
from trazy_analysis.logger import logger
from trazy_analysis.market_data.common import LOG, get_periods
from trazy_analysis.market_data.data_handler import DataHandler
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
)


class HistoricalDataHandler(DataHandler, metaclass=RateLimitedSingletonMeta):
    # properties
    @property
    @classmethod
    @abc.abstractmethod
    def BASE_URL_HISTORICAL_TICKER_DATA(cls) -> str:  # pragma: no cover
        pass

    @property
    @classmethod
    @abc.abstractmethod
    def EARLIEST_AVAILABLE_DATE_FOR_DOWNLOAD(cls) -> date:  # pragma: no cover
        pass

    @property
    @classmethod
    @abc.abstractmethod
    def MAX_DOWNLOAD_FRAME(cls) -> timedelta:  # pragma: no cover
        pass

    @property
    @classmethod
    @abc.abstractmethod
    def TICKER_DATA_RESPONSE_USED_COLUMNS_DTYPE(
        cls,
    ) -> Dict[str, type]:  # pragma: no cover
        pass

    @property
    @classmethod
    @abc.abstractmethod
    def TICKER_DATA_TIMEZONE(cls) -> str:  # pragma: no cover
        pass

    # methods
    @classmethod
    @abc.abstractmethod
    def generate_ticker_data_url(
        cls, ticker: str, period: Tuple[date, date]
    ) -> str:  # pragma: no cover
        raise NotImplementedError

    @classmethod
    def group_ticker_data_by_date(cls, symbol: str, data: str) -> DataFrameGroupBy:
        df = cls.ticker_data_to_dataframe(symbol, data)
        return df.groupby(lambda ind: ind.strftime("%Y%m%d"))

    @classmethod
    def request_ticker_data(cls, ticker: Asset, period: Tuple[date, date]) -> Response:
        ticker_url = cls.generate_ticker_data_url(ticker, period)
        LOG.info("Url for %s: %s", ticker.key(), ticker_url)
        return request(ticker_url)

    @classmethod
    def request_ticker_data_for_period(
        cls,
        ticker: Asset,
        period: np.array,  # [date]
        none_response_periods: Set[Tuple[date, date]] = set(),
        error_response_periods: Dict[Tuple[date, date], str] = {},
    ) -> CandleDataFrame:
        try:
            response = cls.request_ticker_data(ticker, period)
        except Exception as e:
            LOG.error(
                CONNECTION_ERROR_MESSAGE,
                str(e),
                traceback.format_exc(),
            )
            return CandleDataFrame(asset=ticker)
        data: str = response.content.decode(ENCODING)
        period_tuple = (period[0], period[1])
        if response:
            if not cls.ticker_data_is_none(data):
                LOG.info("Ticker data is not none. Downloading data")
                candle_dataframe = cls.ticker_data_to_dataframe(ticker, data)
                return candle_dataframe
            else:
                LOG.info(
                    "No available data for ticker %s for period %s",
                    ticker.key(),
                    period_tuple,
                )
                none_response_periods.add(period_tuple)
        else:
            LOG.info(
                "Ticker %s request error for period %s: status_code = %s, message = %s",
                ticker,
                period_tuple,
                response.status_code,
                data,
            )
            error_response_periods[period_tuple] = "{}: {}".format(
                response.status_code, data
            )
        return CandleDataFrame.from_candle_list(asset=ticker, candles=np.array([], dtype=Candle))

    @classmethod
    def request_ticker_data_from_periods(
        cls, ticker: Asset, periods: List[Tuple[datetime, datetime]]
    ) -> Tuple[CandleDataFrame, Set[Tuple[date, date]], Dict[Tuple[date, date], str]]:
        candle_dataframes: np.array = np.empty(
            shape=periods.shape[0], dtype=CandleDataFrame
        )  # np.array[CandleDataFrame]
        none_response_periods: Set[Tuple[date, date]] = set()
        error_response_periods: Dict[Tuple[date, date], str] = {}
        index = 0
        for period in periods:
            candle_dataframe = cls.request_ticker_data_for_period(
                ticker, period, none_response_periods, error_response_periods
            )
            start_period = period[0]
            start_datetime = timestamp_to_utc(
                datetime(
                    year=start_period.year,
                    month=start_period.month,
                    day=start_period.day,
                    hour=0,
                    minute=0,
                    second=0,
                )
            )
            end_period = period[1]
            end_datetime = timestamp_to_utc(
                datetime(
                    year=end_period.year,
                    month=end_period.month,
                    day=end_period.day,
                    hour=23,
                    minute=59,
                    second=59,
                )
            )
            candle_dataframe = candle_dataframe.loc[start_datetime:end_datetime]
            candle_dataframes[index] = candle_dataframe
            index += 1
        candle_dataframe = CandleDataFrame.concat(candle_dataframes, ticker)
        candle_dataframe.sort_index(inplace=True)
        return (
            candle_dataframe,
            none_response_periods,
            error_response_periods,
        )

    @classmethod
    def request_ticker_data_in_range(
        cls,
        ticker: Asset,
        start: datetime,
        end: datetime = datetime.now(pytz.UTC),
    ) -> Tuple[CandleDataFrame, Set[Tuple[date, date]], Dict[Tuple[date, date], str]]:
        periods = get_periods(cls.MAX_DOWNLOAD_FRAME, start, end)
        (
            candle_dataframe,
            none_response_periods,
            error_response_periods,
        ) = cls.request_ticker_data_from_periods(ticker, periods)
        candle_dataframe = fill_missing_datetimes(
            df=candle_dataframe, time_unit=timedelta(minutes=1)
        )
        if len(periods) != 0:
            candle_dataframe = candle_dataframe.loc[start:end]
        return (
            candle_dataframe,
            none_response_periods,
            error_response_periods,
        )

    @classmethod
    def save_ticker_data_in_csv(
        cls,
        ticker: Asset,
        csv_filename: str,
        start: datetime,
        end: datetime = datetime.now(pytz.UTC),
        sep: str = ",",
    ) -> None:
        (
            candle_dataframe,
            _,
            _,
        ) = cls.request_ticker_data_in_range(ticker, start, end)
        candle_dataframe.to_csv(csv_filename, sep)

    @classmethod
    def save_ticker_data_in_db_storage(
        cls,
        ticker: Asset,
        db_storage: DbStorage,
        start: datetime,
        end: datetime = datetime.now(pytz.UTC),
    ) -> None:
        (
            candle_dataframe,
            _,
            _,
        ) = cls.request_ticker_data_in_range(ticker, start, end)
        db_storage.add_candle_dataframe(candle_dataframe)
