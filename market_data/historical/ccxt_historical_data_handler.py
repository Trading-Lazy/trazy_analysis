import os
import time
from datetime import date, datetime, timedelta
from typing import List, Tuple, Union

import numpy as np
import pytz
from pandas.core.groupby.generic import DataFrameGroupBy

import trazy_analysis.logger
import trazy_analysis.settings
from trazy_analysis.common.ccxt_connector import CcxtConnector
from trazy_analysis.common.helper import datetime_to_epoch, fill_missing_datetimes
from trazy_analysis.common.types import CandleDataFrame
from trazy_analysis.db_storage.db_storage import DbStorage
from trazy_analysis.market_data.ccxt_data_handler import CcxtDataHandler
from trazy_analysis.market_data.common import datetime_from_epoch
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
)


class CcxtHistoricalDataHandler(CcxtDataHandler):
    def __init__(self, ccxt_connector: CcxtConnector):
        super().__init__(ccxt_connector)

    @classmethod
    def group_ticker_data_by_date(
        cls, asset: Asset, raw_candles: List[List[Union[float, int]]]
    ) -> DataFrameGroupBy:
        df = cls.ticker_data_to_dataframe(asset, raw_candles)
        return df.groupby(lambda ind: ind.strftime("%Y%m%d"))

    def is_well_formed(self, candle_dataframe: CandleDataFrame):
        if candle_dataframe.empty:
            return True
        first_row = candle_dataframe.iloc[0]
        first_timestamp = first_row.name
        end_row = candle_dataframe.iloc[-1]
        end_timestamp = end_row.name

        expected_timestamp = first_timestamp
        index = 0
        step = timedelta(minutes=1)
        while expected_timestamp <= end_timestamp:
            current_timestamp = candle_dataframe.iloc[index].name
            if current_timestamp != expected_timestamp:
                return False
            index += 1
            expected_timestamp += step
        return True

    def request_ticker_data_in_range(
        self,
        ticker: Asset,
        start: datetime,
        end: datetime = datetime.now(pytz.UTC),
    ) -> Tuple[CandleDataFrame, List[Tuple[date, date]], List[str]]:
        start = datetime(
            year=start.year,
            month=start.month,
            day=start.day,
            hour=start.hour,
            minute=start.minute,
            tzinfo=start.tzinfo,
        )
        # First call without putting since
        exchange_to_lower = ticker.exchange.lower()
        exchange_instance = self.ccxt_connector.get_exchange_instance(exchange_to_lower)
        empty_candle_dataframe = CandleDataFrame.from_candle_list(
            asset=ticker, candles=np.array([], dtype=Candle)
        )
        try:
            raw_candles = exchange_instance.fetchOHLCV(symbol=ticker.symbol)
            # don't hit the rateLimit or you will be banned
            time.sleep(exchange_instance.rateLimit / 1000)
        except Exception as e:
            error_message = (
                f"There was an error while pulling OHLCV data for {ticker.key()}, "
                f"Exception is: {e}"
            )
            LOG.error(error_message)
            return empty_candle_dataframe, set(), [error_message]

        raw_candles = [
            raw_candle for raw_candle in raw_candles if None not in raw_candle
        ]
        if len(raw_candles) == 0:
            error_message = f"There was no OHLCV data for {ticker.key()}"
            LOG.error(error_message)
            return empty_candle_dataframe, set(), [error_message]

        end_candle_dataframe = self.ticker_data_to_dataframe(ticker, raw_candles)
        end_candle_dataframe = end_candle_dataframe.loc[start:end]
        oldest_candle = raw_candles[0]
        oldest_timestamp = datetime_from_epoch(int(oldest_candle[0]))
        newest_candle = raw_candles[-1]
        newest_timestamp = datetime_from_epoch(int(newest_candle[0]))

        if oldest_timestamp <= start <= newest_timestamp:
            return end_candle_dataframe, set(), []

        if start > newest_timestamp:
            concat_before = True
            newest_timestamp_plus_one = newest_timestamp + timedelta(minutes=1)
            if start > newest_timestamp_plus_one:
                end_candle_dataframe = empty_candle_dataframe
                missing_data_start = start
            else:
                missing_data_start = newest_timestamp_plus_one
            missing_data_end = end
        else:
            # get missing_data
            concat_before = False
            oldest_timestamp_minus_one = oldest_timestamp - timedelta(minutes=1)
            if end < oldest_timestamp_minus_one:
                end_candle_dataframe = empty_candle_dataframe
                missing_data_end = end
            else:
                missing_data_end = oldest_timestamp_minus_one
            missing_data_start = start
        previous_newest_timestamp = None
        current = missing_data_start

        candle_dataframes = []
        while current <= missing_data_end:
            LOG.info(f"Current for {ticker.key()}: {current}")
            since = datetime_to_epoch(current, 1000)
            try:
                raw_candles = exchange_instance.fetchOHLCV(
                    symbol=ticker.symbol, since=since
                )
                # don't hit the rateLimit or you will be banned
                time.sleep(exchange_instance.rateLimit / 1000)
            except Exception as e:
                error_message = f"There was an error while pulling OHLCV data from {ticker.key()}, Exception is: {e}"
                LOG.exception(error_message)
                return empty_candle_dataframe, set(), [error_message]

            raw_candles = [
                raw_candle for raw_candle in raw_candles if None not in raw_candle
            ]
            if len(raw_candles) == 0:
                return (
                    end_candle_dataframe,
                    set(),
                    [],
                )

            candle_dataframe = self.ticker_data_to_dataframe(ticker, raw_candles)
            candle_dataframes.append(candle_dataframe)
            newest_candle = raw_candles[-1]
            newest_timestamp = datetime_from_epoch(int(newest_candle[0]))
            if (
                previous_newest_timestamp is not None
                and previous_newest_timestamp >= newest_timestamp
            ):
                break
            current = newest_timestamp + timedelta(minutes=1)
            previous_newest_timestamp = newest_timestamp

        if len(candle_dataframes) != 0:
            candle_dataframe = CandleDataFrame.concat(candle_dataframes, asset=ticker)
        else:
            candle_dataframe = empty_candle_dataframe

        if not end_candle_dataframe.empty:
            if concat_before:
                limit_timestamp = end_candle_dataframe.iloc[-1].name + timedelta(
                    minutes=1
                )
                candle_dataframe = candle_dataframe.loc[limit_timestamp:end]
                candle_dataframe = CandleDataFrame.concat(
                    [end_candle_dataframe, candle_dataframe], asset=ticker
                )
            else:
                limit_timestamp = end_candle_dataframe.iloc[0].name - timedelta(
                    minutes=1
                )
                candle_dataframe = candle_dataframe.loc[start:limit_timestamp]
                candle_dataframe = CandleDataFrame.concat(
                    [candle_dataframe, end_candle_dataframe], asset=ticker
                )

        candle_dataframe = fill_missing_datetimes(
            df=candle_dataframe, time_unit=timedelta(minutes=1)
        )
        return (
            candle_dataframe,
            set(),
            [],
        )

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
