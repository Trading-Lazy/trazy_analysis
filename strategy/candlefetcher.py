import io
from datetime import datetime, timedelta
from typing import List

import numpy as np
import pandas as pd
from django.db.models.query import QuerySet
from memoization import CachingAlgorithmFlag, cached
from pandas import DataFrame
from pandas_market_calendars import MarketCalendar
from pandasql import sqldf
from pytz import utc

from actionsapi.models import Candle
from common.exchange_calendar_euronext import EuronextExchangeCalendar
from common.utils import candles_to_dict, validate_dataframe_columns
from historical_data.common import DATASETS_DIR, DATE_DIR_FORMAT, DONE_DIR
from historical_data.historical_data_api_access import HistoricalDataApiAccess
from historical_data.meganz_storage import MegaNzStorage

EURONEXT_CAL = EuronextExchangeCalendar()


class CandleFetcher:
    STORAGE = MegaNzStorage()

    @staticmethod
    def query_candles(
        symbol: str, start: datetime, end: datetime = datetime.now()
    ) -> QuerySet:
        candles = Candle.objects.all().filter(
            symbol=symbol, timestamp__gte=start, timestamp__lte=end
        )
        return candles

    @staticmethod
    def resample_candle_data(
        df: DataFrame, timeframe: pd.offsets.DateOffset, business_cal: DataFrame
    ) -> DataFrame:
        required_columns = ["open", "high", "low", "close", "volume"]
        validate_dataframe_columns(df, required_columns)

        df[["open", "high", "low", "close"]] = df[
            ["open", "high", "low", "close"]
        ].astype(float)
        df[["volume"]] = df[["volume"]].astype(int)

        resample_label = "right"
        if timeframe >= pd.offsets.Day(1):
            resample_label = "left"
        df = (
            df.resample(timeframe, label=resample_label, closed="right")
            .agg(
                {
                    "open": "first",
                    "high": np.max,
                    "low": np.min,
                    "close": "last",
                    "volume": np.sum,
                }
            )
            .fillna(method="ffill")
        )

        if timeframe >= pd.offsets.Day(1):
            df.index = df.index.date
            df_resampled = df.reindex(business_cal.index)
            df_resampled.index.names = ["timestamp"]
        else:
            s = """
                select
                    d.timestamp, d.open, d.high, d.low, d.close, d.volume
                from
                    df d join business_cal b on (d.timestamp >= b.market_open and d.timestamp <= b.market_close)
            """
            df_resampled = sqldf(s, locals())
            df_resampled = df_resampled.set_index("timestamp")
            df_resampled.index = df.index.tz_convert("UTC")
        return df_resampled

    @staticmethod
    def fetch_candle_db_data(
        symbol: str, start: datetime, end: datetime = datetime.now(utc),
    ) -> DataFrame:
        candles = CandleFetcher.query_candles(symbol, start, end)
        candles_list = candles_to_dict(candles)
        df = pd.DataFrame(
            candles_list,
            columns=["timestamp", "open", "high", "low", "close", "volume"],
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.set_index("timestamp")
        df.index = df.index.tz_convert("UTC")
        return df

    @staticmethod
    @cached(max_size=128, algorithm=CachingAlgorithmFlag.LFU)
    def fetch_candle_historical_data(
        symbol: str, start: datetime, end: datetime = datetime.now(utc),
    ) -> DataFrame:
        start_date: date = start.date()
        end_date: date = end.date()
        contents: List[str] = []
        index_col = "date"
        cols = [index_col] + HistoricalDataApiAccess.expected_columns
        for i in range(0, (end_date - start_date).days + 1):
            date = start_date + timedelta(days=i)
            date_str = date.strftime(DATE_DIR_FORMAT)
            content = CandleFetcher.STORAGE.get_file_content(
                "{}/{}/{}/{}_{}.csv".format(
                    DATASETS_DIR, date_str, DONE_DIR, symbol, date_str
                )
            )
            if not content:
                continue

            df = pd.read_csv(
                io.StringIO(content),
                sep=",",
                parse_dates=True,
                usecols=cols,
                index_col=0,
            )
            contents.append(df)

        merged_df = pd.concat(contents)
        merged_df.index.names = ["timestamp"]
        merged_df.index = merged_df.index.tz_convert("UTC")
        return merged_df.loc[start:end]

    @staticmethod
    def fetch(
        symbol: str,
        timeframe: pd.offsets.DateOffset,
        business_cal: MarketCalendar,
        start: pd.Timestamp,
        end: pd.Timestamp = pd.Timestamp.now(tz="UTC"),
    ) -> DataFrame:
        df = CandleFetcher.fetch_candle_db_data(symbol, start, end)
        db_latest_datetime = end if df.empty else df.iloc[0].name
        db_latest_datetime -= timedelta(minutes=1)
        if start <= db_latest_datetime:
            historical_df = CandleFetcher.fetch_candle_historical_data(
                symbol, start, db_latest_datetime
            )
            df = pd.concat([historical_df, df])
        if not df.empty:
            business_cal_df = business_cal.schedule(
                start_date=start.strftime("%Y-%m-%d"), end_date=end.strftime("%Y-%m-%d")
            )
            df = CandleFetcher.resample_candle_data(df, timeframe, business_cal_df)
        return df
