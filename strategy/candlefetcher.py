import io
from datetime import datetime, timedelta, timezone
from typing import List

import pandas as pd
from memoization import CachingAlgorithmFlag, cached
from pandas_market_calendars import MarketCalendar

from common.constants import DATE_DIR_FORMAT
from common.helper import resample_candle_data
from common.types import CandleDataFrame
from db_storage.db_storage import DbStorage
from file_storage.common import DATASETS_DIR, DONE_DIR
from file_storage.file_storage import FileStorage
from models.asset import Asset
from models.candle import Candle


class CandleFetcher:
    def __init__(
        self,
        db_storage: DbStorage,
        file_storage: FileStorage,
        market_cal: MarketCalendar = None,
    ):
        self.db_storage = db_storage
        self.file_storage = file_storage
        self.market_cal = market_cal

    def query_candles(
        self,
        asset: Asset,
        start: datetime,
        end: datetime = datetime.now(timezone.utc),
    ) -> List[Candle]:
        candles = self.db_storage.get_candles_in_range(
            asset=asset, start=start, end=end
        )
        return candles

    def fetch_candle_db_data(
        self,
        asset: Asset,
        start: datetime,
        end: datetime = datetime.now(timezone.utc),
    ) -> CandleDataFrame:
        if self.db_storage is None:
            return CandleDataFrame(asset=asset)
        candles = self.query_candles(asset, start, end)
        df = CandleDataFrame.from_candle_list(asset=asset, candles=candles)
        return df

    @cached(max_size=128, algorithm=CachingAlgorithmFlag.LFU)
    def fetch_candle_historical_data(
        self,
        asset: Asset,
        start: datetime,
        end: datetime = datetime.now(timezone.utc),
    ) -> CandleDataFrame:
        if self.file_storage is None:
            return CandleDataFrame(asset=asset)
        start_date = start.date()
        end_date = end.date()
        contents: List[CandleDataFrame] = []
        for i in range(0, (end_date - start_date).days + 1):
            date = start_date + timedelta(days=i)
            date_str = date.strftime(DATE_DIR_FORMAT)
            content = self.file_storage.get_file_content(
                "{}/{}/{}/{}_{}.csv".format(
                    DATASETS_DIR, date_str, DONE_DIR, asset.key(), date_str
                )
            )
            if not content:
                continue

            df = pd.read_csv(
                io.StringIO(content),
                sep=",",
                parse_dates=True,
                usecols=CandleDataFrame.ALL_COLUMNS,
                index_col=0,
                dtype={
                    "open": str,
                    "high": str,
                    "low": str,
                    "close": str,
                    "volume": int,
                },
            )
            contents.append(CandleDataFrame.from_dataframe(df, asset))

        if not contents:
            return CandleDataFrame(asset=asset)

        merged_df = CandleDataFrame.concat(contents, asset)
        start_str = start.strftime("%Y-%m-%d %H:%M:%S%z")
        end_str = end.strftime("%Y-%m-%d %H:%M:%S%z")
        return merged_df.loc[start_str:end_str]

    def fetch(
        self,
        symbol: str,
        time_unit: timedelta,
        start: datetime,
        end: datetime = datetime.now(timezone.utc),
    ) -> CandleDataFrame:
        df = self.fetch_candle_db_data(symbol, start, end)
        if df.empty or start <= df.iloc[0].name:
            if df.empty:
                historical_df_end = end
            else:
                historical_df_end = df.iloc[0].name - timedelta(minutes=1)
            historical_df = self.fetch_candle_historical_data(
                symbol, start, historical_df_end
            )
            if not historical_df.empty:
                df = CandleDataFrame.concat([historical_df, df], symbol)

        if not df.empty:
            df_start = df.iloc[0].name
            df_end = df.iloc[-1].name

            if self.market_cal is not None:
                market_cal_df = self.market_cal.schedule(
                    start_date=df_start.strftime("%Y-%m-%d"),
                    end_date=df_end.strftime("%Y-%m-%d"),
                )
            else:
                market_cal_df = None
            df = resample_candle_data(df, time_unit, market_cal_df)
        return df
