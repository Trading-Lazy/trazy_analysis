from datetime import timedelta

import numpy as np
import pandas as pd
from pandas import DataFrame, DatetimeIndex
from pandas_market_calendars import MarketCalendar

from trazy_analysis.common.utils import timestamp_to_utc, validate_dataframe_columns
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle


class CandleDataFrame(DataFrame):
    INDEX_COLUMN = ["timestamp"]
    DATA_COLUMNS = ["open", "high", "low", "close", "volume"]
    ALL_COLUMNS = INDEX_COLUMN + DATA_COLUMNS

    @property
    def _constructor(self):
        return CandleDataFrame

    _metadata = ["asset"]

    def __init__(self, *args, **kwargs):
        self.asset: Asset = kwargs.pop("asset", None)
        candles_data_in_init = "candles_data" in kwargs
        if candles_data_in_init:
            candles_data = kwargs.pop("candles_data")
            if isinstance(candles_data, DataFrame):
                len_candles_data = len(candles_data.index)
            else:  # dict or list
                len_candles_data = len(candles_data)
            if len_candles_data != 0:
                kwargs.update({"data": candles_data})
        super().__init__(*args, **kwargs)
        if candles_data_in_init:
            datetime_index_exist = isinstance(self.index, DatetimeIndex)
            if "asset" in self.columns.tolist():
                self.drop(["asset"], axis=1, inplace=True)
            if not datetime_index_exist:
                validate_dataframe_columns(self, CandleDataFrame.ALL_COLUMNS)
                self["timestamp"] = pd.to_datetime(self["timestamp"])
                self.set_index("timestamp", inplace=True, verify_integrity=True)
            else:
                validate_dataframe_columns(self, CandleDataFrame.DATA_COLUMNS)
                self.index.name = "timestamp"
            self.index = timestamp_to_utc(self.index)
            self.sort_index(inplace=True)
            # reorder columns
            if list(self.columns) != CandleDataFrame.DATA_COLUMNS:
                for i, colname in enumerate(CandleDataFrame.DATA_COLUMNS):
                    col = self.pop(colname)
                    self.insert(i, colname, col)

    def add_candle(self, candle: Candle) -> None:
        if candle.timestamp in self.index:
            raise ValueError("Index has duplicate keys: {}".format(candle.timestamp))
        new_row = [
            str(candle.open),
            str(candle.high),
            str(candle.low),
            str(candle.close),
            candle.volume,
        ]
        self.loc[candle.timestamp] = new_row
        self.sort_index(inplace=True)

    def get_candle(self, index: int) -> Candle:
        if self.asset is None:
            raise Exception("CandleDataFrame asset is not set")
        row = self.iloc[index]
        return Candle(
            asset=self.asset,
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=int(row["volume"]),
            timestamp=row.name,
        )

    def to_candles(self) -> np.array:
        if self.asset is None:
            raise Exception("CandleDataFrame asset is not set")
        map_index_to_values = self.to_dict(orient="index")
        candles = np.empty(shape=len(map_index_to_values), dtype=Candle)
        for index, timestamp in enumerate(map_index_to_values):
            candle_dict = map_index_to_values[timestamp]
            candle_dict["asset"] = self.asset.to_dict()
            candle_dict["timestamp"] = timestamp
            candle = Candle.from_serializable_dict(candle_dict)
            candles[index] = candle
        return candles

    def append(self, other, *args, **kwargs) -> "CandleDataFrame":
        kwargs.pop("verify_integrity", None)
        candle_dataframe = pd.concat([self, other], *args, **kwargs, verify_integrity=True)
        candle_dataframe.asset = self.asset
        candle_dataframe.sort_index(inplace=True)
        return candle_dataframe

    @staticmethod
    def from_candle_list(asset: Asset, candles: np.array):  # [Candle]
        if candles.size == 0:
            return CandleDataFrame(asset=asset)
        candles_data = [candle.to_serializable_dict() for candle in candles]
        return CandleDataFrame(asset=asset, candles_data=candles_data)

    @staticmethod
    def from_dataframe(df: DataFrame, asset: Asset) -> "CandleDataFrame":
        return CandleDataFrame(asset=asset, candles_data=df)

    @staticmethod
    def concat(candle_dataframes: np.array, asset: Asset) -> "CandleDataFrame":
        concatenated_candle_dataframe = pd.concat(
            candle_dataframes, verify_integrity=True
        )
        concatenated_candle_dataframe.asset = asset
        concatenated_candle_dataframe.sort_index(inplace=True)
        return concatenated_candle_dataframe

    def aggregate(
        self, time_unit: timedelta, market_cal: MarketCalendar
    ) -> "CandleDataFrame":
        if self.empty:
            return CandleDataFrame(asset=self.asset)
        start = self.index[0]
        end = self.index[-1]
        market_cal_df = market_cal.schedule(
            start_date=start.strftime("%Y-%m-%d"),
            end_date=end.strftime("%Y-%m-%d"),
        )

        from trazy_analysis.common.helper import resample_candle_data

        aggregated_candle_dataframe = resample_candle_data(
            self, time_unit, market_cal_df
        )
        return aggregated_candle_dataframe
