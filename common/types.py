from _pydecimal import Decimal
from typing import List

import pandas as pd
from pandas import DataFrame, DatetimeIndex, Index
from pandas_market_calendars import MarketCalendar

from common.utils import timestamp_to_utc, validate_dataframe_columns
from models.candle import Candle


class CandleDataFrame(DataFrame):
    INDEX_COLUMN = ["timestamp"]
    DATA_COLUMNS = ["open", "high", "low", "close", "volume"]
    ALL_COLUMNS = INDEX_COLUMN + DATA_COLUMNS

    @property
    def _constructor(self):
        return CandleDataFrame

    _metadata = ["symbol"]

    def __init__(self, *args, **kwargs):
        self.symbol: str = kwargs.pop("symbol", None)
        candles_data_in_init = "candles_data" in kwargs
        if candles_data_in_init:
            candles_data = kwargs.pop("candles_data")
            kwargs.update({"data": candles_data})
        super().__init__(*args, **kwargs)
        if candles_data_in_init:
            datetime_index_exist = isinstance(self.index, DatetimeIndex)
            if "symbol" in self.columns.tolist():
                self.drop(["symbol"], axis=1, inplace=True)
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
        if self.symbol is None:
            raise Exception("CandleDataFrame symbol is not set")
        row = self.iloc[index]
        return Candle(
            symbol=self.symbol,
            open=Decimal(row["open"]),
            high=Decimal(row["high"]),
            low=Decimal(row["low"]),
            close=Decimal(row["close"]),
            volume=int(row["volume"]),
            timestamp=row.name,
        )

    def to_candles(self) -> List[Candle]:
        if self.symbol is None:
            raise Exception("CandleDataFrame symbol is not set")
        map_index_to_values = self.to_dict(orient="index")
        candles = []
        for timestamp in map_index_to_values:
            candle_dict = map_index_to_values[timestamp]
            candle_dict["symbol"] = self.symbol
            candle_dict["timestamp"] = timestamp
            candle = Candle.from_serializable_dict(candle_dict)
            candles.append(candle)
        return candles

    def append(self, *args, **kwargs) -> "CandleDataFrame":
        kwargs.pop("verify_integrity", None)
        candle_dataframe = super().append(*args, **kwargs, verify_integrity=True)
        candle_dataframe.symbol = self.symbol
        candle_dataframe.sort_index(inplace=True)
        return candle_dataframe

    @staticmethod
    def from_candle_list(symbol: str, candles: List[Candle]):
        if len(candles) == 0:
            return CandleDataFrame(symbol=symbol)
        candles_data = [candle.to_serializable_dict() for candle in candles]
        return CandleDataFrame(symbol=symbol, candles_data=candles_data)

    @staticmethod
    def from_dataframe(df: DataFrame, symbol: str) -> "CandleDataFrame":
        return CandleDataFrame(symbol=symbol, candles_data=df)

    @staticmethod
    def concat(
        candle_dataframes: List["CandleDataFrame"], symbol: str
    ) -> "CandleDataFrame":
        concatenated_candle_dataframe = pd.concat(
            candle_dataframes, verify_integrity=True
        )
        concatenated_candle_dataframe.symbol = symbol
        concatenated_candle_dataframe.sort_index(inplace=True)
        return concatenated_candle_dataframe

    def aggregate(
        self, time_unit: pd.offsets.DateOffset, market_cal: MarketCalendar
    ) -> "CandleDataFrame":
        if self.empty:
            return CandleDataFrame(symbol=self.symbol)
        start = self.index[0]
        end = self.index[-1]
        market_cal_df = market_cal.schedule(
            start_date=start.strftime("%Y-%m-%d"),
            end_date=end.strftime("%Y-%m-%d"),
        )

        from common.helper import resample_candle_data

        aggregated_candle_dataframe = resample_candle_data(
            self, time_unit, market_cal_df
        )
        return aggregated_candle_dataframe
