from datetime import datetime, timedelta
from typing import Any, Callable, Dict

import numpy as np
import pandas as pd
from memoization import CachingAlgorithmFlag, cached
from pandas_market_calendars import MarketCalendar

from common.american_stock_exchange_calendar import AmericanStockExchangeCalendar
from common.helper import get_or_create_nested_dict, round_time
from common.types import CandleDataFrame
from common.utils import timestamp_to_utc
from indicators.common import PriceType
from indicators.indicator import Indicator
from models.candle import Candle


def get_price_selector_function(price_type: PriceType) -> Callable[[Candle], float]:
    if price_type == PriceType.OPEN:
        return lambda candle: candle.open
    elif price_type == PriceType.HIGH:
        return lambda candle: candle.high
    elif price_type == PriceType.LOW:
        return lambda candle: candle.low
    elif price_type == PriceType.CLOSE:
        return lambda candle: candle.close
    else:
        raise Exception("Invalid price_type {}".format(price_type.name))


class RollingWindow(Indicator):
    count = 0
    instances = 0

    def __init__(
        self,
        size: int = None,
        dtype: type = None,
        source_indicator: Indicator = None,
        transform: Callable = lambda new_data: new_data,
        preload=False,
    ):
        RollingWindow.instances += 1
        super().__init__(source_indicator, transform)
        self.dtype = dtype
        self.nb_elts = 0
        self.insert = 0
        self.window = None
        self.ready = False
        self.size = size
        if self.size is not None:
            self.set_size(self.size)
        self.preload = preload

    def set_size(self, period: int):
        self.size = period
        self.window: np.array = np.empty(shape=self.size, dtype=self.dtype)
        self.nb_elts = 0
        self.insert = 0
        self.ready = True

    def prefill(self, filling_array: np.array):
        nb_elts_list = len(filling_array)
        diff = self.size - nb_elts_list
        if not self.preload:
            self.nb_elts = int(min(self.size, nb_elts_list))
        else:
            self.nb_elts = 1
        if diff > 0:
            for i in range(diff, self.size):
                self.window[i] = self.transform(filling_array[i - diff])
        else:
            self.window = np.array(
                [self.transform(elt) for elt in filling_array[-self.size :]],
                dtype=self.dtype,
            )
        self.insert = 0
        self.index = -1
        self.data = None if nb_elts_list == 0 else self.window[-1]

    def handle_new_data(self, new_data) -> None:
        RollingWindow.count += 1
        if not self.preload:
            transformed_data = self.transform(new_data)
            self.data = transformed_data
            self.window[self.insert] = transformed_data
            self.insert = (self.insert + 1) % self.size
            self.nb_elts += 1
            self.nb_elts = int(min(self.nb_elts, self.size))
            self.on_next(transformed_data)
        else:
            self.index = (self.index + 1) % self.size
            if self.index == 0:
                self.nb_elts = 1
            else:
                self.nb_elts += 1
            self.data = self.window[self.index]
            self.on_next(self.data)

    def push(self, new_data: Any = None):
        self.handle_new_data(new_data)

    def filled(self) -> bool:
        if self.size is None:
            return False
        return self.nb_elts == self.size

    def get_real_key(self, key: int):
        return (self.insert - 1 + key + self.size) % self.size

    def __getitem__(self, key) -> Any:
        if not self.preload:
            size = self.size
        else:
            size = self.index + 1
        if isinstance(key, slice):
            start = key.start
            stop = key.stop
            step = key.step
            if not (-size + 1 <= start <= 0) or not (-size + 1 <= stop <= 0):
                raise IndexError("Index out of Data bound")
            real_start = self.get_real_key(start)
            real_stop = self.get_real_key(stop)
            if real_start <= real_stop:
                return self.window[real_start:real_stop:step]
            else:
                return np.concatenate(
                    [self.window[real_start::step], self.window[: real_stop + 1 : step]]
                )
        elif isinstance(key, int):
            if not (-size + 1 <= key <= 0):
                raise IndexError("Index out of Data bound")
            real_key = self.get_real_key(key)
            return self.window[real_key]
        else:
            raise TypeError("Invalid argument type: {}".format(type(key)))

    def map(self, func: Callable) -> "RollingWindow":
        rolling_window_stream = RollingWindow(
            size=self.size,
            source_indicator=self,
            transform=func,
            dtype=self.dtype,
            preload=self.preload,
        )
        if self.filled():
            rolling_window_stream.prefill(self.window.tolist())
        return rolling_window_stream


class TimeFramedCandleRollingWindow(RollingWindow):
    instances = 0
    count = 0

    def __init__(
        self,
        time_unit: pd.offsets.DateOffset,
        market_cal: MarketCalendar,
        size: int = None,
        source_indicator: Indicator = None,
        preload=False,
    ):
        TimeFramedCandleRollingWindow.instances += 1
        super().__init__(
            size=size,
            source_indicator=source_indicator,
            dtype=Candle,
            preload=preload,
        )
        self.time_unit = time_unit
        self.market_cal = market_cal
        self.aggregate_oldest_timestamp = timestamp_to_utc(datetime.min)
        self.aggregated_candle = None
        self.aggregated_df = None
        self.last_candle_added_timestamp = None

    def handle_new_data(self, new_data: Candle) -> None:
        TimeFramedCandleRollingWindow.count += 1
        if not self.preload:
            if self.time_unit == timedelta(minutes=1):
                super().handle_new_data(new_data)
                return

            if (new_data.timestamp - self.time_unit) <= self.aggregate_oldest_timestamp:
                self.aggregated_df.add_candle(new_data)
            else:
                if self.aggregated_df is not None:
                    self.aggregated_df = self.aggregated_df.aggregate(
                        self.time_unit, self.market_cal
                    )
                    self.aggregated_candle = self.aggregated_df.get_candle(0)
                    super().handle_new_data(self.aggregated_candle)
                self.aggregated_df = CandleDataFrame.from_candle_list(
                    symbol=new_data.symbol, candles=np.array([new_data], dtype=Candle)
                )
                self.aggregate_oldest_timestamp = round_time(
                    new_data.timestamp, self.time_unit
                )
        else:
            super().handle_new_data(new_data)


class RollingWindowManager:
    def __init__(self, preload=True):
        self.cache: Dict[str, RollingWindow] = {}
        self.max_periods: Dict[str, int] = {}
        self.preload = preload

    @cached(max_size=128, algorithm=CachingAlgorithmFlag.LFU)
    def __call__(self, symbol: str, period: int = 5) -> RollingWindow:
        if symbol not in self.cache:
            self.max_periods[symbol] = period
            self.cache[symbol] = RollingWindow(dtype=Candle, preload=self.preload)
        elif period > self.max_periods[symbol]:
            self.max_periods[symbol] = period
        return self.cache[symbol]

    def warmup(self, preload_data: Dict[str, np.array]):
        for symbol in self.max_periods:
            max_period = self.max_periods[symbol]
            if symbol in preload_data:
                candles_len = len(preload_data[symbol])
                max_period = int(max(max_period, candles_len))
                self.cache[symbol].set_size(max_period)
                self.cache[symbol].prefill(preload_data[symbol])
            else:
                self.cache[symbol].set_size(max_period)


class TimeFramedCandleRollingWindowManager:
    def __init__(
        self,
        rolling_window_manager: RollingWindowManager,
        market_cal: MarketCalendar = AmericanStockExchangeCalendar(),
        preload=True,
    ):
        self.cache = {}
        self.max_periods = {}
        self.rolling_window_manager = rolling_window_manager
        self.market_cal = market_cal
        self.preload = preload

    @cached(max_size=128, algorithm=CachingAlgorithmFlag.LFU)
    def __call__(
        self, symbol: str, period: int, time_unit: pd.offsets.DateOffset
    ) -> TimeFramedCandleRollingWindow:
        get_or_create_nested_dict(self.cache, symbol)
        get_or_create_nested_dict(self.max_periods, symbol)
        rolling_window = self.rolling_window_manager(symbol, period)
        if time_unit not in self.cache[symbol]:
            self.cache[symbol][time_unit] = TimeFramedCandleRollingWindow(
                time_unit=time_unit,
                market_cal=self.market_cal,
                source_indicator=rolling_window,
                preload=self.preload,
            )
            self.max_periods[symbol][time_unit] = period
        elif period > self.max_periods[symbol][time_unit]:
            self.max_periods[symbol][time_unit] = period
        return self.cache[symbol][time_unit]

    def warmup(self):
        for symbol in self.max_periods:
            for time_unit in self.max_periods[symbol]:
                max_period = self.max_periods[symbol][time_unit]
                preload_data = self.cache[symbol][time_unit].source_indicator.window
                candles_len = len(preload_data)
                max_period = int(max(max_period, candles_len))
                self.cache[symbol][time_unit].set_size(max_period)
                if self.preload:
                    if time_unit == timedelta(minutes=1):
                        self.cache[symbol][time_unit].prefill(preload_data)
                    else:
                        aggregated_df = CandleDataFrame.from_candle_list(
                            symbol=symbol, candles=preload_data
                        ).aggregate(time_unit, self.market_cal)
                        self.cache[symbol][time_unit].prefill(
                            aggregated_df.to_candles()
                        )


class PriceRollingWindowManager:
    def __init__(
        self,
        time_framed_candle_rolling_window_manager: TimeFramedCandleRollingWindowManager,
        market_cal: MarketCalendar = AmericanStockExchangeCalendar(),
        preload=True,
    ):
        self.cache = {}
        self.max_periods = {}
        self.time_framed_candle_rolling_window_manager = (
            time_framed_candle_rolling_window_manager
        )
        self.market_cal = market_cal
        self.preload = preload

    @cached(max_size=128, algorithm=CachingAlgorithmFlag.LFU)
    def __call__(
        self,
        symbol: str,
        period: int,
        time_unit: pd.offsets.DateOffset,
        price_type: PriceType,
    ) -> TimeFramedCandleRollingWindow:
        get_or_create_nested_dict(self.cache, symbol, time_unit)
        get_or_create_nested_dict(self.max_periods, symbol, time_unit)
        candle_rolling_window = self.time_framed_candle_rolling_window_manager(
            symbol=symbol,
            time_unit=time_unit,
            period=period,
        )
        if price_type not in self.cache[symbol][time_unit]:
            price_selector_function = get_price_selector_function(price_type)
            price_rolling_window = candle_rolling_window.map(price_selector_function)
            self.cache[symbol][time_unit][price_type] = price_rolling_window
            self.max_periods[symbol][time_unit][price_type] = period
        elif period > self.max_periods[symbol][time_unit][price_type]:
            self.max_periods[symbol][time_unit][price_type] = period
        return self.cache[symbol][time_unit][price_type]

    def warmup(self):
        for symbol in self.max_periods:
            for time_unit in self.max_periods[symbol]:
                for price_type in self.max_periods[symbol][time_unit]:
                    max_period = self.max_periods[symbol][time_unit][price_type]
                    candle_rolling_window = (
                        self.time_framed_candle_rolling_window_manager(
                            symbol=symbol,
                            time_unit=time_unit,
                            period=max_period,
                        )
                    )
                    self.cache[symbol][time_unit][price_type].set_size(
                        candle_rolling_window.size
                    )
                    if self.preload:
                        self.cache[symbol][time_unit][price_type].prefill(
                            candle_rolling_window.window
                        )
