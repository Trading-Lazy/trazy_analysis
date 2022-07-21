import operator
from collections import deque
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Callable, Dict, Optional, List, Union, Tuple, TypeVar

import numpy as np
import numpy.ma as ma
import pandas as pd
from pandas import DatetimeIndex
from pandas_market_calendars import MarketCalendar
from plotly.basedatatypes import BaseTraceType
import plotly as plt

from trazy_analysis.common.helper import (
    round_time,
    check_type,
)
from trazy_analysis.common.types import CandleDataFrame
from trazy_analysis.common.utils import timestamp_to_utc
from trazy_analysis.indicators.common import PriceType
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.enums import IndicatorMode


def get_price_selector_function(price_type: PriceType) -> Callable[[Candle], float]:
    match price_type:
        case PriceType.OPEN:
            return lambda candle: candle.open
        case PriceType.HIGH:
            return lambda candle: candle.high
        case PriceType.LOW:
            return lambda candle: candle.low
        case PriceType.CLOSE:
            return lambda candle: candle.close
        case PriceType.BODY_HIGH:
            return lambda candle: max(candle.open, candle.close)
        case PriceType.BODY_LOW:
            return lambda candle: min(candle.open, candle.close)
        case _:
            raise Exception("Invalid price_type {}".format(price_type.name))


COLORS = []
for color_class, colors in plt.colors.PLOTLY_SCALES.items():
    for color in colors:
        COLORS.append(color[1])

TIndicator = TypeVar('TIndicator', bound='Indicator')

class Indicator:
    PLOTTING_ATTRIBUTES = []

    def __init__(
        self,
        source: TIndicator | np.ndarray | list[Any] = None,
        transform: Optional[Callable] = None,
        source_minimal_size: Optional[int] = None,
        size: int = 1,
        dtype: Optional[type] = None,
    ):
        self.source = source
        self.transform = transform
        self.source_minimal_size = source_minimal_size
        self.size = size
        self.dtype = dtype
        self.source_dtype: Optional[type] = None
        self.memoize: Optional[bool] = None
        self.mode: Optional[IndicatorMode] = None
        self.indicators: "ReactiveIndicators" = None
        self.window: Optional[np.array] = None
        self.input_window = None
        self.insert = 0
        self.index = -1
        self.callback = lambda elt: self.handle_data(elt)
        self.callbacks = deque()
        self.subscribers = set()
        self.data = None
        self.id = None

    def setup(self, indicators: "ReactiveIndicators"):
        self.indicators = indicators
        self.mode = indicators.mode
        self.memoize = indicators.memoize
        if self.source is None:
            if self.source_minimal_size is None:
                input_window = None
            else:
                input_window = self.indicators.Indicator(
                    source=self.source,
                    source_minimal_size=None,
                    size=self.source_minimal_size,
                )
            if self.size is None:
                self.size = self.source_minimal_size
        else:
            if self.source.size is None and self.source_minimal_size is None:
                self.source_minimal_size = 1
            elif self.source_minimal_size is None:
                self.source_minimal_size = self.source.size

            if self.source.size is None or self.source.size < self.source_minimal_size:
                source_size = 1 if self.source is None else self.source.size
                input_window = self.indicators.Indicator(
                    source=self.source,
                    source_minimal_size=source_size,
                    size=self.source_minimal_size,
                )
                self.source = input_window
                self.source_dtype = self.source.dtype
            else:
                if issubclass(type(self.source), Indicator):
                    input_window = self.source
                    self.source_dtype = self.source.dtype
                elif (
                    issubclass(type(self.source), np.ndarray)
                    and self.transform is not None
                ):
                    input_window = self.indicators.Indicator(
                        source=None,
                        source_minimal_size=None,
                        size=len(self.source),
                    )
                    input_window.fill(self.source)
                    self.source = input_window
                    self.source_dtype = self.source.dtype
                elif issubclass(type(self.source), list) and self.transform is not None:
                    list_len = len(self.source)
                    dtype = type(self.source[0]) if list_len > 0 else None
                    input_window = self.indicators.Indicator(
                        source=None,
                        source_minimal_size=None,
                        size=list_len,
                    )
                    input_window.fill(np.array(self.source, dtype=dtype))
                    self.source = input_window
                else:
                    input_window = None

                if self.size is None:
                    self.size = (
                        self.source.size
                        if self.source.size is not None
                        else self.source_minimal_size
                    )

        self.indicators.add_input_edge((self, input_window))
        self.input_window = input_window

        self.window: np.array = None
        if self.input_window is not None and self.input_window.filled():
            self.transform = (
                (lambda data: data) if self.transform is None else self.transform
            )
            if self.mode == IndicatorMode.BATCH:
                self.initialize_batch()
            else:
                self.initialize_stream()
        elif self.transform is None and (
            issubclass(type(self.source), np.ndarray)
            or issubclass(type(self.source), list)
        ):
            self.transform = lambda data: data
            self.fill(self.source)
            self.source = None
        else:  # not self.input_window.filled()
            self.transform = (
                (lambda data: data) if self.transform is None else self.transform
            )
            if self.size is not None and self.dtype is not None:
                self.window = ma.masked_array(
                    [0] * self.size, mask=True, dtype=self.dtype
                )
            self.data = None

        self.observe(self.source)

    def count(self):
        return 0 if self.window is None else self.window.count()

    def fill(self, array: np.array):
        array_len = len(array)
        if array_len > 0:
            self.dtype = type(array[0])
        elif self.source is not None:
            self.dtype = self.source.dtype
        else:
            self.dtype = None
        diff = self.size - array_len
        if diff > 0:
            mask = [1] * diff
            mask.extend([0] * array_len)
            window = [0] * diff
            window.extend(list(map(self.transform, array)))
            self.window = ma.masked_array(window, mask=mask, dtype=self.dtype)
        else:
            window = list(map(self.transform, array[-self.size :]))
            self.window = ma.masked_array(window, dtype=self.source_dtype, mask=False)
        self.insert = 0
        self.index = -1
        self.data = None if self.mode == IndicatorMode.BATCH else self.window[-1]

    def subscribe(self, callback: Callable, subscriber: TIndicator):
        self.callbacks.append(callback)
        self.subscribers.add(subscriber)
        self.indicators.add_source_edge((subscriber, self))

    def next(self, value: Any):
        for callback in self.callbacks:
            callback(value)

    @staticmethod
    def compute(cls, data: np.ndarray | pd.DataFrame) -> np.ndarray:
        return data

    @property
    @classmethod
    def plotting_attributes(cls) -> list[str]:
        return []

    def get_trace(self, candle_dataframe: CandleDataFrame) -> Optional[BaseTraceType]:
        return None

    def initialize_batch(self):
        window = self.compute(self.input_window.window, self.period)
        self.fill(window)

    def initialize_stream(self):
        self.fill(self.input_window.window)
        self.data = self.window[-1]

    def handle_batch_data(self):
        self.index = (self.index + 1) % self.size
        self.data = self.window[self.index]
        self.next(self.data)

    def handle_stream_data(self, data: Any):
        transformed_data = self.transform(data)
        self.data = transformed_data
        self.dtype = type(self.data)
        if self.window is None:
            self.window = ma.masked_array([0] * self.size, mask=True, dtype=self.dtype)
        self.window[self.insert] = transformed_data
        self.insert = (self.insert + 1) % self.size
        self.next(transformed_data)

    def handle_data(self, data: Any):
        self.dtype = type(data)
        if self.window is None:
            if self.size is None:
                self.size = 1
            self.window = ma.masked_array([0] * self.size, mask=True, dtype=self.dtype)
        match self.mode:
            case IndicatorMode.LIVE:
                self.handle_stream_data(data)
            case IndicatorMode.BATCH:
                self.handle_batch_data()

    def push(self, data: Any = None):
        self.handle_data(data)

    def filled(self) -> bool:
        if self.size is None:
            return False
        return self.window is not None and self.window.count() == self.size

    def get_real_key(self, key: int):
        return (self.insert - 1 + key + self.size) % self.size

    def get_item_slice(self, key: slice, size: int):
        start = key.start
        if start is None:
            start = -self.size + 1
        stop = key.stop
        if stop is None:
            stop = 1
        step = key.step
        if not (-size + 1 <= start <= 0) or not (-size + 1 <= stop <= 1):
            raise IndexError("Index out of Data bound")
        real_start = self.get_real_key(start)
        real_stop = self.get_real_key(stop)
        if start == -self.size + 1 and stop == 1:
            return np.concatenate(
                [self.window[real_start::step], self.window[:real_stop:step]]
            )
        elif real_start <= real_stop:
            return self.window[real_start:real_stop:step]
        else:
            return np.concatenate(
                [self.window[real_start::step], self.window[: real_stop + 1 : step]]
            )

    def get_item_key(self, key: int, size: int):
        if not (-size + 1 <= key <= 0):
            raise IndexError("Index out of Data bound")
        real_key = self.get_real_key(key)
        return self.window[real_key]

    def __getitem__(self, key) -> Any:
        if self.mode == IndicatorMode.BATCH:
            size = self.index + 1
        else:
            size = self.size
        if isinstance(key, slice):
            return self.get_item_slice(key, size)
        elif isinstance(key, int):
            return self.get_item_key(key, size)
        else:
            raise TypeError("Invalid argument type: {}".format(type(key)))

    def get_ordered_window(self) -> Optional[np.ndarray]:
        if self.window.count() == 0:
            return None
        if ma.is_masked(self.window):
            window = self[-self.window.count() + 1 :]
        else:
            window = self[:]
        return window

    def get_trace_coordinates(self, index: DatetimeIndex) -> Tuple[DatetimeIndex, Any]:
        x = list(index)[-self.window.count() :]
        y = self.get_ordered_window()
        return x, y

    def map(self, func: Callable, size: Optional[int] = None) -> TIndicator:
        indicator_stream = self.indicators.Indicator(
            source=self,
            transform=func,
            source_minimal_size=self.source_minimal_size,
            size=self.size if size is None else size,
        )
        if self.filled():
            indicator_stream.fill(self.window.tolist())
        return indicator_stream

    def observe(self, indicator_data: TIndicator):
        self.ignore()
        self.source = indicator_data
        if self.source is not None:
            indicator_data.subscribe(self.callback, self)

    def remove_callback(self, callback: Callable):
        try:
            self.callbacks.remove(callback)
        except ValueError:
            pass

    def remove_subscriber(self, subscriber: TIndicator):
        try:
            self.subscribers.remove(subscriber)
        except KeyError:
            pass

    def ignore(self):
        if self.source is not None:
            self.source.remove_callback(self.callback)
            self.source.remove_subscriber(self)
            self.indicators.remove_edges(self.source, self)
            self.source = None

    def __hash__(self):
        return id(self)

    def indicator_unary_operation(
        self,
        operation_function: Callable[[Any], Any],
        allowed_types: list[type],
    ) -> TIndicator:
        check_type(self.data, allowed_types)
        indicator_data: Indicator = self.indicators.Indicator(
            source=self, transform=operation_function
        )
        return indicator_data

    def indicator_binary_operation_indicator(
        self, other: TIndicator, operation_function: Callable[[Any, Any], Any]
    ) -> TIndicator:
        indicator_zip_data = self.indicators.ZipIndicator(
            source_indicator1=self,
            source_indicator2=other,
            operation_function=operation_function,
        )
        return indicator_zip_data

    def indicator_binary_operation_data(
        self, other, operation_function: Callable[[Any, Any], Any]
    ) -> TIndicator:
        transform = (
            lambda data: (operation_function(data, other)) if data is not None else None
        )
        indicator_data: Indicator = self.indicators.Indicator(
            source=self, transform=transform
        )
        return indicator_data

    def indicator_binary_operation(
        self,
        other,
        operation_function: Callable[[Any, Any], Any],
        allowed_types: list[type],
    ) -> TIndicator:
        check_type(self.data, allowed_types)

        other_is_indicator = isinstance(other, Indicator)
        other_data = other.data if other_is_indicator else other
        check_type(other_data, allowed_types)

        if other_is_indicator:
            return self.indicator_binary_operation_indicator(other, operation_function)
        else:
            return self.indicator_binary_operation_data(other, operation_function)

    def unary_operation(
        self,
        operation_function: Callable[[Any], Any],
        allowed_types: list[type],
    ) -> Optional[int | bool | float | Decimal]:
        if self.data is None:
            return None
        check_type(self.data, allowed_types)
        return operation_function(self.data)

    def binary_operation(
        self,
        other,
        operation_function: Callable[[Any, Any], Any],
        allowed_types: list[type],
    ) -> Optional[int | bool | float | Decimal]:
        if self.data is None:
            return None
        check_type(self.data, allowed_types)
        other_is_indicator = isinstance(other, Indicator)
        other_data = other.data if other_is_indicator else other
        if other_data is None:
            return None
        check_type(other_data, allowed_types)

        if other_is_indicator:
            return operation_function(self.data, other.data)
        else:
            return operation_function(self.data, other)

    def __lt__(self, decimal_or_indicator) -> bool:
        return self.binary_operation(
            decimal_or_indicator, operator.__lt__, [int, float, np.float64, Decimal]
        )

    def __le__(self, decimal_or_indicator) -> bool:
        return self.binary_operation(
            decimal_or_indicator, operator.__le__, [int, float, np.float64, Decimal]
        )

    def __eq__(self, decimal_or_indicator) -> bool:
        return self.binary_operation(
            decimal_or_indicator, operator.__eq__, [int, float, np.float64, Decimal]
        )

    def __ne__(self, decimal_or_indicator) -> bool:
        return self.binary_operation(
            decimal_or_indicator, operator.__ne__, [int, float, np.float64, Decimal]
        )

    def __ge__(self, decimal_or_indicator) -> bool:
        return self.binary_operation(
            decimal_or_indicator, operator.__ge__, [int, float, np.float64, Decimal]
        )

    def __gt__(self, decimal_or_indicator) -> bool:
        return self.binary_operation(
            decimal_or_indicator, operator.__gt__, [int, float, np.float64, Decimal]
        )

    def __add__(self, decimal_or_indicator) -> int | float | np.float64 | Decimal:
        return self.binary_operation(
            decimal_or_indicator, operator.__add__, [int, float, np.float64, Decimal]
        )

    def __iadd__(self, decimal_or_indicator) -> TIndicator:
        return self.indicator_binary_operation(
            decimal_or_indicator, operator.__iadd__, [int, float, np.float64, Decimal]
        )

    def __sub__(self, decimal_or_indicator) -> int | float | np.float64 | Decimal:
        return self.binary_operation(
            decimal_or_indicator, operator.__sub__, [int, float, np.float64, Decimal]
        )

    def __isub__(self, decimal_or_indicator) -> TIndicator:
        return self.indicator_binary_operation(
            decimal_or_indicator, operator.__isub__, [int, float, np.float64, Decimal]
        )

    def __mul__(self, decimal_or_indicator) -> int | float | np.float64 | Decimal:
        return self.binary_operation(
            decimal_or_indicator, operator.__mul__, [int, float, np.float64, Decimal]
        )

    def __imul__(self, decimal_or_indicator) -> TIndicator:
        return self.indicator_binary_operation(
            decimal_or_indicator, operator.__imul__, [int, float, np.float64, Decimal]
        )

    def __truediv__(self, decimal_or_indicator) -> int | float | np.float64 | Decimal:
        return self.binary_operation(
            decimal_or_indicator,
            operator.__truediv__,
            [int, float, np.float64, Decimal],
        )

    def __itruediv__(self, decimal_or_indicator) -> TIndicator:
        return self.indicator_binary_operation(
            decimal_or_indicator,
            operator.__itruediv__,
            [int, float, np.float64, Decimal],
        )

    def __floordiv__(self, decimal_or_indicator) -> int | float | np.float64 | Decimal:
        return self.binary_operation(
            decimal_or_indicator,
            operator.__floordiv__,
            [int, float, np.float64, Decimal],
        )

    def __ifloordiv__(self, decimal_or_indicator) -> TIndicator:
        return self.indicator_binary_operation(
            decimal_or_indicator,
            operator.__ifloordiv__,
            [int, float, np.float64, Decimal],
        )

    def __neg__(self) -> int | float | np.float64 | Decimal:
        return self.unary_operation(operator.__neg__, [int, float, np.float64, Decimal])

    def __and__(self, bool_or_bool_indicator) -> bool:
        return self.binary_operation(bool_or_bool_indicator, operator.__and__, [bool])

    def __iand__(self, bool_or_bool_indicator) -> TIndicator:
        return self.indicator_binary_operation(
            bool_or_bool_indicator, operator.__iand__, [bool]
        )

    def __or__(self, bool_or_bool_indicator) -> bool:
        return self.binary_operation(bool_or_bool_indicator, operator.__or__, [bool])

    def __ior__(self, bool_or_bool_indicator) -> TIndicator:
        return self.indicator_binary_operation(
            bool_or_bool_indicator, operator.__ior__, [bool]
        )

    def __bool__(self) -> bool:
        return self.data

    def lt(self, decimal_or_indicator) -> TIndicator:
        return self.indicator_binary_operation(
            decimal_or_indicator, operator.__lt__, [int, float, np.float64, Decimal]
        )

    def le(self, decimal_or_indicator) -> TIndicator:
        return self.indicator_binary_operation(
            decimal_or_indicator, operator.__le__, [int, float, np.float64, Decimal]
        )

    def eq(self, decimal_or_indicator) -> TIndicator:
        return self.indicator_binary_operation(
            decimal_or_indicator, operator.__eq__, [int, float, np.float64, Decimal]
        )

    def ne(self, decimal_or_indicator) -> TIndicator:
        return self.indicator_binary_operation(
            decimal_or_indicator, operator.__ne__, [int, float, np.float64, Decimal]
        )

    def ge(self, decimal_or_indicator) -> TIndicator:
        return self.indicator_binary_operation(
            decimal_or_indicator, operator.__ge__, [int, float, np.float64, Decimal]
        )

    def gt(self, decimal_or_indicator) -> TIndicator:
        return self.indicator_binary_operation(
            decimal_or_indicator, operator.__gt__, [int, float, np.float64, Decimal]
        )

    def add(self, decimal_or_indicator) -> TIndicator:
        return self.indicator_binary_operation(
            decimal_or_indicator, operator.__add__, [int, float, np.float64, Decimal]
        )

    def sub(self, decimal_or_indicator) -> TIndicator:
        return self.indicator_binary_operation(
            decimal_or_indicator, operator.__sub__, [int, float, np.float64, Decimal]
        )

    def mul(self, decimal_or_indicator) -> TIndicator:
        return self.indicator_binary_operation(
            decimal_or_indicator, operator.__mul__, [int, float, np.float64, Decimal]
        )

    def truediv(self, decimal_or_indicator) -> TIndicator:
        return self.indicator_binary_operation(
            decimal_or_indicator,
            operator.__truediv__,
            [int, float, np.float64, Decimal],
        )

    def floordiv(self, decimal_or_indicator) -> TIndicator:
        return self.indicator_binary_operation(
            decimal_or_indicator,
            operator.__floordiv__,
            [int, float, np.float64, Decimal],
        )

    def neg(self) -> TIndicator:
        return self.indicator_unary_operation(
            operator.__neg__, [int, float, np.float64, Decimal]
        )

    def sand(self, bool_or_bool_indicator) -> TIndicator:
        return self.indicator_binary_operation(
            bool_or_bool_indicator, operator.__and__, [bool]
        )

    def sor(self, bool_or_bool_indicator) -> TIndicator:
        return self.indicator_binary_operation(
            bool_or_bool_indicator, operator.__or__, [bool]
        )


class CandleIndicator(Indicator):
    def __init__(
        self,
        asset: Asset = None,
        time_unit: timedelta = None,
        source: Indicator | np.ndarray | list = None,
        source_minimal_size: int = None,
        size: int = None,
    ):
        super().__init__(
            source=source, source_minimal_size=source_minimal_size, size=size
        )
        self.asset = asset
        self.time_unit = time_unit
        self.prices = {}

    def __call__(self, price_type: PriceType) -> Indicator:
        if price_type not in self.prices:
            self.prices[price_type] = self.map(get_price_selector_function(price_type))
        return self.prices[price_type]

    def set_asset(self, asset: Asset):
        self.asset = asset

    def set_time_unit(self, time_unit: timedelta):
        self.time_unit = time_unit


class TimeFramedCandleIndicator(Indicator):
    def __init__(
        self,
        time_unit: timedelta,
        market_cal: MarketCalendar,
        size: int = None,
        source: Indicator = None,
    ):
        super().__init__(source=source, size=size)
        self.time_unit = time_unit
        self.market_cal = market_cal
        self.aggregate_current_timestamp = timestamp_to_utc(datetime.min)
        self.aggregated_df = None
        self.aggregated_once = False

    def get_aggregated_candle(
        self, remove_incomplete_head: bool = True
    ) -> Optional[Candle]:
        self.aggregated_once = True
        aggregated_df = self.aggregated_df.rescale(
            self.time_unit, self.market_cal, remove_incomplete_head
        )
        if aggregated_df.empty:
            return None
        return aggregated_df.get_candle(0)

    def handle_data(self, data: Candle) -> None:
        if self.mode == IndicatorMode.LIVE:
            if self.time_unit == data.time_unit:
                super().handle_stream_data(data)
                return
            next_aggregate_timestamp = self.aggregate_current_timestamp + self.time_unit
            if data.timestamp < next_aggregate_timestamp:
                self.aggregated_df.add_candle(data)
                if data.timestamp == next_aggregate_timestamp - data.time_unit:
                    aggregated_candle = self.get_aggregated_candle()
                    if aggregated_candle is not None:
                        super().handle_stream_data(aggregated_candle)
                    self.aggregated_df = None
            else:
                if self.aggregated_df is not None:
                    aggregated_candle = self.get_aggregated_candle(
                        remove_incomplete_head=not self.aggregated_once
                    )
                    super().handle_stream_data(aggregated_candle)
                self.aggregated_df = CandleDataFrame.from_candle_list(
                    asset=data.asset, candles=np.array([data], dtype=Candle)
                )
                self.aggregate_current_timestamp = round_time(
                    data.timestamp, self.time_unit
                )
        else:
            super().handle_data(data)


class CandleData:
    def __init__(
        self,
        indicators: "ReactiveIndicators",
        candles: dict[Asset, dict[timedelta, np.array]] = None,
    ):
        self.indicators = indicators
        self.mode = self.indicators.mode
        if self.mode == IndicatorMode.BATCH and candles is None:
            raise Exception(
                "When execution mode is BATCH, you should provide candle data for batch processing"
            )
        self.candles = candles
        self.data = {
            asset: {
                time_unit: self.indicators.CandleIndicator(
                    asset=asset,
                    time_unit=time_unit,
                    source=self.candles[asset][time_unit]
                    if self.mode == IndicatorMode.BATCH
                    else None,
                    size=self.candles[asset][time_unit].size,
                )
                for time_unit in self.candles[asset]
            }
            for asset in self.candles
        }

    def exists(self, asset: Asset, time_unit: timedelta):
        return asset in self.data and time_unit in self.data[asset]

    def __call__(self, asset: Asset, time_unit: timedelta) -> CandleIndicator:
        message = "data is not available in the candle data. Make sure you specified the right feed."
        if asset not in self.data:
            raise Exception(f"{asset} " + message)
        if time_unit not in self.data[asset]:
            raise Exception(f"{asset}-{time_unit} " + message)
        return self.data[asset][time_unit]


class ZipIndicator(Indicator):
    def __init__(
        self,
        source_indicator1: Indicator,
        source_indicator2: Indicator,
        operation_function: Callable,
        transform: Callable = None,
    ):
        super().__init__(transform=transform)
        self.source_indicator1 = source_indicator1
        self.source_indicator2 = source_indicator2
        self.data1_queue = deque()
        self.data2_queue = deque()
        self.operation_function = operation_function
        self.count = 0
        self.data = None
        self.callbacks = deque()
        self.source_indicator1.subscribe(
            lambda data: self.handle_source1_data(data), self
        )
        self.source_indicator2.subscribe(
            lambda data: self.handle_source2_data(data), self
        )

    def handle_stream_data(self, data) -> None:
        data1 = self.data1_queue.popleft()
        data2 = self.data2_queue.popleft()
        if data1 is not None and data2 is not None:
            self.data = self.operation_function(data1, data2)
        else:
            self.data = None
        super().handle_stream_data(self.data)

    def handle_source1_data(self, data: Any) -> None:
        self.data1_queue.append(data)
        if self.count < 0:
            self.handle_stream_data(data)
        self.count += 1

    def handle_source2_data(self, data: Any) -> None:
        self.data2_queue.append(data)
        if self.count > 0:
            self.handle_stream_data(data)
        self.count -= 1
