from _pydecimal import Decimal
from typing import Any, Callable

import pandas as pd
from pandas_market_calendars import MarketCalendar
from rx import Observable

from common.decorators import Singleton
from common.exchange_calendar_euronext import EuronextExchangeCalendar
from common.helper import round_time
from common.types import CandleDataFrame
from common.utils import timestamp_to_utc
from indicators.common import PriceType, get_or_create_nested_dict
from indicators.stream import StreamData
from models.candle import Candle


def get_price_selector_function(price_type: PriceType) -> Callable[[Candle], Decimal]:
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


class RollingWindowStream(StreamData):
    def __init__(
        self,
        period: int,
        prefill_list: list = [],
        source_data: Observable = None,
        transform: Callable = lambda new_data: new_data,
    ):
        super().__init__(source_data, transform)
        self.period: int = period
        self.window: list = [None] * self.period
        self._prefill(prefill_list)

    def _prefill(self, filling_list: list):
        nb_elts = len(filling_list) if filling_list else 0
        diff = self.period - nb_elts
        if diff > 0:
            for i in range(diff, self.period):
                self.window[i] = self.transform(filling_list[i - diff])
        else:
            self.window = list(map(self.transform, filling_list[-self.period :]))

    def _handle_new_data(self, new_data) -> None:
        if len(self.window) >= self.period:
            self.window.pop(0)
        transformed_data = self.transform(new_data)
        self.data = transformed_data
        self.window.append(transformed_data)
        self.on_next(transformed_data)

    def push(self, new_data):
        self._handle_new_data(new_data)

    def filled(self) -> bool:
        return self.window[0] is not None

    def __getitem__(self, key) -> Any:
        if isinstance(key, slice):
            start = key.start
            stop = key.stop
            step = key.step
            if not (-self.period + 1 <= start <= 0) or not (
                -self.period + 1 <= stop <= 0
            ):
                raise IndexError("Index out of Data bound")
            mapped_slice = slice(start - 1, stop - 1, step)
            return self.window[mapped_slice]
        elif isinstance(key, int):
            if not (-self.period + 1 <= key <= 0):
                raise IndexError("Index out of Data bound")
            return self.window[key - 1]
        else:
            raise TypeError("Invalid argument type: {}".format(type(key)))

    def map(self, func: Callable) -> "RollingWindowStream":
        rolling_window_stream = RollingWindowStream(
            self.period,
            prefill_list=self.window if self.filled() else [],
            source_data=self,
            transform=func,
        )
        return rolling_window_stream


class TimeFramedCandleRollingWindowStream(RollingWindowStream):
    def __init__(
        self,
        period: int,
        time_unit: pd.offsets.DateOffset,
        market_cal: MarketCalendar,
        prefill_list: list = [],
        source_data: Observable = None,
    ):
        super().__init__(
            period=period, prefill_list=prefill_list, source_data=source_data,
        )
        self.time_unit = time_unit
        self.market_cal = market_cal
        self.aggregate_oldest_timestamp = timestamp_to_utc(pd.Timestamp.min)
        self.aggregated_candle = None
        self.aggregated_df = None
        self.last_candle_added_timestamp = None

    def _handle_new_data(self, new_data: Candle) -> None:
        if self.time_unit == pd.offsets.Minute(1):
            super()._handle_new_data(new_data)
            return

        if (new_data.timestamp - self.time_unit) <= self.aggregate_oldest_timestamp:
            self.aggregated_df.add_candle(new_data)
        else:
            if self.aggregated_df is not None:
                self.aggregated_df = self.aggregated_df.aggregate(
                    self.time_unit, self.market_cal
                )
                self.aggregated_candle = self.aggregated_df.get_candle(0)
                super()._handle_new_data(self.aggregated_candle)
            self.aggregated_df = CandleDataFrame.from_candle_list(
                symbol=new_data.symbol, candles=[new_data]
            )
            self.aggregate_oldest_timestamp = round_time(
                new_data.timestamp, self.time_unit
            )


@Singleton
class RollingWindowFactory:
    def __init__(self):
        self.cache = {}

    def __call__(self, symbol: str, period: int = 5) -> RollingWindowStream:
        get_or_create_nested_dict(self.cache, symbol)
        if period not in self.cache[symbol]:
            self.cache[symbol][period] = RollingWindowStream(period=period)
        return self.cache[symbol][period]


@Singleton
class TimeFramedCandleRollingWindowFactory:
    def __init__(self):
        self.cache = {}

    def __call__(
        self,
        symbol: str,
        period: int,
        time_unit: pd.offsets.DateOffset,
        market_cal: MarketCalendar = EuronextExchangeCalendar(),
    ) -> TimeFramedCandleRollingWindowStream:
        get_or_create_nested_dict(self.cache, symbol, period, time_unit)

        if market_cal.name not in self.cache[symbol][period][time_unit]:
            from indicators.indicators import RollingWindow

            rolling_window = RollingWindow(symbol)
            self.cache[symbol][period][time_unit][
                market_cal.name
            ] = TimeFramedCandleRollingWindowStream(
                period=period,
                time_unit=time_unit,
                market_cal=market_cal,
                source_data=rolling_window,
            )
        return self.cache[symbol][period][time_unit][market_cal.name]


@Singleton
class PriceRollingWindowFactory:
    def __init__(self):
        self.cache = {}

    def __call__(
        self,
        symbol: str,
        period: int,
        time_unit: pd.offsets.DateOffset,
        price_type: PriceType = PriceType.CLOSE,
        market_cal: MarketCalendar = EuronextExchangeCalendar(),
    ) -> TimeFramedCandleRollingWindowStream:
        get_or_create_nested_dict(self.cache, symbol, period, time_unit, price_type)

        if market_cal.name not in self.cache[symbol][period][time_unit][price_type]:
            from indicators.indicators import TimeFramedCandleRollingWindow

            candle_rolling_window = TimeFramedCandleRollingWindow(
                symbol=symbol, period=period, time_unit=time_unit, market_cal=market_cal
            )
            price_selector_function = get_price_selector_function(price_type)
            price_rolling_window = candle_rolling_window.map(price_selector_function)
            self.cache[symbol][period][time_unit][price_type][
                market_cal.name
            ] = price_rolling_window
        return self.cache[symbol][period][time_unit][price_type][market_cal.name]
