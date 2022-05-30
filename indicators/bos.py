import os
from datetime import datetime, timedelta
from typing import Any, Callable

import numpy as np
import pandas as pd
import pytz
from intervaltree import IntervalTree
from pandas_market_calendars import MarketCalendar

import trazy_analysis
from trazy_analysis.common.crypto_exchange_calendar import CryptoExchangeCalendar
from trazy_analysis.common.helper import get_or_create_nested_dict
from trazy_analysis.indicators.common import PriceType
from trazy_analysis.indicators.indicator import Indicator
from trazy_analysis.indicators.level import Peak
from trazy_analysis.indicators.rolling_window import (
    RollingWindow,
    get_price_selector_function,
    TimeFramedCandleRollingWindowManager,
)
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.enums import CandleDirection

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
)


class PreviousExtrema(RollingWindow):
    def initialize(self):
        current_extrema = None
        extremas = [None] * self.order * 2
        for index in range(-self.peak.nb_elts + 1, 1):
            if self.peak[index]:
                current_extrema = self.rolling_window_stream[index - self.order]
            extremas.append(current_extrema)
        self.prefill(np.array(extremas, dtype=self.odtype))

    def __init__(
        self,
        comparator: Callable,
        order: int,
        method: str = "fractal",
        source_indicator: Indicator = None,
        size: int = 1,
        preload=False,
    ):
        self.comparator = comparator
        self.order = order
        self.method = method
        self.peak = Peak(
            comparator=self.comparator,
            order=self.order,
            method=self.method,
            size=size,
            source_indicator=source_indicator,
            preload=preload,
        )
        self.rolling_window_stream = self.peak.rolling_window_stream
        super().__init__(
            size=size,
            source_indicator=self.rolling_window_stream,
            idtype=float,
            odtype=float,
            preload=preload,
        )

        if self.peak.filled():
            self.initialize()

    def handle_new_data(self, new_data) -> None:
        if not self.preload:
            if self.peak.data:
                current_extrema = self.rolling_window_stream[-self.order]
                super().handle_new_data(current_extrema)
        else:
            if self.peak.data is None:
                self.data = None
                self.on_next(self.data)
                self.index += 1
            else:
                super().handle_new_data(new_data)

    def push(self, new_data: Any = None):
        self.rolling_window_stream.push(new_data)


class ExtremaChange(RollingWindow):
    def initialize(self):
        current_extrema = None
        changes = []
        for index in range(-self.previous_extrema.size + 1, 1):
            extrema_change = False
            if current_extrema != self.previous_extrema[index]:
                current_extrema = self.previous_extrema[index]
                extrema_change = True
            changes.append(extrema_change)

        self.prefill(np.array(changes, dtype=self.odtype))

    def __init__(
        self,
        comparator: Callable,
        order: int,
        method: str = "fractal",
        source_indicator: Indicator = None,
        size: int = 1,
        preload=False,
    ):
        self.comparator = comparator
        self.order = order
        self.method = method
        self.current_extrema = None
        self.previous_extrema = PreviousExtrema(
            comparator=self.comparator,
            order=self.order,
            method=self.method,
            source_indicator=source_indicator,
            size=size,
            preload=preload,
        )
        self.rolling_window_stream = self.previous_extrema.rolling_window_stream
        super().__init__(
            size=size,
            source_indicator=self.rolling_window_stream,
            idtype=float,
            odtype=bool,
            preload=preload,
        )

        if self.previous_extrema.filled():
            self.initialize()

    def handle_new_data(self, new_data) -> None:
        if not self.preload:
            extrema_change = False
            if self.previous_extrema.nb_elts != 0:
                if self.current_extrema != self.previous_extrema.data:
                    self.current_extrema = self.previous_extrema.data
                    extrema_change = True
            super().handle_new_data(extrema_change)
        else:
            if self.previous_extrema.nb_elts == 0:
                self.data = None
                self.on_next(self.data)
                self.index += 1
            else:
                super().handle_new_data(new_data)

    def push(self, new_data: Any = None):
        self.rolling_window_stream.push(new_data)


class CandleBodyShape(RollingWindow):
    def __init__(
        self,
        body_ratio: float,
        comparator: Callable = np.greater,
        source_indicator: Indicator = None,
        size: int = 1,
        preload=False,
    ):
        self.body_ratio = body_ratio
        self.comparator = comparator
        super().__init__(
            size=size, source_indicator=source_indicator, idtype=Candle, preload=preload
        )

    def has_right_shape(self, candle: Candle) -> bool:
        body_width = abs(candle.close - candle.open)
        candle_width = abs(candle.high - candle.low)
        return self.comparator(body_width, candle_width * self.body_ratio)

    def handle_new_data(self, new_data: Candle) -> None:
        if not self.preload:
            super().handle_new_data(self.has_right_shape(new_data))
        else:
            if new_data is None:
                self.data = None
                self.on_next(self.data)
                self.index += 1
            else:
                super().handle_new_data(new_data)


class SmallerCandleBody(CandleBodyShape):
    def __init__(
        self,
        body_ratio: float,
        source_indicator: Indicator = None,
        size: int = 1,
        preload=False,
    ):
        self.body_ratio = body_ratio
        super().__init__(
            body_ratio=body_ratio,
            comparator=np.less,
            source_indicator=source_indicator,
            size=size,
            preload=preload,
        )


class BiggerCandleBody(CandleBodyShape):
    def __init__(
        self,
        body_ratio: float,
        source_indicator: Indicator = None,
        size: int = 1,
        preload=False,
    ):
        self.body_ratio = body_ratio
        super().__init__(
            body_ratio=body_ratio,
            comparator=np.greater,
            source_indicator=source_indicator,
            size=size,
            preload=preload,
        )


class CandleBOS(RollingWindow):
    def get_source_from_base(
        self, source_indicator: Indicator, base="body", price_type=PriceType.BODY_HIGH
    ):
        if base == "body":
            return source_indicator.map(get_price_selector_function(price_type))
        elif base == "candle":
            return source_indicator.map(
                lambda candle: candle.high
                if self.comparator(candle.high, candle.low)
                else candle.low
            )
        else:
            raise Exception(f"base {base} is not a valid base")

    def breakout(self, index_to_check: int, extrema_broke: bool):
        if self.rolling_window_stream.nb_elts < 2:
            return False
        first: float = self.breakout_source[index_to_check - 1]
        second: float = self.breakout_source[index_to_check]
        extrema = self.previous_extrema.data
        return (
            not extrema_broke
            and self.comparator(first, extrema)
            and self.comparator(second, extrema)
            and not self.pin_bar[index_to_check - 1]
            and not self.pin_bar[index_to_check]
        )

    def initialize(self):
        data = self.rolling_window_stream
        len = data.nb_elts
        start = -len + 2
        end = 1
        breaks = [False]
        for index in range(start, end):
            breaks.append(not breaks[-1] and self.breakout(index))
        self.prefill(np.array(breaks, dtype=self.odtype))

    def __init__(
        self,
        comparator: Callable,
        order: int,
        method: str = "fractal",
        extrema_base: str = "body",
        breakout_base: str = "body",
        source_indicator: Indicator = None,
        size: int = 1,
        preload=False,
    ):
        self.comparator = comparator
        self.order = order
        self.method = method
        self.extrema_base = extrema_base
        self.breakout_base = breakout_base
        self.size = max(2, size)

        extrema_source = self.get_source_from_base(source_indicator, extrema_base)
        self.previous_extrema = PreviousExtrema(
            comparator=self.comparator,
            order=self.order,
            method=self.method,
            source_indicator=extrema_source,
            size=size,
            preload=preload,
        )
        self.current_extrema = self.previous_extrema.data

        self.extrema_change = ExtremaChange(
            comparator=self.comparator,
            order=self.order,
            method=self.method,
            source_indicator=extrema_source,
            size=size,
            preload=preload,
        )

        reverse_extrema_source = source_indicator.map(
            get_price_selector_function(PriceType.BODY_LOW)
        )
        self.reverse_extrema_change = ExtremaChange(
            comparator=np.less_equal,
            order=self.order,
            method=self.method,
            source_indicator=reverse_extrema_source,
            size=size,
            preload=preload,
        )
        self.extrema_broke = False

        if issubclass(type(source_indicator), RollingWindow) and (
            not source_indicator.ready or source_indicator.size >= size
        ):
            self.rolling_window_stream = source_indicator
        else:
            self.rolling_window_stream = RollingWindow(
                size=self.size,
                source_indicator=source_indicator,
                idtype=Candle,
                odtype=Candle,
                preload=False,
            )

        self.breakout_source = self.get_source_from_base(
            source_indicator, breakout_base, PriceType.CLOSE
        )

        self.pin_bar = SmallerCandleBody(
            body_ratio=0.3,
            source_indicator=source_indicator,
            size=max(2, size),
            preload=preload,
        )
        super().__init__(
            size=size,
            source_indicator=self.rolling_window_stream,
            idtype=Candle,
            odtype=bool,
            preload=preload,
        )

        self.pois = []
        if self.rolling_window_stream.filled():
            self.initialize()

    def handle_new_data(self, new_data) -> None:
        if self.current_extrema != self.previous_extrema.data:
            self.current_extrema = self.previous_extrema.data
            self.extrema_broke = False
        if not self.preload:
            if self.previous_extrema.nb_elts != 0:
                index_to_check = 0
                breakout = self.breakout(index_to_check, self.extrema_broke)
                super().handle_new_data(breakout)
                if breakout:
                    self.extrema_broke = True
            else:
                super().handle_new_data(False)
        else:
            if self.previous_extrema.nb_elts == 0:
                self.data = None
                self.on_next(self.data)
                self.index += 1
            else:
                super().handle_new_data(new_data)

    def push(self, new_data: Any = None):
        self.rolling_window_stream.push(new_data)


class CandleBosManager:
    def __init__(
        self,
        time_framed_candle_rolling_window_manager: TimeFramedCandleRollingWindowManager,
        market_cal: MarketCalendar = CryptoExchangeCalendar(),
        preload=True,
    ):
        self.cache = {}
        self.time_framed_candle_rolling_window_manager = (
            time_framed_candle_rolling_window_manager
        )
        self.market_cal = market_cal
        self.preload = preload

    def __call__(
        self,
        asset: Asset,
        time_unit: timedelta,
        comparator: Callable = np.greater,
        order: int = 2,
        method: str = "fractal",
        extrema_base: str = "body",
        breakout_base: str = "body",
    ) -> CandleBOS:
        get_or_create_nested_dict(
            self.cache, asset, time_unit, comparator, order, method, extrema_base
        )
        if (
            breakout_base
            not in self.cache[asset][time_unit][comparator][order][method][extrema_base]
        ):
            candle_rolling_window = self.time_framed_candle_rolling_window_manager(
                asset=asset,
                time_unit=time_unit,
                period=order * 2 + 1,
            )
            self.cache[asset][time_unit][comparator][order][method][extrema_base][
                breakout_base
            ] = CandleBOS(
                comparator=comparator,
                order=order,
                method=method,
                extrema_base=extrema_base,
                breakout_base=breakout_base,
                source_indicator=candle_rolling_window,
                preload=self.preload,
            )
        return self.cache[asset][time_unit][comparator][order][method][extrema_base][
            breakout_base
        ]

    def warmup(self):
        for asset in self.cache:
            for time_unit in self.cache[asset]:
                for comparator in self.cache[asset][time_unit]:
                    for order in self.cache[asset][time_unit][comparator]:
                        for method in self.cache[asset][time_unit][comparator][order]:
                            for extrema_base in self.cache[asset][time_unit][
                                comparator
                            ][order][method]:
                                for breakout_base in self.cache[asset][time_unit][
                                    comparator
                                ][order][method][extrema_base]:
                                    candle_bos = self.cache[asset][time_unit][
                                        comparator
                                    ][order][method][extrema_base][breakout_base]
                                    candle_bos.set_size(
                                        candle_bos.rolling_window_stream.size
                                    )
                                    if self.preload:
                                        self.cache[asset][time_unit][comparator][order][
                                            method
                                        ][breakout_base].initialize()


class ImbalanceInfo:
    def __init__(self, diff: float, timestamp: datetime = datetime.now(pytz.UTC)):
        self.diff = diff
        self.timestamp = timestamp


class Imbalance(Indicator):
    def __init__(
        self,
        source_indicator: Indicator = None,
        transform: Callable = lambda new_data: new_data,
    ):
        if issubclass(type(source_indicator), RollingWindow) and (
            not source_indicator.ready or source_indicator.size >= 3
        ):
            self.rolling_window_stream = source_indicator
        else:
            self.rolling_window_stream = RollingWindow(
                size=3, source_indicator=source_indicator, idtype=Candle, preload=False
            )
        super().__init__(source_indicator, transform, idtype=Candle, odtype=bool)
        self.imbalances = IntervalTree()

    @classmethod
    def explicit_gap(cls, first: Candle, second: Candle) -> bool:
        return first.high < second.low or first.low > second.high

    def handle_new_data(self, new_data: Candle) -> None:
        # remove previous imbalances
        low = new_data.low
        high = new_data.high
        for interval in self.imbalances[low:high]:
            self.imbalances.remove(interval)
            begin, end, imbalance_info = interval
            if low <= begin and end <= high:
                continue
            elif low <= begin:
                self.imbalances[high:end] = ImbalanceInfo(
                    diff=end - high, timestamp=imbalance_info.timestamp
                )
            elif end <= high:
                self.imbalances[begin:low] = ImbalanceInfo(
                    diff=low - begin, timestamp=imbalance_info.timestamp
                )
            else:
                self.imbalances[high:end] = ImbalanceInfo(
                    diff=end - high, timestamp=imbalance_info.timestamp
                )
                self.imbalances[begin:low] = ImbalanceInfo(
                    diff=low - begin, timestamp=imbalance_info.timestamp
                )

        # check new imbalances
        if self.rolling_window_stream.nb_elts < 2:
            imbalance = False
            diff = 0
        elif self.rolling_window_stream.nb_elts == 2:
            first = self.rolling_window_stream[-1]
            second = new_data
            imbalance = self.explicit_gap(first, second)
            diff = (
                max(second.low - first.high, first.low - second.high)
                if imbalance
                else 0
            )
            if imbalance:
                low = min(first.high, second.high)
                high = max(first.low, second.low)
                self.imbalances[low:high] = ImbalanceInfo(diff, first.timestamp)
        else:
            first = self.rolling_window_stream[-2]
            second = self.rolling_window_stream[-1]
            third = new_data
            imbalance = self.explicit_gap(second, third) or self.explicit_gap(
                first, third
            )
            diff = max(
                third.low - first.high,
                first.low - third.high,
            )
            if imbalance:
                low = min(first.high, third.high)
                high = max(first.low, third.low)
                self.imbalances[low:high] = ImbalanceInfo(diff, first.timestamp)
        super().handle_new_data((imbalance, diff))


class EngulfingCandle(Indicator):
    def __init__(
        self,
        direction: CandleDirection = CandleDirection.BULLISH,
        source_indicator: Indicator = None,
        transform: Callable = lambda new_data: new_data,
    ):
        self.direction = direction
        if issubclass(type(source_indicator), RollingWindow) and (
            not source_indicator.ready or source_indicator.size >= 2
        ):
            self.rolling_window_stream = source_indicator
        else:
            self.rolling_window_stream = RollingWindow(
                size=2, source_indicator=source_indicator, idtype=Candle, preload=False
            )
        super().__init__(source_indicator, transform)

    def handle_new_data(self, new_data: Candle) -> None:
        if self.rolling_window_stream.nb_elts < 2:
            super().handle_new_data(False)
            return

        previous_candle = self.rolling_window_stream[-1]
        engulfing = (
            new_data.direction == self.direction
            and previous_candle.direction != self.direction
            and (
                previous_candle.direction == CandleDirection.BEARISH
                and previous_candle.close >= new_data.open
                and previous_candle.open <= new_data.close
                or previous_candle.direction == CandleDirection.BULLISH
                and previous_candle.close <= new_data.open
                and previous_candle.open >= new_data.close
            )
        )
        super().handle_new_data(engulfing)


class PoiTouch(Indicator):
    def __init__(
        self,
        comparator: Callable,
        order: int,
        candle_bos: CandleBOS,
        method: str = "fractal",
        extrema_base: str = "body",
        breakout_base: str = "body",
        source_indicator: Indicator = None,
        transform: Callable = lambda new_data: new_data,
        preload: bool = False,
    ):
        self.comparator = comparator
        self.order = order
        self.method = method
        self.extrema_base = extrema_base
        self.breakout_base = breakout_base
        self.rolling_window_stream = source_indicator
        self.candle_bos = candle_bos
        self.preload = preload

        super().__init__(source_indicator, transform, idtype=Candle, odtype=bool)

        self.pending_bos = False
        self.pois = []
        self.min_value = float("inf")
        self.min_value_total = float("inf")
        self.bos_happened = False
        self.previous_extrema_val = None
        self.current_extrema_val = None
        self.poi_touchs = IntervalTree()

    def handle_new_data(self, new_data: Candle) -> None:
        low = new_data.low
        high = new_data.high

        poi_is_touched = False

        time_to_buy = False
        for interval in self.poi_touchs[low:high]:
            LOG.info("we reached and interesting poi")
            begin, end, data = interval
            if (high - end) / (end - begin) >= 0.3:
                time_to_buy = True
                break

        if time_to_buy:
            for interval in self.poi_touchs[low:high]:
                self.poi_touchs.remove(interval)
            poi_is_touched = True

        super().handle_new_data(poi_is_touched)

        self.min_value_total = min(self.min_value_total, low)
        if self.candle_bos.extrema_change.data:
            self.previous_extrema_val = self.current_extrema_val
            self.current_extrema_val = self.candle_bos.previous_extrema.data
        if self.candle_bos.extrema_change.data:
            if not self.bos_happened:
                LOG.info("This is the behaviour expected")
                self.pending_bos = False
                self.pois = []
                self.min_value = float("inf")
                self.min_value_total = float("inf")
            else:
                if self.current_extrema_val >= self.previous_extrema_val:
                    LOG.info("can start filtering pois")
                    LOG.info(
                        f"pois before filtering are: {[str(candle) for candle in self.pois]}"
                    )
                    # pin bar filtering
                    self.pois = filter(
                        lambda candle: (
                            abs(candle.open - candle.close) / (candle.high - candle.low)
                        )
                        >= 0.3,
                        self.pois,
                    )
                    # fibonacci
                    fibo_low = self.min_value_total
                    fibo_high = self.rolling_window_stream[-self.order].high
                    poi_limit = fibo_low + (fibo_high - fibo_low) * 0.382
                    self.pois = list(
                        filter(lambda candle: candle.high <= poi_limit, self.pois)
                    )
                    LOG.info(
                        f"pois after filtering are: {[str(candle) for candle in self.pois]}"
                    )
                    for poi in self.pois:
                        LOG.info("Adding a new poi touch")
                        self.poi_touchs[poi.low: poi.high] = 1

                    self.pending_bos = False
                    self.pois = []
                    self.min_value = float("inf")
                    self.min_value_total = float("inf")
                else:
                    self.bos_happened = False
                    self.pending_bos = False
                    self.pois = []
                    self.min_value = float("inf")
                    self.min_value_total = float("inf")
        elif (
            self.candle_bos.previous_extrema.data is not None
            and self.candle_bos.reverse_extrema_change.data
        ):
            LOG.info("Waiting for BOS")
            self.pending_bos = True
            self.bos_happened = False
            min_value = self.candle_bos.reverse_extrema_change.previous_extrema.data
            if min_value < self.min_value:
                poi: Candle = self.candle_bos.rolling_window_stream[-self.order]
                self.pois = [] if poi.direction == CandleDirection.BULLISH else [poi]
                self.min_value = min_value
        elif self.pending_bos:
            if new_data.direction == CandleDirection.BEARISH:
                self.pois.append(new_data)
            if self.candle_bos.data:
                LOG.info(f"The pois are {[str(candle) for candle in self.pois]}")
                LOG.info(f"This is a real bos and min_value is: {self.min_value}")
                self.bos_happened = True


class PoiTouchManager:
    def __init__(
        self,
        candle_bos_manager: CandleBosManager,
        market_cal: MarketCalendar = CryptoExchangeCalendar(),
        preload=True,
    ):
        self.cache = {}
        self.candle_bos_manager = candle_bos_manager
        self.market_cal = market_cal
        self.preload = preload

    def __call__(
        self,
        asset: Asset,
        time_unit: timedelta,
        comparator: Callable = np.greater,
        order: int = 2,
        method: str = "fractal",
        extrema_base: str = "body",
        breakout_base: str = "body",
    ) -> CandleBOS:
        get_or_create_nested_dict(
            self.cache, asset, time_unit, comparator, order, method, extrema_base
        )
        if (
            breakout_base
            not in self.cache[asset][time_unit][comparator][order][method][extrema_base]
        ):
            candle_bos = self.candle_bos_manager(
                asset,
                time_unit=time_unit,
                comparator=comparator,
                order=order,
                method=method,
                extrema_base=extrema_base,
                breakout_base=breakout_base,
            )
            self.cache[asset][time_unit][comparator][order][method][extrema_base][
                breakout_base
            ] = PoiTouch(
                comparator=comparator,
                order=order,
                candle_bos=candle_bos,
                method=method,
                extrema_base=extrema_base,
                breakout_base=breakout_base,
                source_indicator=candle_bos.rolling_window_stream,
                preload=self.preload,
            )
        return self.cache[asset][time_unit][comparator][order][method][extrema_base][
            breakout_base
        ]

    def warmup(self):
        for asset in self.cache:
            for time_unit in self.cache[asset]:
                for comparator in self.cache[asset][time_unit]:
                    for order in self.cache[asset][time_unit][comparator]:
                        for method in self.cache[asset][time_unit][comparator][order]:
                            for extrema_base in self.cache[asset][time_unit][
                                comparator
                            ][order][method]:
                                for breakout_base in self.cache[asset][time_unit][
                                    comparator
                                ][order][method][extrema_base]:
                                    poi_touch = self.cache[asset][time_unit][
                                        comparator
                                    ][order][method][extrema_base][breakout_base]
                                    poi_touch.candle_bos.set_size(
                                        poi_touch.rolling_window_stream.size
                                    )
                                    if self.preload:
                                        self.cache[asset][time_unit][comparator][order][
                                            method
                                        ][breakout_base].initialize()
