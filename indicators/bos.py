import os
from datetime import datetime
from typing import Any, Callable, Optional

import numpy as np
import pytz
from intervaltree import IntervalTree

import trazy_analysis
from trazy_analysis.indicators.common import PriceType
from trazy_analysis.indicators.indicator import (
    Indicator,
    get_price_selector_function,
)
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.enums import CandleDirection

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
)


class PreviousExtrema(Indicator):
    def initialize_stream(self):
        current_extrema = None
        extremas = [None] * self.order * 2
        for index in range(-self.peak.size + 1, 1):
            if self.peak[index]:
                current_extrema = self.input_window[index - self.order]
            extremas.append(current_extrema)
        self.fill(np.array(extremas, dtype=self.dtype))
        self.data = self.window[-1]

    def __init__(
        self,
        comparator: Callable,
        order: int,
        method: str = "fractal",
        source: Optional[Indicator] = None,
        size: int = 1,
    ):
        self.comparator = comparator
        self.order = order
        self.method = method
        self.peak = None
        self.source = source
        super().__init__(
            source=self.source, size=size, source_minimal_size=(self.order + 1)
        )

    def setup(self, indicators: "ReactiveIndicators"):
        self.peak = indicators.Peak(
            comparator=self.comparator,
            order=self.order,
            method=self.method,
            size=self.size,
            source=self.source,
        )
        super().setup(indicators)

    def handle_stream_data(self, data: Any):
        if self.input_window.count() >= (self.order + 1) and self.peak.data:
            current_extrema = self.input_window[-self.order]
            super().handle_stream_data(current_extrema)
            return
        self.next(self.data)


class ExtremaChange(Indicator):
    def initialize_stream(self):
        current_extrema = None
        changes = []
        for index in range(-self.previous_extrema.size + 1, 1):
            extrema_change = False
            if current_extrema != self.previous_extrema[index]:
                current_extrema = self.previous_extrema[index]
                extrema_change = True
            changes.append(extrema_change)

        self.fill(np.array(changes, dtype=self.dtype))

    def __init__(
        self,
        comparator: Callable,
        order: int,
        method: str = "fractal",
        source: Optional[Indicator] = None,
        size: int = 1,
    ):
        self.comparator = comparator
        self.order = order
        self.method = method
        self.current_extrema = None
        self.previous_extrema = None
        super().__init__(source=source, size=size)

    def setup(self, indicators: "ReactiveIndicators"):
        self.previous_extrema = indicators.PreviousExtrema(
            comparator=self.comparator,
            order=self.order,
            method=self.method,
            source=self.source,
            size=self.size,
        )
        super().setup(indicators)

    def handle_stream_data(self, data: Any):
        extrema_change = False
        if self.previous_extrema.count() != 0:
            if self.current_extrema != self.previous_extrema.data:
                self.current_extrema = self.previous_extrema.data
                extrema_change = True
        super().handle_stream_data(extrema_change)

    def handle_batch_data(self):
        if self.previous_extrema.count() == 0:
            self.data = None
            self.next(self.data)
            self.index += 1
        else:
            super().handle_batch_data()


class CandleBodyShape(Indicator):
    def __init__(
        self,
        body_ratio: float,
        comparator: Callable = np.greater,
        source: Indicator = None,
        size: int = 1,
    ):
        self.body_ratio = body_ratio
        self.comparator = comparator
        super().__init__(source=source, size=size)

    def has_right_shape(self, candle: Candle) -> bool:
        body_width = abs(candle.close - candle.open)
        candle_width = abs(candle.high - candle.low)
        return self.comparator(body_width, candle_width * self.body_ratio)

    def handle_stream_data(self, data: Any):
        super().handle_stream_data(self.has_right_shape(data))


class SmallerCandleBody(CandleBodyShape):
    def __init__(
        self,
        body_ratio: float,
        source: Indicator = None,
        size: int = 1,
    ):
        self.body_ratio = body_ratio
        super().__init__(
            body_ratio=body_ratio, comparator=np.less, source=source, size=size
        )


class BiggerCandleBody(CandleBodyShape):
    def __init__(
        self,
        body_ratio: float,
        source: Indicator = None,
        size: int = 1,
    ):
        self.body_ratio = body_ratio
        super().__init__(
            body_ratio=body_ratio, comparator=np.greater, source=source, size=size
        )


class CandleBOS(Indicator):
    def get_source_from_base(
        self,
        source_indicator: Indicator,
        base="body",
        price_type=PriceType.BODY_HIGH,
    ):
        if base == "body":
            return source_indicator.map(
                get_price_selector_function(price_type), self.size
            )
        elif base == "candle":
            return source_indicator.map(
                lambda candle: candle.high
                if self.comparator(candle.high, candle.low)
                else candle.low,
                self.size,
            )
        else:
            raise Exception(f"base {base} is not a valid base")

    def breakout(self, index_to_check: int, extrema_broke: bool):
        if self.input_window.count() < 2:
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

    def initialize_stream(self):
        data = self.input_window
        len = data.count()
        start = -len + 2
        end = 1
        breaks = [False]
        for index in range(start, end):
            breaks.append(not breaks[-1] and self.breakout(index, self.extrema_broke))
        self.fill(np.array(breaks, dtype=self.dtype))

    def __init__(
        self,
        comparator: Callable,
        order: int,
        method: str = "fractal",
        extrema_base: str = "body",
        breakout_base: str = "body",
        source: Optional[Indicator] = None,
        size: int = 1,
    ):
        self.comparator = comparator
        self.order = order
        self.method = method
        self.extrema_base = extrema_base
        self.breakout_base = breakout_base
        self.pois = []
        self.previous_extrema = None
        self.current_extrema = None
        self.extrema_change = None
        self.extrema_broke = False
        self.breakout_source = None
        size = max(2, size)
        super().__init__(source=source, size=size, source_minimal_size=size)

    def setup(self, indicators: "ReactiveIndicators"):
        extrema_source = self.get_source_from_base(self.source, self.extrema_base)
        self.previous_extrema = indicators.PreviousExtrema(
            comparator=self.comparator,
            order=self.order,
            method=self.method,
            source=extrema_source,
            size=self.size,
        )
        self.current_extrema = self.previous_extrema.data
        self.extrema_change = indicators.ExtremaChange(
            comparator=self.comparator,
            order=self.order,
            method=self.method,
            source=extrema_source,
            size=self.size,
        )

        reverse_extrema_source = self.source.map(
            get_price_selector_function(PriceType.BODY_LOW)
        )
        self.reverse_extrema_change = indicators.ExtremaChange(
            comparator=np.less_equal,
            order=self.order,
            method=self.method,
            source=reverse_extrema_source,
            size=self.size,
        )

        self.breakout_source = self.get_source_from_base(
            self.source, self.breakout_base, PriceType.CLOSE
        )

        self.pin_bar = indicators.SmallerCandleBody(
            body_ratio=0.3,
            source=self.source,
            size=self.size,
        )
        super().setup(indicators)

    def handle_stream_data(self, data: Candle) -> None:
        if self.current_extrema != self.previous_extrema.data:
            self.current_extrema = self.previous_extrema.data
            self.extrema_broke = False
        if self.previous_extrema.count() != 0:
            index_to_check = 0
            breakout = self.breakout(index_to_check, self.extrema_broke)
            super().handle_stream_data(breakout)
            if breakout:
                self.extrema_broke = True
        else:
            super().handle_stream_data(False)


class ImbalanceInfo:
    def __init__(self, diff: float, timestamp: datetime = datetime.now(pytz.UTC)):
        self.diff = diff
        self.timestamp = timestamp


class Imbalance(Indicator):
    def __init__(
        self,
        source: Optional[Indicator] = None,
        transform: Callable = None,
    ):
        super().__init__(
            source=source, transform=transform, source_minimal_size=3, size=1
        )
        self.imbalances = IntervalTree()

    @classmethod
    def explicit_gap(cls, first: Candle, second: Candle) -> bool:
        return first.high < second.low or first.low > second.high

    def handle_stream_data(self, data: Candle) -> None:
        # remove previous imbalances
        low = data.low
        high = data.high
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
        if self.input_window.count() < 2:
            imbalance = False
            diff = 0
        elif self.input_window.count() == 2:
            first = self.input_window[-1]
            second = data
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
            first = self.input_window[-2]
            second = self.input_window[-1]
            third = data
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
        super().handle_stream_data((imbalance, diff))


class EngulfingCandle(Indicator):
    def __init__(
        self,
        direction: CandleDirection = CandleDirection.BULLISH,
        source: Optional[Indicator] = None,
        transform: Callable = None,
    ):
        self.direction = direction
        super().__init__(source=source, transform=transform, source_minimal_size=2)

    def handle_stream_data(self, data: Candle) -> None:
        if self.input_window.count() < 2:
            super().handle_stream_data(False)
            return

        previous_candle = self.input_window[-1]
        engulfing = (
            data.direction == self.direction
            and previous_candle.direction != self.direction
            and (
                previous_candle.direction == CandleDirection.BEARISH
                and previous_candle.close >= data.open
                and previous_candle.open <= data.close
                or previous_candle.direction == CandleDirection.BULLISH
                and previous_candle.close <= data.open
                and previous_candle.open >= data.close
            )
        )
        super().handle_stream_data(engulfing)


class PoiTouch(Indicator):
    def __init__(
        self,
        comparator: Callable,
        order: int,
        candle_bos: CandleBOS,
        method: str = "fractal",
        extrema_base: str = "body",
        breakout_base: str = "body",
        source: Indicator = None,
        transform: Callable = None,
        preload: bool = False,
    ):
        self.comparator = comparator
        self.order = order
        self.method = method
        self.extrema_base = extrema_base
        self.breakout_base = breakout_base
        self.input_window = source
        self.candle_bos = candle_bos
        self.preload = preload

        super().__init__(source, transform, dtype=bool)

        self.pending_bos = False
        self.pois = []
        self.min_value = float("inf")
        self.min_value_total = float("inf")
        self.bos_happened = False
        self.previous_extrema_val = None
        self.current_extrema_val = None
        self.poi_touchs = IntervalTree()

    def handle_stream_data(self, data: Candle) -> None:
        low = data.low
        high = data.high

        poi_is_touched = False

        time_to_buy = False
        for interval in self.poi_touchs[low:high]:
            LOG.info("we reached and interesting poi %s", data.time_unit)
            begin, end, _ = interval
            if (end - low) / (end - begin) >= 0.3:
                time_to_buy = True
                break

        if time_to_buy:
            for interval in self.poi_touchs[low:high]:
                self.poi_touchs.remove(interval)
            poi_is_touched = True

        super().handle_stream_data(poi_is_touched)

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
                    fibo_high = self.input_window[-self.order].high
                    poi_limit = fibo_low + (fibo_high - fibo_low) * 0.382
                    self.pois = list(
                        filter(lambda candle: candle.high <= poi_limit, self.pois)
                    )
                    LOG.info(
                        f"pois after filtering are: {[str(candle) for candle in self.pois]}"
                    )
                    for poi in self.pois:
                        LOG.info("Adding a new poi touch")
                        self.poi_touchs[poi.low : poi.high] = 1

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
            LOG.info(f"Waiting for BOS {data.time_unit}")
            self.pending_bos = True
            self.bos_happened = False
            min_value = self.candle_bos.reverse_extrema_change.previous_extrema.data
            if min_value < self.min_value:
                poi: Candle = self.candle_bos.input_window[-self.order]
                self.pois = [] if poi.direction == CandleDirection.BULLISH else [poi]
                self.min_value = min_value
        elif self.pending_bos:
            if data.direction == CandleDirection.BEARISH:
                self.pois.append(data)
            if self.candle_bos.data:
                LOG.info(f"The pois are {[str(candle) for candle in self.pois]}")
                LOG.info(f"This is a real bos and min_value is: {self.min_value}")
                self.bos_happened = True
