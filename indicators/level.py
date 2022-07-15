from collections import deque
from typing import Callable, Optional

import numpy as np
from intervaltree import IntervalTree
from numpy.ma.core import MaskedConstant

from trazy_analysis.indicators.common import PriceType
from trazy_analysis.indicators.indicator import (
    Indicator,
    get_price_selector_function,
)
from trazy_analysis.models.candle import Candle


class Peak(Indicator):
    def fractal_is_peak(self, index_to_check: int) -> bool:
        data = self.input_window
        for current_order in range(1, self.order + 1):
            for direction in [-1, +1]:
                index = index_to_check + direction * current_order
                first_index = index - direction
                first = data[first_index]
                second_index = index
                second = data[second_index]
                if not self.comparator(first, second):
                    return False
        return True

    def local_extrema_is_peak(self, index_to_check: int) -> bool:
        data = self.input_window
        for current_order in range(1, self.order + 1):
            for direction in [-1, +1]:
                first_index = index_to_check
                first = data[first_index]
                second_index = index_to_check + direction * current_order
                second = data[second_index]
                if not self.comparator(first, second):
                    return False
        return True

    def is_peak(self, index_to_check: int) -> bool:
        if self.method == "fractal":
            return self.fractal_is_peak(index_to_check)
        elif self.method == "local_extrema":
            return self.local_extrema_is_peak(index_to_check)
        else:
            raise Exception(f"method {self.method} is not among the supported method")

    def initialize_stream(self):
        data = self.input_window
        data_len = data.count()
        start = -data_len + self.order + 1
        end = -self.order + 1
        peaks = [False] * self.order * 2
        for index_to_check in range(start, end):
            peaks.append(self.is_peak(index_to_check))
        self.fill(np.array(peaks, dtype=self.dtype))

    def __init__(
        self,
        comparator: Callable,
        order: int = 1,
        method: str = "fractal",
        size: int = 1,
        source: Optional[Indicator] = None,
    ):
        self.order = order
        self.method = method
        self.peak_size = 2 * order + 1
        super().__init__(source=source, source_minimal_size=self.peak_size, size=size)
        self.comparator = comparator

    def handle_stream_data(self, data) -> None:
        index_to_check = -self.order
        if self.input_window.count() < self.peak_size:
            super().handle_stream_data(False)
            return
        super().handle_stream_data(self.is_peak(index_to_check))


class Level:
    def __init__(self, index, level, level_type, merge_distance):
        self.index = index
        self.level = level
        self.level_type = level_type
        self.merge_distance = merge_distance

    def __hash__(self):
        return 0

    def __eq__(self, other):
        return (
            self.level_type == other.level_type
            and abs(self.level - other.level) < self.merge_distance
        )

    def __str__(self):
        return f"({self.index}, {self.level}, {self.level_type})"


class LevelInfo:
    def __init__(self):
        self.min_value = float("inf")
        self.max_value = float("-inf")
        self.power = 0
        self.indexes = deque()

    def update(self, index, size, value):
        self.min_value = min(self.min_value, value)
        self.max_value = max(self.max_value, value)
        self.power += 1
        if len(self.indexes) != 0:
            oldest_index = self.indexes[0]
            if oldest_index < index - size + 1:
                self.indexes.popleft()
                self.power -= 1
        self.indexes.append(index)

    def __str__(self):
        return f"power: {self.power}, range: [{self.min_value}, {self.max_value}]"


class ResistanceLevels(Indicator):
    def __init__(
        self,
        accuracy: int = 2,
        order: int = 2,
        size: int = None,
        source: Indicator = None,
        transform: Callable = None,
    ):
        self.accuracy = accuracy
        self.order = order
        self.peak_size = 2 * order + 1
        super().__init__(
            source=source,
            transform=transform,
            source_minimal_size=self.peak_size,
            size=size,
        )
        self.highs = None
        self.lows = None
        self.merge_dist = None
        self.maximas = None
        self.minimas = None
        self.resistances = IntervalTree()
        self.supports = IntervalTree()
        self.levels = {}
        self.candle_index = -1

    def setup(self, indicators: "ReactiveIndicators"):
        self.highs = self.source.map(get_price_selector_function(PriceType.HIGH), self.peak_size)
        self.lows = self.source.map(get_price_selector_function(PriceType.LOW), self.peak_size)
        self.merge_dist = (
            indicators.Average(size=self.size, source=self.highs).sub(
                indicators.Average(size=self.size, source=self.lows)
            )
        ).truediv(self.accuracy)
        self.maximas = indicators.Peak(
            comparator=np.greater,
            order=self.order,
            method="local_extrema",
            size=self.size,
            source=self.highs,
        )
        self.minimas = indicators.Peak(
            comparator=np.less,
            order=self.order,
            method="local_extrema",
            size=self.size,
            source=self.highs,
        )
        super().setup(indicators)

    def handle_stream_data(self, data: Candle) -> None:
        self.candle_index += 1
        if self.maximas.data:
            last_peak = self.input_window[-self.order]
            level = Level(
                self.candle_index - self.order,
                last_peak.high,
                "resistance",
                self.merge_dist.data,
            )
            level_info = self.levels.get(level, LevelInfo())
            min_value = level_info.min_value
            max_value = level_info.max_value
            if self.resistances.containsi(min_value, max_value, level_info):
                self.resistances.removei(min_value, max_value, level_info)

            level_info.update(self.candle_index - self.order, self.size, level.level)
            self.levels.update({level: level_info})
            min_value = level_info.min_value
            max_value = level_info.max_value
            right = max_value
            if min_value < max_value:
                left = min_value
            else:
                left = max_value - 0.0000001
            self.resistances[left:right] = (level, level_info)
        elif self.minimas.data:
            last_peak = self.input_window[-self.order]
            level = Level(
                self.candle_index - self.order,
                last_peak.low,
                "support",
                self.merge_dist.data,
            )
            level_info = self.levels.get(level, LevelInfo())
            min_value = level_info.min_value
            max_value = level_info.max_value
            if self.supports.containsi(min_value, max_value, level_info):
                self.supports.removei(min_value, max_value, level_info)

            level_info.update(self.candle_index - self.order, self.size, level.level)
            self.levels.update({level: level_info})
            min_value = level_info.min_value
            max_value = level_info.max_value
            left = min_value
            if min_value < max_value:
                right = max_value
            else:
                right = min_value + 0.0000001
            self.supports[left:right] = (level, level_info)

        if self.input_window.count() > 1:
            previous_candle = self.input_window[-1]
            if previous_candle.high < data.high:
                for interval in self.resistances[previous_candle.low : data.high]:
                    level, level_info = interval.data
                    level.level_type = "support"
                    level_info.power -= 1
                    if level_info.power == 0:
                        self.resistances.remove(interval)
                        del self.levels[level]
            if previous_candle is not None and previous_candle.low > data.low:
                for interval in self.supports[data.low : previous_candle.high]:
                    level, level_info = interval.data
                    level_info.power -= 1
                    if level_info.power == 0:
                        self.supports.remove(interval)
                        del self.levels[level]
        super().handle_stream_data(self.levels)

    def __hash__(self):
        return 0

    def __eq__(self, other):
        return True

    def __str__(self):
        return f"({self.index}, {self.level})"


class TradingRange:
    def __init__(self, start_index, end_index, low, high):
        self.min_value = low
        self.max_value = high
        self.start_index = start_index
        self.end_index = end_index
        self.length = self.end_index - self.start_index + 1

    def __str__(self):
        return f"index range: [{self.start_index}, {self.end_index}], range: [{self.min_value}, {self.max_value}]"


class TightTradingRange(Indicator):
    def __init__(
        self,
        size: int,
        min_overlaps: int,
        source: Indicator = None,
        transform: Callable = None,
    ):
        self.size = size
        self.min_overlaps = min_overlaps
        self.candle_index = -1
        self.trading_ranges = IntervalTree()
        super().__init__(
            source=source,
            transform=transform,
            source_minimal_size=self.size,
            size=self.size,
        )

    def overlap(self, intervals):
        # variable to store the maximum
        # count
        ans = 0
        count = 0
        data = []

        # storing the x and y
        # coordinates in data vector
        for i in range(len(intervals)):

            # pushing the x coordinate
            data.append([intervals[i][0], "x"])

            # pushing the y coordinate
            data.append([intervals[i][1], "y"])

        # sorting of ranges
        data = sorted(data)

        # Traverse the data vector to
        # count number of overlaps
        for i in range(len(data)):

            # if x occur it means a new range
            # is added so we increase count
            if data[i][1] == "x":
                count += 1

            # if y occur it means a range
            # is ended so we decrease count
            if data[i][1] == "y":
                count -= 1

            # updating the value of ans
            # after every traversal
            ans = max(ans, count)

        # printing the maximum value
        return ans

    def handle_data(self, data: Candle) -> None:
        self.candle_index += 1
        recent_candles = self.input_window[-self.size + 1 :]
        intervals = []
        min_low = float("inf")
        max_high = float("-inf")
        for candle in recent_candles:
            if candle is not None:
                min_low = min(min_low, candle.low)
                max_high = max(max_high, candle.high)
                intervals.append(
                    (min(candle.open, candle.close), max(candle.open, candle.close))
                )
        overlaps = self.overlap(intervals)
        if overlaps >= self.min_overlaps:
            start_index = self.candle_index + 1 - self.size
            end_index = self.candle_index
            trading_range = TradingRange(start_index, end_index, min_low, max_high)
            self.trading_ranges[start_index : end_index + 1] = trading_range
