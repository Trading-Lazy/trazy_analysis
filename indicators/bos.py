import numpy as np
from typing import Any, Callable

from trazy_analysis.indicators.common import PriceType
from trazy_analysis.indicators.indicator import Indicator
from trazy_analysis.indicators.level import Peak
from trazy_analysis.indicators.rolling_window import (
    RollingWindow,
    get_price_selector_function,
)
from trazy_analysis.models.candle import Candle


class PreviousExtrema(RollingWindow):
    def initialize_previous_extremas(self):
        current_extrema = None
        extremas = [None] * self.order * 2
        for index in range(-self.peak.nb_elts + 1, 1):
            if self.peak[index]:
                current_extrema = self.rolling_window_stream[index - self.order]
            extremas.append(current_extrema)
        self.prefill(np.array(extremas, dtype=self.dtype))

    def __init__(
        self,
        comparator: Callable,
        order: int,
        method: str = "fractal",
        source_indicator: Indicator = None,
        size: int = 1,
        dtype: type = None,
        preload=False,
    ):
        self.comparator = comparator
        self.order = order
        self.method = method
        self.dtype = dtype
        self.peak = Peak(
            comparator=self.comparator,
            order=self.order,
            method=self.method,
            size=size,
            dtype=self.dtype,
            source_indicator=source_indicator,
            preload=preload,
        )
        self.rolling_window_stream = self.peak.rolling_window_stream
        super().__init__(
            size=size,
            source_indicator=self.rolling_window_stream,
            dtype=dtype,
            preload=preload,
        )
        self.prefill(np.zeros(self.size, dtype=dtype))

        if self.peak.filled():
            self.initialize_previous_extremas()

    def handle_new_data(self, new_data) -> None:
        if not self.preload:
            if self.peak.data:
                current_extrema = self.rolling_window_stream[-self.order]
                super().handle_new_data(current_extrema)
            else:
                super().handle_new_data(None)
        else:
            if self.peak.data is None:
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
            size=size,
            source_indicator=source_indicator,
            dtype=Candle,
            preload=preload,
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
    def get_source_from_base(self, source_indicator: Indicator, base="body"):
        sources = {
            "body": source_indicator.map(get_price_selector_function(PriceType.CLOSE)),
            "candle": source_indicator.map(
                lambda candle: candle.high
                if self.comparator(candle.high, candle.low)
                else candle.low
            ),
        }
        return sources[base]

    def breakout(self, index_to_check: int):
        if self.rolling_window_stream.nb_elts < 2:
            return False
        first: float = self.breakout_source[index_to_check - 1]
        second: float = self.breakout_source[index_to_check]
        extrema = self.previous_extrema.data
        return (
                self.comparator(first, extrema)
                and self.comparator(second, extrema)
                and not self.pin_bar[index_to_check - 1]
                and not self.pin_bar[index_to_check]
        )

    def initialize_bos(self):
        data = self.rolling_window_stream
        len = data.nb_elts
        start = -len + 2
        end = 1
        breaks = [False]
        for index in range(start, end):
            breaks.append(self.breakout(index))
        self.prefill(np.array(breaks, dtype=self.dtype))

    def __init__(
        self,
        comparator: Callable,
        order: int,
        method: str = "fractal",
        extrema_base: str = "body",
        breakout_base: str = "body",
        source_indicator: Indicator = None,
        size: int = 1,
        dtype: type = Candle,
        preload=False,
    ):
        self.comparator = comparator
        self.order = order
        self.method = method
        self.extrema_base = extrema_base
        self.breakout_base = breakout_base
        self.dtype = dtype

        extrema_source = self.get_source_from_base(source_indicator, extrema_base)
        self.previous_extrema = PreviousExtrema(
            comparator=self.comparator,
            order=self.order,
            method=self.method,
            size=size,
            dtype=self.dtype,
            source_indicator=extrema_source,
            preload=preload,
        )
        self.rolling_window_stream = self.previous_extrema.rolling_window_stream

        self.breakout_source = self.get_source_from_base(
            source_indicator, breakout_base
        )

        self.pin_bar = SmallerCandleBody(
            body_ratio=0.3, source_indicator=source_indicator, size=size, preload=preload
        )
        super().__init__(
            size=size,
            source_indicator=self.rolling_window_stream,
            dtype=dtype,
            preload=preload,
        )

        if self.rolling_window_stream.filled():
            self.initialize_bos()


    def handle_new_data(self, new_data) -> None:
        if not self.preload:
            if self.previous_extrema.nb_elts != 0:
                index_to_check = 0
                super().handle_new_data(self.breakout(index_to_check))
            else:
                super().handle_new_data(False)
        else:
            if self.peak.data is None:
                self.data = None
                self.on_next(self.data)
                self.index += 1
            else:
                super().handle_new_data(new_data)

    def push(self, new_data: Any = None):
        self.rolling_window_stream.push(new_data)
