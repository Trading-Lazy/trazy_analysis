import random
from typing import Any, Union, Optional, List

import numpy as np
import pandas as pd
import talib
from pandas import DatetimeIndex
from plotly.basedatatypes import BaseTraceType

from trazy_analysis.indicators.indicator import (
    Indicator, COLORS,
)
import plotly.graph_objects as go


class Sma(Indicator):
    def __init__(
        self,
        source: Indicator,
        period: int,
        size: int = None,
    ):
        self.period = period
        self.sum: float = 0.0
        self.oldest: float = 0.0
        super().__init__(
            source=source, source_minimal_size=period, size=size, dtype=float
        )

    def initialize_stream(self):
        self.initialize_batch()
        self.sum = sum(self.input_window.window)
        self.oldest = self.input_window[-self.period + 1]

    @staticmethod
    def compute(data: np.ndarray | pd.DataFrame | pd.Series, period: int) -> np.ndarray:
        return talib.SMA(data, timeperiod=period)

    @staticmethod
    def plotting_attributes() -> list[str]:
        return ["x", "y"]

    def get_trace(self, index: DatetimeIndex) -> Optional[BaseTraceType]:
        x, y = self.get_trace_coordinates(index)
        if y is None:
            return None
        return go.Scatter(
            name=f"Sma {self.period}",
            x=x,
            y=y,
            line=dict(color=random.choice(COLORS), width=2),
        )

    def handle_stream_data(self, data: Any):
        self.sum += data - self.oldest
        if self.input_window.count() >= self.period:
            self.data = self.sum / self.period
            self.oldest = self.input_window[-self.period + 1]
            super().handle_stream_data(self.data)
            return
        self.next(self.data)

    def push(self, data: Any = None):
        self.input_window.push(data)


class Average(Indicator):
    def initialize_stream(self):
        pass

    def __init__(self, size: int, source: Indicator = None):
        self.size = size
        self.sum: float = 0.0
        self.count = 0
        self.oldest: float = 0.0
        super().__init__(source=source, size=size)

    def handle_stream_data(self, data: float) -> None:
        if self.count < self.size:
            self.count += 1
        self.sum += data - self.oldest
        self.data = self.sum / self.count
        if self.input_window.count() >= self.size:
            self.oldest = self.input_window[-self.size + 1]
            super().handle_stream_data(self.data)
            return
        self.next(self.data)

    def push(self, data: Any = None):
        self.input_window.push(data)


class Ema(Indicator):
    def __init__(self, source: Indicator, period: int, size: int = 1):
        super().__init__(
            source=source, source_minimal_size=period, size=size, dtype=float
        )
        self.period = period
        self.factor = 2 / (1 + self.period)
        self.initial_sum = 0
        self.previous_ema = None

    def initialize_stream(self):
        self.initialize_batch()
        self.previous_ema = self[0]
        self.data = self[0]

    def handle_stream_data(self, data: Any):
        if self.input_window.count() < self.period:
            self.initial_sum += data
        elif self.input_window.count() == self.period:
            self.data = self.previous_ema = self.initial_sum / self.period
        else:
            self.data = self.previous_ema = data * self.factor + self.previous_ema * (
                1 - self.factor
            )
            super().handle_stream_data(self.data)
        self.next(self.data)

    @staticmethod
    def compute(data: np.ndarray | pd.DataFrame, period: int) -> np.ndarray:
        return talib.EMA(data, timeperiod=period)
