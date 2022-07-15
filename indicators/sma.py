from typing import Any, Union

import numpy as np
import pandas as pd
import talib

from trazy_analysis.indicators.indicator import (
    Indicator,
)
from trazy_analysis.models.enums import ExecutionMode


class Sma(Indicator):
    def __init__(
        self,
        source: Indicator,
        period: int,
        size: int = 1,
    ):
        self.period = period
        self.sum: float = 0.0
        self.oldest: float = 0.0
        super().__init__(
            source=source, source_minimal_size=period, size=size, dtype=float
        )

    def initialize_batch(self):
        window = Sma.compute(self.input_window.window, self.period)
        self.fill(window)

    def initialize_stream(self):
        self.initialize_batch()
        self.sum = sum(self.input_window.window)
        self.oldest = self.input_window[-self.period + 1]
        self.data = self.window[-1]

    @staticmethod
    def compute(data: Union[np.ndarray, pd.DataFrame], period: int) -> np.ndarray:
        return talib.SMA(data, timeperiod=period)

    def handle_stream_data(self, data: Any):
        self.sum += data - self.oldest
        if self.input_window.count() >= self.period:
            self.data = self.sum / self.period
            self.oldest = self.input_window[-self.period + 1]
            super().handle_stream_data(self.data)
            return
        self.on_next(self.data)

    def handle_batch_data(self):
        if self.index + 1 < self.period - 1:
            self.data = None
            self.on_next(self.data)
            self.index = (self.index + 1) % self.size
        else:
            super().handle_batch_data()

    def push(self, new_data: Any = None):
        self.input_window.push(new_data)


class Average(Indicator):
    def initialize_batch(self):
        pass

    def initialize_stream(self):
        pass

    def __init__(self, size: int, source: Indicator = None):
        self.size = size
        self.sum: float = 0.0
        self.count = 0
        self.oldest: float = 0.0
        super().__init__(source=source, size=size)

    def handle_data(self, new_data: float) -> None:
        if self.count < self.size:
            self.count += 1
        if self.mode == ExecutionMode.LIVE:
            self.sum += new_data - self.oldest
            self.data = self.sum / self.count
            if self.input_window.nb_elts >= self.size:
                self.oldest = self.input_window[-self.size + 1]
                super().handle_stream_data(self.data)
                return
            self.on_next(self.data)
        elif self.mode == ExecutionMode.BATCH:
            super().handle_stream_data(new_data)

    def push(self, new_data: Any = None):
        self.input_window.push(new_data)
