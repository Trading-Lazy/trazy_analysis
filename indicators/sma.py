import numpy as np
import pandas as pd

from trazy_analysis.common.helper import get_or_create_nested_dict
from trazy_analysis.indicators.common import PriceType
from trazy_analysis.indicators.indicator import Indicator
from trazy_analysis.indicators.rolling_window import (
    PriceRollingWindowManager,
    RollingWindow,
)
from trazy_analysis.models.asset import Asset


class Sma(RollingWindow):
    instances = 0
    count = 0

    def initialize_sum(self):
        ret = np.cumsum(self.rolling_window_stream.window, dtype=float)
        if self.period + 1 > len(ret):
            cum_sum_before = 0.0
        else:
            cum_sum_before = ret[-1 - self.period]
        self.sum = ret[-1] - cum_sum_before
        ret[self.period :] = ret[self.period :] - ret[: -self.period]
        moving_averages = ret[self.period - 1 :] / self.period
        super().prefill(moving_averages)
        if self.rolling_window_stream.filled():
            self.oldest = self.rolling_window_stream[-self.period + 1]

    def __init__(
        self,
        period: int,
        source_indicator: Indicator = None,
        dtype: type = None,
        preload=False,
    ):
        Sma.instances += 1
        self.period = period
        self.sum: float = 0.0
        self.oldest: float = 0.0
        self.rolling_window_stream = None
        self.dtype = dtype
        if issubclass(type(source_indicator), RollingWindow) and (
            not source_indicator.ready or source_indicator.size >= self.period
        ):
            self.rolling_window_stream = source_indicator
            size = self.rolling_window_stream.size
        else:
            self.rolling_window_stream = RollingWindow(
                size=self.period,
                source_indicator=source_indicator,
                dtype=self.dtype,
                preload=False,
            )
            size = self.period
        super().__init__(
            size=size,
            source_indicator=self.rolling_window_stream,
            dtype=float,
            preload=preload,
        )
        if self.rolling_window_stream.filled():
            self.initialize_sum()

    def handle_new_data(self, new_data: float) -> None:
        Sma.count += 1
        if not self.preload:
            self.sum += new_data - self.oldest
            if self.rolling_window_stream.nb_elts >= self.period:
                self.data = self.sum / self.period
                self.oldest = self.rolling_window_stream[-self.period + 1]
                super().handle_new_data(self.data)
                return
            self.on_next(self.data)
        else:
            if self.index < self.period:
                self.data = None
                self.on_next(self.data)
                self.index += 1
            else:
                super().handle_new_data(new_data)


class SmaManager:
    def __init__(
        self, price_rolling_window_manager: PriceRollingWindowManager, preload=True
    ):
        self.cache = {}
        self.price_rolling_window_manager = price_rolling_window_manager
        self.preload = preload

    def __call__(
        self,
        asset: Asset,
        period: int,
        time_unit: pd.offsets.DateOffset,
        price_type: PriceType = PriceType.CLOSE,
    ) -> Sma:
        get_or_create_nested_dict(self.cache, asset, period, time_unit)

        if price_type not in self.cache[asset][period][time_unit]:
            price_rolling_window = self.price_rolling_window_manager(
                asset, period, time_unit, price_type
            )
            self.cache[asset][period][time_unit][price_type] = Sma(
                period, price_rolling_window, dtype=float, preload=self.preload
            )
        return self.cache[asset][period][time_unit][price_type]

    def warmup(self):
        for asset in self.cache:
            for period in self.cache[asset]:
                for time_unit in self.cache[asset][period]:
                    for price_type in self.cache[asset][period][time_unit]:
                        sma_stream = self.cache[asset][period][time_unit][price_type]
                        sma_stream.set_size(sma_stream.rolling_window_stream.size)
                        if self.preload:
                            self.cache[asset][period][time_unit][
                                price_type
                            ].initialize_sum()
