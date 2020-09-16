from _pydecimal import Decimal

import pandas as pd
from rx import Observable

from common.decorators import Singleton
from indicators.common import PriceType, get_or_create_nested_dict
from indicators.rolling_window import RollingWindowStream
from indicators.stream import StreamData


class SmaStream(StreamData):
    def initialize_sum(self):
        self.sum: Decimal = sum(self.rolling_window_stream.window, 0)
        self.data = self.sum / self.period
        self.oldest: Decimal = self.rolling_window_stream[-self.period + 1]

    def __init__(
        self, period: int, source_data: StreamData = None,
    ):
        self.period = period
        self.sum: Decimal = 0
        self.oldest: Decimal = 0
        self.rolling_window_stream = None
        super().__init__(source_data=source_data)
        if self.rolling_window_stream.filled():
            self.initialize_sum()

    def _handle_new_data(self, new_data: Decimal) -> None:
        self.sum += new_data - self.oldest
        if self.rolling_window_stream.filled():
            self.data = self.sum / self.period
            self.oldest = self.rolling_window_stream[-self.period + 1]
        self.on_next(self.data)

    def observe(self, observable: Observable):
        if (
            issubclass(type(observable), RollingWindowStream)
            and observable.period >= self.period
        ):
            self.rolling_window_stream = observable
        else:
            self.rolling_window_stream = RollingWindowStream(
                period=self.period, source_data=observable
            )
        super().observe(self.rolling_window_stream)


@Singleton
class SmaFactory:
    def __init__(self):
        self.cache = {}

    def __call__(
        self,
        symbol: str,
        period: int,
        time_unit: pd.offsets.DateOffset,
        price_type: PriceType = PriceType.CLOSE,
    ) -> SmaStream:
        get_or_create_nested_dict(self.cache, symbol, period, time_unit)

        if price_type not in self.cache[symbol][period][time_unit]:
            from indicators.indicators import PriceRollingWindow

            price_rolling_window = PriceRollingWindow(
                symbol, period, time_unit, price_type
            )
            self.cache[symbol][period][time_unit][price_type] = SmaStream(
                period, price_rolling_window
            )
        return self.cache[symbol][period][time_unit][price_type]
