from indicators.rolling_window import (
    PriceRollingWindowManager,
    RollingWindowManager,
    TimeFramedCandleRollingWindowManager,
)
from indicators.sma import SmaManager


class IndicatorsManager:
    def __init__(self):
        self.rolling_window_manager = RollingWindowManager()
        self.time_framed_candle_rolling_window_manager = (
            TimeFramedCandleRollingWindowManager(self)
        )
        self.price_rolling_window_manager = PriceRollingWindowManager(self)
        self.sma_manager = SmaManager(self)

    @property
    def RollingWindow(self):
        return self.rolling_window_manager

    @property
    def TimeFramedCandleRollingWindow(self):
        return self.time_framed_candle_rolling_window_manager

    @property
    def PriceRollingWindow(self):
        return self.price_rolling_window_manager

    @property
    def Sma(self):
        return self.sma_manager
