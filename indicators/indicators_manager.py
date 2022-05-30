from typing import Dict

import numpy as np

from trazy_analysis.indicators.bos import CandleBosManager, PoiTouchManager
from trazy_analysis.indicators.rolling_window import (
    PriceRollingWindowManager,
    RollingWindowManager,
    TimeFramedCandleRollingWindowManager,
)
from trazy_analysis.indicators.sma import SmaManager


class IndicatorsManager:
    def __init__(self, initial_data: Dict[str, np.array] = {}, preload: bool = True):
        self.initial_data = initial_data
        self.preload = preload
        self.rolling_window_manager = RollingWindowManager(preload=preload)
        self.time_framed_candle_rolling_window_manager = (
            TimeFramedCandleRollingWindowManager(
                self.rolling_window_manager, preload=preload
            )
        )
        self.price_rolling_window_manager = PriceRollingWindowManager(
            self.time_framed_candle_rolling_window_manager, preload=preload
        )
        self.sma_manager = SmaManager(
            self.price_rolling_window_manager, preload=preload
        )
        self.candle_bos_manager = CandleBosManager(
            self.time_framed_candle_rolling_window_manager, preload=preload
        )
        self.poi_touch = PoiTouchManager(
            self.candle_bos_manager, preload=preload
        )
        self.warmedup = False

    @property
    def data(self):  # pragma: no cover
        return self.rolling_window_manager

    @property
    def RollingWindow(self):  # pragma: no cover
        return self.rolling_window_manager

    @property
    def TimeFramedCandleRollingWindow(self):  # pragma: no cover
        return self.time_framed_candle_rolling_window_manager

    @property
    def PriceRollingWindow(self):  # pragma: no cover
        return self.price_rolling_window_manager

    @property
    def Sma(self):  # pragma: no cover
        return self.sma_manager

    @property
    def CandleBos(self):  # pragma: no cover
        return self.candle_bos_manager

    @property
    def PoiTouch(self):  # pragma: no cover
        return self.poi_touch

    def warmup(self):
        if not self.warmedup:
            self.rolling_window_manager.warmup(
                self.initial_data if self.preload else {}
            )
            self.time_framed_candle_rolling_window_manager.warmup()
            self.price_rolling_window_manager.warmup()
            self.sma_manager.warmup()
            self.candle_bos_manager.warmup()
            self.poi_touch.warmup()
            self.warmedup = True
