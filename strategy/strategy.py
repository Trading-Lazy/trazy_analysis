import abc
import os
from collections import deque

import logger
import settings
from common.clock import Clock
from indicators.indicator import Indicator
from indicators.indicators_manager import IndicatorsManager
from models.candle import Candle
from models.event import SignalEvent
from models.signal import Signal
from order_manager.order_manager import OrderManager

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)


class Strategy(Indicator):
    def __init__(
        self,
        symbol: str,
        order_manager: OrderManager,
        events: deque,
        indicators_manager: IndicatorsManager = IndicatorsManager(),
    ):
        super().__init__()
        self.symbol = symbol
        self.order_manager = order_manager
        self.events = events
        self.indicators_manager = indicators_manager
        self.is_opened = False
        self.name = self.__class__.__name__

    @abc.abstractmethod
    def generate_signal(self, candle) -> Signal:  # pragma: no cover
        raise NotImplementedError

    def process_candle(self, candle: Candle, clock: Clock):
        signal = self.generate_signal(candle, clock)
        if signal is not None:
            self.events.append(SignalEvent(signal))
            return signal
