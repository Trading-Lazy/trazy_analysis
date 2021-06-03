import abc
import os
from collections import deque
from typing import List

import logger
import settings
from common.clock import Clock
from indicators.indicator import Indicator
from indicators.indicators_manager import IndicatorsManager
from models.candle import Candle
from models.event import SignalEvent
from models.signal import Signal
from order_manager.order_manager import OrderManager
from strategy.context import Context

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)


class Strategy(Indicator):
    def __init__(
        self,
        context: Context,
        order_manager: OrderManager,
        events: deque,
        indicators_manager: IndicatorsManager = IndicatorsManager()
    ):
        super().__init__()
        self.context = context
        self.order_manager = order_manager
        self.events = events
        self.indicators_manager = indicators_manager
        self.name = self.__class__.__name__

    @abc.abstractmethod
    def generate_signals(self, context: Context, clock: Clock) -> List[Signal]:  # pragma: no cover
        raise NotImplementedError

    def process_context(self, context: Context, clock: Clock) -> List[Signal]:
        signals = self.generate_signals(context, clock)
        for signal in signals:
            if signal is not None:
                self.events.append(SignalEvent(signal))
        return signals
