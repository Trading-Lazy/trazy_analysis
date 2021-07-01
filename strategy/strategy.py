import abc
import os
from collections import deque
from typing import Dict, List, Union

import logger
import settings
from common.clock import Clock
from indicators.indicator import Indicator
from indicators.indicators_manager import IndicatorsManager
from models.event import SignalEvent
from models.signal import Signal
from order_manager.order_manager import OrderManager
from strategy.context import Context

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)


class Strategy(Indicator):
    __metaclass__ = abc.ABCMeta

    @classmethod
    @abc.abstractmethod
    def DEFAULT_PARAMETERS(cls) -> Dict[str, float]:  # pragma: no cover
        pass

    def __init__(
        self,
        context: Context,
        order_manager: OrderManager,
        events: deque,
        indicators_manager: IndicatorsManager = IndicatorsManager(),
        parameters: Union[Dict[str, float], None] = None
    ):
        super().__init__()
        self.context = context
        self.order_manager = order_manager
        self.events = events
        self.indicators_manager = indicators_manager
        self.parameters = parameters if parameters is not None else self.DEFAULT_PARAMETERS
        self.name = self.__class__.__name__

    @abc.abstractmethod
    def generate_signals(
        self, context: Context, clock: Clock
    ) -> List[Signal]:  # pragma: no cover
        raise NotImplementedError

    def process_context(self, context: Context, clock: Clock) -> List[Signal]:
        signals = self.generate_signals(context, clock)
        self.events.append(SignalEvent(signals))
        return signals
