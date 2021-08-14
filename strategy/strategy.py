import abc
import os
from collections import deque
from typing import Dict, List

import trazy_analysis.logger
import trazy_analysis.settings
from trazy_analysis.common.clock import Clock
from trazy_analysis.indicators.indicator import Indicator
from trazy_analysis.indicators.indicators_manager import IndicatorsManager
from trazy_analysis.models.event import SignalEvent
from trazy_analysis.models.signal import Signal
from trazy_analysis.order_manager.order_manager import OrderManager
from trazy_analysis.strategy.context import Context

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
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
        parameters: Dict[str, float],
        indicators_manager: IndicatorsManager = IndicatorsManager(),
    ):
        super().__init__()
        self.context = context
        self.order_manager = order_manager
        self.events = events
        self.parameters = parameters
        self.indicators_manager = indicators_manager
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
