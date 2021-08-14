from collections import deque
from typing import Dict, List

from trazy_analysis.common.clock import Clock
from trazy_analysis.indicators.indicators_manager import IndicatorsManager
from trazy_analysis.models.signal import Signal
from trazy_analysis.order_manager.order_manager import OrderManager
from trazy_analysis.strategy.context import Context
from trazy_analysis.strategy.strategy import Strategy


class IdleStrategy(Strategy):
    def __init__(
        self,
        context: Context,
        order_manager: OrderManager,
        events: deque,
        parameters: Dict[str, float],
        indicators_manager: IndicatorsManager = IndicatorsManager(),
    ):
        super().__init__(context, order_manager, events, parameters, indicators_manager)

    def generate_signals(
        self, context: Context, clock: Clock
    ) -> List[Signal]:  # pragma: no cover
        return []
