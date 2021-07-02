from collections import deque
from typing import Dict, List, Union

from common.clock import Clock
from indicators.indicators_manager import IndicatorsManager
from models.signal import Signal
from order_manager.order_manager import OrderManager
from strategy.context import Context
from strategy.strategy import Strategy


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
