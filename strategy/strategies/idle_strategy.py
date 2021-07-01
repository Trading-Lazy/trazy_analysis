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
        indicators_manager: IndicatorsManager = IndicatorsManager(),
        parameters: Union[Dict[str, float], None] = None
    ):
        super().__init__(context, order_manager, events, indicators_manager, parameters)

    def generate_signals(
        self, context: Context, clock: Clock
    ) -> List[Signal]:  # pragma: no cover
        return []
