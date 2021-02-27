from collections import deque

from common.clock import Clock
from indicators.indicators_manager import IndicatorsManager
from models.candle import Candle
from models.signal import Signal
from order_manager.order_manager import OrderManager
from strategy.strategy import Strategy


class IdleStrategy(Strategy):
    def __init__(
        self,
        symbol: str,
        order_manager: OrderManager,
        events: deque,
        indicators_manager: IndicatorsManager = IndicatorsManager(),
    ):
        super().__init__(symbol, order_manager, events, indicators_manager)

    def generate_signal(
        self, candle: Candle, clock: Clock
    ) -> Signal:  # pragma: no cover
        return None
