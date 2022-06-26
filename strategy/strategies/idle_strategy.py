from collections import deque
from datetime import timedelta
from typing import Dict, List

from trazy_analysis.common.clock import Clock
from trazy_analysis.indicators.indicators_manager import IndicatorsManager
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.signal import Signal
from trazy_analysis.order_manager.order_manager import OrderManager
from trazy_analysis.strategy.context import Context
from trazy_analysis.strategy.strategy import Strategy


class IdleStrategy(Strategy):
    def __init__(
        self,
        asset: Asset,
        time_unit: timedelta,
        order_manager: OrderManager,
        events: deque,
        parameters: Dict[str, float],
        indicators_manager: IndicatorsManager = IndicatorsManager(),
    ):
        super().__init__(
            asset, time_unit, order_manager, events, parameters, indicators_manager
        )

    def current(self, candle: Candle, clock: Clock) -> None:  # pragma: no cover
        pass
