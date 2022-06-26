from collections import deque
from datetime import timedelta
from typing import Dict

from trazy_analysis.common.clock import Clock
from trazy_analysis.indicators.crossover import Crossover
from trazy_analysis.indicators.indicators_manager import IndicatorsManager
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.enums import Action, Direction
from trazy_analysis.models.parameter import Discrete, Parameter
from trazy_analysis.models.signal import Signal
from trazy_analysis.order_manager.order_manager import OrderManager
from trazy_analysis.strategy.strategy import LOG, Strategy


class SmaCrossoverStrategy(Strategy):
    DEFAULT_PARAMETERS = {
        "short_sma": 9,
        "long_sma": 65,
    }

    DEFAULT_PARAMETERS_SPACE = {
        "short_sma": Discrete([5, 75]),
        "long_sma": Discrete([100, 200]),
    }

    def __init__(
        self,
        asset: Asset,
        time_unit: timedelta,
        order_manager: OrderManager,
        events: deque,
        parameters: Dict[str, Parameter],
        indicators_manager: IndicatorsManager = IndicatorsManager(),
    ):
        super().__init__(
            asset, time_unit, order_manager, events, parameters, indicators_manager
        )
        self.short_sma = self.indicators_manager.Sma(
            asset,
            time_unit=time_unit,
            period=self.parameters["short_sma"],
        )
        self.long_sma = self.indicators_manager.Sma(
            asset,
            time_unit=time_unit,
            period=self.parameters["long_sma"],
        )
        self.crossover = Crossover(self.short_sma, self.long_sma)

    def current(self, candle: Candle, clock: Clock) -> None:
        LOG.info("short_sma = %s", self.short_sma.data)
        LOG.info("long_sma = %s", self.long_sma.data)
        LOG.info("crossover = %s", self.crossover.data)
        LOG.info("crossover state = %s", self.crossover.state.name)
        if self.crossover > 0:
            self.add_signal(
                Signal(
                    action=Action.BUY,
                    direction=Direction.LONG,
                    confidence_level=1.0,
                    strategy=self.name,
                    asset=candle.asset,
                    root_candle_timestamp=candle.timestamp,
                    parameters={},
                    clock=clock,
                )
            )
        elif self.crossover < 0:
            self.add_signal(
                Signal(
                    action=Action.SELL,
                    direction=Direction.LONG,
                    confidence_level=1.0,
                    strategy=self.name,
                    asset=candle.asset,
                    root_candle_timestamp=candle.timestamp,
                    parameters={},
                    clock=clock,
                )
            )
