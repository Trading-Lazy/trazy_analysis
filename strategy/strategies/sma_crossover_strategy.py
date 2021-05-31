from collections import deque
from datetime import timedelta

from common.clock import Clock
from indicators.crossover import Crossover
from indicators.indicators_manager import IndicatorsManager
from models.asset import Asset
from models.candle import Candle
from models.enums import Action, Direction
from models.signal import Signal
from order_manager.order_manager import OrderManager
from strategy.strategy import LOG, Strategy


class SmaCrossoverStrategy(Strategy):
    SHORT_SMA = 9
    LONG_SMA = 65

    def __init__(
        self,
        asset: Asset,
        order_manager: OrderManager,
        events: deque,
        indicators_manager: IndicatorsManager = IndicatorsManager(),
    ):
        super().__init__(asset, order_manager, events, indicators_manager)
        self.short_sma = self.indicators_manager.Sma(
            asset,
            period=SmaCrossoverStrategy.SHORT_SMA,
            time_unit=timedelta(minutes=1),
        )
        self.long_sma = self.indicators_manager.Sma(
            asset,
            period=SmaCrossoverStrategy.LONG_SMA,
            time_unit=timedelta(minutes=1),
        )
        self.crossover = Crossover(self.short_sma, self.long_sma)

    def generate_signal(self, candle: Candle, clock: Clock) -> Signal:
        LOG.info("short_sma = %s", self.short_sma.data)
        LOG.info("long_sma = %s", self.long_sma.data)
        LOG.info("crossover = %s", self.crossover.data)
        LOG.info("crossover state = %s", self.crossover.state.name)
        if self.crossover > 0:
            signal = Signal(
                action=Action.BUY,
                direction=Direction.LONG,
                confidence_level=1.0,
                strategy=self.name,
                asset=self.asset,
                root_candle_timestamp=candle.timestamp,
                parameters={},
                clock=clock,
            )
            return signal
        elif self.crossover < 0:
            signal = Signal(
                action=Action.SELL,
                direction=Direction.LONG,
                confidence_level=1.0,
                strategy=self.name,
                asset=self.asset,
                root_candle_timestamp=candle.timestamp,
                parameters={},
                clock=clock,
            )
            return signal
