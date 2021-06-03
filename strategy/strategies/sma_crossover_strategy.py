from collections import deque
from datetime import timedelta
from typing import List

from common.clock import Clock
from indicators.crossover import Crossover
from indicators.indicators_manager import IndicatorsManager
from models.asset import Asset
from models.enums import Action, Direction
from models.signal import Signal
from order_manager.order_manager import OrderManager
from strategy.context import Context
from strategy.strategy import LOG, Strategy


class SmaCrossoverStrategy(Strategy):
    SHORT_SMA = 9
    LONG_SMA = 65

    def __init__(
        self,
        context: Context,
        order_manager: OrderManager,
        events: deque,
        indicators_manager: IndicatorsManager = IndicatorsManager(),
    ):
        super().__init__(context, order_manager, events, indicators_manager)
        self.short_sma = {
            asset: self.indicators_manager.Sma(
                asset,
                period=SmaCrossoverStrategy.SHORT_SMA,
                time_unit=timedelta(minutes=1),
            )
            for asset in context.candles
        }
        self.long_sma = {
            asset: self.indicators_manager.Sma(
                asset,
                period=SmaCrossoverStrategy.LONG_SMA,
                time_unit=timedelta(minutes=1),
            )
            for asset in context.candles
        }
        self.crossover = {
            asset: Crossover(self.short_sma[asset], self.long_sma[asset]) for asset in context.candles
        }

    def generate_signals(self, context: Context, clock: Clock) -> List[Signal]:
        signals = []
        for candle in context.get_last_candles():
            LOG.info("short_sma = %s", self.short_sma[candle.asset].data)
            LOG.info("long_sma = %s", self.long_sma[candle.asset].data)
            LOG.info("crossover = %s", self.crossover[candle.asset].data)
            LOG.info("crossover state = %s", self.crossover[candle.asset].state.name)
            signal = None
            if self.crossover[candle.asset] > 0:
                signal = Signal(
                    action=Action.BUY,
                    direction=Direction.LONG,
                    confidence_level=1.0,
                    strategy=self.name,
                    asset=candle.asset,
                    root_candle_timestamp=context.current_timestamp,
                    parameters={},
                    clock=clock,
                )
            elif self.crossover[candle.asset] < 0:
                signal = Signal(
                    action=Action.SELL,
                    direction=Direction.LONG,
                    confidence_level=1.0,
                    strategy=self.name,
                    asset=candle.asset,
                    root_candle_timestamp=context.current_timestamp,
                    parameters={},
                    clock=clock,
                )
            if signal is not None:
                signals.append(signal)
            return signals
