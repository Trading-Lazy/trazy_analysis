from decimal import Decimal

import pandas as pd

from common.clock import Clock
from db_storage.db_storage import DbStorage
from indicators.crossover import Crossover
from indicators.indicators import IndicatorsManager
from models.candle import Candle
from models.enums import Action, Direction
from models.signal import Signal
from order_manager.order_manager import OrderManager
from strategy.strategy import LOG, Strategy


class ReactiveSmaCrossoverStrategy(Strategy):
    def __init__(
        self,
        symbol: str,
        db_storage: DbStorage,
        order_manager: OrderManager,
        indicators_manager: IndicatorsManager = IndicatorsManager(),
    ):
        super().__init__(symbol, db_storage, order_manager, indicators_manager)
        self.short_sma = self.indicators_manager.Sma(
            symbol,
            period=9,
            time_unit=pd.offsets.Minute(1),
        )
        self.long_sma = self.indicators_manager.Sma(
            symbol,
            period=65,
            time_unit=pd.offsets.Minute(1),
        )
        self.crossover = Crossover(self.short_sma, self.long_sma)

    def generate_signal(self, candle: Candle, clock: Clock) -> Signal:
        LOG.info("short_sma = {}".format(self.short_sma.data))
        LOG.info("long_sma = {}".format(self.long_sma.data))
        LOG.info("crossover = {}".format(self.crossover.data))
        LOG.info("crossover state = {}".format(self.crossover.state.name))
        if self.crossover > 0:
            signal = Signal(
                action=Action.BUY,
                direction=Direction.LONG,
                confidence_level=Decimal("1.0"),
                strategy=self.name,
                symbol=candle.symbol,
                root_candle_timestamp=candle.timestamp,
                parameters={},
                clock=clock,
            )
            return signal
        elif self.crossover < 0:
            signal = Signal(
                action=Action.SELL,
                direction=Direction.LONG,
                confidence_level=Decimal("1.0"),
                strategy=self.name,
                symbol=candle.symbol,
                root_candle_timestamp=candle.timestamp,
                parameters={},
                clock=clock,
            )
            return signal
