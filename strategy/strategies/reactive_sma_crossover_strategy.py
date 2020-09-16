import pandas as pd

from broker.broker import Broker
from db_storage.db_storage import DbStorage
from indicators.crossover import Crossover
from indicators.indicators import Sma
from models.action import Action
from models.candle import Candle
from models.enums import ActionType, PositionType
from strategy.strategy import LOG, Strategy


class ReactiveSmaCrossoverStrategy(Strategy):
    def __init__(self, symbol: str, db_storage: DbStorage, broker: Broker):
        super().__init__(symbol, db_storage, broker)
        self.short_sma = Sma(symbol, period=9, time_unit=pd.offsets.Minute(1),)
        self.long_sma = Sma(symbol, period=65, time_unit=pd.offsets.Minute(1),)
        self.crossover = Crossover(self.short_sma, self.long_sma)
        self.position = False

    def compute_action(self, candle: Candle) -> Action:
        LOG.info("short_sma = {}".format(self.short_sma.data))
        LOG.info("long_sma = {}".format(self.long_sma.data))
        LOG.info("crossover = {}".format(self.crossover.data))
        LOG.info("crossover state = {}".format(self.crossover.state.name))
        if not self.position:
            if self.crossover > 0:
                self.position = True
                self.broker.submit_order(
                    candle.symbol, PositionType.LONG, ActionType.BUY, candle.close, 1
                )
                # create buy action
                return Action(
                    strategy=self.name,
                    symbol=candle.symbol,
                    candle_timestamp=candle.timestamp,
                    confidence_level=1,
                    action_type=ActionType.BUY,
                    position_type=PositionType.LONG,
                    size=1,
                    parameters={},
                )
        elif self.crossover < 0:
            self.position = False
            self.broker.submit_order(
                candle.symbol, PositionType.LONG, ActionType.SELL, candle.close, 1
            )
            # create sell action
            return Action(
                strategy=self.name,
                symbol=candle.symbol,
                candle_timestamp=candle.timestamp,
                confidence_level=1,
                action_type=ActionType.SELL,
                position_type=PositionType.LONG,
                size=1,
                parameters={},
            )
