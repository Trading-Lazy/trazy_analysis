import os
from typing import List

from pymongo.errors import DuplicateKeyError

import logger
import settings
from broker.broker import Broker
from candles_queue.candles_queue import CandlesQueue
from db_storage.db_storage import DbStorage
from indicators.indicators import RollingWindow
from models.candle import Candle
from strategy.strategy import Strategy

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)


class DataConsumer:
    def _init_strategy_instance(self, strategy_class: type):
        for symbol in self.symbols:
            if issubclass(strategy_class, Strategy):
                self.strategies_instances.append(
                    strategy_class(symbol, self.db_storage, self.broker)
                )

    def _init_strategy_instances(self):
        for strategy_class in self.strategies_classes:
            self._init_strategy_instance(strategy_class)

    def __init__(
        self,
        symbols: List[str],
        candles_queue: CandlesQueue,
        db_storage: DbStorage,
        broker: Broker,
        strategies_classes: List[type] = [],
        save_candles=False,
    ):
        self.symbols = sorted(set(symbols))
        self.candles_queue = candles_queue
        self.db_storage = db_storage
        self.broker = broker
        self.strategies_classes = strategies_classes
        self.strategies_instances: List[Strategy] = []
        self._init_strategy_instances()
        self.save_candles = save_candles

    def add_strategy(self, strategy_class: type):
        self._init_strategy_instance(strategy_class)

    def run_strategy(self, strategy: Strategy, candle: Candle):
        strategy.process_candle(candle)

    def run_strategies(self, candle: Candle):
        for strategy in self.strategies_instances:
            self.run_strategy(strategy, candle)
            LOG.info("Strategy results: {}".format(strategy.broker.cash))

    def handle_new_candle_callback(self, candle_json: str):
        LOG.info("Dequeue new candle: {}".format(candle_json))
        candle = Candle.from_json(candle_json)
        if candle.symbol in self.symbols:
            add_to_db = (
                self.save_candles
                and not self.db_storage.candle_with_identifier_exists(
                    candle.symbol, candle.timestamp
                )
            )
            LOG.info("add_to_db: {}".format(add_to_db))
            if not self.save_candles or add_to_db:
                RollingWindow(candle.symbol).on_next(candle)
            self.run_strategies(candle)
            if add_to_db:
                self.db_storage.add_candle(candle)

    def start(self):
        self.candles_queue.add_consumer(self.handle_new_candle_callback)
