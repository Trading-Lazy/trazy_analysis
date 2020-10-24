import os
from typing import List

import logger
import settings
from candles_queue.candles_queue import CandlesQueue
from db_storage.db_storage import DbStorage
from indicators.indicators import RollingWindow
from models.candle import Candle
from order_manager.order_management import OrderManager
from strategy.strategy import Strategy

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)


class DataConsumer:
    def _init_strategy_instance(self, strategy_class: type):
        for symbol in self.symbols:
            if issubclass(strategy_class, Strategy):
                self.strategies_instances.append(
                    strategy_class(symbol, self.db_storage, self.order_manager)
                )

    def _init_strategy_instances(self):
        for strategy_class in self.strategies_classes:
            self._init_strategy_instance(strategy_class)

    def _subscribe_broker_to_data_stream(self):
        for symbol in self.symbols:
            RollingWindow(symbol).subscribe(lambda candle: self.order_manager.update_broker(candle.timestamp))

    def __init__(
        self,
        symbols: List[str],
        candles_queue: CandlesQueue,
        db_storage: DbStorage,
        order_manager: OrderManager,
        strategies_classes: List[type] = [],
        save_candles=False,
    ):
        self.symbols = sorted(set(symbols))
        self.candles_queue = candles_queue
        self.db_storage = db_storage
        self.order_manager = order_manager
        self.strategies_classes = strategies_classes
        self.strategies_instances: List[Strategy] = []
        #self._subscribe_broker_to_data_stream()
        self._init_strategy_instances()
        self.save_candles = save_candles

    def add_strategy(self, strategy_class: type):
        self._init_strategy_instance(strategy_class)

    def run_strategy(self, strategy: Strategy, candle: Candle):
        strategy.process_candle(candle)

    def run_strategies(self, candle: Candle):
        for strategy in self.strategies_instances:
            self.run_strategy(strategy, candle)
            LOG.info("Strategy results: {}".format(strategy.order_manager.broker.get_portfolio_cash_balance()))

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
                RollingWindow(candle.symbol).push(candle)
                self.order_manager.update_broker(candle.timestamp)
            self.run_strategies(candle)
            if add_to_db:
                self.db_storage.add_candle(candle)

    def start(self):
        self.candles_queue.add_consumer(self.handle_new_candle_callback)
