import os
from datetime import timedelta
from typing import List

from rx import interval

import logger
import settings
from candles_queue.candles_queue import CandlesQueue
from db_storage.db_storage import DbStorage
from indicators.indicators_manager import IndicatorsManager
from models.candle import Candle
from order_manager.order_manager import OrderManager
from strategy.strategy import Strategy

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)


class DataConsumer:
    def _init_strategy_instance(self, strategy_class: type):
        for symbol in self.symbols:
            if issubclass(strategy_class, Strategy):
                self.strategy_instances.append(
                    strategy_class(
                        symbol,
                        self.db_storage,
                        self.order_manager,
                        self.indicators_manager,
                    )
                )

    def _init_strategy_instances(self):
        for strategy_class in self.strategies_classes:
            self._init_strategy_instance(strategy_class)

    def _subscribe_broker_to_data_stream(self):
        for symbol in self.symbols:
            self.indicators_manager.RollingWindow(symbol).subscribe(
                lambda candle: self.broker.update_price(candle)
            )
            if not self.live:
                self.indicators_manager.RollingWindow(symbol).subscribe(
                    lambda candle: self.broker.execute_open_orders()
                )
            self.indicators_manager.RollingWindow(symbol).subscribe(
                lambda candle: self.broker.close_all_open_positions(candle.symbol)
            )

    def _subscribe_order_manager_to_data_stream(self):
        for symbol in self.symbols:
            self.indicators_manager.RollingWindow(symbol).subscribe(
                lambda candle: self.clock.update(symbol, candle.timestamp)
            )
            self.indicators_manager.RollingWindow(symbol).subscribe(
                lambda candle: self.order_manager.process_pending_signals()
            )

    def _subscribe_order_manager_to_strategy_instances(self):
        for strategy_instance in self.strategy_instances:
            strategy_instance.subscribe(
                lambda signal: self.order_manager.check_signal(signal)
            )

    def __init__(
        self,
        symbols: List[str],
        candles_queue: CandlesQueue,
        db_storage: DbStorage,
        order_manager: OrderManager,
        indicators_manager: IndicatorsManager,
        strategies_classes: List[type] = [],
        save_candles=False,
        live=False,
        frequency=timedelta(seconds=10),
    ):
        self.symbols = sorted(set(symbols))
        self.candles_queue = candles_queue
        self.db_storage = db_storage
        self.order_manager = order_manager
        self.broker = self.order_manager.broker
        self.clock = self.order_manager.clock
        self.indicators_manager = indicators_manager
        self.strategies_classes = strategies_classes
        self.strategy_instances: List[Strategy] = []
        self._init_strategy_instances()
        self.indicators_manager.warmup()
        self.live = live
        self._subscribe_broker_to_data_stream()
        if not self.live:
            self._subscribe_order_manager_to_data_stream()
        else:
            self.process_signals_interval = interval(frequency)
            self.process_signals_interval.subscribe(lambda _: self.broker.synchronize())
            self.process_signals_interval.subscribe(
                lambda _: self.order_manager.process_pending_signals()
            )
            self.process_signals_interval.subscribe(
                lambda _: self.broker.execute_open_orders()
            )
        self._subscribe_order_manager_to_strategy_instances()
        self.save_candles = save_candles

    def add_strategy(self, strategy_class: type):
        self._init_strategy_instance(strategy_class)

    def run_strategy(self, strategy: Strategy, candle: Candle):
        strategy.process_candle(candle, self.clock)

    def run_strategies(self, candle: Candle):
        for strategy in self.strategy_instances:
            self.run_strategy(strategy, candle)
            LOG.info(
                "Strategy results: %s",
                strategy.order_manager.broker.get_portfolio_cash_balance(),
            )

    def handle_new_candle_callback(self, candle: Candle):
        LOG.info("Dequeue new candle: %s", candle.to_json())
        if candle.asset in self.symbols:
            add_to_db = (
                self.save_candles
                and not self.db_storage.candle_with_identifier_exists(
                    candle.asset, candle.timestamp
                )
            )
            LOG.info("Add to db: %s", add_to_db)
            if not self.save_candles or add_to_db:
                self.indicators_manager.RollingWindow(candle.asset).push(candle)
                self.run_strategies(candle)
            if add_to_db:
                self.db_storage.add_candle(candle)

    def start(self):
        self.candles_queue.add_consumer(self.handle_new_candle_callback)
        for symbol in self.symbols:
            self.candles_queue.add_on_complete_callback(
                symbol,
                lambda symbol: self.broker.close_all_open_positions(
                    symbol=symbol, end_of_day=False
                ),
            )
