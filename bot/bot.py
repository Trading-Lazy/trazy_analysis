import glob
import importlib
import inspect
import os
from typing import List, Set

from pathlib import Path

from bot.data_consumer import DataConsumer
from bot.data_flow import DataFlow
from broker.simulated_broker import SimulatedBroker
from candles_queue.candles_queue import CandlesQueue
from candles_queue.fake_queue import FakeQueue
from common.clock import SimulatedClock
from db_storage.mongodb_storage import MongoDbStorage
from feed.feed import CsvFeed, Feed
from order_manager.order_creator import OrderCreator
from order_manager.order_manager import OrderManager
from order_manager.position_sizer import PositionSizer
from settings import DATABASE_NAME, DATABASE_URL
from strategy.strategies.reactive_sma_crossover_strategy import (
    ReactiveSmaCrossoverStrategy,
)
from strategy.strategy import Strategy


def get_strategies_classes(
    strategies_module_path, strategies_module_fullname
) -> List[type]:
    strategies_module_name = str(Path(strategies_module_path).name)
    strategies_dir = strategies_module_path
    strategies_classes: Set[type] = set()
    for file in glob.glob(strategies_dir + "/*.py"):
        name = os.path.splitext(os.path.basename(file))[0]

        # Ignore __ files
        if name.startswith("__"):
            continue

        module = importlib.import_module(
            strategies_module_fullname + "." + name, package=strategies_module_name
        )

        for member in dir(module):
            strategy_class = getattr(module, member)
            if (
                strategy_class
                and inspect.isclass(strategy_class)
                and issubclass(strategy_class, Strategy)
            ):
                strategies_classes.add(strategy_class)
    return list(strategies_classes)


if __name__ == "__main__":
    symbols = ["AAPL"]
    candles_queue: CandlesQueue = FakeQueue("candles")

    feed: Feed = CsvFeed(
        {"AAPL": "test/data/aapl_candles_three_years.csv"}, candles_queue
    )

    db_storage = MongoDbStorage(DATABASE_NAME, DATABASE_URL)
    db_storage.clean_all_signals()
    db_storage.clean_all_orders()
    db_storage.clean_all_candles()

    strategies = [ReactiveSmaCrossoverStrategy]
    clock = SimulatedClock()
    broker = SimulatedBroker(clock, initial_funds=10000.0)
    broker.subscribe_funds_to_portfolio(10000.0)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(broker=broker)
    order_manager = OrderManager(
        broker=broker, position_sizer=position_sizer, order_creator=order_creator
    )
    data_consumer = DataConsumer(
        symbols, candles_queue, db_storage, order_manager, strategies
    )

    data_flow = DataFlow(feed, data_consumer)
    data_flow.start()
