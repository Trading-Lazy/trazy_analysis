import glob
import importlib
import inspect
import os
from decimal import Decimal
from typing import List, Set

from pathlib import Path

from bot.data_consumer import DataConsumer
from bot.data_flow import DataFlow
from broker.simulatedbroker import SimulatedBroker
from candles_queue.candles_queue import CandlesQueue
from candles_queue.rabbit_mq import RabbitMq
from db_storage.mongodb_storage import MongoDbStorage
from feed.feed import Feed, LiveFeed
from market_data.live.live_data_handler import LiveDataHandler
from market_data.live.tiingo_live_data_handler import TiingoLiveDataHandler
from settings import CLOUDAMQP_URL, DATABASE_NAME, DATABASE_URL, RABBITMQ_QUEUE_NAME
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
    candles_queue: CandlesQueue = RabbitMq(RABBITMQ_QUEUE_NAME, CLOUDAMQP_URL)

    live_data_handler: LiveDataHandler = TiingoLiveDataHandler()
    feed: Feed = LiveFeed(symbols, candles_queue, live_data_handler, minutes=1)

    db_storage = MongoDbStorage(DATABASE_NAME, DATABASE_URL)
    strategies = [ReactiveSmaCrossoverStrategy]
    broker = SimulatedBroker(cash=Decimal("10000"))
    data_consumer = DataConsumer(symbols, candles_queue, db_storage, broker, strategies, save_candles=True)

    data_flow = DataFlow(feed, data_consumer)
    data_flow.start()
