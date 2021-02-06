import glob
import importlib
import inspect
import os
from decimal import Decimal
from pathlib import Path
from typing import List, Set

from bot.data_consumer import DataConsumer
from bot.data_flow import DataFlow
from broker.degiro_broker import DegiroBroker
from candles_queue.candles_queue import CandlesQueue
from candles_queue.rabbit_mq import RabbitMq
from common.clock import LiveClock
from db_storage.mongodb_storage import MongoDbStorage
from feed.feed import Feed, LiveFeed
from market_data.live.live_data_handler import LiveDataHandler
from market_data.live.tiingo_live_data_handler import TiingoLiveDataHandler
from order_manager.order_creator import OrderCreator
from order_manager.order_manager import OrderManager
from order_manager.position_sizer import PositionSizer
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
    symbols = ["SHIP"]
    candles_queue: CandlesQueue = RabbitMq(RABBITMQ_QUEUE_NAME, CLOUDAMQP_URL)

    live_data_handler: LiveDataHandler = TiingoLiveDataHandler()
    feed: Feed = LiveFeed(symbols, candles_queue, live_data_handler, minutes=1)

    db_storage = MongoDbStorage(DATABASE_NAME, DATABASE_URL)
    strategies = [ReactiveSmaCrossoverStrategy]
    clock = LiveClock()
    broker = DegiroBroker(clock)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(
        broker=broker, trailing_stop_order_pct=Decimal("0.15"), with_cover=True
    )
    order_manager = OrderManager(
        broker=broker,
        position_sizer=position_sizer,
        order_creator=order_creator,
        for_simulation=False,
    )
    data_consumer = DataConsumer(
        symbols, candles_queue, db_storage, order_manager, strategies, save_candles=True
    )

    data_flow = DataFlow(feed, data_consumer)
    data_flow.start()
