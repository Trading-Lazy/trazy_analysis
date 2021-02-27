import glob
import importlib
import inspect
import os
from collections import deque
from pathlib import Path
from typing import List, Set

from bot.data_consumer import DataConsumer
from bot.data_flow import DataFlow
from broker.binance_broker import BinanceBroker
from candles_queue.candles_queue import CandlesQueue
from candles_queue.fake_queue import FakeQueue
from common.clock import LiveClock
from db_storage.mongodb_storage import MongoDbStorage
from feed.feed import Feed, LiveFeed
from indicators.indicators_manager import IndicatorsManager
from market_data.live.tiingo_live_crypto_data_handler import TiingoLiveCryptoDataHandler
from order_manager.order_creator import OrderCreator
from order_manager.order_manager import OrderManager
from order_manager.position_sizer import PositionSizer
from settings import DATABASE_NAME, DATABASE_URL
from strategy.strategies.sma_crossover_strategy import (
    SmaCrossoverStrategy,
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
    symbols = ["XRPUSD"]
    candles_queue: CandlesQueue = FakeQueue("candles")
    live_data_handler = TiingoLiveCryptoDataHandler()

    feed: Feed = LiveFeed(symbols, candles_queue, live_data_handler)

    db_storage = MongoDbStorage(DATABASE_NAME, DATABASE_URL)
    db_storage.clean_all_signals()
    db_storage.clean_all_orders()
    db_storage.clean_all_candles()

    strategies = [SmaCrossoverStrategy]
    clock = LiveClock()
    events = deque
    broker = BinanceBroker(clock, events)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(
        broker=broker, with_cover=True, trailing_stop_order_pct=0.05
    )
    order_manager = OrderManager(
        broker=broker, position_sizer=position_sizer, order_creator=order_creator
    )
    indicators_manager = IndicatorsManager(preload=False)
    data_consumer = DataConsumer(
        symbols=symbols,
        candles_queue=candles_queue,
        db_storage=db_storage,
        order_manager=order_manager,
        strategies_classes=strategies,
        save_candles=True,
        indicators_manager=indicators_manager,
        live=True,
    )

    data_flow = DataFlow(feed, data_consumer)
    data_flow.start()
