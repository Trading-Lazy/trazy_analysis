import glob
import importlib
import inspect
import os
from collections import deque
from pathlib import Path
from typing import List, Set

from bot.event_loop import EventLoop
from broker.binance_broker import BinanceBroker

# from broker.degiro_broker import DegiroBroker
from broker.degiro_broker import DegiroBroker
from common.clock import LiveClock
from feed.feed import Feed, LiveFeed
from indicators.indicators_manager import IndicatorsManager
from market_data.live.binance_live_data_handler import BinanceLiveDataHandler
from market_data.live.tiingo_live_data_handler import TiingoLiveDataHandler
from order_manager.order_creator import OrderCreator
from order_manager.order_manager import OrderManager
from order_manager.position_sizer import PositionSizer
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


# if __name__ == "__main__":
#     symbols = ["XRPUSDT"]
#     events = deque()
#     live_data_handler = BinanceLiveDataHandler()
#
#     feed: Feed = LiveFeed(symbols, events, live_data_handler)
#
#     strategies = [SmaCrossoverStrategy]
#     clock = LiveClock()
#     broker = BinanceBroker(clock, events)
#     position_sizer = PositionSizer(broker)
#     order_creator = OrderCreator(
#         broker=broker, with_cover=True, trailing_stop_order_pct=0.002
#     )
#     order_manager = OrderManager(
#         events=events,
#         broker=broker,
#         position_sizer=position_sizer,
#         order_creator=order_creator,
#     )
#     indicators_manager = IndicatorsManager(preload=False)
#     event_loop = EventLoop(
#         events=events,
#         symbols=symbols,
#         feed=feed,
#         order_manager=order_manager,
#         strategies_classes=strategies,
#         indicators_manager=indicators_manager,
#         live=True,
#         close_at_end_of_day=False
#     )
#     event_loop.loop()

if __name__ == "__main__":
    symbols = ["XRPUSDT"]
    events = deque()
    live_data_handler = BinanceLiveDataHandler()

    feed: Feed = LiveFeed(symbols, events, live_data_handler)

    strategies = [SmaCrossoverStrategy]
    clock = LiveClock()
    broker = BinanceBroker(clock, events)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(
        broker=broker, with_cover=True, trailing_stop_order_pct=0.002
    )
    order_manager = OrderManager(
        events=events,
        broker=broker,
        position_sizer=position_sizer,
        order_creator=order_creator,
        filter_at_end_of_day=False,
    )
    indicators_manager = IndicatorsManager(preload=False)
    event_loop = EventLoop(
        events=events,
        symbols=symbols,
        feed=feed,
        order_manager=order_manager,
        strategies_classes=strategies,
        indicators_manager=indicators_manager,
        live=True,
        close_at_end_of_day=False,
    )
    event_loop.loop()
