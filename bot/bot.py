import glob
import importlib
import inspect
import os
from collections import deque
from pathlib import Path
from typing import List, Set

from trazy_analysis.bot.event_loop import EventLoop
from trazy_analysis.broker.binance_broker import BinanceBroker
from trazy_analysis.broker.broker_manager import BrokerManager
from trazy_analysis.common.clock import LiveClock
from trazy_analysis.feed.feed import Feed, LiveFeed
from trazy_analysis.market_data.live.binance_live_data_handler import (
    BinanceLiveDataHandler,
)
from trazy_analysis.models.enums import ExecutionMode
from trazy_analysis.order_manager.order_creator import OrderCreator
from trazy_analysis.order_manager.order_manager import OrderManager
from trazy_analysis.order_manager.position_sizer import PositionSizer
from trazy_analysis.strategy.strategies.sma_crossover_strategy import (
    SmaCrossoverStrategy,
)
from trazy_analysis.strategy.strategy import StrategyBase

EXCHANGE = "IEX"


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
                and issubclass(strategy_class, StrategyBase)
            ):
                strategies_classes.add(strategy_class)
    return list(strategies_classes)


if __name__ == "__main__":
    symbols = ["XRPUSDT"]
    events = deque()
    live_data_handler = BinanceLiveDataHandler()

    feed: Feed = LiveFeed(symbols, live_data_handler, events)

    strategies = [SmaCrossoverStrategy]
    clock = LiveClock()
    broker = BinanceBroker(clock, events)
    broker_manager = BrokerManager(brokers_per_exchange={EXCHANGE: broker})
    position_sizer = PositionSizer(broker_manager=broker_manager)
    order_creator = OrderCreator(
        broker_manager=broker_manager,
        trailing_stop_order_pct=0.002,
        with_trailing_cover=True,
    )
    order_manager = OrderManager(
        events=events,
        broker_manager=broker_manager,
        position_sizer=position_sizer,
        order_creator=order_creator,
        clock=clock,
        filter_at_end_of_day=False,
    )
    event_loop = EventLoop(
        events=events,
        assets=[],
        feed=feed,
        order_manager=order_manager,
        strategies_parameters=strategies,
        mode=ExecutionMode.LIVE,
        close_at_end_of_day=False,
    )
    event_loop.loop()
