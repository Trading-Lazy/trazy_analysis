from collections import deque
from time import time

from bot.event_loop import EventLoop
from broker.simulated_broker import SimulatedBroker
from common.clock import SimulatedClock
from feed.feed import CsvFeed, Feed
from indicators.indicators_manager import IndicatorsManager
from order_manager.order_creator import OrderCreator
from order_manager.order_manager import OrderManager
from order_manager.position_sizer import PositionSizer
from strategy.strategies.sma_crossover_strategy import (
    SmaCrossoverStrategy,
)


def test_reactive_sma_crossover_strategy():
    symbols = ["AAPL"]
    events = deque()

    feed: Feed = CsvFeed({"AAPL": "test/data/aapl_candles_one_day.csv"}, events)

    strategies = [SmaCrossoverStrategy]
    clock = SimulatedClock()
    broker = SimulatedBroker(clock, events, initial_funds=10000.0)
    broker.subscribe_funds_to_portfolio(10000.0)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(broker=broker)
    order_manager = OrderManager(
        events=events,
        broker=broker,
        position_sizer=position_sizer,
        order_creator=order_creator,
    )
    indicators_manager = IndicatorsManager(preload=True, initial_data=feed.candles)
    event_loop = EventLoop(
        events=events,
        symbols=symbols,
        feed=feed,
        order_manager=order_manager,
        strategies_classes=strategies,
        indicators_manager=indicators_manager,
    )
    start = time()
    event_loop.loop()
    print(time() - start)

    assert broker.get_portfolio_cash_balance() == 10010.955
