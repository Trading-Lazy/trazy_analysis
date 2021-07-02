from collections import deque
from time import time

from bot.event_loop import EventLoop
from broker.broker_manager import BrokerManager
from broker.simulated_broker import SimulatedBroker
from common.clock import SimulatedClock
from feed.feed import CsvFeed, Feed
from indicators.indicators_manager import IndicatorsManager
from models.asset import Asset
from order_manager.order_creator import OrderCreator
from order_manager.order_manager import OrderManager
from order_manager.position_sizer import PositionSizer
from strategy.strategies.sma_crossover_strategy import (
    SmaCrossoverStrategy,
)

AAPL_SYMBOL = "AAPL"
EXCHANGE = "IEX"
AAPL_ASSET = Asset(symbol=AAPL_SYMBOL, exchange=EXCHANGE)


def test_sma_crossover_strategy():
    assets = [AAPL_ASSET]
    events = deque()

    feed: Feed = CsvFeed({AAPL_ASSET: "test/data/aapl_candles_one_day.csv"}, events)

    strategies = {SmaCrossoverStrategy: [SmaCrossoverStrategy.DEFAULT_PARAMETERS]}
    clock = SimulatedClock()
    broker = SimulatedBroker(clock, events, initial_funds=10000.0)
    broker.subscribe_funds_to_portfolio(10000.0)
    broker_manager = BrokerManager(brokers={EXCHANGE: broker}, clock=clock)
    position_sizer = PositionSizer(broker_manager=broker_manager)
    order_creator = OrderCreator(broker_manager=broker_manager)
    order_manager = OrderManager(
        events=events,
        broker_manager=broker_manager,
        position_sizer=position_sizer,
        order_creator=order_creator,
    )
    indicators_manager = IndicatorsManager(preload=True, initial_data=feed.candles)
    event_loop = EventLoop(
        events=events,
        assets=assets,
        feed=feed,
        order_manager=order_manager,
        strategies_parameters=strategies,
        indicators_manager=indicators_manager,
    )
    event_loop.loop()

    assert broker.get_portfolio_cash_balance() == 10010.955
