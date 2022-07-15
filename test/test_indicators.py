from collections import deque
from datetime import timedelta

from trazy_analysis.bot.event_loop import EventLoop
from trazy_analysis.broker.broker_manager import BrokerManager
from trazy_analysis.broker.simulated_broker import SimulatedBroker
from trazy_analysis.common.clock import SimulatedClock
from trazy_analysis.feed.feed import CsvFeed, Feed
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.enums import ExecutionMode
from trazy_analysis.order_manager.order_creator import OrderCreator
from trazy_analysis.order_manager.order_manager import OrderManager
from trazy_analysis.order_manager.position_sizer import PositionSizer
from trazy_analysis.strategy.strategies.sma_crossover_strategy import (
    SmaCrossoverStrategy,
)

EXCHANGE = "IEX"


def test_sma_crossover_strategy_preload_data():
    aapl_asset = Asset(symbol="AAPL", exchange="IEX")
    assets = {aapl_asset: timedelta(minutes=1)}
    events = deque()

    feed: Feed = CsvFeed(
        csv_filenames={
            aapl_asset: {timedelta(minutes=1): "test/data/aapl_candles_one_day.csv"}
        },
        events=events,
    )

    strategies = {SmaCrossoverStrategy: SmaCrossoverStrategy.DEFAULT_PARAMETERS}
    clock = SimulatedClock()
    broker = SimulatedBroker(clock, events, initial_funds=10000.0)
    broker.subscribe_funds_to_portfolio(10000.0)
    broker_manager = BrokerManager(brokers={EXCHANGE: broker})
    position_sizer = PositionSizer(broker_manager=broker_manager)
    order_creator = OrderCreator(broker_manager=broker_manager)
    order_manager = OrderManager(
        events=events,
        broker_manager=broker_manager,
        position_sizer=position_sizer,
        order_creator=order_creator,
        clock=clock,
    )
    event_loop = EventLoop(
        events=events,
        assets=assets,
        feed=feed,
        order_manager=order_manager,
        strategies_parameters=strategies,
        mode=ExecutionMode.LIVE,
    )
    event_loop.loop()

    assert broker.get_portfolio_cash_balance() == 10010.955
