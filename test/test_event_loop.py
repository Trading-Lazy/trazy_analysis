from collections import deque
from datetime import datetime
from unittest.mock import call, patch

from bot.event_loop import EventLoop
from broker.broker_manager import BrokerManager
from broker.simulated_broker import SimulatedBroker
from common.clock import SimulatedClock
from common.exchange_calendar_euronext import EuronextExchangeCalendar
from feed.feed import CsvFeed
from indicators.indicators_manager import IndicatorsManager
from models.asset import Asset
from models.candle import Candle
from order_manager.order_creator import OrderCreator
from order_manager.order_manager import OrderManager
from order_manager.position_sizer import PositionSizer
from strategy.strategies.idle_strategy import IdleStrategy
from strategy.strategies.sma_crossover_strategy import (
    SmaCrossoverStrategy,
)

QUEUE_NAME = "candles"
EXCHANGE = "IEX"
AAPL_SYMBOL = "AAPL"
AAPL_ASSET = Asset(symbol=AAPL_SYMBOL, exchange=EXCHANGE)
GOOGL_SYMBOL = "GOOGL"
GOOGL_ASSET = Asset(symbol=GOOGL_SYMBOL, exchange=EXCHANGE)
CANDLE = Candle(
    asset=AAPL_ASSET,
    open=355.15,
    high=355.15,
    low=353.74,
    close=353.84,
    volume=3254,
    timestamp=datetime.strptime("2020-06-11 13:30:00+0000", "%Y-%m-%d %H:%M:%S%z"),
)
CANDLE_JSON = CANDLE.to_json()
FUND = 10000
START_TIMESTAMP = datetime.strptime("2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
MARKET_CAL = EuronextExchangeCalendar()
CLOCK = SimulatedClock()
EVENTS = deque()
FEED = CsvFeed(
    {Asset(symbol="AAPL", exchange="IEX"): "test/data/aapl_candles_one_day.csv"}, EVENTS
)


def test_init_live():
    assets = [AAPL_ASSET, GOOGL_ASSET, AAPL_ASSET]
    strategies_classes = [SmaCrossoverStrategy, IdleStrategy]
    broker = SimulatedBroker(clock=CLOCK, events=EVENTS, initial_funds=FUND)
    broker_manager = BrokerManager(brokers={EXCHANGE: broker}, clock=CLOCK)
    position_sizer = PositionSizer(broker_manager=broker_manager)
    order_creator = OrderCreator(broker_manager=broker_manager)
    order_manager = OrderManager(EVENTS, broker_manager, position_sizer, order_creator)
    indicators_manager = IndicatorsManager(preload=False)
    event_loop = EventLoop(
        events=EVENTS,
        assets=assets,
        feed=FEED,
        order_manager=order_manager,
        indicators_manager=indicators_manager,
        strategies_classes=strategies_classes,
        live=True,
    )

    assert event_loop.assets == [AAPL_ASSET, GOOGL_ASSET]
    assert event_loop.strategies_classes == strategies_classes

    assert len(event_loop.strategy_instances) == 4
    assert isinstance(event_loop.strategy_instances[0], SmaCrossoverStrategy)
    assert event_loop.strategy_instances[0].asset == AAPL_ASSET
    assert isinstance(event_loop.strategy_instances[1], SmaCrossoverStrategy)
    assert event_loop.strategy_instances[1].asset == GOOGL_ASSET
    assert isinstance(event_loop.strategy_instances[2], IdleStrategy)
    assert event_loop.strategy_instances[2].asset == AAPL_ASSET
    assert isinstance(event_loop.strategy_instances[3], IdleStrategy)
    assert event_loop.strategy_instances[3].asset == GOOGL_ASSET


def test_init_backtest():
    assets = [AAPL_ASSET, GOOGL_ASSET, AAPL_ASSET]
    strategies_classes = [SmaCrossoverStrategy, IdleStrategy]
    broker = SimulatedBroker(clock=CLOCK, events=EVENTS, initial_funds=FUND)
    broker_manager = BrokerManager(brokers={EXCHANGE: broker}, clock=CLOCK)
    position_sizer = PositionSizer(broker_manager=broker_manager)
    order_creator = OrderCreator(broker_manager=broker_manager)
    order_manager = OrderManager(EVENTS, broker_manager, position_sizer, order_creator)
    indicators_manager = IndicatorsManager(preload=False)
    event_loop = EventLoop(
        events=EVENTS,
        assets=assets,
        feed=FEED,
        order_manager=order_manager,
        indicators_manager=indicators_manager,
        strategies_classes=strategies_classes,
    )

    assert event_loop.assets == [AAPL_ASSET, GOOGL_ASSET]
    assert event_loop.strategies_classes == strategies_classes

    assert len(event_loop.strategy_instances) == 4
    assert isinstance(event_loop.strategy_instances[0], SmaCrossoverStrategy)
    assert event_loop.strategy_instances[0].asset == AAPL_ASSET
    assert isinstance(event_loop.strategy_instances[1], SmaCrossoverStrategy)
    assert event_loop.strategy_instances[1].asset == GOOGL_ASSET
    assert isinstance(event_loop.strategy_instances[2], IdleStrategy)
    assert event_loop.strategy_instances[2].asset == AAPL_ASSET
    assert isinstance(event_loop.strategy_instances[3], IdleStrategy)
    assert event_loop.strategy_instances[3].asset == GOOGL_ASSET


@patch("strategy.strategies.sma_crossover_strategy.SmaCrossoverStrategy.process_candle")
def test_run_strategy(process_candle_mocked):
    assets = [AAPL_ASSET, GOOGL_ASSET]
    strategies_classes = [SmaCrossoverStrategy]
    broker = SimulatedBroker(clock=CLOCK, events=EVENTS, initial_funds=FUND)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(broker_manager=broker)
    order_manager = OrderManager(EVENTS, broker, position_sizer, order_creator)
    indicators_manager = IndicatorsManager(preload=False)
    event_loop = EventLoop(
        events=EVENTS,
        assets=assets,
        feed=FEED,
        order_manager=order_manager,
        indicators_manager=indicators_manager,
        strategies_classes=strategies_classes,
    )
    strategy_object = event_loop.strategy_instances[0]
    event_loop.run_strategy(strategy_object, CANDLE)

    process_candle_calls = [call(CANDLE, CLOCK)]
    process_candle_mocked.assert_has_calls(process_candle_calls)


@patch("bot.event_loop.EventLoop.run_strategy")
def test_run_strategies(run_strategy_mocked):
    assets = [AAPL_ASSET, GOOGL_ASSET]
    strategies_classes = [SmaCrossoverStrategy, IdleStrategy]
    broker = SimulatedBroker(clock=CLOCK, events=EVENTS, initial_funds=FUND)
    broker_manager = BrokerManager(brokers={EXCHANGE: broker}, clock=CLOCK)
    position_sizer = PositionSizer(broker_manager=broker_manager)
    order_creator = OrderCreator(broker_manager=broker_manager)
    order_manager = OrderManager(EVENTS, broker_manager, position_sizer, order_creator)
    indicators_manager = IndicatorsManager(preload=False)
    event_loop = EventLoop(
        events=EVENTS,
        assets=assets,
        feed=FEED,
        order_manager=order_manager,
        indicators_manager=indicators_manager,
        strategies_classes=strategies_classes,
    )

    event_loop.run_strategies(CANDLE)

    run_strategy_calls = [
        call(event_loop.strategy_instances[0], CANDLE),
        call(event_loop.strategy_instances[1], CANDLE),
        call(event_loop.strategy_instances[2], CANDLE),
        call(event_loop.strategy_instances[3], CANDLE),
    ]
    run_strategy_mocked.assert_has_calls(run_strategy_calls)


def test_run_backtest():
    assets = [AAPL_ASSET, GOOGL_ASSET]
    strategies_classes = [SmaCrossoverStrategy, IdleStrategy]

    broker = SimulatedBroker(clock=CLOCK, events=EVENTS, initial_funds=FUND)
    broker_manager = BrokerManager(brokers={EXCHANGE: broker}, clock=CLOCK)
    position_sizer = PositionSizer(broker_manager=broker_manager)
    order_creator = OrderCreator(broker_manager=broker_manager)
    order_manager = OrderManager(EVENTS, broker_manager, position_sizer, order_creator)
    indicators_manager = IndicatorsManager(preload=False)
    event_loop = EventLoop(
        events=EVENTS,
        assets=assets,
        feed=FEED,
        order_manager=order_manager,
        indicators_manager=indicators_manager,
        strategies_classes=strategies_classes,
    )
    event_loop.loop()
