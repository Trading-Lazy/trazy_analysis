from collections import deque
from datetime import datetime
from unittest.mock import call, patch

from bot.event_loop import EventLoop
from broker.simulated_broker import SimulatedBroker
from common.clock import SimulatedClock
from common.exchange_calendar_euronext import EuronextExchangeCalendar
from feed.feed import CsvFeed
from indicators.indicators_manager import IndicatorsManager
from models.candle import Candle
from order_manager.order_creator import OrderCreator
from order_manager.order_manager import OrderManager
from order_manager.position_sizer import PositionSizer
from strategy.strategies.idle_strategy import IdleStrategy
from strategy.strategies.sma_crossover_strategy import (
    SmaCrossoverStrategy,
)

QUEUE_NAME = "candles"
AAPL_SYMBOL = "AAPL"
GOOGL_SYMBOL = "GOOGL"
CANDLE = Candle(
    symbol=AAPL_SYMBOL,
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
FEED = CsvFeed({"AAPL": "test/data/aapl_candles_one_day.csv"}, EVENTS)


def test_init_live():
    symbols = [AAPL_SYMBOL, GOOGL_SYMBOL, AAPL_SYMBOL]
    strategies_classes = [SmaCrossoverStrategy, IdleStrategy]
    broker = SimulatedBroker(clock=CLOCK, events=EVENTS, initial_funds=FUND)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(broker=broker)
    order_manager = OrderManager(EVENTS, broker, position_sizer, order_creator)
    indicators_manager = IndicatorsManager(preload=False)
    event_loop = EventLoop(
        events=EVENTS,
        symbols=symbols,
        feed=FEED,
        order_manager=order_manager,
        indicators_manager=indicators_manager,
        strategies_classes=strategies_classes,
        live=True,
    )

    assert event_loop.symbols == [AAPL_SYMBOL, GOOGL_SYMBOL]
    assert event_loop.strategies_classes == strategies_classes

    assert len(event_loop.strategy_instances) == 4
    assert isinstance(event_loop.strategy_instances[0], SmaCrossoverStrategy)
    assert event_loop.strategy_instances[0].symbol == AAPL_SYMBOL
    assert isinstance(event_loop.strategy_instances[1], SmaCrossoverStrategy)
    assert event_loop.strategy_instances[1].symbol == GOOGL_SYMBOL
    assert isinstance(event_loop.strategy_instances[2], IdleStrategy)
    assert event_loop.strategy_instances[2].symbol == AAPL_SYMBOL
    assert isinstance(event_loop.strategy_instances[3], IdleStrategy)
    assert event_loop.strategy_instances[3].symbol == GOOGL_SYMBOL


def test_init_backtest():
    symbols = [AAPL_SYMBOL, GOOGL_SYMBOL, AAPL_SYMBOL]
    strategies_classes = [SmaCrossoverStrategy, IdleStrategy]
    broker = SimulatedBroker(clock=CLOCK, events=EVENTS, initial_funds=FUND)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(broker=broker)
    order_manager = OrderManager(EVENTS, broker, position_sizer, order_creator)
    indicators_manager = IndicatorsManager(preload=False)
    event_loop = EventLoop(
        events=EVENTS,
        symbols=symbols,
        feed=FEED,
        order_manager=order_manager,
        indicators_manager=indicators_manager,
        strategies_classes=strategies_classes,
    )

    assert event_loop.symbols == [AAPL_SYMBOL, GOOGL_SYMBOL]
    assert event_loop.strategies_classes == strategies_classes

    assert len(event_loop.strategy_instances) == 4
    assert isinstance(event_loop.strategy_instances[0], SmaCrossoverStrategy)
    assert event_loop.strategy_instances[0].symbol == AAPL_SYMBOL
    assert isinstance(event_loop.strategy_instances[1], SmaCrossoverStrategy)
    assert event_loop.strategy_instances[1].symbol == GOOGL_SYMBOL
    assert isinstance(event_loop.strategy_instances[2], IdleStrategy)
    assert event_loop.strategy_instances[2].symbol == AAPL_SYMBOL
    assert isinstance(event_loop.strategy_instances[3], IdleStrategy)
    assert event_loop.strategy_instances[3].symbol == GOOGL_SYMBOL


@patch("strategy.strategies.sma_crossover_strategy.SmaCrossoverStrategy.process_candle")
def test_run_strategy(process_candle_mocked):
    symbols = [AAPL_SYMBOL, GOOGL_SYMBOL]
    strategies_classes = [SmaCrossoverStrategy]
    broker = SimulatedBroker(clock=CLOCK, events=EVENTS, initial_funds=FUND)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(broker=broker)
    order_manager = OrderManager(EVENTS, broker, position_sizer, order_creator)
    indicators_manager = IndicatorsManager(preload=False)
    event_loop = EventLoop(
        events=EVENTS,
        symbols=symbols,
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
    symbols = [AAPL_SYMBOL, GOOGL_SYMBOL]
    strategies_classes = [SmaCrossoverStrategy, IdleStrategy]
    broker = SimulatedBroker(clock=CLOCK, events=EVENTS, initial_funds=FUND)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(broker=broker)
    order_manager = OrderManager(EVENTS, broker, position_sizer, order_creator)
    indicators_manager = IndicatorsManager(preload=False)
    event_loop = EventLoop(
        events=EVENTS,
        symbols=symbols,
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


def test_run_live():
    symbols = [AAPL_SYMBOL, GOOGL_SYMBOL]
    strategies_classes = [SmaCrossoverStrategy, IdleStrategy]

    broker = SimulatedBroker(clock=CLOCK, events=EVENTS, initial_funds=FUND)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(broker=broker)
    order_manager = OrderManager(EVENTS, broker, position_sizer, order_creator)
    indicators_manager = IndicatorsManager(preload=False)
    event_loop = EventLoop(
        events=EVENTS,
        symbols=symbols,
        feed=FEED,
        order_manager=order_manager,
        indicators_manager=indicators_manager,
        strategies_classes=strategies_classes,
        live=True,
    )
    event_loop.loop()


def test_run_backtest():
    symbols = [AAPL_SYMBOL, GOOGL_SYMBOL]
    strategies_classes = [SmaCrossoverStrategy, IdleStrategy]

    broker = SimulatedBroker(clock=CLOCK, events=EVENTS, initial_funds=FUND)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(broker=broker)
    order_manager = OrderManager(EVENTS, broker, position_sizer, order_creator)
    indicators_manager = IndicatorsManager(preload=False)
    event_loop = EventLoop(
        events=EVENTS,
        symbols=symbols,
        feed=FEED,
        order_manager=order_manager,
        indicators_manager=indicators_manager,
        strategies_classes=strategies_classes,
    )
    event_loop.loop()
