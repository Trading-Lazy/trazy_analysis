from collections import deque
from datetime import datetime, timedelta
from unittest.mock import call, patch

from trazy_analysis.bot.event_loop import EventLoop
from trazy_analysis.broker.broker_manager import BrokerManager
from trazy_analysis.broker.simulated_broker import SimulatedBroker
from trazy_analysis.common.clock import SimulatedClock
from pandas_market_calendars.exchange_calendar_eurex import EUREXExchangeCalendar
from trazy_analysis.feed.feed import CsvFeed
from trazy_analysis.indicators.indicators_manager import IndicatorsManager
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle
from trazy_analysis.order_manager.order_creator import OrderCreator
from trazy_analysis.order_manager.order_manager import OrderManager
from trazy_analysis.order_manager.position_sizer import PositionSizer
from trazy_analysis.strategy.strategies.idle_strategy import IdleStrategy
from trazy_analysis.strategy.strategies.sma_crossover_strategy import (
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
MARKET_CAL = EUREXExchangeCalendar()
CLOCK = SimulatedClock()
EVENTS = deque()
FEED = CsvFeed(
    csv_filenames={
        Asset(symbol="AAPL", exchange="IEX"): {
            timedelta(minutes=1): "test/data/aapl_candles_one_day.csv"
        }
    },
    events=EVENTS,
)


def test_init_live():
    time_unit = timedelta(minutes=1)
    assets = {AAPL_ASSET: time_unit, GOOGL_ASSET: time_unit}
    strategies_classes = {
        SmaCrossoverStrategy: SmaCrossoverStrategy.DEFAULT_PARAMETERS,
        IdleStrategy: IdleStrategy.DEFAULT_PARAMETERS,
    }
    broker = SimulatedBroker(clock=CLOCK, events=EVENTS, initial_funds=FUND)
    broker_manager = BrokerManager(brokers={EXCHANGE: broker})
    position_sizer = PositionSizer(broker_manager=broker_manager)
    order_creator = OrderCreator(broker_manager=broker_manager)
    order_manager = OrderManager(
        EVENTS, broker_manager, position_sizer, order_creator, CLOCK
    )
    indicators_manager = IndicatorsManager(preload=False)
    event_loop = EventLoop(
        events=EVENTS,
        assets=assets,
        feed=FEED,
        order_manager=order_manager,
        indicators_manager=indicators_manager,
        strategies_parameters=strategies_classes,
        live=True,
    )

    assert event_loop.assets == {AAPL_ASSET: [time_unit], GOOGL_ASSET: [time_unit]}
    assert event_loop.strategies_parameters == strategies_classes

    assert len(event_loop.strategy_instances) == 4
    assert list(event_loop.context.candles.keys()) == [AAPL_ASSET, GOOGL_ASSET]
    assert isinstance(event_loop.context.candles[AAPL_ASSET][time_unit], deque)
    assert len(event_loop.context.candles[AAPL_ASSET][time_unit]) == 0
    assert isinstance(event_loop.context.candles[GOOGL_ASSET][time_unit], deque)
    assert len(event_loop.context.candles[GOOGL_ASSET][time_unit]) == 0
    assert isinstance(event_loop.strategy_instances[0], SmaCrossoverStrategy)
    assert isinstance(event_loop.strategy_instances[1], SmaCrossoverStrategy)
    assert isinstance(event_loop.strategy_instances[2], IdleStrategy)
    assert isinstance(event_loop.strategy_instances[3], IdleStrategy)


def test_init_backtest():
    time_unit = timedelta(minutes=1)
    assets = {AAPL_ASSET: timedelta(minutes=1), GOOGL_ASSET: timedelta(minutes=1)}
    strategies_classes = {
        SmaCrossoverStrategy: SmaCrossoverStrategy.DEFAULT_PARAMETERS,
        IdleStrategy: IdleStrategy.DEFAULT_PARAMETERS,
    }
    broker = SimulatedBroker(clock=CLOCK, events=EVENTS, initial_funds=FUND)
    broker_manager = BrokerManager(brokers={EXCHANGE: broker})
    position_sizer = PositionSizer(broker_manager=broker_manager)
    order_creator = OrderCreator(broker_manager=broker_manager)
    order_manager = OrderManager(
        EVENTS, broker_manager, position_sizer, order_creator, CLOCK
    )
    indicators_manager = IndicatorsManager(preload=False)
    event_loop = EventLoop(
        events=EVENTS,
        assets=assets,
        feed=FEED,
        order_manager=order_manager,
        indicators_manager=indicators_manager,
        strategies_parameters=strategies_classes,
    )

    assert event_loop.assets == {AAPL_ASSET: [time_unit], GOOGL_ASSET: [time_unit]}
    assert event_loop.strategies_parameters == strategies_classes

    assert len(event_loop.strategy_instances) == 4
    assert list(event_loop.context.candles.keys()) == [AAPL_ASSET, GOOGL_ASSET]
    assert isinstance(event_loop.context.candles[AAPL_ASSET][time_unit], deque)
    assert len(event_loop.context.candles[AAPL_ASSET][time_unit]) == 0
    assert isinstance(event_loop.context.candles[GOOGL_ASSET][time_unit], deque)
    assert len(event_loop.context.candles[GOOGL_ASSET][time_unit]) == 0
    assert isinstance(event_loop.strategy_instances[0], SmaCrossoverStrategy)
    assert isinstance(event_loop.strategy_instances[1], SmaCrossoverStrategy)
    assert isinstance(event_loop.strategy_instances[2], IdleStrategy)
    assert isinstance(event_loop.strategy_instances[3], IdleStrategy)


@patch(
    "trazy_analysis.strategy.strategies.sma_crossover_strategy.SmaCrossoverStrategy.process_context"
)
def test_run_strategy(process_context):
    assets = {AAPL_ASSET: timedelta(minutes=1), GOOGL_ASSET: timedelta(minutes=1)}
    strategies_classes = {SmaCrossoverStrategy: SmaCrossoverStrategy.DEFAULT_PARAMETERS}
    broker = SimulatedBroker(clock=CLOCK, events=EVENTS, initial_funds=FUND)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(broker_manager=broker)
    order_manager = OrderManager(EVENTS, broker, position_sizer, order_creator, CLOCK)
    indicators_manager = IndicatorsManager(preload=False)
    event_loop = EventLoop(
        events=EVENTS,
        assets=assets,
        feed=FEED,
        order_manager=order_manager,
        indicators_manager=indicators_manager,
        strategies_parameters=strategies_classes,
    )
    strategy_object = event_loop.strategy_instances[0]
    event_loop.run_strategy(strategy_object)

    process_context_calls = [call(event_loop.context, CLOCK)]
    process_context.assert_has_calls(process_context_calls)


@patch("trazy_analysis.bot.event_loop.EventLoop.run_strategy")
def test_run_strategies(run_strategy_mocked):
    assets = {AAPL_ASSET: timedelta(minutes=1), GOOGL_ASSET: timedelta(minutes=1)}
    strategies_classes = {
        SmaCrossoverStrategy: SmaCrossoverStrategy.DEFAULT_PARAMETERS,
        IdleStrategy: IdleStrategy.DEFAULT_PARAMETERS,
    }
    broker = SimulatedBroker(clock=CLOCK, events=EVENTS, initial_funds=FUND)
    broker_manager = BrokerManager(brokers={EXCHANGE: broker})
    position_sizer = PositionSizer(broker_manager=broker_manager)
    order_creator = OrderCreator(broker_manager=broker_manager)
    order_manager = OrderManager(
        EVENTS, broker_manager, position_sizer, order_creator, CLOCK
    )
    indicators_manager = IndicatorsManager(preload=False)
    event_loop = EventLoop(
        events=EVENTS,
        assets=assets,
        feed=FEED,
        order_manager=order_manager,
        indicators_manager=indicators_manager,
        strategies_parameters=strategies_classes,
    )

    event_loop.run_strategies()

    run_strategy_calls = [
        call(event_loop.strategy_instances[0]),
        call(event_loop.strategy_instances[1]),
    ]
    run_strategy_mocked.assert_has_calls(run_strategy_calls)


def test_run_backtest():
    assets = {AAPL_ASSET: timedelta(minutes=1), GOOGL_ASSET: timedelta(minutes=1)}
    strategies_classes = {
        SmaCrossoverStrategy: SmaCrossoverStrategy.DEFAULT_PARAMETERS,
        IdleStrategy: IdleStrategy.DEFAULT_PARAMETERS,
    }

    broker = SimulatedBroker(clock=CLOCK, events=EVENTS, initial_funds=FUND)
    broker_manager = BrokerManager(brokers={EXCHANGE: broker})
    position_sizer = PositionSizer(broker_manager=broker_manager)
    order_creator = OrderCreator(broker_manager=broker_manager)
    order_manager = OrderManager(
        EVENTS, broker_manager, position_sizer, order_creator, CLOCK
    )
    indicators_manager = IndicatorsManager(preload=False)
    event_loop = EventLoop(
        events=EVENTS,
        assets=assets,
        feed=FEED,
        order_manager=order_manager,
        indicators_manager=indicators_manager,
        strategies_parameters=strategies_classes,
    )
    event_loop.loop()
