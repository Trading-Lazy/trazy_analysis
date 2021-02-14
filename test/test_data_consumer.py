from datetime import datetime, timedelta
from unittest.mock import call, patch

from bot.data_consumer import DataConsumer
from broker.simulated_broker import SimulatedBroker
from candles_queue.fake_queue import FakeQueue
from common.clock import SimulatedClock
from common.exchange_calendar_euronext import EuronextExchangeCalendar
from db_storage.mongodb_storage import MongoDbStorage
from indicators.indicators import IndicatorsManager
from models.candle import Candle
from order_manager.order_creator import OrderCreator
from order_manager.order_manager import OrderManager
from order_manager.position_sizer import PositionSizer
from settings import DATABASE_NAME, DATABASE_URL
from strategy.strategies.idle_strategy import IdleStrategy
from strategy.strategies.reactive_sma_crossover_strategy import (
    ReactiveSmaCrossoverStrategy,
)

QUEUE_NAME = "candles"
DB_STORAGE = MongoDbStorage(DATABASE_NAME, DATABASE_URL)
AAPL_SYMBOL = "AAPL_SYMBOL"
GOOGL_SYMBOL = "GOOGL_SYMBOL"
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


def test_init_live():
    symbols = [AAPL_SYMBOL, GOOGL_SYMBOL, AAPL_SYMBOL]
    candles_queue = FakeQueue(QUEUE_NAME)
    strategies_classes = [ReactiveSmaCrossoverStrategy, IdleStrategy]
    broker = SimulatedBroker(clock=CLOCK, initial_funds=FUND)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(broker=broker)
    order_manager = OrderManager(broker, position_sizer, order_creator)
    indicators_manager = IndicatorsManager(preload=False)
    data_consumer = DataConsumer(
        symbols=symbols,
        candles_queue=candles_queue,
        db_storage=DB_STORAGE,
        order_manager=order_manager,
        indicators_manager=indicators_manager,
        strategies_classes=strategies_classes,
        save_candles=True,
    )

    assert data_consumer.symbols == [AAPL_SYMBOL, GOOGL_SYMBOL]
    assert data_consumer.db_storage == DB_STORAGE
    assert data_consumer.candles_queue == candles_queue
    assert data_consumer.strategies_classes == strategies_classes

    assert len(data_consumer.strategy_instances) == 4
    assert isinstance(data_consumer.strategy_instances[0], ReactiveSmaCrossoverStrategy)
    assert data_consumer.strategy_instances[0].symbol == AAPL_SYMBOL
    assert isinstance(data_consumer.strategy_instances[1], ReactiveSmaCrossoverStrategy)
    assert data_consumer.strategy_instances[1].symbol == GOOGL_SYMBOL
    assert isinstance(data_consumer.strategy_instances[2], IdleStrategy)
    assert data_consumer.strategy_instances[2].symbol == AAPL_SYMBOL
    assert isinstance(data_consumer.strategy_instances[3], IdleStrategy)
    assert data_consumer.strategy_instances[3].symbol == GOOGL_SYMBOL

    for strategy_object in data_consumer.strategy_instances:
        assert strategy_object.db_storage == DB_STORAGE


def test_init_simulation():
    symbols = [AAPL_SYMBOL, GOOGL_SYMBOL, AAPL_SYMBOL]
    candles_queue = FakeQueue(QUEUE_NAME)
    strategies_classes = [ReactiveSmaCrossoverStrategy, IdleStrategy]
    broker = SimulatedBroker(clock=CLOCK, initial_funds=FUND)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(broker=broker)
    order_manager = OrderManager(broker, position_sizer, order_creator)
    indicators_manager = IndicatorsManager(preload=False)
    data_consumer = DataConsumer(
        symbols=symbols,
        candles_queue=candles_queue,
        db_storage=DB_STORAGE,
        order_manager=order_manager,
        indicators_manager=indicators_manager,
        strategies_classes=strategies_classes,
        save_candles=True,
    )

    assert data_consumer.symbols == [AAPL_SYMBOL, GOOGL_SYMBOL]
    assert data_consumer.db_storage == DB_STORAGE
    assert data_consumer.candles_queue == candles_queue
    assert data_consumer.strategies_classes == strategies_classes

    assert len(data_consumer.strategy_instances) == 4
    assert isinstance(data_consumer.strategy_instances[0], ReactiveSmaCrossoverStrategy)
    assert data_consumer.strategy_instances[0].symbol == AAPL_SYMBOL
    assert isinstance(data_consumer.strategy_instances[1], ReactiveSmaCrossoverStrategy)
    assert data_consumer.strategy_instances[1].symbol == GOOGL_SYMBOL
    assert isinstance(data_consumer.strategy_instances[2], IdleStrategy)
    assert data_consumer.strategy_instances[2].symbol == AAPL_SYMBOL
    assert isinstance(data_consumer.strategy_instances[3], IdleStrategy)
    assert data_consumer.strategy_instances[3].symbol == GOOGL_SYMBOL

    for strategy_object in data_consumer.strategy_instances:
        assert strategy_object.db_storage == DB_STORAGE


def test_add_strategy():
    symbols = [AAPL_SYMBOL, GOOGL_SYMBOL]
    candles_queue = FakeQueue(QUEUE_NAME)
    strategies_classes = [ReactiveSmaCrossoverStrategy]
    broker = SimulatedBroker(clock=CLOCK, initial_funds=FUND)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(broker=broker)
    order_manager = OrderManager(broker, position_sizer, order_creator)
    indicators_manager = IndicatorsManager(preload=False)
    data_consumer = DataConsumer(
        symbols=symbols,
        candles_queue=candles_queue,
        db_storage=DB_STORAGE,
        order_manager=order_manager,
        indicators_manager=indicators_manager,
        strategies_classes=strategies_classes,
        save_candles=True,
    )

    data_consumer.add_strategy(IdleStrategy)
    assert len(data_consumer.strategy_instances) == 4
    assert isinstance(data_consumer.strategy_instances[2], IdleStrategy)
    assert data_consumer.strategy_instances[2].symbol == AAPL_SYMBOL
    assert isinstance(data_consumer.strategy_instances[3], IdleStrategy)
    assert data_consumer.strategy_instances[3].symbol == GOOGL_SYMBOL
    for strategy_object in data_consumer.strategy_instances[2:]:
        assert strategy_object.db_storage == DB_STORAGE


@patch(
    "strategy.strategies.reactive_sma_crossover_strategy.ReactiveSmaCrossoverStrategy.process_candle"
)
def test_run_strategy(process_candle_mocked):
    symbols = [AAPL_SYMBOL, GOOGL_SYMBOL]
    candles_queue = FakeQueue(QUEUE_NAME)
    strategies_classes = [ReactiveSmaCrossoverStrategy]
    broker = SimulatedBroker(clock=CLOCK, initial_funds=FUND)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(broker=broker)
    order_manager = OrderManager(broker, position_sizer, order_creator)
    indicators_manager = IndicatorsManager(preload=False)
    data_consumer = DataConsumer(
        symbols=symbols,
        candles_queue=candles_queue,
        db_storage=DB_STORAGE,
        order_manager=order_manager,
        indicators_manager=indicators_manager,
        strategies_classes=strategies_classes,
        save_candles=True,
    )
    strategy_object = data_consumer.strategy_instances[0]
    data_consumer.run_strategy(strategy_object, CANDLE)

    process_candle_calls = [call(CANDLE, CLOCK)]
    process_candle_mocked.assert_has_calls(process_candle_calls)


@patch("bot.data_consumer.DataConsumer.run_strategy")
def test_run_strategies(run_strategy_mocked):
    symbols = [AAPL_SYMBOL, GOOGL_SYMBOL]
    candles_queue = FakeQueue(QUEUE_NAME)
    strategies_classes = [ReactiveSmaCrossoverStrategy, IdleStrategy]
    broker = SimulatedBroker(clock=CLOCK, initial_funds=FUND)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(broker=broker)
    order_manager = OrderManager(broker, position_sizer, order_creator)
    indicators_manager = IndicatorsManager(preload=False)
    data_consumer = DataConsumer(
        symbols=symbols,
        candles_queue=candles_queue,
        db_storage=DB_STORAGE,
        order_manager=order_manager,
        indicators_manager=indicators_manager,
        strategies_classes=strategies_classes,
        save_candles=True,
    )

    data_consumer.run_strategies(CANDLE)

    run_strategy_calls = [
        call(data_consumer.strategy_instances[0], CANDLE),
        call(data_consumer.strategy_instances[1], CANDLE),
        call(data_consumer.strategy_instances[2], CANDLE),
        call(data_consumer.strategy_instances[3], CANDLE),
    ]
    run_strategy_mocked.assert_has_calls(run_strategy_calls)


@patch("db_storage.mongodb_storage.MongoDbStorage.add_candle")
@patch("db_storage.mongodb_storage.MongoDbStorage.candle_with_identifier_exists")
@patch("bot.data_consumer.DataConsumer.run_strategies")
@patch("indicators.rolling_window.RollingWindowStream.on_next")
def test_handle_new_candle_callback(
    on_next_mocked,
    run_strategies_mocked,
    candle_with_identifier_exists_mocked,
    add_candle_mocked,
):
    symbols = [AAPL_SYMBOL, GOOGL_SYMBOL]
    candles_queue = FakeQueue(QUEUE_NAME)
    strategies_classes = [ReactiveSmaCrossoverStrategy, IdleStrategy]
    candle_with_identifier_exists_mocked.return_value = False

    broker = SimulatedBroker(clock=CLOCK, initial_funds=FUND)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(broker=broker)
    order_manager = OrderManager(broker, position_sizer, order_creator)
    indicators_manager = IndicatorsManager(preload=False)
    data_consumer = DataConsumer(
        symbols=symbols,
        candles_queue=candles_queue,
        db_storage=DB_STORAGE,
        order_manager=order_manager,
        indicators_manager=indicators_manager,
        strategies_classes=strategies_classes,
        save_candles=True,
    )

    DB_STORAGE.clean_all_candles()
    data_consumer.handle_new_candle_callback(CANDLE)

    rolling_window_manager = data_consumer.indicators_manager.rolling_window_manager
    assert AAPL_SYMBOL in rolling_window_manager.cache
    assert rolling_window_manager.cache[AAPL_SYMBOL].on_next == on_next_mocked

    on_next_calls = [call(CANDLE)]
    on_next_mocked.assert_has_calls(on_next_calls)

    run_strategies_calls = [call(CANDLE)]
    run_strategies_mocked.assert_has_calls(run_strategies_calls)

    add_candle_calls = [call(CANDLE)]
    add_candle_mocked.assert_has_calls(add_candle_calls)

    DB_STORAGE.clean_all_candles()


@patch("rx.core.observable.observable.Observable.subscribe")
@patch("candles_queue.fake_queue.FakeQueue.add_consumer")
def test_start_live(add_consumer_with_ack_mocked, subscribe_mocked):
    symbols = [AAPL_SYMBOL, GOOGL_SYMBOL]
    candles_queue = FakeQueue(QUEUE_NAME)
    strategies_classes = [ReactiveSmaCrossoverStrategy, IdleStrategy]

    broker = SimulatedBroker(clock=CLOCK, initial_funds=FUND)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(broker=broker)
    order_manager = OrderManager(broker, position_sizer, order_creator)
    indicators_manager = IndicatorsManager(preload=False)
    data_consumer = DataConsumer(
        symbols=symbols,
        candles_queue=candles_queue,
        db_storage=DB_STORAGE,
        order_manager=order_manager,
        indicators_manager=indicators_manager,
        strategies_classes=strategies_classes,
        save_candles=True,
        live=True,
        frequency=timedelta(seconds=1),
    )
    data_consumer.start()

    add_consumer_with_ack_calls = [call(data_consumer.handle_new_candle_callback)]
    add_consumer_with_ack_mocked.assert_has_calls(add_consumer_with_ack_calls)

    assert subscribe_mocked.call_count == 2


@patch("candles_queue.fake_queue.FakeQueue.add_consumer")
def test_start_simulation(add_consumer_with_ack_mocked):
    symbols = [AAPL_SYMBOL, GOOGL_SYMBOL]
    candles_queue = FakeQueue(QUEUE_NAME)
    strategies_classes = [ReactiveSmaCrossoverStrategy, IdleStrategy]

    broker = SimulatedBroker(clock=CLOCK, initial_funds=FUND)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(broker=broker)
    order_manager = OrderManager(broker, position_sizer, order_creator)
    indicators_manager = IndicatorsManager(preload=False)
    data_consumer = DataConsumer(
        symbols=symbols,
        candles_queue=candles_queue,
        db_storage=DB_STORAGE,
        order_manager=order_manager,
        indicators_manager=indicators_manager,
        strategies_classes=strategies_classes,
        save_candles=True,
    )
    data_consumer.start()

    add_consumer_with_ack_calls = [call(data_consumer.handle_new_candle_callback)]
    add_consumer_with_ack_mocked.assert_has_calls(add_consumer_with_ack_calls)
