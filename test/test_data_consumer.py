from decimal import Decimal
from unittest.mock import call, patch

import pandas as pd
from pymongo.errors import DuplicateKeyError

from bot.data_consumer import DataConsumer
from broker.simulatedbroker import SimulatedBroker
from candles_queue.simple_queue import SimpleQueue
from db_storage.mongodb_storage import MongoDbStorage
from indicators.rolling_window import RollingWindowFactory
from models.candle import Candle
from settings import DATABASE_NAME, DATABASE_URL
from strategy.strategies.buy_and_sell_long_strategy import BuyAndSellLongStrategy
from strategy.strategies.reactive_sma_crossover_strategy import (
    ReactiveSmaCrossoverStrategy,
)

QUEUE_NAME = "candles"
DB_STORAGE = MongoDbStorage(DATABASE_NAME, DATABASE_URL)
AAPL_SYMBOL = "AAPL_SYMBOL"
GOOGL_SYMBOL = "GOOGL_SYMBOL"
CANDLE = Candle(
    symbol=AAPL_SYMBOL,
    open=Decimal("355.15"),
    high=Decimal("355.15"),
    low=Decimal("353.74"),
    close=Decimal("353.84"),
    volume=3254,
    timestamp=pd.Timestamp("2020-06-11 13:30:00+00:00"),
)
CANDLE_JSON = CANDLE.to_json()
FUND = Decimal("10000")
BROKER = SimulatedBroker(cash=FUND)


def test___init__():
    symbols = [AAPL_SYMBOL, GOOGL_SYMBOL, AAPL_SYMBOL]
    candles_queue = SimpleQueue(QUEUE_NAME)
    strategies_classes = [ReactiveSmaCrossoverStrategy, BuyAndSellLongStrategy]
    BROKER.reset()
    BROKER.add_cash(FUND)
    data_consumer = DataConsumer(
        symbols, candles_queue, DB_STORAGE, BROKER, strategies_classes, save_candles=True
    )

    assert data_consumer.symbols == [AAPL_SYMBOL, GOOGL_SYMBOL]
    assert data_consumer.db_storage == DB_STORAGE
    assert data_consumer.candles_queue == candles_queue
    assert data_consumer.strategies_classes == strategies_classes

    assert len(data_consumer.strategies_instances) == 4
    assert isinstance(
        data_consumer.strategies_instances[0], ReactiveSmaCrossoverStrategy
    )
    assert data_consumer.strategies_instances[0].symbol == AAPL_SYMBOL
    assert isinstance(
        data_consumer.strategies_instances[1], ReactiveSmaCrossoverStrategy
    )
    assert data_consumer.strategies_instances[1].symbol == GOOGL_SYMBOL
    assert isinstance(data_consumer.strategies_instances[2], BuyAndSellLongStrategy)
    assert data_consumer.strategies_instances[2].symbol == AAPL_SYMBOL
    assert isinstance(data_consumer.strategies_instances[3], BuyAndSellLongStrategy)
    assert data_consumer.strategies_instances[3].symbol == GOOGL_SYMBOL

    for strategy_object in data_consumer.strategies_instances:
        assert strategy_object.db_storage == DB_STORAGE


def test_add_strategy():
    symbols = [AAPL_SYMBOL, GOOGL_SYMBOL]
    candles_queue = SimpleQueue(QUEUE_NAME)
    strategies_classes = [ReactiveSmaCrossoverStrategy]
    BROKER.reset()
    BROKER.add_cash(FUND)
    data_consumer = DataConsumer(
        symbols, candles_queue, DB_STORAGE, BROKER, strategies_classes, save_candles=True
    )

    data_consumer.add_strategy(BuyAndSellLongStrategy)
    assert len(data_consumer.strategies_instances) == 4
    assert isinstance(data_consumer.strategies_instances[2], BuyAndSellLongStrategy)
    assert data_consumer.strategies_instances[2].symbol == AAPL_SYMBOL
    assert isinstance(data_consumer.strategies_instances[3], BuyAndSellLongStrategy)
    assert data_consumer.strategies_instances[3].symbol == GOOGL_SYMBOL
    for strategy_object in data_consumer.strategies_instances[2:]:
        assert strategy_object.db_storage == DB_STORAGE


@patch(
    "strategy.strategies.reactive_sma_crossover_strategy.ReactiveSmaCrossoverStrategy.process_candle"
)
def test_run_strategy(process_candle_mocked):
    symbols = [AAPL_SYMBOL, GOOGL_SYMBOL]
    candles_queue = SimpleQueue(QUEUE_NAME)
    strategies_classes = [ReactiveSmaCrossoverStrategy]
    BROKER.reset()
    BROKER.add_cash(FUND)
    data_consumer = DataConsumer(
        symbols, candles_queue, DB_STORAGE, BROKER, strategies_classes, save_candles=True
    )
    strategy_object = data_consumer.strategies_instances[0]
    data_consumer.run_strategy(strategy_object, CANDLE)

    process_candle_calls = [call(CANDLE)]
    process_candle_mocked.assert_has_calls(process_candle_calls)


@patch("bot.data_consumer.DataConsumer.run_strategy")
def test_run_strategies(run_strategy_mocked):
    symbols = [AAPL_SYMBOL, GOOGL_SYMBOL]
    candles_queue = SimpleQueue(QUEUE_NAME)
    strategies_classes = [ReactiveSmaCrossoverStrategy, BuyAndSellLongStrategy]
    BROKER.reset()
    BROKER.add_cash(FUND)
    data_consumer = DataConsumer(
        symbols, candles_queue, DB_STORAGE, BROKER, strategies_classes, save_candles=True
    )

    data_consumer.run_strategies(CANDLE)

    run_strategy_calls = [
        call(data_consumer.strategies_instances[0], CANDLE),
        call(data_consumer.strategies_instances[1], CANDLE),
        call(data_consumer.strategies_instances[2], CANDLE),
        call(data_consumer.strategies_instances[3], CANDLE),
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
    candles_queue = SimpleQueue(QUEUE_NAME)
    strategies_classes = [ReactiveSmaCrossoverStrategy, BuyAndSellLongStrategy]
    candle_with_identifier_exists_mocked.return_value = False

    BROKER.reset()
    BROKER.add_cash(FUND)
    data_consumer = DataConsumer(
        symbols, candles_queue, DB_STORAGE, BROKER, strategies_classes, save_candles=True
    )

    DB_STORAGE.clean_all_candles()
    data_consumer.handle_new_candle_callback(CANDLE_JSON)

    assert AAPL_SYMBOL in RollingWindowFactory().cache
    assert 5 in RollingWindowFactory().cache[AAPL_SYMBOL]
    assert RollingWindowFactory().cache[AAPL_SYMBOL][5].on_next == on_next_mocked

    on_next_calls = [call(CANDLE)]
    on_next_mocked.assert_has_calls(on_next_calls)

    run_strategies_calls = [call(CANDLE)]
    run_strategies_mocked.assert_has_calls(run_strategies_calls)

    add_candle_calls = [call(CANDLE)]
    add_candle_mocked.assert_has_calls(add_candle_calls)

    DB_STORAGE.clean_all_candles()


@patch("candles_queue.simple_queue.SimpleQueue.add_consumer_with_ack")
def test_start(add_consumer_with_ack_mocked):
    symbols = [AAPL_SYMBOL, GOOGL_SYMBOL]
    candles_queue = SimpleQueue(QUEUE_NAME)
    strategies_classes = [ReactiveSmaCrossoverStrategy, BuyAndSellLongStrategy]

    BROKER.reset()
    BROKER.add_cash(FUND)
    data_consumer = DataConsumer(
        symbols, candles_queue, DB_STORAGE, BROKER, strategies_classes, save_candles=True
    )
    data_consumer.start()

    add_consumer_with_ack_calls = [call(data_consumer.handle_new_candle_callback)]
    add_consumer_with_ack_mocked.assert_has_calls(add_consumer_with_ack_calls)
