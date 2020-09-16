from decimal import Decimal

from bot.data_consumer import DataConsumer
from bot.data_flow import DataFlow
from broker.simulatedbroker import SimulatedBroker
from candles_queue.candles_queue import CandlesQueue
from candles_queue.simple_queue import SimpleQueue
from db_storage.mongodb_storage import MongoDbStorage
from feed.feed import CsvFeed, Feed
from settings import DATABASE_NAME, DATABASE_URL
from strategy.strategies.reactive_sma_crossover_strategy import (
    ReactiveSmaCrossoverStrategy,
)


def test_reactive_sma_crossover_strategy():
    symbols = ["AAPL"]
    candles_queue: CandlesQueue = SimpleQueue("candles")

    feed: Feed = CsvFeed({"AAPL": "test/data/aapl_candles_one_day.csv"}, candles_queue)

    db_storage = MongoDbStorage(DATABASE_NAME, DATABASE_URL)
    db_storage.clean_all_actions()
    db_storage.clean_all_candles()

    strategies = [ReactiveSmaCrossoverStrategy]
    broker = SimulatedBroker(cash=Decimal("10000"))
    data_consumer = DataConsumer(symbols, candles_queue, db_storage, broker, strategies)

    data_flow = DataFlow(feed, data_consumer)
    data_flow.start()

    assert data_consumer.broker.cash == Decimal("9867.210")
