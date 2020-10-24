from decimal import Decimal
from unittest.mock import call, patch

import pandas as pd

from bot.data_consumer import DataConsumer
from bot.data_flow import DataFlow
from broker.simulated_broker import SimulatedBroker
from candles_queue.candles_queue import CandlesQueue
from candles_queue.simple_queue import SimpleQueue
from common.exchange_calendar_euronext import EuronextExchangeCalendar
from db_storage.mongodb_storage import MongoDbStorage
from feed.feed import CsvFeed, Feed
from settings import DATABASE_NAME, DATABASE_URL
from strategy.strategies.reactive_sma_crossover_strategy import (
    ReactiveSmaCrossoverStrategy,
)


@patch("feed.feed.CsvFeed.start")
@patch("bot.data_consumer.DataConsumer.start")
def test_start(data_consumer_start_mocked, feed_start_mocked):
    candles_queue: CandlesQueue = SimpleQueue("candles")

    feed: Feed = CsvFeed(
        {"AAPL": "test/data/aapl_candles.csv"}, candles_queue,
    )
    symbols = ["AAPL"]
    db_storage = MongoDbStorage(DATABASE_NAME, DATABASE_URL)
    db_storage.clean_all_candles()
    db_storage.clean_all_orders()
    strategies = [ReactiveSmaCrossoverStrategy]

    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz="UTC")
    market_cal = EuronextExchangeCalendar()
    broker = SimulatedBroker(market_cal, start_timestamp, initial_funds=Decimal("10000"))
    data_consumer = DataConsumer(symbols, candles_queue, db_storage, broker, strategies)

    data_flow = DataFlow(feed, data_consumer)
    data_flow.start()

    data_consumer_start_calls = [call()]
    data_consumer_start_mocked.assert_has_calls(data_consumer_start_calls)

    feed_start_calls = [call()]
    feed_start_mocked.assert_has_calls(feed_start_calls)
