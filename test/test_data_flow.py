from unittest.mock import call, patch

from bot.data_consumer import DataConsumer
from bot.data_flow import DataFlow
from broker.simulated_broker import SimulatedBroker
from candles_queue.candles_queue import CandlesQueue
from candles_queue.fake_queue import FakeQueue
from common.clock import SimulatedClock
from db_storage.mongodb_storage import MongoDbStorage
from feed.feed import CsvFeed, Feed
from indicators.indicators import IndicatorsManager
from order_manager.order_creator import OrderCreator
from order_manager.order_manager import OrderManager
from order_manager.position_sizer import PositionSizer
from settings import DATABASE_NAME, DATABASE_URL
from strategy.strategies.reactive_sma_crossover_strategy import (
    ReactiveSmaCrossoverStrategy,
)


@patch("feed.feed.CsvFeed.start")
@patch("bot.data_consumer.DataConsumer.start")
def test_start(data_consumer_start_mocked, feed_start_mocked):
    candles_queue: CandlesQueue = FakeQueue("candles")

    feed: Feed = CsvFeed(
        {"AAPL": "test/data/aapl_candles.csv"},
        candles_queue,
    )
    symbols = ["AAPL"]
    db_storage = MongoDbStorage(DATABASE_NAME, DATABASE_URL)
    db_storage.clean_all_candles()
    db_storage.clean_all_orders()
    strategies = [ReactiveSmaCrossoverStrategy]

    clock = SimulatedClock()
    broker = SimulatedBroker(clock=clock, initial_funds=10000)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(broker=broker)
    order_manager = OrderManager(broker, position_sizer, order_creator)
    indicators_manager = IndicatorsManager(preload=False)
    data_consumer = DataConsumer(
        symbols=symbols,
        candles_queue=candles_queue,
        db_storage=db_storage,
        order_manager=order_manager,
        indicators_manager=indicators_manager,
        strategies_classes=strategies,
    )

    data_flow = DataFlow(feed, data_consumer)
    data_flow.start()

    data_consumer_start_calls = [call()]
    data_consumer_start_mocked.assert_has_calls(data_consumer_start_calls)

    feed_start_calls = [call()]
    feed_start_mocked.assert_has_calls(feed_start_calls)
