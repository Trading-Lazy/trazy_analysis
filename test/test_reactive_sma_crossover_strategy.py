from decimal import Decimal

from bot.data_consumer import DataConsumer
from bot.data_flow import DataFlow
from broker.simulated_broker import SimulatedBroker
from candles_queue.candles_queue import CandlesQueue
from candles_queue.simple_queue import SimpleQueue
from common.clock import SimulatedClock
from db_storage.mongodb_storage import MongoDbStorage
from feed.feed import CsvFeed, Feed
from order_manager.order_creator import OrderCreator
from order_manager.order_manager import OrderManager
from order_manager.position_sizer import PositionSizer
from settings import DATABASE_NAME, DATABASE_URL
from strategy.strategies.reactive_sma_crossover_strategy import (
    ReactiveSmaCrossoverStrategy,
)


def test_reactive_sma_crossover_strategy():
    symbols = ["AAPL"]
    candles_queue: CandlesQueue = SimpleQueue("candles")

    feed: Feed = CsvFeed({"AAPL": "test/data/aapl_candles_one_day.csv"}, candles_queue)

    db_storage = MongoDbStorage(DATABASE_NAME, DATABASE_URL)
    db_storage.clean_all_signals()
    db_storage.clean_all_orders()
    db_storage.clean_all_candles()

    strategies = [ReactiveSmaCrossoverStrategy]
    clock = SimulatedClock()
    broker = SimulatedBroker(clock, initial_funds=Decimal("10000"))
    broker.subscribe_funds_to_portfolio(Decimal("10000"))
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(broker=broker)
    order_manager = OrderManager(
        broker=broker, position_sizer=position_sizer, order_creator=order_creator
    )
    data_consumer = DataConsumer(
        symbols, candles_queue, db_storage, order_manager, strategies
    )

    data_flow = DataFlow(feed, data_consumer)
    data_flow.start()

    assert broker.get_portfolio_cash_balance() == Decimal("10010.955")
