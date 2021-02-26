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
from strategy.strategies.sma_crossover_strategy import (
    SmaCrossoverStrategy,
)


def test_reactive_sma_crossover_strategy_preload_data():
    symbols = ["AAPL"]
    candles_queue: CandlesQueue = FakeQueue("candles")

    feed: Feed = CsvFeed({"AAPL": "test/data/aapl_candles_one_day.csv"}, candles_queue)

    db_storage = MongoDbStorage(DATABASE_NAME, DATABASE_URL)
    db_storage.clean_all_signals()
    db_storage.clean_all_orders()
    db_storage.clean_all_candles()

    strategies = [SmaCrossoverStrategy]
    clock = SimulatedClock()
    broker = SimulatedBroker(clock, initial_funds=10000.0)
    broker.subscribe_funds_to_portfolio(10000.0)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(broker=broker)
    order_manager = OrderManager(
        broker=broker, position_sizer=position_sizer, order_creator=order_creator
    )
    indicators_manager = IndicatorsManager(initial_data=feed.candles)
    data_consumer = DataConsumer(
        symbols=symbols,
        candles_queue=candles_queue,
        db_storage=None,
        order_manager=order_manager,
        strategies_classes=strategies,
        indicators_manager=indicators_manager,
    )
    data_flow = DataFlow(feed, data_consumer)
    # start = time.time()
    data_flow.start()

    assert broker.get_portfolio_cash_balance() == 10010.955
