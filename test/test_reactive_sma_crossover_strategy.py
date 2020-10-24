from decimal import Decimal

import pandas as pd
import plotly.graph_objects as go
import matplotlib.pyplot as plt

from bot.data_consumer import DataConsumer
from bot.data_flow import DataFlow
from broker.simulated_broker import SimulatedBroker
from candles_queue.candles_queue import CandlesQueue
from candles_queue.simple_queue import SimpleQueue
from common.american_stock_exchange_calendar import AmericanStockExchangeCalendar
from db_storage.mongodb_storage import MongoDbStorage
from feed.feed import CsvFeed, Feed
from market_data.historical.tiingo_historical_data_handler import (
    TiingoHistoricalDataHandler,
)
from order_manager.order_management import OrderManager
from order_manager.position_sizer import PositionSizer
from settings import DATABASE_NAME, DATABASE_URL
from strategy.strategies.reactive_sma_crossover_strategy import (
    ReactiveSmaCrossoverStrategy,
)


def test_reactive_sma_crossover_strategy():
    symbols = ["AAPL"]
    candles_queue: CandlesQueue = SimpleQueue("candles")

    feed: Feed = CsvFeed({"AAPL": "test/data/aapl_candles_one_day.csv"}, candles_queue)

    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
    #     print(feed.candle_dataframe)
    # feed.candle_dataframe["MA65"] = feed.candle_dataframe["close"].rolling(65).mean()
    # feed.candle_dataframe["MA9"] = feed.candle_dataframe["close"].rolling(9).mean()
    #
    # fig, ax = plt.subplots()
    # plt.xticks(rotation=45)
    # plt.xlabel("Date")
    # plt.ylabel("Price")
    # plt.title("EUR/USD")
    #
    # feed.candle_dataframe["open"]
    # feed.candle_dataframe[["open", "high", "low", "close"]] = feed.candle_dataframe[
    #     ["open", "high", "low", "close"]
    # ].apply(pd.to_numeric)
    #
    # # plot the candlesticks
    # fig = go.Figure(
    #     data=[
    #         go.Candlestick(
    #             x=feed.candle_dataframe.index,
    #             open=feed.candle_dataframe.open,
    #             high=feed.candle_dataframe.high,
    #             low=feed.candle_dataframe.low,
    #             close=feed.candle_dataframe.close,
    #         ),
    #         go.Scatter(
    #             x=feed.candle_dataframe.index,
    #             y=feed.candle_dataframe.MA9,
    #             line=dict(color="orange", width=1),
    #         ),
    #         go.Scatter(
    #             x=feed.candle_dataframe.index,
    #             y=feed.candle_dataframe.MA65,
    #             line=dict(color="green", width=1),
    #         ),
    #     ]
    # )
    # fig.show()

    db_storage = MongoDbStorage(DATABASE_NAME, DATABASE_URL)
    db_storage.clean_all_orders()
    db_storage.clean_all_candles()

    strategies = [ReactiveSmaCrossoverStrategy]
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz="UTC")
    market_cal = AmericanStockExchangeCalendar()
    broker = SimulatedBroker(market_cal, start_timestamp=start_timestamp, initial_funds=Decimal("10000"))
    broker.subscribe_funds_to_portfolio(Decimal("10000"))
    position_sizer = PositionSizer()
    order_manager = OrderManager(broker, position_sizer)
    data_consumer = DataConsumer(symbols, candles_queue, db_storage, order_manager, strategies)

    data_flow = DataFlow(feed, data_consumer)
    data_flow.start()

    #assert data_consumer.broker.get_portfolio_cash_balance() == Decimal("10001.565")
