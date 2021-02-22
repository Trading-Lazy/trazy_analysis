from collections import deque
from datetime import datetime

import pytest

from bot.data_consumer import DataConsumer
from bot.data_flow import DataFlow
from broker.fixed_fee_model import FixedFeeModel
from broker.simulated_broker import SimulatedBroker
from candles_queue.candles_queue import CandlesQueue
from candles_queue.fake_queue import FakeQueue
from common.clock import SimulatedClock
from db_storage.mongodb_storage import MongoDbStorage
from feed.feed import CsvFeed, Feed
from indicators.indicators import IndicatorsManager
from models.candle import Candle
from models.enums import Action, Direction, OrderType
from models.order import Order
from order_manager.order_creator import OrderCreator
from order_manager.order_manager import OrderManager
from order_manager.position_sizer import PositionSizer
from portfolio.portfolio import Portfolio
from settings import DATABASE_NAME, DATABASE_URL
from strategy.strategies.reactive_sma_crossover_strategy import (
    ReactiveSmaCrossoverStrategy,
)
from test.tools.tools import not_raises


def test_initial_settings_for_default_simulated_broker():
    start_timestamp = datetime.strptime(
        "2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )

    # Test a default SimulatedBroker
    clock = SimulatedClock()
    sb1 = SimulatedBroker(clock)

    assert sb1.base_currency == "EUR"
    assert sb1.cash_balances["USD"] == 0.0
    assert type(sb1.fee_model) == FixedFeeModel

    tcb1 = {"EUR": float("0.0"), "USD": float("0.0")}
    portfolio1 = Portfolio(timestamp=start_timestamp, currency=sb1.base_currency)

    assert sb1.cash_balances == tcb1
    assert sb1.portfolio == portfolio1
    open_orders_queue1 = sb1.open_orders
    assert len(open_orders_queue1) == 0

    # Test a SimulatedBroker with some parameters set
    sb2 = SimulatedBroker(
        clock=clock, base_currency="EUR", initial_funds=1e6, fee_model=FixedFeeModel()
    )

    assert sb2.base_currency == "EUR"
    assert sb2.cash_balances["EUR"] == 1e6
    assert type(sb2.fee_model) == FixedFeeModel

    tcb2 = {"EUR": 1000000.0, "USD": float("0.0")}
    portfolio2 = Portfolio(
        timestamp=start_timestamp,
        currency=sb2.base_currency,
    )

    assert sb2.cash_balances == tcb2
    assert sb2.portfolio == portfolio2
    open_orders_queue2 = sb2.open_orders
    assert len(open_orders_queue2) == 0


def test_bad_set_base_currency():
    clock = SimulatedClock()
    with pytest.raises(ValueError):
        SimulatedBroker(clock=clock, base_currency="XYZ")


def test_good_set_base_currency():
    clock = SimulatedClock()
    sb = SimulatedBroker(clock=clock, base_currency="EUR")
    assert sb.base_currency == "EUR"


def test_bad_set_initial_funds():
    clock = SimulatedClock()
    with pytest.raises(ValueError):
        SimulatedBroker(clock=clock, initial_funds=float("-56.34"))


def test_good_set_initial_funds():
    clock = SimulatedClock()
    with not_raises(ValueError):
        sb = SimulatedBroker(clock=clock, initial_funds=1e4)
    sb.cash_balances["USD"] == 1e4


def test_all_cases_of_set_broker_commission():
    # Broker commission is None
    clock = SimulatedClock()
    sb1 = SimulatedBroker(clock=clock)
    assert sb1.fee_model.__class__.__name__ == "FixedFeeModel"

    # Broker commission is specified as a subclass
    # of FeeModel abstract base class
    bc2 = FixedFeeModel()
    sb2 = SimulatedBroker(clock=clock, fee_model=bc2)
    assert sb2.fee_model.__class__.__name__ == "FixedFeeModel"

    # FeeModel is mis-specified and thus
    # raises a TypeError
    with pytest.raises(TypeError):
        SimulatedBroker(clock=clock, fee_model="bad_fee_model")


def test_set_cash_balances():
    # Zero initial funds
    clock = SimulatedClock()
    sb1 = SimulatedBroker(clock=clock)
    tcb1 = {"EUR": 0.0, "USD": 0.0}
    sb1._set_cash_balances(initial_funds=0.0)
    assert sb1.cash_balances == tcb1

    # Non-zero initial funds
    sb2 = SimulatedBroker(clock=clock, initial_funds=12345.0)
    tcb2 = {"EUR": 12345.0, "USD": 0.0}
    sb2._set_cash_balances(initial_funds=12345.0)
    assert sb2.cash_balances == tcb2


def test_set_initial_open_orders():
    clock = SimulatedClock()
    sb = SimulatedBroker(clock=clock)
    assert type(sb._set_initial_open_orders()) == deque
    assert len(sb._set_initial_open_orders()) == 0


def test_subscribe_funds_to_account():
    clock = SimulatedClock()
    sb = SimulatedBroker(clock=clock)

    # Raising ValueError with negative amount
    with pytest.raises(ValueError):
        sb.subscribe_funds_to_account(-4306.23)

    # Correctly setting cash_balances for a positive amount
    sb.subscribe_funds_to_account(165303.23)
    assert sb.cash_balances[sb.base_currency] == 165303.23


def test_withdraw_funds_from_account():
    clock = SimulatedClock()
    sb = SimulatedBroker(clock=clock, initial_funds=1000000)

    # Raising ValueError with negative amount
    with pytest.raises(ValueError):
        sb.withdraw_funds_from_account(-4306.23)

    # Raising ValueError for lack of cash
    with pytest.raises(ValueError):
        sb.withdraw_funds_from_account(2000000)

    # Correctly setting cash_balances for a positive amount
    sb.withdraw_funds_from_account(300000)
    assert sb.cash_balances[sb.base_currency] == 700000


def test_get_account_cash_balance():
    clock = SimulatedClock()
    sb = SimulatedBroker(clock=clock, initial_funds=1000.0)

    # If currency is None, return the cash balances
    sbcb1 = sb.get_cash_balance()
    tcb1 = {"EUR": 1000.0, "USD": 0.0}
    assert sbcb1 == tcb1

    # If the currency code isn't in the cash_balances
    # dictionary, then raise ValueError
    with pytest.raises(ValueError):
        sb.get_cash_balance(currency="XYZ")

    # Otherwise, return appropriate cash balance
    assert sb.get_cash_balance(currency="EUR") == 1000.0
    assert sb.get_cash_balance(currency="USD") == 0.0


def test_get_account_total_market_value():
    clock = SimulatedClock()
    sb = SimulatedBroker(clock=clock)

    # Subscribe all necessary funds and create portfolios
    sb.subscribe_funds_to_account(300000.0)
    sb.subscribe_funds_to_portfolio(100000.0)

    symbol1 = "AAA"
    timestamp = datetime.strptime("2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    clock.update_time(symbol1, timestamp)
    order1 = Order(
        symbol=symbol1,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        clock=clock,
    )
    candle1 = Candle(
        symbol=symbol1,
        open=567.0,
        high=567.0,
        low=567.0,
        close=567.0,
        volume=100,
        timestamp=timestamp,
    )
    sb.submit_order(order1)
    sb.submit_order(order1)
    sb.update_price(candle1)
    sb.execute_open_orders()

    symbol2 = "BBB"
    clock.update_time(symbol2, timestamp)
    order2 = Order(
        symbol=symbol2,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        clock=clock,
    )
    candle2 = Candle(
        symbol=symbol2,
        open=123.0,
        high=123.0,
        low=123.0,
        close=123.0,
        volume=100,
    )
    sb.submit_order(order2)
    sb.submit_order(order2)
    sb.update_price(candle2)
    sb.execute_open_orders()

    # Check that the market value is correct
    res_market_value = sb.get_portfolio_total_market_value()
    test_market_value = 69000.0
    assert res_market_value == test_market_value


def test_create_portfolio():
    clock = SimulatedClock()
    sb = SimulatedBroker(clock=clock)

    # If portfolio_id isn't in the dictionary, then check it
    # was created correctly, along with the orders dictionary
    assert isinstance(sb.portfolio, Portfolio)
    assert isinstance(sb.open_orders, deque)


def test_subscribe_funds_to_portfolio():
    clock = SimulatedClock()
    sb = SimulatedBroker(clock=clock, initial_funds=0)

    # Raising ValueError with negative amount
    with pytest.raises(ValueError):
        sb.subscribe_funds_to_portfolio(-4306.23)

    # Add in cash balance to the account
    sb.subscribe_funds_to_account(165303.23)

    # Raising ValueError if not enough cash
    with pytest.raises(ValueError):
        sb.subscribe_funds_to_portfolio(200000.00)

    # If everything else worked, check balances are correct
    sb.subscribe_funds_to_portfolio(100000.00)
    assert sb.cash_balances[sb.base_currency] == pytest.approx(65303.23, 0.001)
    assert sb.portfolio.cash == 100000.00


def test_withdraw_funds_from_portfolio():
    clock = SimulatedClock()
    sb = SimulatedBroker(clock=clock)

    # Raising ValueError with negative amount
    with pytest.raises(ValueError):
        sb.withdraw_funds_from_portfolio(-4306.23)

    # Add in cash balance to the account
    sb.subscribe_funds_to_account(165303.23)
    sb.subscribe_funds_to_portfolio(100000.00)

    # Raising ValueError if not enough cash
    with pytest.raises(ValueError):
        sb.withdraw_funds_from_portfolio(200000.00)

    # If everything else worked, check balances are correct
    sb.withdraw_funds_from_portfolio(50000.00)
    assert sb.cash_balances[sb.base_currency] == pytest.approx(115303.23, 0.001)
    assert sb.portfolio.cash == 50000.00


def test_get_portfolio_cash_balance():
    clock = SimulatedClock()
    sb = SimulatedBroker(clock=clock)

    # Raising ValueError if portfolio_id not in keys
    assert sb.get_portfolio_cash_balance() == 0.0

    # Create fund transfers and portfolio
    sb.subscribe_funds_to_account(175000.0)
    sb.subscribe_funds_to_portfolio(100000.00)

    # Check correct values obtained after cash transfers
    assert sb.get_portfolio_cash_balance() == 100000.0


def test_get_portfolio_total_market_value():
    clock = SimulatedClock()
    sb = SimulatedBroker(clock=clock)

    # Raising KeyError if portfolio_id not in keys
    assert sb.get_portfolio_total_market_value() == 0.0

    # Create fund transfers and portfolio
    sb.subscribe_funds_to_account(175000.0)
    sb.subscribe_funds_to_portfolio(100000.00)

    # Check correct values obtained after cash transfers
    assert sb.get_portfolio_total_equity() == 100000.0


def test_submit_order():
    # Positive direction
    symbol = "EQ:RDSB"
    timestamp = datetime.strptime("2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    candle = Candle(
        symbol=symbol,
        open=53.47,
        high=53.47,
        low=53.47,
        close=53.47,
        volume=1,
        timestamp=timestamp,
    )

    clock = SimulatedClock()
    clock.update_time(symbol, timestamp)
    sbwp = SimulatedBroker(clock=clock)
    sbwp.subscribe_funds_to_account(175000.0)
    sbwp.subscribe_funds_to_portfolio(100000.00)
    sbwp.update_price(candle)
    size = 1000
    order = Order(
        symbol=symbol,
        action=Action.BUY,
        direction=Direction.LONG,
        size=size,
        signal_id="1",
        clock=clock,
    )
    sbwp.submit_order(order)
    sbwp.execute_open_orders()

    port = sbwp.portfolio
    assert port.cash == 46530.0
    assert port.total_market_value == 53470.0
    assert port.total_equity == 100000.0
    assert port.pos_handler.positions[symbol][Direction.LONG].unrealised_pnl == 0.0
    assert port.pos_handler.positions[symbol][Direction.LONG].market_value == 53470.0
    assert port.pos_handler.positions[symbol][Direction.LONG].net_size == 1000

    # Negative direction
    sbwp = SimulatedBroker(clock=clock)
    sbwp.subscribe_funds_to_account(175000.0)
    sbwp.subscribe_funds_to_portfolio(100000.00)
    sbwp.update_price(candle)
    size = 1000
    order = Order(
        symbol=symbol,
        action=Action.SELL,
        direction=Direction.SHORT,
        size=size,
        signal_id="1",
        clock=clock,
    )
    sbwp.submit_order(order)
    sbwp.execute_open_orders()

    port = sbwp.portfolio
    assert port.cash == 153470.00
    assert port.total_market_value == -53470.00
    assert port.total_equity == 100000.0
    assert port.pos_handler.positions[symbol][Direction.SHORT].unrealised_pnl == 0.0
    assert port.pos_handler.positions[symbol][Direction.SHORT].market_value == -53470.00
    assert port.pos_handler.positions[symbol][Direction.SHORT].net_size == -1000


def test_execute_market_order():
    clock = SimulatedClock()
    sb = SimulatedBroker(clock=clock)

    # Subscribe all necessary funds and create portfolios
    sb.subscribe_funds_to_account(300000.0)
    sb.subscribe_funds_to_portfolio(100000.0)

    symbol = "AAA"
    timestamp = datetime.strptime("2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    clock.update_time(symbol, timestamp)
    order = Order(
        symbol=symbol,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        clock=clock,
    )
    candle = Candle(
        symbol=symbol,
        open=567.0,
        high=567.0,
        low=567.0,
        close=567.0,
        volume=100,
        timestamp=timestamp,
    )
    sb.update_price(candle)
    sb.execute_market_order(order)

    # Check that the market value is correct
    res_market_value = sb.get_portfolio_total_market_value()
    test_market_value = 56700.0
    assert res_market_value == test_market_value

    # test negative cash balance
    sb.execute_market_order(order)
    assert sb.get_portfolio_cash_balance() == 43300.0


def test_execute_limit_order():
    symbols = ["AAPL"]
    candles_queue: CandlesQueue = FakeQueue("candles")

    feed: Feed = CsvFeed(
        {"AAPL": "test/data/aapl_candles_one_day_limit_order.csv"}, candles_queue
    )

    db_storage = MongoDbStorage(DATABASE_NAME, DATABASE_URL)

    strategies = [ReactiveSmaCrossoverStrategy]
    clock = SimulatedClock()
    broker = SimulatedBroker(clock, initial_funds=10000)
    broker.subscribe_funds_to_portfolio(10000)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(
        broker=broker,
        fixed_order_type=OrderType.LIMIT,
        limit_order_pct=0.0004,
    )
    order_manager = OrderManager(
        broker=broker, position_sizer=position_sizer, order_creator=order_creator
    )
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

    assert broker.get_portfolio_cash_balance() == pytest.approx(10012.845, 0.0001)


def test_execute_stop_order():
    symbols = ["AAPL"]
    candles_queue: CandlesQueue = FakeQueue("candles")

    feed: Feed = CsvFeed(
        {"AAPL": "test/data/aapl_candles_one_day_stop_order.csv"}, candles_queue
    )

    db_storage = MongoDbStorage(DATABASE_NAME, DATABASE_URL)

    strategies = [ReactiveSmaCrossoverStrategy]
    clock = SimulatedClock()
    broker = SimulatedBroker(clock, initial_funds=10000)
    broker.subscribe_funds_to_portfolio(10000)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(
        broker=broker, fixed_order_type=OrderType.STOP, stop_order_pct=0.0004
    )
    order_manager = OrderManager(
        broker=broker, position_sizer=position_sizer, order_creator=order_creator
    )
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

    assert broker.get_portfolio_cash_balance() == 9996.01


def test_execute_target_order():
    symbols = ["AAPL"]
    candles_queue: CandlesQueue = FakeQueue("candles")

    feed: Feed = CsvFeed(
        {"AAPL": "test/data/aapl_candles_one_day_target_order.csv"}, candles_queue
    )

    db_storage = MongoDbStorage(DATABASE_NAME, DATABASE_URL)

    strategies = [ReactiveSmaCrossoverStrategy]
    clock = SimulatedClock()
    broker = SimulatedBroker(clock, initial_funds=10000)
    broker.subscribe_funds_to_portfolio(10000)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(
        broker=broker,
        fixed_order_type=OrderType.TARGET,
        target_order_pct=0.0004,
    )
    order_manager = OrderManager(
        broker=broker, position_sizer=position_sizer, order_creator=order_creator
    )
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

    assert broker.get_portfolio_cash_balance() == pytest.approx(9998.74, 0.001)


def test_execute_trailing_stop_order():
    symbols = ["AAPL"]
    candles_queue: CandlesQueue = FakeQueue("candles")

    feed: Feed = CsvFeed(
        {"AAPL": "test/data/aapl_candles_one_day_trailing_stop_order.csv"},
        candles_queue=candles_queue,
    )

    db_storage = MongoDbStorage(DATABASE_NAME, DATABASE_URL)

    strategies = [ReactiveSmaCrossoverStrategy]
    clock = SimulatedClock()
    broker = SimulatedBroker(clock, initial_funds=10000)
    broker.subscribe_funds_to_portfolio(10000)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(
        broker=broker,
        fixed_order_type=OrderType.TRAILING_STOP,
        trailing_stop_order_pct=0.0004,
    )
    order_manager = OrderManager(
        broker=broker, position_sizer=position_sizer, order_creator=order_creator
    )
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

    assert broker.get_portfolio_cash_balance() == pytest.approx(10009.345, 0.0001)


def test_execute_cover_order():
    symbols = ["AAPL"]
    candles_queue: CandlesQueue = FakeQueue("candles")

    feed: Feed = CsvFeed(
        {"AAPL": "test/data/aapl_candles_one_day_cover_order.csv"}, candles_queue
    )

    db_storage = MongoDbStorage(DATABASE_NAME, DATABASE_URL)
    db_storage.clean_all_orders()
    db_storage.clean_all_candles()

    strategies = [ReactiveSmaCrossoverStrategy]
    clock = SimulatedClock()
    broker = SimulatedBroker(clock, initial_funds=10000)
    broker.subscribe_funds_to_portfolio(10000)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(broker=broker, with_cover=True)
    order_manager = OrderManager(
        broker=broker, position_sizer=position_sizer, order_creator=order_creator
    )
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

    assert broker.get_portfolio_cash_balance() == pytest.approx(10009.59, 0.001)


def test_execute_bracket_order():
    symbols = ["AAPL"]
    candles_queue: CandlesQueue = FakeQueue("candles")

    feed: Feed = CsvFeed(
        {"AAPL": "test/data/aapl_candles_one_day_cover_order.csv"}, candles_queue
    )

    db_storage = MongoDbStorage(DATABASE_NAME, DATABASE_URL)
    db_storage.clean_all_orders()
    db_storage.clean_all_candles()

    strategies = [ReactiveSmaCrossoverStrategy]
    clock = SimulatedClock()
    broker = SimulatedBroker(clock, initial_funds=10000)
    broker.subscribe_funds_to_portfolio(10000)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(broker=broker, with_bracket=True)
    order_manager = OrderManager(
        broker=broker, position_sizer=position_sizer, order_creator=order_creator
    )
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

    assert broker.get_portfolio_cash_balance() == 10007.175
