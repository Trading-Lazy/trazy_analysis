from collections import deque
from unittest.mock import PropertyMock, call, patch

from bot.event_loop import EventLoop
from broker.broker import Broker
from broker.broker_manager import BrokerManager
from broker.simulated_broker import SimulatedBroker
from common.clock import SimulatedClock
from feed.feed import CsvFeed, Feed
from indicators.indicators_manager import IndicatorsManager
from models.asset import Asset
from models.enums import Action, Direction, OrderStatus
from models.multiple_order import MultipleOrder, SequentialOrder
from models.order import Order
from order_manager.order_creator import OrderCreator
from order_manager.order_manager import OrderManager
from order_manager.position_sizer import PositionSizer
from portfolio.portfolio import Portfolio
from strategy.strategies.sma_crossover_strategy import (
    SmaCrossoverStrategy,
)

EXCHANGE = "IEX"


def test_init():
    clock = SimulatedClock()
    events = deque()
    base_currency = "EUR"
    supported_currencies = ["USD", "EUR"]
    initial_funds = 10000

    broker = Broker(
        clock=clock,
        events=events,
        base_currency=base_currency,
        supported_currencies=supported_currencies,
    )
    broker.cash_balances[base_currency] = initial_funds
    port = Portfolio(currency=base_currency)
    assert broker.base_currency == base_currency
    assert broker.supported_currencies == supported_currencies
    assert broker.cash_balances == {"EUR": float("10000"), "USD": float("0.0")}
    assert type(broker.open_orders) == deque
    assert len(broker.open_orders) == 0
    assert broker.last_prices == {}
    assert broker.portfolio == port


@patch("portfolio.portfolio.Portfolio.total_market_value", new_callable=PropertyMock)
def test_get_portfolio_total_market_value(total_market_value_mocked):
    clock = SimulatedClock()
    events = deque()
    base_currency = "EUR"
    broker = Broker(clock=clock, events=events, base_currency=base_currency)
    broker.get_portfolio_total_market_value()
    total_market_value_mocked_calls = [call()]
    total_market_value_mocked.assert_has_calls(total_market_value_mocked_calls)


@patch("portfolio.portfolio.Portfolio.total_equity", new_callable=PropertyMock)
def test_get_portfolio_total_equity(total_equity_mocked):
    clock = SimulatedClock()
    events = deque()
    base_currency = "EUR"
    broker = Broker(clock=clock, events=events, base_currency=base_currency)
    broker.get_portfolio_total_equity()
    total_equity_mocked_calls = [call()]
    total_equity_mocked.assert_has_calls(total_equity_mocked_calls)


@patch("portfolio.portfolio.Portfolio.portfolio_to_dict")
def test_get_portfolio_as_dict(portfolio_to_dict_mocked):
    clock = SimulatedClock()
    events = deque()
    base_currency = "EUR"
    broker = Broker(clock=clock, events=events, base_currency=base_currency)
    broker.get_portfolio_as_dict()
    portfolio_to_dict_mocked_calls = [call()]
    portfolio_to_dict_mocked.assert_has_calls(portfolio_to_dict_mocked_calls)


def test_get_portfolio_cash_balance():
    clock = SimulatedClock()
    events = deque()
    base_currency = "EUR"
    initial_funds = 10000
    broker = Broker(clock=clock, events=events, base_currency=base_currency)
    broker.cash_balances[base_currency] = initial_funds
    portfolio_cash = 7000
    broker.subscribe_funds_to_portfolio(amount=portfolio_cash)
    assert broker.get_portfolio_cash_balance() == portfolio_cash


def test_subscribe_funds_to_portfolio():
    clock = SimulatedClock()
    events = deque()
    base_currency = "EUR"
    initial_funds = 10000
    broker = Broker(clock=clock, events=events, base_currency=base_currency)
    broker.cash_balances[base_currency] = initial_funds
    portfolio_cash = 7000
    broker.subscribe_funds_to_portfolio(amount=portfolio_cash)
    assert broker.get_portfolio_cash_balance() == portfolio_cash


def test_withdraw_funds_from_portfolio():
    clock = SimulatedClock()
    events = deque()
    base_currency = "EUR"
    initial_funds = 10000
    broker = Broker(clock=clock, events=events, base_currency=base_currency)
    broker.cash_balances[base_currency] = initial_funds
    subscription_cash = 7000
    withdrawal_cash = 4000
    remaining_cash = 3000
    broker.subscribe_funds_to_portfolio(amount=subscription_cash)
    broker.withdraw_funds_from_portfolio(amount=withdrawal_cash)
    assert broker.get_portfolio_cash_balance() == remaining_cash


def test_submit_order_single_order():
    clock = SimulatedClock()
    events = deque()
    base_currency = "EUR"
    initial_funds = 10000
    broker = Broker(clock=clock, events=events, base_currency=base_currency)
    broker.cash_balances[base_currency] = initial_funds
    broker.subscribe_funds_to_portfolio(10000.0)
    symbol1 = "AAA"
    asset1 = Asset(symbol=symbol1, exchange="BINANCE")
    order1 = Order(
        asset=asset1,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        clock=clock,
    )
    broker.submit_order(order1)
    assert len(broker.open_orders) == 1
    assert broker.open_orders.popleft() == order1
    assert order1.status == OrderStatus.SUBMITTED


def test_submit_order_multiple_order():
    clock = SimulatedClock()
    events = deque()
    base_currency = "EUR"
    initial_funds = 10000
    broker = Broker(clock=clock, events=events, base_currency=base_currency)
    broker.cash_balances[base_currency] = initial_funds
    broker.subscribe_funds_to_portfolio(10000.0)
    symbol1 = "AAA"
    asset1 = Asset(symbol=symbol1, exchange="BINANCE")
    order1 = Order(
        asset=asset1,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        clock=clock,
    )
    symbol2 = "BBB"
    asset2 = Asset(symbol=symbol2, exchange="BINANCE")
    order2 = Order(
        asset=asset2,
        action=Action.BUY,
        direction=Direction.LONG,
        size=150,
        signal_id="1",
        clock=clock,
    )
    orders = [order1, order2]
    multiple_order = MultipleOrder(orders=orders)
    broker.submit_order(multiple_order)
    assert len(broker.open_orders) == len(orders)
    assert broker.open_orders.popleft() == order1
    assert order1.status == OrderStatus.SUBMITTED
    assert broker.open_orders.popleft() == order2
    assert order2.status == OrderStatus.SUBMITTED


def test_submit_order_sequential_order():
    clock = SimulatedClock()
    events = deque()
    base_currency = "EUR"
    initial_funds = 10000
    broker = Broker(clock=clock, events=events, base_currency=base_currency)
    broker.cash_balances[base_currency] = initial_funds
    broker.subscribe_funds_to_portfolio(10000.0)
    symbol1 = "AAA"
    asset1 = Asset(symbol=symbol1, exchange="BINANCE")
    order1 = Order(
        asset=asset1,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        clock=clock,
    )
    symbol2 = "BBB"
    asset2 = Asset(symbol=symbol2, exchange="BINANCE")
    order2 = Order(
        asset=asset2,
        action=Action.BUY,
        direction=Direction.LONG,
        size=150,
        signal_id="1",
        clock=clock,
    )
    orders = [order1, order2]
    sequential_order = SequentialOrder(orders=orders)
    broker.submit_order(sequential_order)
    assert len(broker.open_orders) == 2
    assert broker.open_orders.popleft() == order1
    assert broker.open_orders.popleft() == order2
    assert order1.status == OrderStatus.SUBMITTED


def test_close_all_open_positions_at_end_of_day():
    aapl_asset = Asset(symbol="AAPL", exchange=EXCHANGE)
    assets = [aapl_asset]
    events = deque()

    feed: Feed = CsvFeed(
        {aapl_asset: "test/data/aapl_candles_one_day_positions_opened_end_of_day.csv"},
        events,
    )

    strategies = {SmaCrossoverStrategy: [None]}
    clock = SimulatedClock()
    broker = SimulatedBroker(clock, events, initial_funds=10000.0)
    broker.subscribe_funds_to_portfolio(10000.0)
    broker_manager = BrokerManager(brokers={EXCHANGE: broker}, clock=clock)
    position_sizer = PositionSizer(broker_manager=broker_manager)
    order_creator = OrderCreator(broker_manager=broker_manager)
    order_manager = OrderManager(
        events=events,
        broker_manager=broker_manager,
        position_sizer=position_sizer,
        order_creator=order_creator,
    )
    indicators_manager = IndicatorsManager(initial_data=feed.candles)
    event_loop = EventLoop(
        events=events,
        assets=assets,
        feed=feed,
        order_manager=order_manager,
        strategies_parameters=strategies,
        indicators_manager=indicators_manager,
    )
    event_loop.loop()

    assert broker.get_portfolio_cash_balance() == 10016.415


def test_close_all_open_positions_at_end_of_feed_data():
    aapl_asset = Asset(symbol="AAPL", exchange=EXCHANGE)
    assets = [aapl_asset]
    events = deque()

    feed: Feed = CsvFeed(
        {
            aapl_asset: "test/data/aapl_candles_one_day_positions_opened_end_of_feed_data.csv"
        },
        events,
    )

    strategies = {SmaCrossoverStrategy: [None]}
    clock = SimulatedClock()
    broker = SimulatedBroker(clock, events, initial_funds=10000.0)
    broker.subscribe_funds_to_portfolio(10000.0)
    broker_manager = BrokerManager(brokers={EXCHANGE: broker}, clock=clock)
    position_sizer = PositionSizer(broker_manager=broker_manager)
    order_creator = OrderCreator(broker_manager=broker_manager)
    order_manager = OrderManager(
        events=events,
        broker_manager=broker_manager,
        position_sizer=position_sizer,
        order_creator=order_creator,
    )
    indicators_manager = IndicatorsManager(initial_data=feed.candles)
    event_loop = EventLoop(
        events=events,
        assets=assets,
        feed=feed,
        order_manager=order_manager,
        strategies_parameters=strategies,
        indicators_manager=indicators_manager,
    )
    event_loop.loop()

    assert broker.get_portfolio_cash_balance() == 10014.35
