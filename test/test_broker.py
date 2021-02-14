import queue
from decimal import Decimal
from unittest.mock import PropertyMock, call, patch

from broker.broker import Broker
from common.clock import SimulatedClock
from models.enums import Action, Direction, OrderStatus
from models.multiple_order import MultipleOrder, SequentialOrder
from models.order import Order
from portfolio.portfolio import Portfolio


def test_init():
    clock = SimulatedClock()
    base_currency = "EUR"
    supported_currencies = ["USD", "EUR"]
    initial_funds = Decimal("10000")

    broker = Broker(
        clock=clock,
        base_currency=base_currency,
        supported_currencies=supported_currencies,
    )
    broker.cash_balances[base_currency] = initial_funds
    port = Portfolio(currency=base_currency)
    assert broker.base_currency == base_currency
    assert broker.supported_currencies == supported_currencies
    assert broker.cash_balances == {"EUR": Decimal("10000"), "USD": Decimal("0.0")}
    assert type(broker.open_orders) == queue.Queue
    assert broker.open_orders.qsize() == 0
    assert broker.last_prices == {}
    assert broker.portfolio == port


@patch("portfolio.portfolio.Portfolio.total_market_value", new_callable=PropertyMock)
def test_get_portfolio_total_market_value(total_market_value_mocked):
    clock = SimulatedClock()
    base_currency = "EUR"
    broker = Broker(clock=clock, base_currency=base_currency)
    broker.get_portfolio_total_market_value()
    total_market_value_mocked_calls = [call()]
    total_market_value_mocked.assert_has_calls(total_market_value_mocked_calls)


@patch("portfolio.portfolio.Portfolio.total_equity", new_callable=PropertyMock)
def test_get_portfolio_total_equity(total_equity_mocked):
    clock = SimulatedClock()
    base_currency = "EUR"
    broker = Broker(clock=clock, base_currency=base_currency)
    broker.get_portfolio_total_equity()
    total_equity_mocked_calls = [call()]
    total_equity_mocked.assert_has_calls(total_equity_mocked_calls)


@patch("portfolio.portfolio.Portfolio.portfolio_to_dict")
def test_get_portfolio_as_dict(portfolio_to_dict_mocked):
    clock = SimulatedClock()
    base_currency = "EUR"
    broker = Broker(clock=clock, base_currency=base_currency)
    broker.get_portfolio_as_dict()
    portfolio_to_dict_mocked_calls = [call()]
    portfolio_to_dict_mocked.assert_has_calls(portfolio_to_dict_mocked_calls)


def test_get_portfolio_cash_balance():
    clock = SimulatedClock()
    base_currency = "EUR"
    initial_funds = Decimal("10000")
    broker = Broker(clock=clock, base_currency=base_currency)
    broker.cash_balances[base_currency] = initial_funds
    portfolio_cash = Decimal("7000")
    broker.subscribe_funds_to_portfolio(amount=portfolio_cash)
    assert broker.get_portfolio_cash_balance() == portfolio_cash


def test_subscribe_funds_to_portfolio():
    clock = SimulatedClock()
    base_currency = "EUR"
    initial_funds = Decimal("10000")
    broker = Broker(clock=clock, base_currency=base_currency)
    broker.cash_balances[base_currency] = initial_funds
    portfolio_cash = Decimal("7000")
    broker.subscribe_funds_to_portfolio(amount=portfolio_cash)
    assert broker.get_portfolio_cash_balance() == portfolio_cash


def test_withdraw_funds_from_portfolio():
    clock = SimulatedClock()
    base_currency = "EUR"
    initial_funds = Decimal("10000")
    broker = Broker(clock=clock, base_currency=base_currency)
    broker.cash_balances[base_currency] = initial_funds
    subscription_cash = Decimal("7000")
    withdrawal_cash = Decimal("4000")
    remaining_cash = Decimal("3000")
    broker.subscribe_funds_to_portfolio(amount=subscription_cash)
    broker.withdraw_funds_from_portfolio(amount=withdrawal_cash)
    assert broker.get_portfolio_cash_balance() == remaining_cash


def test_submit_order_single_order():
    clock = SimulatedClock()
    base_currency = "EUR"
    initial_funds = Decimal("10000")
    broker = Broker(clock=clock, base_currency=base_currency)
    broker.cash_balances[base_currency] = initial_funds
    broker.subscribe_funds_to_portfolio(Decimal("10000.0"))
    symbol1 = "AAA"
    order1 = Order(
        symbol=symbol1,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        clock=clock,
    )
    broker.submit_order(order1)
    assert broker.open_orders.qsize() == 1
    assert broker.open_orders.get() == order1
    assert order1.status == OrderStatus.SUBMITTED


def test_submit_order_multiple_order():
    clock = SimulatedClock()
    base_currency = "EUR"
    initial_funds = Decimal("10000")
    broker = Broker(clock=clock, base_currency=base_currency)
    broker.cash_balances[base_currency] = initial_funds
    broker.subscribe_funds_to_portfolio(Decimal("10000.0"))
    symbol1 = "AAA"
    order1 = Order(
        symbol=symbol1,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        clock=clock,
    )
    symbol2 = "BBB"
    order2 = Order(
        symbol=symbol2,
        action=Action.BUY,
        direction=Direction.LONG,
        size=150,
        signal_id="1",
        clock=clock,
    )
    orders = [order1, order2]
    multiple_order = MultipleOrder(orders=orders)
    broker.submit_order(multiple_order)
    assert broker.open_orders.qsize() == len(orders)
    assert broker.open_orders.get() == order1
    assert order1.status == OrderStatus.SUBMITTED
    assert broker.open_orders.get() == order2
    assert order2.status == OrderStatus.SUBMITTED


def test_submit_order_sequential_order():
    clock = SimulatedClock()
    base_currency = "EUR"
    initial_funds = Decimal("10000")
    broker = Broker(clock=clock, base_currency=base_currency)
    broker.cash_balances[base_currency] = initial_funds
    broker.subscribe_funds_to_portfolio(Decimal("10000.0"))
    symbol1 = "AAA"
    order1 = Order(
        symbol=symbol1,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        clock=clock,
    )
    symbol2 = "BBB"
    order2 = Order(
        symbol=symbol2,
        action=Action.BUY,
        direction=Direction.LONG,
        size=150,
        signal_id="1",
        clock=clock,
    )
    orders = [order1, order2]
    sequential_order = SequentialOrder(orders=orders)
    broker.submit_order(sequential_order)
    assert broker.open_orders.qsize() == 2
    assert broker.open_orders.get() == order1
    assert broker.open_orders.get() == order2
    assert order1.status == OrderStatus.SUBMITTED
