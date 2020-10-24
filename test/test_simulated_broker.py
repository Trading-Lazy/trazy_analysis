import queue
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest
import pytz

from broker.common import DEFAULT_PORTFOLIO_ID, DEFAULT_PORTFOLIO_NAME
from common.exchange_calendar_euronext import EuronextExchangeCalendar
from indicators.indicators import RollingWindow
from models.candle import Candle
from models.enums import Action, Direction, OrderType
from models.order import Order
from portfolio.portfolio import Portfolio
from broker.simulated_broker import SimulatedBroker
from broker.fixed_fee_model import FixedFeeModel
from position.transaction import Transaction


class OrderMock(object):
    def __init__(
        self,
        symbol,
        size,
        action=Action.BUY,
        direction=Direction.LONG,
        order_type=OrderType.MARKET_ORDER,
        order_id=None,
    ):
        self.symbol = symbol
        self.size = size
        self.action = action
        self.direction = direction
        self.order_type = order_type
        self.order_id = 1 if order_id is None else order_id


class AssetMock(object):
    def __init__(self, name, symbol):
        self.name = name
        self.symbol = symbol


def test_initial_settings_for_default_simulated_broker():
    """
    Tests that the SimulatedBroker settings are set
    correctly for default settings.
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    market_cal = EuronextExchangeCalendar()

    # Test a default SimulatedBroker
    sb1 = SimulatedBroker(market_cal, start_timestamp)

    assert sb1.start_timestamp == start_timestamp
    assert sb1.current_timestamp == start_timestamp
    assert sb1.market_cal == market_cal
    assert sb1.account_id is None
    assert sb1.base_currency == "USD"
    assert sb1.initial_funds == Decimal("0.0")
    assert type(sb1.fee_model) == FixedFeeModel

    tcb1 = {"EUR": Decimal("0.0"), "USD": Decimal("0.0")}
    default_portfolio1 = Portfolio(
        start_timestamp,
        currency=sb1.base_currency,
        portfolio_id=DEFAULT_PORTFOLIO_ID,
        name=DEFAULT_PORTFOLIO_NAME,
    )

    assert sb1.cash_balances == tcb1
    assert sb1.portfolios == {"default": default_portfolio1}
    assert list(sb1.open_orders.keys()) == ["default"]
    open_orders_queue1 = sb1.open_orders["default"]
    assert open_orders_queue1.qsize() == 0

    # Test a SimulatedBroker with some parameters set
    sb2 = SimulatedBroker(market_cal, account_id="ACCT1234", start_timestamp=start_timestamp, base_currency="EUR",
                          initial_funds=1e6, fee_model=FixedFeeModel())

    assert sb2.start_timestamp == start_timestamp
    assert sb2.current_timestamp == start_timestamp
    assert sb2.market_cal == market_cal
    assert sb2.account_id == "ACCT1234"
    assert sb2.base_currency == "EUR"
    assert sb2.initial_funds == 1e6
    assert type(sb2.fee_model) == FixedFeeModel

    tcb2 = {"EUR": 1000000.0, "USD": Decimal("0.0")}
    default_portfolio2 = Portfolio(
        start_timestamp,
        currency=sb2.base_currency,
        portfolio_id=DEFAULT_PORTFOLIO_ID,
        name=DEFAULT_PORTFOLIO_NAME,
    )

    assert sb2.cash_balances == tcb2
    assert sb2.portfolios == {"default": default_portfolio2}
    assert list(sb2.open_orders.keys()) == ["default"]
    open_orders_queue2 = sb2.open_orders["default"]
    assert open_orders_queue2.qsize() == 0


def test_bad_set_base_currency():
    """
    Checks _set_base_currency raises ValueError
    if a non-supported currency is attempted to be
    set as the base currency.
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    market_cal = EuronextExchangeCalendar()

    with pytest.raises(ValueError):
        SimulatedBroker(market_cal, start_timestamp, base_currency="XYZ")


def test_good_set_base_currency():
    """
    Checks _set_base_currency sets the currency
    correctly if it is supported by QSTrader.
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    market_cal = EuronextExchangeCalendar()

    sb = SimulatedBroker(market_cal, start_timestamp, base_currency="EUR")
    assert sb.base_currency == "EUR"


def test_bad_set_initial_funds():
    """
    Checks _set_initial_funds raises ValueError
    if initial funds amount is negative.
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    market_cal = EuronextExchangeCalendar()

    with pytest.raises(ValueError):
        SimulatedBroker(market_cal, start_timestamp, initial_funds=Decimal("-56.34"))


def test_good_set_initial_funds():
    """
    Checks _set_initial_funds sets the initial funds
    correctly if it is a positive floating point value.
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    market_cal = EuronextExchangeCalendar()

    sb = SimulatedBroker(market_cal, start_timestamp, initial_funds=1e4)
    assert sb._set_initial_funds(1e4) == 1e4


def test_all_cases_of_set_broker_commission():
    """
    Tests that _set_broker_commission correctly sets the
    appropriate broker commission model depending upon
    user choice.
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    market_cal = EuronextExchangeCalendar()

    # Broker commission is None
    sb1 = SimulatedBroker(market_cal, start_timestamp)
    assert sb1.fee_model.__class__.__name__ == "FixedFeeModel"

    # Broker commission is specified as a subclass
    # of FeeModel abstract base class
    bc2 = FixedFeeModel()
    sb2 = SimulatedBroker(market_cal, start_timestamp, fee_model=bc2)
    assert sb2.fee_model.__class__.__name__ == "FixedFeeModel"

    # FeeModel is mis-specified and thus
    # raises a TypeError
    with pytest.raises(TypeError):
        SimulatedBroker(market_cal, start_timestamp, fee_model="bad_fee_model")


def test_set_cash_balances():
    """
    Checks _set_cash_balances for zero and non-zero
    initial_funds.
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    market_cal = EuronextExchangeCalendar()

    # Zero initial funds
    sb1 = SimulatedBroker(market_cal, start_timestamp, initial_funds=Decimal("0.0"))
    tcb1 = {"EUR": Decimal("0.0"), "USD": Decimal("0.0")}
    assert sb1._set_cash_balances() == tcb1

    # Non-zero initial funds
    sb2 = SimulatedBroker(market_cal, start_timestamp, initial_funds=Decimal("12345.0"))
    tcb2 = {"EUR": Decimal("0.0"), "USD": Decimal("12345.0")}
    assert sb2._set_cash_balances() == tcb2


def test_set_initial_portfolios():
    """
    Check _set_initial_portfolios method for return
    of an empty dictionary.
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    market_cal = EuronextExchangeCalendar()

    sb = SimulatedBroker(market_cal, start_timestamp)
    assert sb._set_initial_portfolios() == {}


def test_set_initial_open_orders():
    """
    Check _set_initial_open_orders method for return
    of an empty dictionary.
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    market_cal = EuronextExchangeCalendar()

    sb = SimulatedBroker(market_cal, start_timestamp)
    assert sb._set_initial_open_orders() == {}


def test_subscribe_funds_to_account():
    """
    Tests subscribe_funds_to_account method for:
    * Raising ValueError with negative amount
    * Correctly setting cash_balances for a positive amount
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    market_cal = EuronextExchangeCalendar()

    sb = SimulatedBroker(market_cal, start_timestamp)

    # Raising ValueError with negative amount
    with pytest.raises(ValueError):
        sb.subscribe_funds_to_account(Decimal("-4306.23"))

    # Correctly setting cash_balances for a positive amount
    sb.subscribe_funds_to_account(Decimal("165303.23"))
    assert sb.cash_balances[sb.base_currency] == Decimal("165303.23")


def test_withdraw_funds_from_account():
    """
    Tests withdraw_funds_from_account method for:
    * Raising ValueError with negative amount
    * Raising ValueError for lack of cash
    * Correctly setting cash_balances for positive amount
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    market_cal = EuronextExchangeCalendar()

    sb = SimulatedBroker(market_cal, start_timestamp, initial_funds=1e6)

    # Raising ValueError with negative amount
    with pytest.raises(ValueError):
        sb.withdraw_funds_from_account(Decimal("-4306.23"))

    # Raising ValueError for lack of cash
    with pytest.raises(ValueError):
        sb.withdraw_funds_from_account(2e6)

    # Correctly setting cash_balances for a positive amount
    sb.withdraw_funds_from_account(3e5)
    assert sb.cash_balances[sb.base_currency] == 7e5


def test_get_account_cash_balance():
    """
    Tests get_account_cash_balance method for:
    * If currency is None, return the cash_balances
    * If the currency code isn't in the cash_balances
    dictionary, then raise ValueError
    * Otherwise, return the appropriate cash balance
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    market_cal = EuronextExchangeCalendar()

    sb = SimulatedBroker(market_cal, start_timestamp, initial_funds=Decimal("1000.0"))

    # If currency is None, return the cash balances
    sbcb1 = sb.get_account_cash_balance()
    tcb1 = {"EUR": Decimal("0.0"), "USD": Decimal("1000.0")}
    assert sbcb1 == tcb1

    # If the currency code isn't in the cash_balances
    # dictionary, then raise ValueError
    with pytest.raises(ValueError):
        sb.get_account_cash_balance(currency="XYZ")

    # Otherwise, return appropriate cash balance
    assert sb.get_account_cash_balance(currency="USD") == Decimal("1000.0")
    assert sb.get_account_cash_balance(currency="EUR") == Decimal("0.0")


def test_get_account_total_market_value():
    """
    Tests get_account_total_market_value method for:
    * The correct market values after cash is subscribed.
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    market_cal = EuronextExchangeCalendar()

    sb = SimulatedBroker(market_cal, start_timestamp)

    # Subscribe all necessary funds and create portfolios
    sb.subscribe_funds_to_account(Decimal("300000.0"))
    sb.create_portfolio(portfolio_id="1", name="My Portfolio #1")
    sb.create_portfolio(portfolio_id="2", name="My Portfolio #1")
    sb.create_portfolio(portfolio_id="3", name="My Portfolio #1")
    sb.subscribe_funds_to_portfolio(Decimal("100000.0"), "1")
    sb.subscribe_funds_to_portfolio(Decimal("100000.0"), "2")
    sb.subscribe_funds_to_portfolio(Decimal("100000.0"), "3")

    symbol1 = "AAA"
    order1 = Order(generation_time=None)
    candle1 = Candle(
        symbol=symbol1,
        open=Decimal("567.0"),
        high=Decimal("567.0"),
        low=Decimal("567.0"),
        close=Decimal("567.0"),
        volume=100,
    )
    sb.submit_order(order1, "1")
    sb.submit_order(order1, "3")
    RollingWindow(symbol1).push(candle1)
    sb.update(start_timestamp)

    symbol2 = "BBB"
    order2 = Order(generation_time=None)
    candle2 = Candle(
        symbol=symbol1,
        open=Decimal("123.0"),
        high=Decimal("123.0"),
        low=Decimal("123.0"),
        close=Decimal("123.0"),
        volume=100,
    )
    sb.submit_order(order2, "2")
    sb.submit_order(order2, "3")
    RollingWindow(symbol2).push(candle2)
    sb.update(start_timestamp)

    # Check that the market value is correct
    res_market_value = sb.get_account_total_market_value()
    test_market_value = {
        "1": Decimal("56700.0"),
        "2": Decimal("12300.0"),
        "3": Decimal("69000.0"),
        "default": Decimal("0.0"),
        "master": Decimal("138000.0"),
    }
    assert res_market_value == test_market_value


def test_get_account_total_equity():
    """
    Tests get_account_total_equity method for:
    * The correct market values after cash is subscribed.
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    market_cal = EuronextExchangeCalendar()

    sb = SimulatedBroker(market_cal, start_timestamp)

    # Subscribe all necessary funds and create portfolios
    sb.subscribe_funds_to_account(Decimal("300000.0"))
    sb.create_portfolio(portfolio_id="1", name="My Portfolio #1")
    sb.create_portfolio(portfolio_id="2", name="My Portfolio #1")
    sb.create_portfolio(portfolio_id="3", name="My Portfolio #1")
    sb.subscribe_funds_to_portfolio(Decimal("100000.0"), "1")
    sb.subscribe_funds_to_portfolio(Decimal("100000.0"), "2")
    sb.subscribe_funds_to_portfolio(Decimal("100000.0"), "3")

    # Check that the market value is correct
    res_equity = sb.get_account_total_equity()
    test_equity = {
        "1": Decimal("100000.0"),
        "2": Decimal("100000.0"),
        "3": Decimal("100000.0"),
        "default": Decimal("0.0"),
        "master": Decimal("300000.0"),
    }
    assert res_equity == test_equity


def test_create_portfolio():
    """
    Tests create_portfolio method for:
    * If portfolio_id already in the dictionary keys,
    raise ValueError
    * If it isn't, check that they portfolio and open
    orders dictionary was created correctly.
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    market_cal = EuronextExchangeCalendar()

    sb = SimulatedBroker(market_cal, start_timestamp)

    # If portfolio_id isn't in the dictionary, then check it
    # was created correctly, along with the orders dictionary
    sb.create_portfolio(portfolio_id="1234", name="My Portfolio")
    assert "1234" in sb.portfolios
    assert isinstance(sb.portfolios["1234"], Portfolio)
    assert "1234" in sb.open_orders
    assert isinstance(sb.open_orders["1234"], queue.Queue)

    # If portfolio is already in the dictionary
    # then raise ValueError
    with pytest.raises(ValueError):
        sb.create_portfolio(portfolio_id="1234", name="My Portfolio")


def test_list_all_portfolio():
    """
    Tests list_all_portfolios method for:
    * If empty portfolio dictionary, return empty list
    * If non-empty, return sorted list via the portfolio IDs
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    market_cal = EuronextExchangeCalendar()

    sb = SimulatedBroker(market_cal, start_timestamp)

    # If empty portfolio dictionary, return empty list
    default_portfolio = Portfolio(
        start_timestamp,
        currency=sb.base_currency,
        portfolio_id=DEFAULT_PORTFOLIO_ID,
        name=DEFAULT_PORTFOLIO_NAME,
    )
    assert sb.list_all_portfolios() == [default_portfolio]

    # If non-empty, return sorted list via the portfolio IDs
    sb.create_portfolio(portfolio_id="1234", name="My Portfolio #1")
    sb.create_portfolio(portfolio_id="z154", name="My Portfolio #2")
    sb.create_portfolio(portfolio_id="abcd", name="My Portfolio #3")

    res_ports = sorted([p.portfolio_id for p in sb.list_all_portfolios()])
    test_ports = ["1234", "abcd", "default", "z154"]
    assert res_ports == test_ports


def test_subscribe_funds_to_portfolio():
    """
    Tests subscribe_funds_to_portfolio method for:
    * Raising ValueError with negative amount
    * Raising ValueError if portfolio does not exist
    * Correctly setting cash_balances for a positive amount
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    market_cal = EuronextExchangeCalendar()

    sb = SimulatedBroker(market_cal, start_timestamp)

    # Raising ValueError with negative amount
    with pytest.raises(ValueError):
        sb.subscribe_funds_to_portfolio(Decimal("-4306.23"), "1234")

    # Raising KeyError if portfolio doesn't exist
    with pytest.raises(KeyError):
        sb.subscribe_funds_to_portfolio(Decimal("5432.12"), "1234")

    # Add in cash balance to the account
    sb.create_portfolio(portfolio_id="1234", name="My Portfolio #1")
    sb.subscribe_funds_to_account(Decimal("165303.23"))

    # Raising ValueError if not enough cash
    with pytest.raises(ValueError):
        sb.subscribe_funds_to_portfolio(Decimal("200000.00"), "1234")

    # If everything else worked, check balances are correct
    sb.subscribe_funds_to_portfolio(Decimal("100000.00"), "1234")
    assert sb.cash_balances[sb.base_currency] == Decimal("65303.23")
    assert sb.portfolios["1234"].cash == Decimal("100000.00")


def test_withdraw_funds_from_portfolio():
    """
    Tests withdraw_funds_from_portfolio method for:
    * Raising ValueError with negative amount
    * Raising ValueError if portfolio does not exist
    * Raising ValueError for a lack of cash
    * Correctly setting cash_balances for a positive amount
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    market_cal = EuronextExchangeCalendar()

    sb = SimulatedBroker(market_cal, start_timestamp)

    # Raising ValueError with negative amount
    with pytest.raises(ValueError):
        sb.withdraw_funds_from_portfolio(Decimal("-4306.23"), "1234")

    # Raising KeyError if portfolio doesn't exist
    with pytest.raises(KeyError):
        sb.withdraw_funds_from_portfolio(Decimal("5432.12"), "1234")

    # Add in cash balance to the account
    sb.create_portfolio(portfolio_id="1234", name="My Portfolio #1")
    sb.subscribe_funds_to_account(Decimal("165303.23"))
    sb.subscribe_funds_to_portfolio(Decimal("100000.00"), "1234")

    # Raising ValueError if not enough cash
    with pytest.raises(ValueError):
        sb.withdraw_funds_from_portfolio(Decimal("200000.00"), "1234")

    # If everything else worked, check balances are correct
    sb.withdraw_funds_from_portfolio(Decimal("50000.00"), "1234")
    assert sb.cash_balances[sb.base_currency] == Decimal("115303.23")
    assert sb.portfolios["1234"].cash == Decimal("50000.00")


def test_get_portfolio_cash_balance():
    """
    Tests get_portfolio_cash_balance method for:
    * Raising ValueError if portfolio_id not in keys
    * Correctly obtaining the value after cash transfers
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    market_cal = EuronextExchangeCalendar()

    sb = SimulatedBroker(market_cal, start_timestamp)

    # Raising ValueError if portfolio_id not in keys
    with pytest.raises(ValueError):
        sb.get_portfolio_cash_balance("5678")

    # Create fund transfers and portfolio
    sb.create_portfolio(portfolio_id="1234", name="My Portfolio #1")
    sb.subscribe_funds_to_account(Decimal("175000.0"))
    sb.subscribe_funds_to_portfolio(Decimal("100000.00"), "1234")

    # Check correct values obtained after cash transfers
    assert sb.get_portfolio_cash_balance("1234") == Decimal("100000.0")


def test_get_portfolio_total_market_value():
    """
    Tests get_portfolio_total_market_value method for:
    * Raising ValueError if portfolio_id not in keys
    * Correctly obtaining the market value after cash transfers
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    market_cal = EuronextExchangeCalendar()

    sb = SimulatedBroker(market_cal, start_timestamp)

    # Raising KeyError if portfolio_id not in keys
    with pytest.raises(KeyError):
        sb.get_portfolio_total_market_value("5678")

    # Create fund transfers and portfolio
    sb.create_portfolio(portfolio_id="1234", name="My Portfolio #1")
    sb.subscribe_funds_to_account(Decimal("175000.0"))
    sb.subscribe_funds_to_portfolio(Decimal("100000.00"), "1234")

    # Check correct values obtained after cash transfers
    assert sb.get_portfolio_total_equity("1234") == Decimal("100000.0")


def test_submit_order():
    """
    Tests the execute_order method for:
    * Raises ValueError if no portfolio_id
    * Raises ValueError if bid/ask is (np.NaN, np.NaN)
    * Checks that bid/ask are correctly set dependent
    upon order direction
    * Checks that portfolio values are correct after
    carrying out a transaction
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)

    # Raising KeyError if portfolio_id not in keys
    market_cal = EuronextExchangeCalendar()

    sb = SimulatedBroker(market_cal, start_timestamp)
    symbol = "EQ:RDSB"
    size = 100
    order = OrderMock(symbol, size)
    with pytest.raises(KeyError):
        sb.submit_order(order, "1234")

    # Raises ValueError if bid/ask is (np.NaN, np.NaN)
    market_cal = EuronextExchangeCalendar()
    sbnp = SimulatedBroker(market_cal, start_timestamp)
    sbnp.create_portfolio(portfolio_id="1234", name="My Portfolio #1")
    size = 100
    order = OrderMock(symbol, size)
    with pytest.raises(ValueError):
        sbnp._execute_order(order, "1234")

    # Checks that bid/ask are correctly set dependent on
    # order direction

    # Positive direction
    market_cal = EuronextExchangeCalendar()
    candle = Candle(
        symbol=symbol,
        open=Decimal("53.47"),
        high=Decimal("53.47"),
        low=Decimal("53.47"),
        close=Decimal("53.47"),
        volume=1,
    )
    RollingWindow(symbol).push(candle)

    sbwp = SimulatedBroker(market_cal, start_timestamp)
    sbwp.create_portfolio(portfolio_id="1234", name="My Portfolio #1")
    sbwp.subscribe_funds_to_account(Decimal("175000.0"))
    sbwp.subscribe_funds_to_portfolio(Decimal("100000.00"), "1234")
    size = 1000
    order = OrderMock(symbol, size)
    sbwp.submit_order(order, "1234")
    sbwp.update(start_timestamp)

    port = sbwp.portfolios["1234"]
    assert port.cash == Decimal("46530.0")
    assert port.total_market_value == Decimal("53470.0")
    assert port.total_equity == Decimal("100000.0")
    assert port.pos_handler.positions[symbol][Direction.LONG].unrealised_pnl == Decimal(
        "0.0"
    )
    assert port.pos_handler.positions[symbol][Direction.LONG].market_value == Decimal(
        "53470.0"
    )
    assert port.pos_handler.positions[symbol][Direction.LONG].net_size == 1000

    # Negative direction
    market_cal = EuronextExchangeCalendar()
    sbwp = SimulatedBroker(market_cal, start_timestamp)
    sbwp.create_portfolio(portfolio_id="1234", name="My Portfolio #1")
    sbwp.subscribe_funds_to_account(Decimal("175000.0"))
    sbwp.subscribe_funds_to_portfolio(Decimal("100000.00"), "1234")
    size = 1000
    order = OrderMock(symbol, size, action=Action.SELL, direction=Direction.SHORT)
    sbwp.submit_order(order, "1234")
    sbwp.update(start_timestamp)

    port = sbwp.portfolios["1234"]
    assert port.cash == Decimal("153470.00")
    assert port.total_market_value == Decimal("-53470.00")
    assert port.total_equity == Decimal("100000.0")
    assert port.pos_handler.positions[symbol][Direction.SHORT].unrealised_pnl == Decimal(
        "0.0"
    )
    assert port.pos_handler.positions[symbol][Direction.SHORT].market_value == Decimal(
        "-53470.00"
    )
    assert port.pos_handler.positions[symbol][Direction.SHORT].net_size == -1000


def test_update_sets_correct_time():
    """
    Tests that the update method sets the current
    time correctly.
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    new_timestamp = pd.Timestamp("2017-10-07 08:00:00", tz=pytz.UTC)
    market_cal = EuronextExchangeCalendar()

    sb = SimulatedBroker(market_cal, start_timestamp)
    sb.update(new_timestamp)
    assert sb.current_timestamp == new_timestamp

def test_execute_market_order():
    pass

def test_execute_limit_order():
    pass

def test_execute_stop_order():
    pass

def test_execute_stop_limit_order():
    pass

def test_execute_trailing_stop_order():
    pass
