from decimal import Decimal
from unittest.mock import Mock, call, patch

import pandas as pd
import pytest

from broker.simulated_broker import SimulatedBroker
from common.exchange_calendar_euronext import EuronextExchangeCalendar
from db_storage.mongodb_storage import MongoDbStorage
from models.order import Order
from models.candle import Candle
from models.enums import Action, Direction, OrderType
from settings import DATABASE_NAME
from simulator.simulation import Simulation
from strategy.strategies.buy_and_sell_long_strategy import BuyAndSellLongStrategy
from strategy.strategies.dumb_long_strategy import DumbLongStrategy

# disable logging into a file
from test.tools.tools import compare_orders_list

DB_STORAGE = MongoDbStorage(DATABASE_NAME)
SYMBOL = "IVV"
FUND = Decimal("10000")
START_TIMESTAMP = pd.Timestamp("2017-10-05 08:00:00", tz="UTC")
MARKET_CAL = EuronextExchangeCalendar()
BROKER = SimulatedBroker(MARKET_CAL, START_TIMESTAMP, initial_funds=FUND)
SIMULATION = Simulation(
    DumbLongStrategy(SYMBOL, DB_STORAGE, BROKER), DB_STORAGE, log=False
)

BUY_LONG_AMOUNT_OK = 5
SELL_LONG_AMOUNT_OK = 3
SELL_SHORT_AMOUNT_OK = 5
BUY_SHORT_AMOUNT_OK = 3

BUY_LONG_AMOUNT_KO = 3
SELL_LONG_AMOUNT_KO = 5
SELL_SHORT_AMOUNT_KO = 3
BUY_SHORT_AMOUNT_KO = 5

FUND = Decimal("1000")
COMMISSION = Decimal("0.001")
COST_ESTIMATE_BUY_OK = Decimal("300")
COST_ESTIMATE_SELL = Decimal("400")
COST_ESTIMATE_BUY_KO = Decimal("1500")
TOTAL_COST_ESTIMATE_OK = Decimal("800")
TOTAL_COST_ESTIMATE_KO = Decimal("1200")

UNIT_COST_ESTIMATE = Decimal("100")

STRATEGY_NAME = "strategy"
ORDERS = [
    Order(),
    Order(),
    Order(),
    Order(),
    Order(),
    Order(),
]

CANDLES = [
    Candle(
        symbol=SYMBOL,
        open=Decimal("94.1200"),
        high=Decimal("94.1500"),
        low=Decimal("94.0000"),
        close=Decimal("94.1300"),
        volume=7,
        timestamp=pd.Timestamp("2020-05-08 14:17:00", tz="UTC"),
    ),
    Candle(
        symbol=SYMBOL,
        open=Decimal("94.0700"),
        high=Decimal("94.1000"),
        low=Decimal("93.9500"),
        close=Decimal("94.0800"),
        volume=91,
        timestamp=pd.Timestamp("2020-05-08 14:24:00", tz="UTC"),
    ),
    Candle(
        symbol=SYMBOL,
        open=Decimal("94.0700"),
        high=Decimal("94.1000"),
        low=Decimal("93.9500"),
        close=Decimal("94.0800"),
        volume=0,
        timestamp=pd.Timestamp("2020-05-08 14:24:56", tz="UTC"),
    ),
    Candle(
        symbol=SYMBOL,
        open=Decimal("94.1700"),
        high=Decimal("94.1800"),
        low=Decimal("94.0500"),
        close=Decimal("94.1800"),
        volume=0,
        timestamp=pd.Timestamp("2020-05-08 14:35:00", tz="UTC"),
    ),
    Candle(
        symbol=SYMBOL,
        open=Decimal("94.1900"),
        high=Decimal("94.2200"),
        low=Decimal("94.0700"),
        close=Decimal("94.2000"),
        volume=0,
        timestamp=pd.Timestamp("2020-05-08 14:41:00", tz="UTC"),
    ),
    Candle(
        symbol=SYMBOL,
        open=Decimal("94.1900"),
        high=Decimal("94.2200"),
        low=Decimal("94.0700"),
        close=Decimal("94.2000"),
        volume=7,
        timestamp=pd.Timestamp("2020-05-08 14:41:58", tz="UTC"),
    ),
]


@pytest.fixture
def simulation_fixture():
    SIMULATION.reset()


def test_reset(simulation_fixture):
    SIMULATION.candles = CANDLES
    SIMULATION.cash = Decimal("200")
    SIMULATION.commission = Decimal("0.001")
    SIMULATION.portfolio_value = Decimal("100")
    SIMULATION.portfolio_value = Decimal("2000")
    SIMULATION.last_transaction_price = Decimal("100")
    SIMULATION.last_transaction_price = {
        Direction.LONG: {
            Action.BUY: Decimal("200"),
            Action.SELL: Decimal("201"),
        },
        Direction.SHORT: {
            Action.SELL: Decimal("202"),
            Action.BUY: Decimal("199"),
        },
    }
    SIMULATION.shares_amounts = {Direction.LONG: 17, Direction.SHORT: 13}

    SIMULATION.reset()

    assert SIMULATION.candles == None
    assert SIMULATION.cash == Decimal("0")
    assert SIMULATION.commission == Decimal("0")
    assert SIMULATION.portfolio_value == Decimal("0")
    assert SIMULATION.last_transaction_price == {
        Direction.LONG: {
            Action.BUY: Decimal("0"),
            Action.SELL: Decimal("0"),
        },
        Direction.SHORT: {
            Action.SELL: Decimal("0"),
            Action.BUY: Decimal("0"),
        },
    }
    assert SIMULATION.shares_amounts == {
        Direction.LONG: Decimal("0"),
        Direction.SHORT: Decimal("0"),
    }


def test_fund(simulation_fixture):
    SIMULATION.fund(1000)
    assert SIMULATION.cash == 1000
    SIMULATION.fund(1500)
    assert SIMULATION.cash == 2500


def test_get_orders(simulation_fixture):
    SIMULATION.fund(FUND)
    SIMULATION.candles = CANDLES
    SIMULATION.strategy = Mock()
    SIMULATION.strategy.generate_signal.side_effect = ORDERS
    orders = SIMULATION.get_orders()

    expected_orders = ORDERS
    assert compare_orders_list(orders, expected_orders)


def test_get_orders_one_none_action(simulation_fixture):
    SIMULATION.fund(FUND)
    SIMULATION.candles = CANDLES
    SIMULATION.strategy = Mock()
    SIMULATION.strategy.generate_signal.side_effect = [
        ORDERS[0],
        ORDERS[1],
        ORDERS[2],
        ORDERS[3],
        ORDERS[4],
        None,
    ]
    orders = SIMULATION.get_orders()

    expected_orders = [ORDERS[0], ORDERS[1], ORDERS[2], ORDERS[3], ORDERS[4]]
    assert compare_orders_list(orders, expected_orders)


def test_is_closed_position(simulation_fixture):
    assert SIMULATION.is_closed_position(Direction.LONG, Action.BUY) == False
    assert SIMULATION.is_closed_position(Direction.LONG, Action.SELL) == True
    assert SIMULATION.is_closed_position(Direction.SHORT, Action.SELL) == False
    assert SIMULATION.is_closed_position(Direction.SHORT, Action.BUY) == True


def test_validate_shares_amounts_buy_long_ok(simulation_fixture):
    SIMULATION.validate_shares_amounts(Direction.LONG, Action.BUY, BUY_LONG_AMOUNT_OK)


def test_validate_shares_amounts_sell_long_ok(simulation_fixture):
    SIMULATION.shares_amounts[Direction.LONG] = BUY_LONG_AMOUNT_OK
    SIMULATION.validate_shares_amounts(Direction.LONG, Action.SELL, SELL_LONG_AMOUNT_OK)


def test_validate_shares_amounts_sell_long_ko(simulation_fixture):
    SIMULATION.shares_amounts[Direction.LONG] = BUY_LONG_AMOUNT_KO
    with pytest.raises(Exception):
        SIMULATION.validate_shares_amounts(
            Direction.LONG, Action.SELL, SELL_LONG_AMOUNT_KO
        )


def test_validate_shares_amounts_sell_short_ok(simulation_fixture):
    SIMULATION.validate_shares_amounts(
        Direction.SHORT, Action.SELL, SELL_SHORT_AMOUNT_OK
    )


def test_validate_shares_amounts_buy_short_ok(simulation_fixture):
    SIMULATION.shares_amounts[Direction.SHORT] = SELL_SHORT_AMOUNT_OK
    SIMULATION.validate_shares_amounts(Direction.SHORT, Action.BUY, BUY_SHORT_AMOUNT_OK)


def test_validate_shares_amounts_buy_short_ko(simulation_fixture):
    SIMULATION.shares_amounts[Direction.SHORT] = SELL_SHORT_AMOUNT_KO
    with pytest.raises(Exception):
        SIMULATION.validate_shares_amounts(
            Direction.SHORT, Action.BUY, BUY_SHORT_AMOUNT_KO
        )


def test_validate_cash_buy_ok(simulation_fixture):
    SIMULATION.fund(FUND)
    SIMULATION.validate_cash(Action.BUY, TOTAL_COST_ESTIMATE_OK)


def test_validate_cash_buy_ko(simulation_fixture):
    SIMULATION.fund(FUND)
    with pytest.raises(Exception):
        SIMULATION.validate_cash(Action.BUY, TOTAL_COST_ESTIMATE_KO)


def test_validate_cash_sell_ok(simulation_fixture):
    SIMULATION.fund(FUND)
    SIMULATION.validate_cash(Action.SELL, TOTAL_COST_ESTIMATE_OK)


def test_validate_transaction_buy_long_ok(simulation_fixture):
    SIMULATION.fund(FUND)
    SIMULATION.validate_transaction(
        Direction.LONG, Action.BUY, BUY_LONG_AMOUNT_OK, TOTAL_COST_ESTIMATE_OK
    )


def test_validate_transaction_buy_long_ko(simulation_fixture):
    SIMULATION.fund(FUND)
    with pytest.raises(Exception):
        SIMULATION.validate_transaction(
            Direction.LONG,
            Action.BUY,
            BUY_LONG_AMOUNT_OK,
            TOTAL_COST_ESTIMATE_KO,
        )


def test_validate_transaction_sell_long_ok(simulation_fixture):
    SIMULATION.shares_amounts[Direction.LONG] = BUY_LONG_AMOUNT_OK
    SIMULATION.validate_transaction(
        Direction.LONG, Action.SELL, SELL_LONG_AMOUNT_OK, TOTAL_COST_ESTIMATE_OK
    )


def test_validate_transaction_sell_long_ko(simulation_fixture):
    SIMULATION.shares_amounts[Direction.LONG] = BUY_LONG_AMOUNT_KO
    with pytest.raises(Exception):
        SIMULATION.validate_transaction(
            Direction.LONG,
            Action.SELL,
            SELL_LONG_AMOUNT_KO,
            TOTAL_COST_ESTIMATE_OK,
        )


def test_validate_transaction_sell_short_ok(simulation_fixture):
    SIMULATION.validate_transaction(
        Direction.SHORT,
        Action.SELL,
        SELL_SHORT_AMOUNT_OK,
        TOTAL_COST_ESTIMATE_OK,
    )


def test_validate_transaction_sell_short_ko(simulation_fixture):
    SIMULATION.validate_transaction(
        Direction.SHORT,
        Action.SELL,
        SELL_SHORT_AMOUNT_OK,
        TOTAL_COST_ESTIMATE_KO,
    )


def test_validate_transaction_buy_short_ok(simulation_fixture):
    SIMULATION.fund(FUND)
    SIMULATION.shares_amounts[Direction.SHORT] = SELL_SHORT_AMOUNT_OK
    SIMULATION.validate_transaction(
        Direction.SHORT, Action.BUY, BUY_SHORT_AMOUNT_OK, TOTAL_COST_ESTIMATE_OK
    )


def test_validate_transaction_buy_short_ko(simulation_fixture):
    SIMULATION.fund(FUND)
    SIMULATION.shares_amounts[Direction.SHORT] = SELL_SHORT_AMOUNT_KO
    with pytest.raises(Exception):
        SIMULATION.validate_transaction(
            Direction.SHORT,
            Action.BUY,
            BUY_SHORT_AMOUNT_KO,
            TOTAL_COST_ESTIMATE_OK,
        )


def test_update_shares_ok(simulation_fixture):

    SIMULATION.update_shares_amounts(Direction.LONG, Action.BUY, BUY_LONG_AMOUNT_OK)
    assert SIMULATION.shares_amounts[Direction.LONG] == BUY_LONG_AMOUNT_OK

    SIMULATION.update_shares_amounts(Direction.LONG, Action.SELL, SELL_LONG_AMOUNT_OK)
    assert (
        SIMULATION.shares_amounts[Direction.LONG]
        == BUY_LONG_AMOUNT_OK - SELL_LONG_AMOUNT_OK
    )

    SIMULATION.update_shares_amounts(Direction.SHORT, Action.SELL, SELL_SHORT_AMOUNT_OK)
    assert SIMULATION.shares_amounts[Direction.SHORT] == -SELL_SHORT_AMOUNT_OK

    SIMULATION.update_shares_amounts(Direction.SHORT, Action.BUY, BUY_SHORT_AMOUNT_OK)
    assert (
        SIMULATION.shares_amounts[Direction.SHORT]
        == BUY_SHORT_AMOUNT_OK - SELL_SHORT_AMOUNT_OK
    )


def test_update_portfolio_value(simulation_fixture):
    SIMULATION.update_shares_amounts(Direction.LONG, Action.BUY, BUY_LONG_AMOUNT_OK)
    unit_cost_estimate = 10
    SIMULATION.update_portfolio_value(Direction.LONG, unit_cost_estimate)
    assert SIMULATION.portfolio_value == BUY_LONG_AMOUNT_OK * unit_cost_estimate


def test_update_cash_without_commission(simulation_fixture):
    SIMULATION.fund(FUND)

    commission_amount = 0
    SIMULATION.update_cash(Action.BUY, COST_ESTIMATE_BUY_OK, commission_amount)
    assert SIMULATION.cash == FUND - COST_ESTIMATE_BUY_OK

    SIMULATION.update_cash(Action.SELL, 400, commission_amount)
    assert SIMULATION.cash == (FUND - COST_ESTIMATE_BUY_OK + COST_ESTIMATE_SELL)


def test_update_cash_with_commission(simulation_fixture):
    SIMULATION.fund(FUND)

    commission_amount = COST_ESTIMATE_BUY_OK * COMMISSION
    SIMULATION.update_cash(Action.BUY, COST_ESTIMATE_BUY_OK, commission_amount)
    assert SIMULATION.cash == FUND - COST_ESTIMATE_BUY_OK - commission_amount

    SIMULATION.update_cash(Action.SELL, 400, commission_amount)
    assert SIMULATION.cash == (
        FUND - COST_ESTIMATE_BUY_OK + COST_ESTIMATE_SELL - 2 * commission_amount
    )


def test_compute_profit(simulation_fixture):
    SIMULATION.last_transaction_price = {
        Direction.LONG: {
            Action.BUY: Decimal("200"),
            Action.SELL: Decimal("201"),
        },
        Direction.SHORT: {
            Action.SELL: Decimal("250"),
            Action.BUY: Decimal("150"),
        },
    }
    assert SIMULATION.compute_profit(Direction.LONG) == Decimal("1")
    assert SIMULATION.compute_profit(Direction.SHORT) == Decimal("100")


def test_position_buy_sell_long_ok(simulation_fixture):
    SIMULATION.fund(FUND)

    SIMULATION.position(
        Direction.LONG, Action.BUY, UNIT_COST_ESTIMATE, BUY_LONG_AMOUNT_OK
    )
    assert SIMULATION.shares_amounts[Direction.LONG] == BUY_LONG_AMOUNT_OK
    assert (
        SIMULATION.portfolio_value
        == SIMULATION.shares_amounts[Direction.LONG] * UNIT_COST_ESTIMATE
    )
    assert SIMULATION.cash == FUND - SIMULATION.portfolio_value

    SIMULATION.position(
        Direction.LONG, Action.SELL, UNIT_COST_ESTIMATE, SELL_LONG_AMOUNT_OK
    )
    assert (
        SIMULATION.shares_amounts[Direction.LONG]
        == BUY_LONG_AMOUNT_OK - SELL_LONG_AMOUNT_OK
    )
    assert (
        SIMULATION.portfolio_value
        == SIMULATION.shares_amounts[Direction.LONG] * UNIT_COST_ESTIMATE
    )
    assert SIMULATION.cash == FUND - SIMULATION.portfolio_value


def test_position_buy_sell_long_ko(simulation_fixture):
    SIMULATION.fund(FUND)

    SIMULATION.position(
        Direction.LONG, Action.BUY, UNIT_COST_ESTIMATE, BUY_LONG_AMOUNT_KO
    )
    assert SIMULATION.shares_amounts[Direction.LONG] == BUY_LONG_AMOUNT_KO
    assert (
        SIMULATION.portfolio_value
        == SIMULATION.shares_amounts[Direction.LONG] * UNIT_COST_ESTIMATE
    )
    assert SIMULATION.cash == FUND - SIMULATION.portfolio_value
    cash_before_call = SIMULATION.cash
    with pytest.raises(Exception):
        SIMULATION.position(
            Direction.LONG, Action.SELL, UNIT_COST_ESTIMATE, SELL_LONG_AMOUNT_KO
        )
    assert SIMULATION.shares_amounts[Direction.LONG] == BUY_LONG_AMOUNT_KO
    assert (
        SIMULATION.portfolio_value
        == SIMULATION.shares_amounts[Direction.LONG] * UNIT_COST_ESTIMATE
    )
    assert SIMULATION.cash == cash_before_call


def test_position_sell_buy_short_ok(simulation_fixture):
    SIMULATION.fund(FUND)

    SIMULATION.position(
        Direction.SHORT, Action.SELL, UNIT_COST_ESTIMATE, SELL_SHORT_AMOUNT_OK
    )
    assert SIMULATION.shares_amounts[Direction.SHORT] == -SELL_SHORT_AMOUNT_OK
    assert (
        SIMULATION.portfolio_value
        == SIMULATION.shares_amounts[Direction.SHORT] * UNIT_COST_ESTIMATE
    )
    assert SIMULATION.cash == FUND - SIMULATION.portfolio_value

    SIMULATION.position(
        Direction.SHORT, Action.BUY, UNIT_COST_ESTIMATE, BUY_SHORT_AMOUNT_OK
    )
    assert (
        SIMULATION.shares_amounts[Direction.SHORT]
        == BUY_SHORT_AMOUNT_OK - SELL_SHORT_AMOUNT_OK
    )
    assert (
        SIMULATION.portfolio_value
        == SIMULATION.shares_amounts[Direction.SHORT] * UNIT_COST_ESTIMATE
    )
    assert SIMULATION.cash == FUND - SIMULATION.portfolio_value


def test_position_sell_buy_short_ko(simulation_fixture):
    SIMULATION.fund(FUND)

    SIMULATION.position(
        Direction.SHORT, Action.SELL, UNIT_COST_ESTIMATE, SELL_SHORT_AMOUNT_KO
    )
    assert SIMULATION.shares_amounts[Direction.SHORT] == -SELL_SHORT_AMOUNT_KO
    assert (
        SIMULATION.portfolio_value
        == SIMULATION.shares_amounts[Direction.SHORT] * UNIT_COST_ESTIMATE
    )
    assert SIMULATION.cash == FUND - SIMULATION.portfolio_value

    with pytest.raises(Exception):
        SIMULATION.position(
            Direction.SHORT, Action.BUY, UNIT_COST_ESTIMATE, BUY_SHORT_AMOUNT_KO
        )
    assert SIMULATION.shares_amounts[Direction.SHORT] == -SELL_SHORT_AMOUNT_KO
    assert (
        SIMULATION.portfolio_value
        == SIMULATION.shares_amounts[Direction.SHORT] * UNIT_COST_ESTIMATE
    )
    assert SIMULATION.cash == FUND - SIMULATION.portfolio_value


@patch("db_storage.mongodb_storage.MongoDbStorage.get_candle_by_identifier")
def test_run_without_commission(get_candle_by_identifier_mocked, simulation_fixture):
    SIMULATION.fund(FUND)

    SIMULATION.candles = CANDLES
    buy_and_sell_long_strategy = BuyAndSellLongStrategy(SYMBOL, DB_STORAGE, BROKER)
    SIMULATION.strategy = buy_and_sell_long_strategy

    get_candle_by_identifier_mocked.side_effect = [
        CANDLES[0],
        CANDLES[1],
        CANDLES[2],
        CANDLES[3],
        CANDLES[4],
        CANDLES[5],
    ]
    SIMULATION.run()
    calls = [
        call("IVV", pd.Timestamp("2020-05-08 14:17:00+0000", tz="UTC")),
        call("IVV", pd.Timestamp("2020-05-08 14:24:00+0000", tz="UTC")),
        call("IVV", pd.Timestamp("2020-05-08 14:24:56+0000", tz="UTC")),
        call("IVV", pd.Timestamp("2020-05-08 14:35:00+0000", tz="UTC")),
        call("IVV", pd.Timestamp("2020-05-08 14:41:00+0000", tz="UTC")),
        call("IVV", pd.Timestamp("2020-05-08 14:41:58+0000", tz="UTC")),
    ]
    get_candle_by_identifier_mocked.assert_has_calls(calls)

    expected_cash = FUND
    sign = Decimal("-1")
    minus_one = Decimal("-1")
    for candle in CANDLES:
        expected_cash += sign * candle.close
        sign *= minus_one
    assert SIMULATION.shares_amounts[Direction.LONG] == 0
    assert SIMULATION.portfolio_value == (
        SIMULATION.shares_amounts[Direction.LONG] * CANDLES[-1].close
    )
    assert SIMULATION.cash == expected_cash


@patch("db_storage.mongodb_storage.MongoDbStorage.get_candle_by_identifier")
def test_run_with_commission(get_candle_by_identifier_mocked, simulation_fixture):
    SIMULATION.fund(FUND)
    SIMULATION.commission = COMMISSION

    SIMULATION.candles = CANDLES
    buy_and_sell_long_strategy = BuyAndSellLongStrategy(SYMBOL, DB_STORAGE, BROKER)
    SIMULATION.strategy = buy_and_sell_long_strategy

    get_candle_by_identifier_mocked.side_effect = [
        CANDLES[0],
        CANDLES[1],
        CANDLES[2],
        CANDLES[3],
        CANDLES[4],
        CANDLES[5],
    ]
    SIMULATION.run()
    calls = [
        call("IVV", pd.Timestamp("2020-05-08 14:17:00+0000", tz="UTC")),
        call("IVV", pd.Timestamp("2020-05-08 14:24:00+0000", tz="UTC")),
        call("IVV", pd.Timestamp("2020-05-08 14:24:56+0000", tz="UTC")),
        call("IVV", pd.Timestamp("2020-05-08 14:35:00+0000", tz="UTC")),
        call("IVV", pd.Timestamp("2020-05-08 14:41:00+0000", tz="UTC")),
        call("IVV", pd.Timestamp("2020-05-08 14:41:58+0000", tz="UTC")),
    ]
    get_candle_by_identifier_mocked.assert_has_calls(calls)

    expected_cash = FUND
    sign = Decimal("-1")
    minus_one = Decimal("-1")
    for candle in CANDLES:
        expected_cash += sign * candle.close
        expected_cash -= candle.close * SIMULATION.commission
        sign *= minus_one
    assert SIMULATION.shares_amounts[Direction.LONG] == 0
    assert SIMULATION.portfolio_value == (
        SIMULATION.shares_amounts[Direction.LONG] * CANDLES[-1].close
    )
    assert SIMULATION.cash == expected_cash
