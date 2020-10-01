from decimal import Decimal
from unittest.mock import Mock, call, patch

import pandas as pd
import pytest

from broker.simulatedbroker import SimulatedBroker
from db_storage.mongodb_storage import MongoDbStorage
from models.action import Action
from models.candle import Candle
from models.enums import ActionType, PositionType
from settings import DATABASE_NAME
from simulator.simulation import Simulation
from strategy.strategies.buy_and_sell_long_strategy import BuyAndSellLongStrategy
from strategy.strategies.dumb_long_strategy import DumbLongStrategy

# disable logging into a file
from test.tools.tools import compare_actions_list

DB_STORAGE = MongoDbStorage(DATABASE_NAME)
SYMBOL = "IVV"
BROKER = SimulatedBroker(cash=Decimal("10000"))
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
ACTIONS = [
    Action(
        action_type=ActionType.BUY,
        position_type=PositionType.LONG,
        size=1,
        confidence_level=1,
        strategy=STRATEGY_NAME,
        symbol=SYMBOL,
        candle_timestamp=pd.Timestamp("2020-05-08 14:17:00", tz="UTC"),
        parameters={},
    ),
    Action(
        action_type=ActionType.SELL,
        position_type=PositionType.LONG,
        size=1,
        confidence_level=1,
        strategy=STRATEGY_NAME,
        symbol=SYMBOL,
        candle_timestamp=pd.Timestamp("2020-05-08 14:24:00", tz="UTC"),
        parameters={},
    ),
    Action(
        action_type=ActionType.BUY,
        position_type=PositionType.LONG,
        size=1,
        confidence_level=1,
        strategy=STRATEGY_NAME,
        symbol=SYMBOL,
        candle_timestamp=pd.Timestamp("2020-05-08 14:24:56", tz="UTC"),
        parameters={},
    ),
    Action(
        action_type=ActionType.SELL,
        position_type=PositionType.LONG,
        size=1,
        confidence_level=1,
        strategy=STRATEGY_NAME,
        symbol=SYMBOL,
        candle_timestamp=pd.Timestamp("2020-05-08 14:35:00", tz="UTC"),
        parameters={},
    ),
    Action(
        action_type=ActionType.BUY,
        position_type=PositionType.LONG,
        size=1,
        confidence_level=1,
        strategy=STRATEGY_NAME,
        symbol=SYMBOL,
        candle_timestamp=pd.Timestamp("2020-05-08 14:41:00", tz="UTC"),
        parameters={},
    ),
    Action(
        action_type=ActionType.SELL,
        position_type=PositionType.LONG,
        size=1,
        confidence_level=1,
        strategy=STRATEGY_NAME,
        symbol=SYMBOL,
        candle_timestamp=pd.Timestamp("2020-05-08 14:41:58", tz="UTC"),
        parameters={},
    ),
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
        PositionType.LONG: {
            ActionType.BUY: Decimal("200"),
            ActionType.SELL: Decimal("201"),
        },
        PositionType.SHORT: {
            ActionType.SELL: Decimal("202"),
            ActionType.BUY: Decimal("199"),
        },
    }
    SIMULATION.shares_amounts = {PositionType.LONG: 17, PositionType.SHORT: 13}

    SIMULATION.reset()

    assert SIMULATION.candles == None
    assert SIMULATION.cash == Decimal("0")
    assert SIMULATION.commission == Decimal("0")
    assert SIMULATION.portfolio_value == Decimal("0")
    assert SIMULATION.last_transaction_price == {
        PositionType.LONG: {
            ActionType.BUY: Decimal("0"),
            ActionType.SELL: Decimal("0"),
        },
        PositionType.SHORT: {
            ActionType.SELL: Decimal("0"),
            ActionType.BUY: Decimal("0"),
        },
    }
    assert SIMULATION.shares_amounts == {
        PositionType.LONG: Decimal("0"),
        PositionType.SHORT: Decimal("0"),
    }


def test_fund(simulation_fixture):
    SIMULATION.fund(1000)
    assert SIMULATION.cash == 1000
    SIMULATION.fund(1500)
    assert SIMULATION.cash == 2500


def test_get_actions(simulation_fixture):
    SIMULATION.fund(FUND)
    SIMULATION.candles = CANDLES
    SIMULATION.strategy = Mock()
    SIMULATION.strategy.compute_action.side_effect = ACTIONS
    actions = SIMULATION.get_actions()

    expected_actions = ACTIONS
    assert compare_actions_list(actions, expected_actions)


def test_get_actions_one_none_action(simulation_fixture):
    SIMULATION.fund(FUND)
    SIMULATION.candles = CANDLES
    SIMULATION.strategy = Mock()
    SIMULATION.strategy.compute_action.side_effect = [
        ACTIONS[0],
        ACTIONS[1],
        ACTIONS[2],
        ACTIONS[3],
        ACTIONS[4],
        None,
    ]
    actions = SIMULATION.get_actions()

    expected_actions = [ACTIONS[0], ACTIONS[1], ACTIONS[2], ACTIONS[3], ACTIONS[4]]
    assert compare_actions_list(actions, expected_actions)


def test_is_closed_position(simulation_fixture):
    assert SIMULATION.is_closed_position(PositionType.LONG, ActionType.BUY) == False
    assert SIMULATION.is_closed_position(PositionType.LONG, ActionType.SELL) == True
    assert SIMULATION.is_closed_position(PositionType.SHORT, ActionType.SELL) == False
    assert SIMULATION.is_closed_position(PositionType.SHORT, ActionType.BUY) == True


def test_validate_shares_amounts_buy_long_ok(simulation_fixture):
    SIMULATION.validate_shares_amounts(
        PositionType.LONG, ActionType.BUY, BUY_LONG_AMOUNT_OK
    )


def test_validate_shares_amounts_sell_long_ok(simulation_fixture):
    SIMULATION.shares_amounts[PositionType.LONG] = BUY_LONG_AMOUNT_OK
    SIMULATION.validate_shares_amounts(
        PositionType.LONG, ActionType.SELL, SELL_LONG_AMOUNT_OK
    )


def test_validate_shares_amounts_sell_long_ko(simulation_fixture):
    SIMULATION.shares_amounts[PositionType.LONG] = BUY_LONG_AMOUNT_KO
    with pytest.raises(Exception):
        SIMULATION.validate_shares_amounts(
            PositionType.LONG, ActionType.SELL, SELL_LONG_AMOUNT_KO
        )


def test_validate_shares_amounts_sell_short_ok(simulation_fixture):
    SIMULATION.validate_shares_amounts(
        PositionType.SHORT, ActionType.SELL, SELL_SHORT_AMOUNT_OK
    )


def test_validate_shares_amounts_buy_short_ok(simulation_fixture):
    SIMULATION.shares_amounts[PositionType.SHORT] = SELL_SHORT_AMOUNT_OK
    SIMULATION.validate_shares_amounts(
        PositionType.SHORT, ActionType.BUY, BUY_SHORT_AMOUNT_OK
    )


def test_validate_shares_amounts_buy_short_ko(simulation_fixture):
    SIMULATION.shares_amounts[PositionType.SHORT] = SELL_SHORT_AMOUNT_KO
    with pytest.raises(Exception):
        SIMULATION.validate_shares_amounts(
            PositionType.SHORT, ActionType.BUY, BUY_SHORT_AMOUNT_KO
        )


def test_validate_cash_buy_ok(simulation_fixture):
    SIMULATION.fund(FUND)
    SIMULATION.validate_cash(ActionType.BUY, TOTAL_COST_ESTIMATE_OK)


def test_validate_cash_buy_ko(simulation_fixture):
    SIMULATION.fund(FUND)
    with pytest.raises(Exception):
        SIMULATION.validate_cash(ActionType.BUY, TOTAL_COST_ESTIMATE_KO)


def test_validate_cash_sell_ok(simulation_fixture):
    SIMULATION.fund(FUND)
    SIMULATION.validate_cash(ActionType.SELL, TOTAL_COST_ESTIMATE_OK)


def test_validate_transaction_buy_long_ok(simulation_fixture):
    SIMULATION.fund(FUND)
    SIMULATION.validate_transaction(
        PositionType.LONG, ActionType.BUY, BUY_LONG_AMOUNT_OK, TOTAL_COST_ESTIMATE_OK
    )


def test_validate_transaction_buy_long_ko(simulation_fixture):
    SIMULATION.fund(FUND)
    with pytest.raises(Exception):
        SIMULATION.validate_transaction(
            PositionType.LONG,
            ActionType.BUY,
            BUY_LONG_AMOUNT_OK,
            TOTAL_COST_ESTIMATE_KO,
        )


def test_validate_transaction_sell_long_ok(simulation_fixture):
    SIMULATION.shares_amounts[PositionType.LONG] = BUY_LONG_AMOUNT_OK
    SIMULATION.validate_transaction(
        PositionType.LONG, ActionType.SELL, SELL_LONG_AMOUNT_OK, TOTAL_COST_ESTIMATE_OK
    )


def test_validate_transaction_sell_long_ko(simulation_fixture):
    SIMULATION.shares_amounts[PositionType.LONG] = BUY_LONG_AMOUNT_KO
    with pytest.raises(Exception):
        SIMULATION.validate_transaction(
            PositionType.LONG,
            ActionType.SELL,
            SELL_LONG_AMOUNT_KO,
            TOTAL_COST_ESTIMATE_OK,
        )


def test_validate_transaction_sell_short_ok(simulation_fixture):
    SIMULATION.validate_transaction(
        PositionType.SHORT,
        ActionType.SELL,
        SELL_SHORT_AMOUNT_OK,
        TOTAL_COST_ESTIMATE_OK,
    )


def test_validate_transaction_sell_short_ko(simulation_fixture):
    SIMULATION.validate_transaction(
        PositionType.SHORT,
        ActionType.SELL,
        SELL_SHORT_AMOUNT_OK,
        TOTAL_COST_ESTIMATE_KO,
    )


def test_validate_transaction_buy_short_ok(simulation_fixture):
    SIMULATION.fund(FUND)
    SIMULATION.shares_amounts[PositionType.SHORT] = SELL_SHORT_AMOUNT_OK
    SIMULATION.validate_transaction(
        PositionType.SHORT, ActionType.BUY, BUY_SHORT_AMOUNT_OK, TOTAL_COST_ESTIMATE_OK
    )


def test_validate_transaction_buy_short_ko(simulation_fixture):
    SIMULATION.fund(FUND)
    SIMULATION.shares_amounts[PositionType.SHORT] = SELL_SHORT_AMOUNT_KO
    with pytest.raises(Exception):
        SIMULATION.validate_transaction(
            PositionType.SHORT,
            ActionType.BUY,
            BUY_SHORT_AMOUNT_KO,
            TOTAL_COST_ESTIMATE_OK,
        )


def test_update_shares_ok(simulation_fixture):

    SIMULATION.update_shares_amounts(
        PositionType.LONG, ActionType.BUY, BUY_LONG_AMOUNT_OK
    )
    assert SIMULATION.shares_amounts[PositionType.LONG] == BUY_LONG_AMOUNT_OK

    SIMULATION.update_shares_amounts(
        PositionType.LONG, ActionType.SELL, SELL_LONG_AMOUNT_OK
    )
    assert (
        SIMULATION.shares_amounts[PositionType.LONG]
        == BUY_LONG_AMOUNT_OK - SELL_LONG_AMOUNT_OK
    )

    SIMULATION.update_shares_amounts(
        PositionType.SHORT, ActionType.SELL, SELL_SHORT_AMOUNT_OK
    )
    assert SIMULATION.shares_amounts[PositionType.SHORT] == -SELL_SHORT_AMOUNT_OK

    SIMULATION.update_shares_amounts(
        PositionType.SHORT, ActionType.BUY, BUY_SHORT_AMOUNT_OK
    )
    assert (
        SIMULATION.shares_amounts[PositionType.SHORT]
        == BUY_SHORT_AMOUNT_OK - SELL_SHORT_AMOUNT_OK
    )


def test_update_portfolio_value(simulation_fixture):
    SIMULATION.update_shares_amounts(
        PositionType.LONG, ActionType.BUY, BUY_LONG_AMOUNT_OK
    )
    unit_cost_estimate = 10
    SIMULATION.update_portfolio_value(PositionType.LONG, unit_cost_estimate)
    assert SIMULATION.portfolio_value == BUY_LONG_AMOUNT_OK * unit_cost_estimate


def test_update_cash_without_commission(simulation_fixture):
    SIMULATION.fund(FUND)

    commission_amount = 0
    SIMULATION.update_cash(ActionType.BUY, COST_ESTIMATE_BUY_OK, commission_amount)
    assert SIMULATION.cash == FUND - COST_ESTIMATE_BUY_OK

    SIMULATION.update_cash(ActionType.SELL, 400, commission_amount)
    assert SIMULATION.cash == (FUND - COST_ESTIMATE_BUY_OK + COST_ESTIMATE_SELL)


def test_update_cash_with_commission(simulation_fixture):
    SIMULATION.fund(FUND)

    commission_amount = COST_ESTIMATE_BUY_OK * COMMISSION
    SIMULATION.update_cash(ActionType.BUY, COST_ESTIMATE_BUY_OK, commission_amount)
    assert SIMULATION.cash == FUND - COST_ESTIMATE_BUY_OK - commission_amount

    SIMULATION.update_cash(ActionType.SELL, 400, commission_amount)
    assert SIMULATION.cash == (
        FUND - COST_ESTIMATE_BUY_OK + COST_ESTIMATE_SELL - 2 * commission_amount
    )


def test_compute_profit(simulation_fixture):
    SIMULATION.last_transaction_price = {
        PositionType.LONG: {
            ActionType.BUY: Decimal("200"),
            ActionType.SELL: Decimal("201"),
        },
        PositionType.SHORT: {
            ActionType.SELL: Decimal("250"),
            ActionType.BUY: Decimal("150"),
        },
    }
    assert SIMULATION.compute_profit(PositionType.LONG) == Decimal("1")
    assert SIMULATION.compute_profit(PositionType.SHORT) == Decimal("100")


def test_position_buy_sell_long_ok(simulation_fixture):
    SIMULATION.fund(FUND)

    SIMULATION.position(
        PositionType.LONG, ActionType.BUY, UNIT_COST_ESTIMATE, BUY_LONG_AMOUNT_OK
    )
    assert SIMULATION.shares_amounts[PositionType.LONG] == BUY_LONG_AMOUNT_OK
    assert (
        SIMULATION.portfolio_value
        == SIMULATION.shares_amounts[PositionType.LONG] * UNIT_COST_ESTIMATE
    )
    assert SIMULATION.cash == FUND - SIMULATION.portfolio_value

    SIMULATION.position(
        PositionType.LONG, ActionType.SELL, UNIT_COST_ESTIMATE, SELL_LONG_AMOUNT_OK
    )
    assert (
        SIMULATION.shares_amounts[PositionType.LONG]
        == BUY_LONG_AMOUNT_OK - SELL_LONG_AMOUNT_OK
    )
    assert (
        SIMULATION.portfolio_value
        == SIMULATION.shares_amounts[PositionType.LONG] * UNIT_COST_ESTIMATE
    )
    assert SIMULATION.cash == FUND - SIMULATION.portfolio_value


def test_position_buy_sell_long_ko(simulation_fixture):
    SIMULATION.fund(FUND)

    SIMULATION.position(
        PositionType.LONG, ActionType.BUY, UNIT_COST_ESTIMATE, BUY_LONG_AMOUNT_KO
    )
    assert SIMULATION.shares_amounts[PositionType.LONG] == BUY_LONG_AMOUNT_KO
    assert (
        SIMULATION.portfolio_value
        == SIMULATION.shares_amounts[PositionType.LONG] * UNIT_COST_ESTIMATE
    )
    assert SIMULATION.cash == FUND - SIMULATION.portfolio_value
    cash_before_call = SIMULATION.cash
    with pytest.raises(Exception):
        SIMULATION.position(
            PositionType.LONG, ActionType.SELL, UNIT_COST_ESTIMATE, SELL_LONG_AMOUNT_KO
        )
    assert SIMULATION.shares_amounts[PositionType.LONG] == BUY_LONG_AMOUNT_KO
    assert (
        SIMULATION.portfolio_value
        == SIMULATION.shares_amounts[PositionType.LONG] * UNIT_COST_ESTIMATE
    )
    assert SIMULATION.cash == cash_before_call


def test_position_sell_buy_short_ok(simulation_fixture):
    SIMULATION.fund(FUND)

    SIMULATION.position(
        PositionType.SHORT, ActionType.SELL, UNIT_COST_ESTIMATE, SELL_SHORT_AMOUNT_OK
    )
    assert SIMULATION.shares_amounts[PositionType.SHORT] == -SELL_SHORT_AMOUNT_OK
    assert (
        SIMULATION.portfolio_value
        == SIMULATION.shares_amounts[PositionType.SHORT] * UNIT_COST_ESTIMATE
    )
    assert SIMULATION.cash == FUND - SIMULATION.portfolio_value

    SIMULATION.position(
        PositionType.SHORT, ActionType.BUY, UNIT_COST_ESTIMATE, BUY_SHORT_AMOUNT_OK
    )
    assert (
        SIMULATION.shares_amounts[PositionType.SHORT]
        == BUY_SHORT_AMOUNT_OK - SELL_SHORT_AMOUNT_OK
    )
    assert (
        SIMULATION.portfolio_value
        == SIMULATION.shares_amounts[PositionType.SHORT] * UNIT_COST_ESTIMATE
    )
    assert SIMULATION.cash == FUND - SIMULATION.portfolio_value


def test_position_sell_buy_short_ko(simulation_fixture):
    SIMULATION.fund(FUND)

    SIMULATION.position(
        PositionType.SHORT, ActionType.SELL, UNIT_COST_ESTIMATE, SELL_SHORT_AMOUNT_KO
    )
    assert SIMULATION.shares_amounts[PositionType.SHORT] == -SELL_SHORT_AMOUNT_KO
    assert (
        SIMULATION.portfolio_value
        == SIMULATION.shares_amounts[PositionType.SHORT] * UNIT_COST_ESTIMATE
    )
    assert SIMULATION.cash == FUND - SIMULATION.portfolio_value

    with pytest.raises(Exception):
        SIMULATION.position(
            PositionType.SHORT, ActionType.BUY, UNIT_COST_ESTIMATE, BUY_SHORT_AMOUNT_KO
        )
    assert SIMULATION.shares_amounts[PositionType.SHORT] == -SELL_SHORT_AMOUNT_KO
    assert (
        SIMULATION.portfolio_value
        == SIMULATION.shares_amounts[PositionType.SHORT] * UNIT_COST_ESTIMATE
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
    assert SIMULATION.shares_amounts[PositionType.LONG] == 0
    assert SIMULATION.portfolio_value == (
        SIMULATION.shares_amounts[PositionType.LONG] * CANDLES[-1].close
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
    assert SIMULATION.shares_amounts[PositionType.LONG] == 0
    assert SIMULATION.portfolio_value == (
        SIMULATION.shares_amounts[PositionType.LONG] * CANDLES[-1].close
    )
    assert SIMULATION.cash == expected_cash
