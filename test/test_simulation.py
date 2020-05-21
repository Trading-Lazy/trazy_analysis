import logging
from datetime import datetime
from decimal import Decimal
from typing import List
from unittest.mock import patch, call, Mock

import pytest

from simulator.simulation import Simulation
from actionsapi.models import PositionType, ActionType, Candle, Action
from strategy.constants import DATE_FORMAT
from strategy.strategies.BuyAndSellLongStrategy import BuyAndSellLongStrategy
from strategy.strategies.DumbLongStrategy import DumbLongStrategy

# disable logging into a file
SIMULATION = Simulation(DumbLongStrategy(), log=False)
SYMBOL = "ANX.PA"

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

UNIT_COST_ESTIMATE = Decimal("100")

STRATEGY_NAME = "strategy"
ACTIONS = [
    Action(
        action_type=ActionType.BUY,
        position_type=PositionType.LONG,
        amount=1,
        confidence_level=1,
        strategy=STRATEGY_NAME,
        symbol=SYMBOL,
        candle_id=1,
        parameters={},
    ),
    Action(
        action_type=ActionType.SELL,
        position_type=PositionType.LONG,
        amount=1,
        confidence_level=1,
        strategy=STRATEGY_NAME,
        symbol=SYMBOL,
        candle_id=2,
        parameters={},
    ),
    Action(
        action_type=ActionType.BUY,
        position_type=PositionType.LONG,
        amount=1,
        confidence_level=1,
        strategy=STRATEGY_NAME,
        symbol=SYMBOL,
        candle_id=3,
        parameters={},
    ),
    Action(
        action_type=ActionType.SELL,
        position_type=PositionType.LONG,
        amount=1,
        confidence_level=1,
        strategy=STRATEGY_NAME,
        symbol=SYMBOL,
        candle_id=4,
        parameters={},
    ),
    Action(
        action_type=ActionType.BUY,
        position_type=PositionType.LONG,
        amount=1,
        confidence_level=1,
        strategy=STRATEGY_NAME,
        symbol=SYMBOL,
        candle_id=5,
        parameters={},
    ),
    Action(
        action_type=ActionType.SELL,
        position_type=PositionType.LONG,
        amount=1,
        confidence_level=1,
        strategy=STRATEGY_NAME,
        symbol=SYMBOL,
        candle_id=6,
        parameters={},
    )
]

CANDLES = [
    Candle(
        id=1,
        symbol=SYMBOL,
        open=Decimal("94.1200"),
        high=Decimal("94.1500"),
        low=Decimal("94.0000"),
        close=Decimal("94.1300"),
        volume=7,
        timestamp="2020-05-08 14:17:00",
    ),
    Candle(
        id=2,
        symbol=SYMBOL,
        open=Decimal("94.0700"),
        high=Decimal("94.1000"),
        low=Decimal("93.9500"),
        close=Decimal("94.0800"),
        volume=91,
        timestamp="2020-05-08 14:24:00",
    ),
    Candle(
        id=3,
        symbol=SYMBOL,
        open=Decimal("94.0700"),
        high=Decimal("94.1000"),
        low=Decimal("93.9500"),
        close=Decimal("94.0800"),
        volume=0,
        timestamp="2020-05-08 14:24:56",
    ),
    Candle(
        id=4,
        symbol=SYMBOL,
        open=Decimal("94.1700"),
        high=Decimal("94.1800"),
        low=Decimal("94.0500"),
        close=Decimal("94.1800"),
        volume=0,
        timestamp="2020-05-08 14:35:00",
    ),
    Candle(
        id=5,
        symbol=SYMBOL,
        open=Decimal("94.1900"),
        high=Decimal("94.2200"),
        low=Decimal("94.0700"),
        close=Decimal("94.2000"),
        volume=0,
        timestamp="2020-05-08 14:41:00",
    ),
    Candle(
        id=6,
        symbol=SYMBOL,
        open=Decimal("94.1900"),
        high=Decimal("94.2200"),
        low=Decimal("94.0700"),
        close=Decimal("94.2000"),
        volume=7,
        timestamp="2020-05-08 14:41:58",
    ),
]


@pytest.fixture
def simulation_fixture():
    SIMULATION.reset()


def test_reset(simulation_fixture):
    SIMULATION.candles = CANDLES
    SIMULATION.cash = Decimal('200')
    SIMULATION.commission = Decimal('0.001')
    SIMULATION.portfolio_value = Decimal('100')
    SIMULATION.portfolio_value = Decimal('2000')
    SIMULATION.last_transaction_price = Decimal('100')
    SIMULATION.last_transaction_price = {
        PositionType.LONG: {ActionType.BUY: Decimal('200'), ActionType.SELL: Decimal('201')},
        PositionType.SHORT: {ActionType.SELL: Decimal('202'), ActionType.BUY: Decimal('199')},
    }
    SIMULATION.shares_amounts = {PositionType.LONG: 17, PositionType.SHORT: 13}

    SIMULATION.reset()

    assert SIMULATION.candles == None
    assert SIMULATION.cash == Decimal('0')
    assert SIMULATION.commission == Decimal('0')
    assert SIMULATION.portfolio_value == Decimal('0')
    assert SIMULATION.last_transaction_price == {
        PositionType.LONG: {ActionType.BUY: Decimal('0'), ActionType.SELL: Decimal('0')},
        PositionType.SHORT: {ActionType.SELL: Decimal('0'), ActionType.BUY: Decimal('0')},
    }
    assert SIMULATION.shares_amounts == {PositionType.LONG: Decimal('0'), PositionType.SHORT: Decimal('0')}


def test_fund(simulation_fixture):
    SIMULATION.fund(1000)
    assert SIMULATION.cash == 1000
    SIMULATION.fund(1500)
    assert SIMULATION.cash == 2500


def compare_action(action1: Action, action2: Action) -> bool:
    return (
        action1.action_type == action2.action_type
        and action1.position_type == action2.position_type
        and action1.amount == action2.amount
        and action1.confidence_level == action2.confidence_level
        and action1.strategy == action2.strategy
        and action1.symbol == action2.symbol
        and action1.candle_id == action2.candle_id
        and action1.parameters == action2.parameters
    )


def compare_actions_list(
    actions_list1: List[Action], actions_list2: List[Action]
) -> bool:
    if len(actions_list1) != len(actions_list2):
        return False
    length = len(actions_list1)
    for i in range(0, length):
        if not compare_action(actions_list1[i], actions_list2[i]):
            return False
    return True


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
        None
    ]
    actions = SIMULATION.get_actions()

    expected_actions = [
        ACTIONS[0],
        ACTIONS[1],
        ACTIONS[2],
        ACTIONS[3],
        ACTIONS[4]
    ]
    assert compare_actions_list(actions, expected_actions)


def test_is_closed_position(simulation_fixture):
    assert SIMULATION.is_closed_position(PositionType.LONG, ActionType.BUY) == False
    assert SIMULATION.is_closed_position(PositionType.LONG, ActionType.SELL) == True
    assert SIMULATION.is_closed_position(PositionType.SHORT, ActionType.SELL) == False
    assert SIMULATION.is_closed_position(PositionType.SHORT, ActionType.BUY) == True


def test_validate_transaction_buy_long_ok(simulation_fixture):
    SIMULATION.validate_transaction(PositionType.LONG, ActionType.BUY, 5)


def test_validate_transaction_sell_long_ok(simulation_fixture):
    SIMULATION.shares_amounts[PositionType.LONG] = 5
    SIMULATION.validate_transaction(PositionType.LONG, ActionType.SELL, 5)


def test_validate_transaction_sell_long_ko(simulation_fixture):
    SIMULATION.shares_amounts[PositionType.LONG] = 3
    with pytest.raises(Exception):
        SIMULATION.validate_transaction(PositionType.LONG, ActionType.SELL, 5)


def test_validate_transaction_sell_short_ok(simulation_fixture):
    SIMULATION.validate_transaction(PositionType.SHORT, ActionType.SELL, 5)


def test_validate_transaction_buy_short_ok(simulation_fixture):
    SIMULATION.shares_amounts[PositionType.SHORT] = 5
    SIMULATION.validate_transaction(PositionType.SHORT, ActionType.BUY, 5)


def test_validate_transaction_buy_short_ko(simulation_fixture):
    SIMULATION.shares_amounts[PositionType.SHORT] = 3
    with pytest.raises(Exception):
        SIMULATION.validate_transaction(PositionType.SHORT, ActionType.BUY, 5)


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


def test_update_shares_ko(simulation_fixture):
    SIMULATION.update_shares_amounts(
        PositionType.LONG, ActionType.BUY, BUY_LONG_AMOUNT_KO
    )
    assert SIMULATION.shares_amounts[PositionType.LONG] == BUY_LONG_AMOUNT_KO

    with pytest.raises(Exception):
        SIMULATION.update_shares_amounts(
            PositionType.LONG, ActionType.SELL, SELL_LONG_AMOUNT_KO
        )
    assert SIMULATION.shares_amounts[PositionType.LONG] == BUY_LONG_AMOUNT_KO

    SIMULATION.update_shares_amounts(
        PositionType.SHORT, ActionType.SELL, SELL_SHORT_AMOUNT_KO
    )
    assert SIMULATION.shares_amounts[PositionType.SHORT] == -SELL_SHORT_AMOUNT_KO

    with pytest.raises(Exception):
        SIMULATION.update_shares_amounts(
            PositionType.SHORT, ActionType.BUY, BUY_SHORT_AMOUNT_KO
        )
    assert SIMULATION.shares_amounts[PositionType.SHORT] == -SELL_SHORT_AMOUNT_KO


def test_update_portfolio_value(simulation_fixture):
    SIMULATION.update_shares_amounts(
        PositionType.LONG, ActionType.BUY, BUY_LONG_AMOUNT_OK
    )
    unit_cost_estimate = 10
    SIMULATION.update_portfolio_value(PositionType.LONG, unit_cost_estimate)
    assert SIMULATION.portfolio_value == BUY_LONG_AMOUNT_OK * unit_cost_estimate


def test_update_cash_ok(simulation_fixture):
    SIMULATION.fund(FUND)

    SIMULATION.update_cash(ActionType.BUY, COST_ESTIMATE_BUY_OK)
    assert SIMULATION.cash == FUND - COST_ESTIMATE_BUY_OK

    SIMULATION.update_cash(ActionType.SELL, 400)
    assert SIMULATION.cash == (FUND - COST_ESTIMATE_BUY_OK + COST_ESTIMATE_SELL)


def test_update_cash_ko(simulation_fixture):
    SIMULATION.fund(FUND)

    with pytest.raises(Exception):
        SIMULATION.update_cash(ActionType.BUY, COST_ESTIMATE_BUY_KO)
    assert SIMULATION.cash == FUND

    SIMULATION.update_cash(ActionType.SELL, COST_ESTIMATE_SELL)
    assert SIMULATION.cash == FUND + COST_ESTIMATE_SELL


def test_compute_profit(simulation_fixture):
    SIMULATION.last_transaction_price = {
        PositionType.LONG: {ActionType.BUY: Decimal('200'), ActionType.SELL: Decimal('201')},
        PositionType.SHORT: {ActionType.SELL: Decimal('250'), ActionType.BUY: Decimal('150')},
    }
    assert SIMULATION.compute_profit(PositionType.LONG) == Decimal('1')
    assert SIMULATION.compute_profit(PositionType.SHORT) == Decimal('100')


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

    with pytest.raises(Exception):
        SIMULATION.position(
            PositionType.LONG, ActionType.SELL, UNIT_COST_ESTIMATE, SELL_LONG_AMOUNT_KO
        )
    assert SIMULATION.shares_amounts[PositionType.LONG] == BUY_LONG_AMOUNT_KO
    assert (
        SIMULATION.portfolio_value
        == SIMULATION.shares_amounts[PositionType.LONG] * UNIT_COST_ESTIMATE
    )
    assert SIMULATION.cash == FUND - SIMULATION.portfolio_value


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


@patch("actionsapi.models.Candle.objects")
def test_run_without_commission(candle_objects_mock, simulation_fixture):
    SIMULATION.fund(FUND)

    SIMULATION.candles = CANDLES
    buy_and_sell_long_strategy = BuyAndSellLongStrategy()
    SIMULATION.strategy = buy_and_sell_long_strategy

    candle_objects_mock.get.side_effect = [
        CANDLES[0],
        CANDLES[1],
        CANDLES[2],
        CANDLES[3],
        CANDLES[4],
        CANDLES[5],
    ]
    SIMULATION.run()
    calls = [call(id=1), call(id=2), call(id=3), call(id=4), call(id=5), call(id=6)]
    candle_objects_mock.get.assert_has_calls(calls)

    expected_cash = FUND
    sign = Decimal("-1")
    minus_one = Decimal("-1")
    for candle in CANDLES:
        expected_cash += sign * candle.close
        sign *= minus_one
    if sign == minus_one:
        assert SIMULATION.shares_amounts[PositionType.LONG] == 0
    else:
        assert SIMULATION.shares_amounts[PositionType.LONG] == 1
    SIMULATION.portfolio_value = (
        SIMULATION.shares_amounts[PositionType.LONG] * CANDLES[-1].close
    )
    assert SIMULATION.cash == expected_cash


@patch("actionsapi.models.Candle.objects")
def test_run_with_commission(candle_objects_mock, simulation_fixture):
    SIMULATION.fund(FUND)
    SIMULATION.commission = COMMISSION

    SIMULATION.candles = CANDLES
    buy_and_sell_long_strategy = BuyAndSellLongStrategy()
    SIMULATION.strategy = buy_and_sell_long_strategy

    candle_objects_mock.get.side_effect = [
        CANDLES[0],
        CANDLES[1],
        CANDLES[2],
        CANDLES[3],
        CANDLES[4],
        CANDLES[5],
    ]
    SIMULATION.run()
    calls = [call(id=1), call(id=2), call(id=3), call(id=4), call(id=5), call(id=6)]
    candle_objects_mock.get.assert_has_calls(calls)

    expected_cash = FUND
    sign = Decimal("-1")
    minus_one = Decimal("-1")
    for candle in CANDLES:
        expected_cash += sign * candle.close
        expected_cash -= candle.close * SIMULATION.commission
        sign *= minus_one
    if sign == minus_one:
        assert SIMULATION.shares_amounts[PositionType.LONG] == 0
    else:
        assert SIMULATION.shares_amounts[PositionType.LONG] == 1
    SIMULATION.portfolio_value = (
        SIMULATION.shares_amounts[PositionType.LONG] * CANDLES[-1].close
    )
    assert SIMULATION.cash == expected_cash
