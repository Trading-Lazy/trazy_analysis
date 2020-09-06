from decimal import Decimal

import pandas as pd
import pytest

from broker.simulatedbroker import SimulatedBroker
from db_storage.mongodb_storage import MongoDbStorage
from models.action import Action
from models.candle import Candle
from models.enums import ActionType, PositionType
from settings import DATABASE_NAME

# disable logging into a file

DB_STORAGE = MongoDbStorage(DATABASE_NAME)
SYMBOL = "IVV"

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
SIMULATED_BROKER = SimulatedBroker(FUND, COMMISSION)
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
    SIMULATED_BROKER.reset()


def test_reset(simulation_fixture):
    SIMULATED_BROKER.cash = Decimal("200")
    SIMULATED_BROKER.commission = Decimal("0.001")
    SIMULATED_BROKER.portfolio_value = Decimal("100")
    SIMULATED_BROKER.portfolio_value = Decimal("2000")
    SIMULATED_BROKER.last_transaction_price = Decimal("100")
    SIMULATED_BROKER.last_transaction_price = {
        PositionType.LONG: {
            ActionType.BUY: Decimal("200"),
            ActionType.SELL: Decimal("201"),
        },
        PositionType.SHORT: {
            ActionType.SELL: Decimal("202"),
            ActionType.BUY: Decimal("199"),
        },
    }
    SIMULATED_BROKER.positions_sizes = {PositionType.LONG: 17, PositionType.SHORT: 13}

    SIMULATED_BROKER.reset()

    assert SIMULATED_BROKER.cash == Decimal("0")
    assert SIMULATED_BROKER.commission == Decimal("0")
    assert SIMULATED_BROKER.portfolio_value == Decimal("0")
    assert SIMULATED_BROKER.last_transaction_price == {
        PositionType.LONG: {
            ActionType.BUY: Decimal("0"),
            ActionType.SELL: Decimal("0"),
        },
        PositionType.SHORT: {
            ActionType.SELL: Decimal("0"),
            ActionType.BUY: Decimal("0"),
        },
    }
    assert SIMULATED_BROKER.positions_sizes == {
        PositionType.LONG: Decimal("0"),
        PositionType.SHORT: Decimal("0"),
    }


def test_fund(simulation_fixture):
    SIMULATED_BROKER.add_cash(1000)
    assert SIMULATED_BROKER.cash == 1000
    SIMULATED_BROKER.add_cash(1500)
    assert SIMULATED_BROKER.cash == 2500


def test_is_closed_position(simulation_fixture):
    assert (
        SIMULATED_BROKER.is_closed_position(PositionType.LONG, ActionType.BUY) == False
    )
    assert (
        SIMULATED_BROKER.is_closed_position(PositionType.LONG, ActionType.SELL) == True
    )
    assert (
        SIMULATED_BROKER.is_closed_position(PositionType.SHORT, ActionType.SELL)
        == False
    )
    assert (
        SIMULATED_BROKER.is_closed_position(PositionType.SHORT, ActionType.BUY) == True
    )


def test_validate_positions_sizes_buy_long_ok(simulation_fixture):
    SIMULATED_BROKER.validate_positions_sizes(
        PositionType.LONG, ActionType.BUY, BUY_LONG_AMOUNT_OK
    )


def test_validate_positions_sizes_sell_long_ok(simulation_fixture):
    SIMULATED_BROKER.positions_sizes[PositionType.LONG] = BUY_LONG_AMOUNT_OK
    SIMULATED_BROKER.validate_positions_sizes(
        PositionType.LONG, ActionType.SELL, SELL_LONG_AMOUNT_OK
    )


def test_validate_positions_sizes_sell_long_ko(simulation_fixture):
    SIMULATED_BROKER.positions_sizes[PositionType.LONG] = BUY_LONG_AMOUNT_KO
    with pytest.raises(Exception):
        SIMULATED_BROKER.validate_positions_sizes(
            PositionType.LONG, ActionType.SELL, SELL_LONG_AMOUNT_KO
        )


def test_validate_positions_sizes_sell_short_ok(simulation_fixture):
    SIMULATED_BROKER.validate_positions_sizes(
        PositionType.SHORT, ActionType.SELL, SELL_SHORT_AMOUNT_OK
    )


def test_validate_positions_sizes_buy_short_ok(simulation_fixture):
    SIMULATED_BROKER.positions_sizes[PositionType.SHORT] = SELL_SHORT_AMOUNT_OK
    SIMULATED_BROKER.validate_positions_sizes(
        PositionType.SHORT, ActionType.BUY, BUY_SHORT_AMOUNT_OK
    )


def test_validate_positions_sizes_buy_short_ko(simulation_fixture):
    SIMULATED_BROKER.positions_sizes[PositionType.SHORT] = SELL_SHORT_AMOUNT_KO
    with pytest.raises(Exception):
        SIMULATED_BROKER.validate_positions_sizes(
            PositionType.SHORT, ActionType.BUY, BUY_SHORT_AMOUNT_KO
        )


def test_validate_cash_buy_ok(simulation_fixture):
    SIMULATED_BROKER.add_cash(FUND)
    SIMULATED_BROKER.validate_cash(ActionType.BUY, TOTAL_COST_ESTIMATE_OK)


def test_validate_cash_buy_ko(simulation_fixture):
    SIMULATED_BROKER.add_cash(FUND)
    with pytest.raises(Exception):
        SIMULATED_BROKER.validate_cash(ActionType.BUY, TOTAL_COST_ESTIMATE_KO)


def test_validate_cash_sell_ok(simulation_fixture):
    SIMULATED_BROKER.add_cash(FUND)
    SIMULATED_BROKER.validate_cash(ActionType.SELL, TOTAL_COST_ESTIMATE_OK)


def test_validate_transaction_buy_long_ok(simulation_fixture):
    SIMULATED_BROKER.add_cash(FUND)
    SIMULATED_BROKER.validate_transaction(
        PositionType.LONG, ActionType.BUY, BUY_LONG_AMOUNT_OK, TOTAL_COST_ESTIMATE_OK
    )


def test_validate_transaction_buy_long_ko(simulation_fixture):
    SIMULATED_BROKER.add_cash(FUND)
    with pytest.raises(Exception):
        SIMULATED_BROKER.validate_transaction(
            PositionType.LONG,
            ActionType.BUY,
            BUY_LONG_AMOUNT_OK,
            TOTAL_COST_ESTIMATE_KO,
        )


def test_validate_transaction_sell_long_ok(simulation_fixture):
    SIMULATED_BROKER.positions_sizes[PositionType.LONG] = BUY_LONG_AMOUNT_OK
    SIMULATED_BROKER.validate_transaction(
        PositionType.LONG, ActionType.SELL, SELL_LONG_AMOUNT_OK, TOTAL_COST_ESTIMATE_OK
    )


def test_validate_transaction_sell_long_ko(simulation_fixture):
    SIMULATED_BROKER.positions_sizes[PositionType.LONG] = BUY_LONG_AMOUNT_KO
    with pytest.raises(Exception):
        SIMULATED_BROKER.validate_transaction(
            PositionType.LONG,
            ActionType.SELL,
            SELL_LONG_AMOUNT_KO,
            TOTAL_COST_ESTIMATE_OK,
        )


def test_validate_transaction_sell_short_ok(simulation_fixture):
    SIMULATED_BROKER.validate_transaction(
        PositionType.SHORT,
        ActionType.SELL,
        SELL_SHORT_AMOUNT_OK,
        TOTAL_COST_ESTIMATE_OK,
    )


def test_validate_transaction_sell_short_ko(simulation_fixture):
    SIMULATED_BROKER.validate_transaction(
        PositionType.SHORT,
        ActionType.SELL,
        SELL_SHORT_AMOUNT_OK,
        TOTAL_COST_ESTIMATE_KO,
    )


def test_validate_transaction_buy_short_ok(simulation_fixture):
    SIMULATED_BROKER.add_cash(FUND)
    SIMULATED_BROKER.positions_sizes[PositionType.SHORT] = SELL_SHORT_AMOUNT_OK
    SIMULATED_BROKER.validate_transaction(
        PositionType.SHORT, ActionType.BUY, BUY_SHORT_AMOUNT_OK, TOTAL_COST_ESTIMATE_OK
    )


def test_validate_transaction_buy_short_ko(simulation_fixture):
    SIMULATED_BROKER.add_cash(FUND)
    SIMULATED_BROKER.positions_sizes[PositionType.SHORT] = SELL_SHORT_AMOUNT_KO
    with pytest.raises(Exception):
        SIMULATED_BROKER.validate_transaction(
            PositionType.SHORT,
            ActionType.BUY,
            BUY_SHORT_AMOUNT_KO,
            TOTAL_COST_ESTIMATE_OK,
        )


def test_update_shares_ok(simulation_fixture):

    SIMULATED_BROKER.update_positions_sizes(
        PositionType.LONG, ActionType.BUY, BUY_LONG_AMOUNT_OK
    )
    assert SIMULATED_BROKER.positions_sizes[PositionType.LONG] == BUY_LONG_AMOUNT_OK

    SIMULATED_BROKER.update_positions_sizes(
        PositionType.LONG, ActionType.SELL, SELL_LONG_AMOUNT_OK
    )
    assert (
        SIMULATED_BROKER.positions_sizes[PositionType.LONG]
        == BUY_LONG_AMOUNT_OK - SELL_LONG_AMOUNT_OK
    )

    SIMULATED_BROKER.update_positions_sizes(
        PositionType.SHORT, ActionType.SELL, SELL_SHORT_AMOUNT_OK
    )
    assert SIMULATED_BROKER.positions_sizes[PositionType.SHORT] == -SELL_SHORT_AMOUNT_OK

    SIMULATED_BROKER.update_positions_sizes(
        PositionType.SHORT, ActionType.BUY, BUY_SHORT_AMOUNT_OK
    )
    assert (
        SIMULATED_BROKER.positions_sizes[PositionType.SHORT]
        == BUY_SHORT_AMOUNT_OK - SELL_SHORT_AMOUNT_OK
    )


def test_update_portfolio_value(simulation_fixture):
    SIMULATED_BROKER.update_positions_sizes(
        PositionType.LONG, ActionType.BUY, BUY_LONG_AMOUNT_OK
    )
    unit_cost_estimate = 10
    SIMULATED_BROKER.update_portfolio_value(PositionType.LONG, unit_cost_estimate)
    assert SIMULATED_BROKER.portfolio_value == BUY_LONG_AMOUNT_OK * unit_cost_estimate


def test_update_cash_without_commission(simulation_fixture):
    SIMULATED_BROKER.add_cash(FUND)

    commission_amount = 0
    SIMULATED_BROKER.update_cash(
        ActionType.BUY, COST_ESTIMATE_BUY_OK, commission_amount
    )
    assert SIMULATED_BROKER.cash == FUND - COST_ESTIMATE_BUY_OK

    SIMULATED_BROKER.update_cash(ActionType.SELL, 400, commission_amount)
    assert SIMULATED_BROKER.cash == (FUND - COST_ESTIMATE_BUY_OK + COST_ESTIMATE_SELL)


def test_update_cash_with_commission(simulation_fixture):
    SIMULATED_BROKER.add_cash(FUND)

    commission_amount = COST_ESTIMATE_BUY_OK * COMMISSION
    SIMULATED_BROKER.update_cash(
        ActionType.BUY, COST_ESTIMATE_BUY_OK, commission_amount
    )
    assert SIMULATED_BROKER.cash == FUND - COST_ESTIMATE_BUY_OK - commission_amount

    SIMULATED_BROKER.update_cash(ActionType.SELL, 400, commission_amount)
    assert SIMULATED_BROKER.cash == (
        FUND - COST_ESTIMATE_BUY_OK + COST_ESTIMATE_SELL - 2 * commission_amount
    )


def test_compute_profit(simulation_fixture):
    SIMULATED_BROKER.last_transaction_price = {
        PositionType.LONG: {
            ActionType.BUY: Decimal("200"),
            ActionType.SELL: Decimal("201"),
        },
        PositionType.SHORT: {
            ActionType.SELL: Decimal("250"),
            ActionType.BUY: Decimal("150"),
        },
    }
    assert SIMULATED_BROKER.compute_profit(PositionType.LONG) == Decimal("1")
    assert SIMULATED_BROKER.compute_profit(PositionType.SHORT) == Decimal("100")


def test_position_buy_sell_long_ok(simulation_fixture):
    SIMULATED_BROKER.add_cash(FUND)

    SIMULATED_BROKER.submit_order(
        SYMBOL,
        PositionType.LONG,
        ActionType.BUY,
        UNIT_COST_ESTIMATE,
        BUY_LONG_AMOUNT_OK,
    )
    assert SIMULATED_BROKER.positions_sizes[PositionType.LONG] == BUY_LONG_AMOUNT_OK
    assert (
        SIMULATED_BROKER.portfolio_value
        == SIMULATED_BROKER.positions_sizes[PositionType.LONG] * UNIT_COST_ESTIMATE
    )
    assert SIMULATED_BROKER.cash == FUND - SIMULATED_BROKER.portfolio_value

    SIMULATED_BROKER.submit_order(
        SYMBOL,
        PositionType.LONG,
        ActionType.SELL,
        UNIT_COST_ESTIMATE,
        SELL_LONG_AMOUNT_OK,
    )
    assert (
        SIMULATED_BROKER.positions_sizes[PositionType.LONG]
        == BUY_LONG_AMOUNT_OK - SELL_LONG_AMOUNT_OK
    )
    assert (
        SIMULATED_BROKER.portfolio_value
        == SIMULATED_BROKER.positions_sizes[PositionType.LONG] * UNIT_COST_ESTIMATE
    )
    assert SIMULATED_BROKER.cash == FUND - SIMULATED_BROKER.portfolio_value


def test_position_buy_sell_long_ko(simulation_fixture):
    SIMULATED_BROKER.add_cash(FUND)

    SIMULATED_BROKER.submit_order(
        SYMBOL,
        PositionType.LONG,
        ActionType.BUY,
        UNIT_COST_ESTIMATE,
        BUY_LONG_AMOUNT_KO,
    )
    assert SIMULATED_BROKER.positions_sizes[PositionType.LONG] == BUY_LONG_AMOUNT_KO
    assert (
        SIMULATED_BROKER.portfolio_value
        == SIMULATED_BROKER.positions_sizes[PositionType.LONG] * UNIT_COST_ESTIMATE
    )
    assert SIMULATED_BROKER.cash == FUND - SIMULATED_BROKER.portfolio_value
    cash_before_call = SIMULATED_BROKER.cash
    with pytest.raises(Exception):
        SIMULATED_BROKER.submit_order(
            SYMBOL,
            PositionType.LONG,
            ActionType.SELL,
            UNIT_COST_ESTIMATE,
            SELL_LONG_AMOUNT_KO,
        )
    assert SIMULATED_BROKER.positions_sizes[PositionType.LONG] == BUY_LONG_AMOUNT_KO
    assert (
        SIMULATED_BROKER.portfolio_value
        == SIMULATED_BROKER.positions_sizes[PositionType.LONG] * UNIT_COST_ESTIMATE
    )
    assert SIMULATED_BROKER.cash == cash_before_call


def test_position_sell_buy_short_ok(simulation_fixture):
    SIMULATED_BROKER.add_cash(FUND)

    SIMULATED_BROKER.submit_order(
        SYMBOL,
        PositionType.SHORT,
        ActionType.SELL,
        UNIT_COST_ESTIMATE,
        SELL_SHORT_AMOUNT_OK,
    )
    assert SIMULATED_BROKER.positions_sizes[PositionType.SHORT] == -SELL_SHORT_AMOUNT_OK
    assert (
        SIMULATED_BROKER.portfolio_value
        == SIMULATED_BROKER.positions_sizes[PositionType.SHORT] * UNIT_COST_ESTIMATE
    )
    assert SIMULATED_BROKER.cash == FUND - SIMULATED_BROKER.portfolio_value

    SIMULATED_BROKER.submit_order(
        SYMBOL,
        PositionType.SHORT,
        ActionType.BUY,
        UNIT_COST_ESTIMATE,
        BUY_SHORT_AMOUNT_OK,
    )
    assert (
        SIMULATED_BROKER.positions_sizes[PositionType.SHORT]
        == BUY_SHORT_AMOUNT_OK - SELL_SHORT_AMOUNT_OK
    )
    assert (
        SIMULATED_BROKER.portfolio_value
        == SIMULATED_BROKER.positions_sizes[PositionType.SHORT] * UNIT_COST_ESTIMATE
    )
    assert SIMULATED_BROKER.cash == FUND - SIMULATED_BROKER.portfolio_value


def test_position_sell_buy_short_ko(simulation_fixture):
    SIMULATED_BROKER.add_cash(FUND)

    SIMULATED_BROKER.submit_order(
        SYMBOL,
        PositionType.SHORT,
        ActionType.SELL,
        UNIT_COST_ESTIMATE,
        SELL_SHORT_AMOUNT_KO,
    )
    assert SIMULATED_BROKER.positions_sizes[PositionType.SHORT] == -SELL_SHORT_AMOUNT_KO
    assert (
        SIMULATED_BROKER.portfolio_value
        == SIMULATED_BROKER.positions_sizes[PositionType.SHORT] * UNIT_COST_ESTIMATE
    )
    assert SIMULATED_BROKER.cash == FUND - SIMULATED_BROKER.portfolio_value

    with pytest.raises(Exception):
        SIMULATED_BROKER.submit_order(
            SYMBOL,
            PositionType.SHORT,
            ActionType.BUY,
            UNIT_COST_ESTIMATE,
            BUY_SHORT_AMOUNT_KO,
        )
    assert SIMULATED_BROKER.positions_sizes[PositionType.SHORT] == -SELL_SHORT_AMOUNT_KO
    assert (
        SIMULATED_BROKER.portfolio_value
        == SIMULATED_BROKER.positions_sizes[PositionType.SHORT] * UNIT_COST_ESTIMATE
    )
    assert SIMULATED_BROKER.cash == FUND - SIMULATED_BROKER.portfolio_value
