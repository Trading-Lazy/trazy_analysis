from datetime import datetime
from decimal import Decimal

import pandas as pd

from simulator.simulator import Simulator
from actionsapi.models import Candle, PositionType
from strategy.constants import DATE_FORMAT
from strategy.strategies.BuyAndSellLongStrategy import BuyAndSellLongStrategy
from strategy.strategies.DumbLongStrategy import DumbLongStrategy
from strategy.strategies.DumbShortStrategy import DumbShortStrategy
from strategy.strategies.SellAndBuyShortStrategy import SellAndBuyShortStrategy
from test.tools.tools import compare_candles_list, clean_candles_in_db

SYMBOL = "ANX.PA"
OBJECT_ID_BASE = "5eae9ddd4d6f4e006f67c9c"
CANDLES = [
    Candle(
        _id=OBJECT_ID_BASE + "1",
        symbol=SYMBOL,
        open=Decimal("94.1200"),
        high=Decimal("94.1500"),
        low=Decimal("94.0000"),
        close=Decimal("94.1300"),
        volume=7,
        timestamp=pd.Timestamp("2020-05-08 14:17:00", tz='UTC'),
    ),
    Candle(
        _id=OBJECT_ID_BASE + "2",
        symbol=SYMBOL,
        open=Decimal("94.0700"),
        high=Decimal("94.1000"),
        low=Decimal("93.9500"),
        close=Decimal("94.0800"),
        volume=91,
        timestamp=pd.Timestamp("2020-05-08 14:24:00", tz='UTC'),
    ),
    Candle(
        _id=OBJECT_ID_BASE + "3",
        symbol=SYMBOL,
        open=Decimal("94.0700"),
        high=Decimal("94.1000"),
        low=Decimal("93.9500"),
        close=Decimal("94.0800"),
        volume=0,
        timestamp=pd.Timestamp("2020-05-08 14:24:56", tz='UTC'),
    ),
    Candle(
        _id=OBJECT_ID_BASE + "4",
        symbol=SYMBOL,
        open=Decimal("94.1700"),
        high=Decimal("94.1800"),
        low=Decimal("94.0500"),
        close=Decimal("94.1800"),
        volume=0,
        timestamp=pd.Timestamp("2020-05-08 14:35:00", tz='UTC'),
    ),
    Candle(
        _id=OBJECT_ID_BASE + "5",
        symbol=SYMBOL,
        open=Decimal("94.1900"),
        high=Decimal("94.2200"),
        low=Decimal("94.0700"),
        close=Decimal("94.2000"),
        volume=0,
        timestamp=pd.Timestamp("2020-05-08 14:41:00", tz='UTC'),
    ),
    Candle(
        _id=OBJECT_ID_BASE + "6",
        symbol=SYMBOL,
        open=Decimal("94.1900"),
        high=Decimal("94.2200"),
        low=Decimal("94.0700"),
        close=Decimal("94.2000"),
        volume=7,
        timestamp=pd.Timestamp("2020-05-08 14:41:58", tz='UTC'),
    ),
]
FUND = Decimal("1000")
COMMISSION = Decimal("0.001")


def test_add_strategy():
    simulator = Simulator()
    simulator.add_strategy(DumbLongStrategy(), False)
    simulator.add_strategy(DumbShortStrategy(), False)
    assert len(simulator.simulations) == 2


def test_add_candles_from_dataframe():
    simulator = Simulator()
    columns_values = {
        "_id": [
            OBJECT_ID_BASE + "1",
            OBJECT_ID_BASE + "2",
            OBJECT_ID_BASE + "3",
            OBJECT_ID_BASE + "4",
            OBJECT_ID_BASE + "5",
            OBJECT_ID_BASE + "6",
        ],
        "timestamp": [
            "2020-05-08 14:17:00",
            "2020-05-08 14:24:00",
            "2020-05-08 14:24:56",
            "2020-05-08 14:35:00",
            "2020-05-08 14:41:00",
            "2020-05-08 14:41:58",
        ],
        "symbol": ["ANX.PA", "ANX.PA", "ANX.PA", "ANX.PA", "ANX.PA", "ANX.PA"],
        "open": [
            Decimal("94.1200"),
            Decimal("94.0700"),
            Decimal("94.0700"),
            Decimal("94.1700"),
            Decimal("94.1900"),
            Decimal("94.1900"),
        ],
        "high": [
            Decimal("94.1500"),
            Decimal("94.1000"),
            Decimal("94.1000"),
            Decimal("94.1800"),
            Decimal("94.2200"),
            Decimal("94.2200"),
        ],
        "low": [
            Decimal("94.0000"),
            Decimal("93.9500"),
            Decimal("93.9500"),
            Decimal("94.0500"),
            Decimal("94.0700"),
            Decimal("94.0700"),
        ],
        "close": [
            Decimal("94.1300"),
            Decimal("94.0800"),
            Decimal("94.0800"),
            Decimal("94.1800"),
            Decimal("94.2000"),
            Decimal("94.2000"),
        ],
        "volume": [7, 91, 0, 0, 0, 7],
    }
    df = pd.DataFrame(
        columns_values,
        columns=[
            "_id",
            "timestamp",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ],
    )
    simulator.add_candles_from_dataframe(df)
    assert compare_candles_list(simulator.candles, CANDLES)


def test_add_candles_data_from_db():
    clean_candles_in_db()

    for candle in CANDLES:
        candle.save()

    simulator = Simulator()
    simulator.add_candles_from_db(SYMBOL, CANDLES[0].timestamp, CANDLES[-1].timestamp)
    clean_candles_in_db()
    assert compare_candles_list(simulator.candles, CANDLES)


def test_add_candles_from_csv():
    simulator = Simulator()
    simulator.add_candles_from_csv("test/data/candles.csv")
    assert compare_candles_list(simulator.candles, CANDLES)


def test_set_candles():
    simulator = Simulator()
    simulator.add_strategy(DumbLongStrategy(), False)
    simulator.add_strategy(DumbShortStrategy(), False)
    simulator.set_candles(CANDLES)
    assert compare_candles_list(simulator.candles, CANDLES)
    for simulation in simulator.simulations:
        assert compare_candles_list(simulation.candles, CANDLES)


def test_fund():
    simulator = Simulator()
    simulator.add_strategy(DumbLongStrategy(), False)
    simulator.add_strategy(DumbShortStrategy(), False)
    simulator.fund(FUND)
    assert simulator.cash == FUND
    for simulation in simulator.simulations:
        assert simulation.cash == FUND


def test_set_commission():
    simulator = Simulator()
    simulator.add_strategy(DumbLongStrategy(), False)
    simulator.add_strategy(DumbShortStrategy(), False)
    simulator.set_commission(COMMISSION)
    assert simulator.commission == COMMISSION
    for simulation in simulator.simulations:
        assert simulation.commission == COMMISSION


def test_run_simulation_without_commission():
    simulator = Simulator()

    start = CANDLES[0].timestamp
    end = CANDLES[-1].timestamp
    clean_candles_in_db()
    for candle in CANDLES:
        candle.save()
    simulator.add_candles_from_db("ANX.PA", start, end)

    simulator.fund(FUND)
    buyAndSellLongStrategy = BuyAndSellLongStrategy()
    sellAndBuyShortStrategy = SellAndBuyShortStrategy()
    simulator.add_strategy(buyAndSellLongStrategy, False)
    simulator.add_strategy(sellAndBuyShortStrategy, False)
    expected_final_states = {
        buyAndSellLongStrategy.name: {
            "shares_amounts": {
                PositionType.LONG: Decimal("0"),
                PositionType.SHORT: Decimal("0"),
            },
            "portfolio_value": Decimal("0"),
            "cash": Decimal("1000.05"),
        },
        sellAndBuyShortStrategy.name: {
            "shares_amounts": {
                PositionType.LONG: Decimal("0"),
                PositionType.SHORT: Decimal("0"),
            },
            "portfolio_value": Decimal("0"),
            "cash": Decimal("999.95"),
        },
    }
    final_states = simulator.run()
    while not final_states.empty():
        final_state = final_states.get()
        strategy_name = final_state["strategy_name"]
        assert (
            final_state["shares_amounts"][PositionType.LONG]
            == expected_final_states[strategy_name]["shares_amounts"][PositionType.LONG]
        )
        assert (
            final_state["portfolio_value"].normalize()
            == expected_final_states[strategy_name]["portfolio_value"].normalize()
        )
        assert (
            final_state["cash"].normalize()
            == expected_final_states[strategy_name]["cash"].normalize()
        )

    clean_candles_in_db()


def test_run_simulation_with_commission():
    simulator = Simulator()

    start= CANDLES[0].timestamp
    end = CANDLES[-1].timestamp
    clean_candles_in_db()
    for candle in CANDLES:
        candle.save()
    simulator.add_candles_from_db("ANX.PA", start, end)

    simulator.fund(FUND)
    simulator.set_commission(COMMISSION)
    buyAndSellLongStrategy = BuyAndSellLongStrategy()
    sellAndBuyShortStrategy = SellAndBuyShortStrategy()
    simulator.add_strategy(buyAndSellLongStrategy, False)
    simulator.add_strategy(sellAndBuyShortStrategy, False)
    expected_final_states = {
        buyAndSellLongStrategy.name: {
            "shares_amounts": {
                PositionType.LONG: Decimal("0"),
                PositionType.SHORT: Decimal("0"),
            },
            "portfolio_value": Decimal("0"),
            "cash": Decimal("999.48513"),
        },
        sellAndBuyShortStrategy.name: {
            "shares_amounts": {
                PositionType.LONG: Decimal("0"),
                PositionType.SHORT: Decimal("0"),
            },
            "portfolio_value": Decimal("0"),
            "cash": Decimal("999.38513"),
        },
    }
    final_states = simulator.run()
    while not final_states.empty():
        final_state = final_states.get()
        strategy_name = final_state["strategy_name"]
        assert (
            final_state["shares_amounts"][PositionType.LONG]
            == expected_final_states[strategy_name]["shares_amounts"][PositionType.LONG]
        )
        assert (
            final_state["portfolio_value"].normalize()
            == expected_final_states[strategy_name]["portfolio_value"].normalize()
        )
        assert (
            final_state["cash"].normalize()
            == expected_final_states[strategy_name]["cash"].normalize()
        )

    clean_candles_in_db()
