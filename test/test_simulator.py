from decimal import Decimal
from multiprocessing import Queue

import pandas as pd

from broker.simulated_broker import SimulatedBroker
from common.exchange_calendar_euronext import EuronextExchangeCalendar
from db_storage.mongodb_storage import MongoDbStorage
from file_storage.meganz_file_storage import MegaNzFileStorage
from models.candle import Candle
from models.enums import Direction
from settings import DATABASE_NAME
from simulator.simulation import Simulation
from simulator.simulator import Simulator
from strategy.strategies.buy_and_sell_long_strategy import BuyAndSellLongStrategy
from strategy.strategies.dumb_long_strategy import DumbLongStrategy
from strategy.strategies.dumb_short_strategy import DumbShortStrategy
from strategy.strategies.sell_and_buy_short_strategy import SellAndBuyShortStrategy
from test.tools.tools import compare_candles_list

SYMBOL = "ANX.PA"
DB_STORAGE = MongoDbStorage(DATABASE_NAME)
FILE_STORAGE = MegaNzFileStorage()
MARKET_CAL = EuronextExchangeCalendar()
OBJECT_ID_BASE = "5eae9ddd4d6f4e006f67c9c"
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
FUND = Decimal("1000")
COMMISSION = Decimal("0.001")
START_TIMESTAMP = pd.Timestamp("2017-10-05 08:00:00", tz="UTC")
BROKER = SimulatedBroker(MARKET_CAL, START_TIMESTAMP, initial_funds=FUND)


def test_add_strategy():
    simulator = Simulator(DB_STORAGE)
    simulator.add_strategy(DumbLongStrategy(SYMBOL, DB_STORAGE, BROKER), False)
    simulator.add_strategy(DumbShortStrategy(SYMBOL, DB_STORAGE, BROKER), False)
    assert len(simulator.simulations) == 2


def test_add_candles_from_dataframe():
    simulator = Simulator(DB_STORAGE)
    columns_values = {
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
    DB_STORAGE.clean_all_candles()

    for candle in CANDLES:
        DB_STORAGE.add_candle(candle)

    simulator = Simulator(DB_STORAGE)
    simulator.add_candles_from_db(
        DB_STORAGE,
        FILE_STORAGE,
        MARKET_CAL,
        SYMBOL,
        CANDLES[0].timestamp,
        CANDLES[-1].timestamp,
    )
    DB_STORAGE.clean_all_candles()
    assert compare_candles_list(simulator.candles, CANDLES)


def test_add_candles_from_csv():
    simulator = Simulator(DB_STORAGE)
    simulator.add_candles_from_csv("test/data/candles.csv")
    assert compare_candles_list(simulator.candles, CANDLES)


def test_set_candles():
    simulator = Simulator(DB_STORAGE)
    simulator.add_strategy(DumbLongStrategy(SYMBOL, DB_STORAGE, BROKER), False)
    simulator.add_strategy(DumbShortStrategy(SYMBOL, DB_STORAGE, BROKER), False)
    simulator.set_candles(CANDLES)
    assert compare_candles_list(simulator.candles, CANDLES)
    for simulation in simulator.simulations:
        assert compare_candles_list(simulation.candles, CANDLES)


def test_fund():
    simulator = Simulator(DB_STORAGE)
    simulator.add_strategy(DumbLongStrategy(SYMBOL, DB_STORAGE, BROKER), False)
    simulator.add_strategy(DumbShortStrategy(SYMBOL, DB_STORAGE, BROKER), False)
    simulator.fund(FUND)
    assert simulator.cash == FUND
    for simulation in simulator.simulations:
        assert simulation.cash == FUND


def test_set_commission():
    simulator = Simulator(DB_STORAGE)
    simulator.add_strategy(DumbLongStrategy(SYMBOL, DB_STORAGE, BROKER), False)
    simulator.add_strategy(DumbShortStrategy(SYMBOL, DB_STORAGE, BROKER), False)
    simulator.set_commission(COMMISSION)
    assert simulator.commission == COMMISSION
    for simulation in simulator.simulations:
        assert simulation.commission == COMMISSION


def test_run_simulation():
    simulator = Simulator(DB_STORAGE)
    buyAndSellLongStrategy = BuyAndSellLongStrategy(SYMBOL, DB_STORAGE, BROKER)
    DB_STORAGE.clean_all_candles()
    for candle in CANDLES:
        DB_STORAGE.add_candle(candle)
    simulation = Simulation(
        buyAndSellLongStrategy, DB_STORAGE, CANDLES, FUND, COMMISSION, SYMBOL, False
    )
    final_states = Queue()
    simulator.run_simulation(simulation, final_states)

    expected_final_states = {
        buyAndSellLongStrategy.name: {
            "shares_amounts": {
                Direction.LONG: Decimal("0"),
                Direction.SHORT: Decimal("0"),
            },
            "portfolio_value": Decimal("0"),
            "cash": Decimal("999.48513"),
        }
    }
    while not final_states.empty():
        final_state = final_states.get()
        strategy_name = final_state["strategy_name"]
        assert (
            final_state["shares_amounts"][Direction.LONG]
            == expected_final_states[strategy_name]["shares_amounts"][Direction.LONG]
        )
        assert (
            final_state["portfolio_value"].normalize()
            == expected_final_states[strategy_name]["portfolio_value"].normalize()
        )
        assert (
            final_state["cash"].normalize()
            == expected_final_states[strategy_name]["cash"].normalize()
        )
    DB_STORAGE.clean_all_candles()


def test_run_without_commission():
    simulator = Simulator(DB_STORAGE)

    start = CANDLES[0].timestamp
    end = CANDLES[-1].timestamp
    DB_STORAGE.clean_all_candles()
    for candle in CANDLES:
        DB_STORAGE.add_candle(candle)
    simulator.add_candles_from_db(
        DB_STORAGE, FILE_STORAGE, MARKET_CAL, "ANX.PA", start, end
    )

    simulator.fund(FUND)
    buyAndSellLongStrategy = BuyAndSellLongStrategy(SYMBOL, DB_STORAGE, BROKER)
    sellAndBuyShortStrategy = SellAndBuyShortStrategy(SYMBOL, DB_STORAGE, BROKER)
    simulator.add_strategy(buyAndSellLongStrategy, False)
    simulator.add_strategy(sellAndBuyShortStrategy, False)
    expected_final_states = {
        buyAndSellLongStrategy.name: {
            "shares_amounts": {
                Direction.LONG: Decimal("0"),
                Direction.SHORT: Decimal("0"),
            },
            "portfolio_value": Decimal("0"),
            "cash": Decimal("1000.05"),
        },
        sellAndBuyShortStrategy.name: {
            "shares_amounts": {
                Direction.LONG: Decimal("0"),
                Direction.SHORT: Decimal("0"),
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
            final_state["shares_amounts"][Direction.LONG]
            == expected_final_states[strategy_name]["shares_amounts"][Direction.LONG]
        )
        assert (
            final_state["portfolio_value"].normalize()
            == expected_final_states[strategy_name]["portfolio_value"].normalize()
        )
        assert (
            final_state["cash"].normalize()
            == expected_final_states[strategy_name]["cash"].normalize()
        )

    DB_STORAGE.clean_all_candles()


def test_run_with_commission():
    simulator = Simulator(DB_STORAGE)

    start = CANDLES[0].timestamp
    end = CANDLES[-1].timestamp
    DB_STORAGE.clean_all_candles()
    for candle in CANDLES:
        DB_STORAGE.add_candle(candle)
    simulator.add_candles_from_db(
        DB_STORAGE, FILE_STORAGE, MARKET_CAL, "ANX.PA", start, end
    )

    simulator.fund(FUND)
    simulator.set_commission(COMMISSION)
    buyAndSellLongStrategy = BuyAndSellLongStrategy(SYMBOL, DB_STORAGE, BROKER)
    sellAndBuyShortStrategy = SellAndBuyShortStrategy(SYMBOL, DB_STORAGE, BROKER)
    simulator.add_strategy(buyAndSellLongStrategy, False)
    simulator.add_strategy(sellAndBuyShortStrategy, False)
    expected_final_states = {
        buyAndSellLongStrategy.name: {
            "shares_amounts": {
                Direction.LONG: Decimal("0"),
                Direction.SHORT: Decimal("0"),
            },
            "portfolio_value": Decimal("0"),
            "cash": Decimal("999.48513"),
        },
        sellAndBuyShortStrategy.name: {
            "shares_amounts": {
                Direction.LONG: Decimal("0"),
                Direction.SHORT: Decimal("0"),
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
            final_state["shares_amounts"][Direction.LONG]
            == expected_final_states[strategy_name]["shares_amounts"][Direction.LONG]
        )
        assert (
            final_state["portfolio_value"].normalize()
            == expected_final_states[strategy_name]["portfolio_value"].normalize()
        )
        assert (
            final_state["cash"].normalize()
            == expected_final_states[strategy_name]["cash"].normalize()
        )

    DB_STORAGE.clean_all_candles()
