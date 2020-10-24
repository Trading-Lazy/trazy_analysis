import math
from decimal import Decimal

import numpy as np
import pandas as pd

from broker.simulated_broker import SimulatedBroker
from common.constants import DATE_FORMAT
from common.exchange_calendar_euronext import EuronextExchangeCalendar
from db_storage.mongodb_storage import MongoDbStorage
from file_storage.meganz_file_storage import MegaNzFileStorage
from models.order import Order
from models.candle import Candle
from models.enums import Action, Direction, OrderType
from settings import DATABASE_NAME
from strategy.strategies.sma_crossover_strategy import SmaCrossoverStrategy

SYMBOL = "ANX.PA"
DB_STORAGE = MongoDbStorage(DATABASE_NAME)
FILE_STORAGE = MegaNzFileStorage()
FUND = Decimal("10000")
START_TIMESTAMP = pd.Timestamp("2017-10-05 08:00:00", tz="UTC")
MARKET_CAL = EuronextExchangeCalendar()
BROKER = SimulatedBroker(MARKET_CAL, START_TIMESTAMP, initial_funds=FUND)
SCO: SmaCrossoverStrategy = SmaCrossoverStrategy(SYMBOL, DB_STORAGE)


def get_df_hist() -> pd.DataFrame:
    df_hist = {
        "timestamp": [
            "2020-05-08 14:00:00",
            "2020-05-08 14:30:00",
            "2020-05-08 15:00:00",
            "2020-05-08 15:30:00",
            "2020-05-08 16:00:00",
        ],
        "symbol": ["ANX.PA", "ANX.PA", "ANX.PA", "ANX.PA", "ANX.PA"],
        "open": [94.12, 94.07, 94.07, 94.17, 94.17],
        "high": [94.15, 94.10, 94.10, 94.18, 94.18],
        "low": [94.00, 93.95, 93.95, 94.05, 94.05],
        "close": [94.13, 94.08, 94.08, 94.18, 94.18],
        "volume": [7, 91, 0, 0, 0],
    }
    df_hist = pd.DataFrame(
        df_hist,
        columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"],
    )
    df_hist["timestamp"] = pd.to_datetime(df_hist["timestamp"], format=DATE_FORMAT)
    df_hist.set_index("timestamp", inplace=True)
    return df_hist


def test_smacrossover_get_time_offset():
    SCO.set_interval("1 day")
    assert SCO.get_time_offset() == pd.offsets.Day(1)

    SCO.set_interval("30 minute")
    assert SCO.get_time_offset() == pd.offsets.Minute(30)


def test_smacrossover_get_candles_with_signals_positions():
    SCO.set_parameters({"interval": "30 minute", "short_period": 2, "long_period": 4})

    df_signals_positions = SCO.get_candles_with_signals_positions(get_df_hist())

    expected_df_signals_positions = {
        "timestamp": [
            "2020-05-08 14:00:00",
            "2020-05-08 14:30:00",
            "2020-05-08 15:00:00",
            "2020-05-08 15:30:00",
            "2020-05-08 16:00:00",
        ],
        "signals": [0.0, 0.0, 0.0, 1.0, 1.0],
        "positions": [np.nan, 0.0, 0.0, 1.0, 0.0],
    }
    expected_df_signals_positions = pd.DataFrame(
        expected_df_signals_positions, columns=["timestamp", "signals", "positions"]
    )
    expected_df_signals_positions["timestamp"] = pd.to_datetime(
        expected_df_signals_positions["timestamp"], format=DATE_FORMAT
    )
    expected_df_signals_positions.set_index("timestamp", inplace=True)

    assert math.isnan(df_signals_positions.iloc[0]["positions"])

    comparison = (
        df_signals_positions[["signals", "positions"]].iloc[1:, :]
        == expected_df_signals_positions[["signals", "positions"]].iloc[1:, :]
    )

    assert comparison.all().all()

    assert math.isnan(df_signals_positions.iloc[0]["positions"])
    assert df_signals_positions.iloc[0]["signals"] == 0


def test_smacrossover_conclude_action_position():
    # action = None and position = None
    df_signals_positions = {
        "timestamp": [
            "2020-05-08 14:00:00",
            "2020-05-08 14:30:00",
            "2020-05-08 15:00:00",
            "2020-05-08 15:30:00",
            "2020-05-08 16:00:00",
        ],
        "signals": [0.0, 0.0, 0.0, 1.0, 1.0],
        "positions": [np.nan, 0.0, 0.0, 1.0, 0.0],
    }
    df_signals_positions = pd.DataFrame(
        df_signals_positions, columns=["timestamp", "signals", "positions"]
    )
    df_signals_positions["timestamp"] = pd.to_datetime(
        df_signals_positions["timestamp"], format=DATE_FORMAT
    )
    df_signals_positions.set_index("timestamp", inplace=True)

    action, position = SmaCrossoverStrategy.conclude_action_position(
        df_signals_positions
    )
    assert action is None and position is None

    # action = BUY and position = LONG
    df_signals_positions = {
        "timestamp": [
            "2020-05-08 14:00:00",
            "2020-05-08 14:30:00",
            "2020-05-08 15:00:00",
            "2020-05-08 15:30:00",
            "2020-05-08 16:00:00",
        ],
        "signals": [0.0, 0.0, 0.0, 0.0, 1.0],
        "positions": [np.nan, 0.0, 0.0, 0.0, 1.0],
    }
    df_signals_positions = pd.DataFrame(
        df_signals_positions, columns=["timestamp", "signals", "positions"]
    )
    df_signals_positions["timestamp"] = pd.to_datetime(
        df_signals_positions["timestamp"], format=DATE_FORMAT
    )
    df_signals_positions.set_index("timestamp", inplace=True)
    action, position = SmaCrossoverStrategy.conclude_action_position(
        df_signals_positions
    )
    assert action == Action.BUY and position == Direction.LONG

    # action = BUY and position = LONG
    df_signals_positions = {
        "timestamp": [
            "2020-05-08 14:00:00",
            "2020-05-08 14:30:00",
            "2020-05-08 15:00:00",
            "2020-05-08 15:30:00",
            "2020-05-08 16:00:00",
        ],
        "signals": [0.0, 0.0, 0.0, 0.0, -1.0],
        "positions": [np.nan, 0.0, 0.0, 0.0, -1.0],
    }
    df_signals_positions = pd.DataFrame(
        df_signals_positions, columns=["timestamp", "signals", "positions"]
    )
    df_signals_positions["timestamp"] = pd.to_datetime(
        df_signals_positions["timestamp"], format=DATE_FORMAT
    )
    df_signals_positions.set_index("timestamp", inplace=True)
    action, position = SmaCrossoverStrategy.conclude_action_position(
        df_signals_positions
    )
    assert action == Action.SELL and position == Direction.LONG


def test_smacrossover_build_order():
    candle = Candle(
        symbol="ANX.PA",
        open=94.10,
        high=94.12,
        low=94.00,
        close=94.12,
        volume=2,
    )
    order1 = SCO.build_order(candle, Action.BUY, Direction.LONG)
    assert order1.action == Action.BUY
    assert order1.direction == Direction.LONG
    assert order1.symbol == "ANX.PA"
    assert order1.strategy == SmaCrossoverStrategy.__name__

    order2 = SCO.build_order(candle, Action.SELL, Direction.LONG)
    assert order2.action == Action.SELL
    assert order2.direction == Direction.LONG
    assert order2.symbol == "ANX.PA"
    assert order2.strategy == SmaCrossoverStrategy.__name__


def test_smacrossover_calc_strategy():
    SCO.set_parameters({"interval": "30 minute", "short_period": 2, "long_period": 4})

    df_hist = {
        "timestamp": [
            "2020-05-08 14:00:00",
            "2020-05-08 14:30:00",
            "2020-05-08 15:00:00",
            "2020-05-08 15:30:00",
            "2020-05-08 16:00:00",
        ],
        "symbol": ["ANX.PA", "ANX.PA", "ANX.PA", "ANX.PA", "ANX.PA"],
        "open": [94.12, 94.07, 94.07, 94.17, 94.17],
        "high": [94.15, 94.10, 94.10, 94.18, 94.18],
        "low": [94.00, 93.95, 93.95, 94.05, 94.05],
        "close": [94.13, 94.08, 94.08, 94.10, 94.18],
        "volume": [7, 91, 0, 0, 0],
    }
    df_hist = pd.DataFrame(
        df_hist,
        columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"],
    )
    df_hist["timestamp"] = pd.to_datetime(df_hist["timestamp"], format=DATE_FORMAT)
    df_hist.set_index("timestamp", inplace=True)
    candle: Candle = Candle(
        symbol="ANX.PA",
        open=94.10,
        high=94.12,
        low=94.00,
        close=94.12,
        volume=2,
    )
    order: Order = SCO.calc_strategy(candle, df_hist)
    assert order.action == Action.BUY
    assert order.direction == Direction.LONG
    assert order.root_candle_timestamp == candle.timestamp


def test_get_last_order():
    DB_STORAGE.clean_all_orders()
    DB_STORAGE.clean_all_candles()
    candle: Candle = Candle(
        symbol="ANX.PA",
        open=Decimal("94.10"),
        high=Decimal("94.12"),
        low=Decimal("94.00"),
        close=Decimal("94.12"),
        volume=2,
        timestamp=pd.Timestamp("2020-05-22 13:00:00", tz="UTC"),
    )
    DB_STORAGE.add_candle(candle)
    order = Order(generation_time=pd.Timestamp("2020-05-22 14:00:00", tz="UTC"))
    DB_STORAGE.add_order(order)
    SCO.set_parameters({"short_period": 3, "long_period": 8, "interval": "1 day"})
    last_order = SCO.get_last_order()
    assert last_order == order
    DB_STORAGE.clean_all_orders()
    DB_STORAGE.clean_all_candles()
