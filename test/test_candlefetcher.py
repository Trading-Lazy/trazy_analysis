from datetime import datetime
from decimal import Decimal

import pandas as pd

from strategy.candlefetcher import CandleFetcher
from strategy.constants import DATE_FORMAT
from actionsapi.models import Candle
from tools.tools import clean_candles_in_db, compare_candles_list

SYMBOL = "ANX.PA"
CANDLES = [
    Candle(
        _id=1,
        symbol=SYMBOL,
        open=Decimal("94.1200"),
        high=Decimal("94.1500"),
        low=Decimal("94.0000"),
        close=Decimal("94.1300"),
        volume=7,
        timestamp="2020-05-08 14:17:00",
    ),
    Candle(
        _id=2,
        symbol=SYMBOL,
        open=Decimal("94.0700"),
        high=Decimal("94.1000"),
        low=Decimal("93.9500"),
        close=Decimal("94.0800"),
        volume=91,
        timestamp="2020-05-08 14:24:00",
    ),
    Candle(
        _id=3,
        symbol=SYMBOL,
        open=Decimal("94.0700"),
        high=Decimal("94.1000"),
        low=Decimal("93.9500"),
        close=Decimal("94.0800"),
        volume=0,
        timestamp="2020-05-08 14:24:56",
    ),
    Candle(
        _id=4,
        symbol=SYMBOL,
        open=Decimal("94.1700"),
        high=Decimal("94.1800"),
        low=Decimal("94.0500"),
        close=Decimal("94.1800"),
        volume=0,
        timestamp="2020-05-08 14:35:00",
    ),
    Candle(
        _id=5,
        symbol=SYMBOL,
        open=Decimal("94.1900"),
        high=Decimal("94.2200"),
        low=Decimal("94.0700"),
        close=Decimal("94.2000"),
        volume=0,
        timestamp="2020-05-08 14:41:00",
    ),
    Candle(
        _id=6,
        symbol=SYMBOL,
        open=Decimal("94.1900"),
        high=Decimal("94.2200"),
        low=Decimal("94.0700"),
        close=Decimal("94.2000"),
        volume=7,
        timestamp="2020-05-08 14:41:58",
    ),
]


def test_get_candles_from_db():
    clean_candles_in_db()
    for candle in CANDLES:
        candle.save()
    start_str, end_str = CANDLES[0].timestamp, CANDLES[-1].timestamp
    start = datetime.strptime(start_str, DATE_FORMAT)
    end = datetime.strptime(end_str, DATE_FORMAT)
    candles = CandleFetcher.get_candles_from_db(SYMBOL, start, end)
    assert compare_candles_list(candles, CANDLES)
    clean_candles_in_db()


def test_resample_candle_data():
    candles = {
        "timestamp": [
            datetime.strptime("2020-05-08 14:17:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:24:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:24:56", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:35:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:41:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:41:58", DATE_FORMAT),
        ],
        "symbol": ["ANX.PA", "ANX.PA", "ANX.PA", "ANX.PA", "ANX.PA", "ANX.PA"],
        "open": [94.1200, 94.0700, 94.0700, 94.1700, 94.1900, 94.1900],
        "high": [94.1500, 94.1000, 94.1000, 94.1800, 94.2200, 94.2200],
        "low": [94.0000, 93.9500, 93.9500, 94.0500, 94.0700, 94.0700],
        "close": [94.1300, 94.0800, 94.0800, 94.1800, 94.2000, 94.2000],
        "volume": [7, 91, 0, 0, 0, 0],
    }
    df = pd.DataFrame(
        candles,
        columns=["timestamp", "symbol", "open", "high", "low", "close", "volume",],
    )
    df = CandleFetcher.resample_candle_data(df, pd.offsets.Minute(5))

    expected_df_candles = {
        "timestamp": [
            datetime.strptime("2020-05-08 14:20:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:25:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:30:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:35:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:40:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:45:00", DATE_FORMAT),
        ],
        "symbol": ["ANX.PA", "ANX.PA", "ANX.PA", "ANX.PA", "ANX.PA", "ANX.PA"],
        "open": [94.12, 94.07, 94.07, 94.17, 94.17, 94.19],
        "high": [94.15, 94.10, 94.10, 94.18, 94.18, 94.22],
        "low": [94.00, 93.95, 93.95, 94.05, 94.05, 94.07],
        "close": [94.13, 94.08, 94.08, 94.18, 94.18, 94.20],
        "volume": [7, 91, 0, 0, 0, 0],
    }
    expected_df = pd.DataFrame(
        expected_df_candles,
        columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"],
    )
    expected_df.index = pd.to_datetime(expected_df.timestamp, format=DATE_FORMAT)
    expected_df = expected_df.drop(["timestamp"], axis=1)

    assert (df == expected_df).all(axis=None)


def test_fetch():
    expected_df_candles = {
        "timestamp": [
            datetime.strptime("2020-05-08 14:20:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:25:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:30:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:35:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:40:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:45:00", DATE_FORMAT),
        ],
        "symbol": ["ANX.PA", "ANX.PA", "ANX.PA", "ANX.PA", "ANX.PA", "ANX.PA"],
        "open": [94.12, 94.07, 94.07, 94.17, 94.17, 94.19],
        "high": [94.15, 94.10, 94.10, 94.18, 94.18, 94.22],
        "low": [94.00, 93.95, 93.95, 94.05, 94.05, 94.07],
        "close": [94.13, 94.08, 94.08, 94.18, 94.18, 94.20],
        "volume": [7, 91, 0, 0, 0, 7],
    }
    expected_df = pd.DataFrame(
        expected_df_candles,
        columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"],
    )
    expected_df.index = pd.to_datetime(expected_df.timestamp, format=DATE_FORMAT)
    expected_df = expected_df.drop(["timestamp"], axis=1)

    clean_candles_in_db()
    for candle in CANDLES:
        candle.save()
    start_str, end_str = CANDLES[0].timestamp, CANDLES[-1].timestamp
    start = datetime.strptime(start_str, DATE_FORMAT)
    end = datetime.strptime(end_str, DATE_FORMAT)
    df = CandleFetcher.fetch(SYMBOL, pd.offsets.Minute(5), start, end)
    clean_candles_in_db()

    assert (df == expected_df).all(axis=None)
