from decimal import Decimal

import pandas as pd

from models.candle import Candle

CANDLE1 = Candle(
    symbol="IVV",
    open=Decimal("25"),
    high=Decimal("25.5"),
    low=Decimal("24.8"),
    close=Decimal("25.3"),
    volume=100,
    timestamp=pd.Timestamp("2020-05-08 14:17:00+00:00"),
)
CANDLE1_DICT = {
    "symbol": "IVV",
    "open": "25",
    "high": "25.5",
    "low": "24.8",
    "close": "25.3",
    "volume": 100,
    "timestamp": "2020-05-08 14:17:00+00:00",
}

CANDLE2 = Candle(
    symbol="IVV",
    open=Decimal("25"),
    high=Decimal("25.5"),
    low=Decimal("24.8"),
    close=Decimal("25.3"),
    volume=100,
    timestamp=pd.Timestamp("2020-05-08 14:17:00+00:00"),
)
CANDLE3 = Candle(
    symbol="IVV",
    open=Decimal("25"),
    high=Decimal("25.5"),
    low=Decimal("24.8"),
    close=Decimal("25.4"),
    volume=100,
    timestamp=pd.Timestamp("2020-05-08 14:17:00+00:00"),
)


def test_eq():
    assert CANDLE1 == CANDLE2
    assert not CANDLE2 == CANDLE3
    assert not CANDLE1 == object()


def test_ne():
    assert not CANDLE1 != CANDLE2
    assert CANDLE2 != CANDLE3


def test_from_serializable_dict():
    assert Candle.from_serializable_dict(CANDLE1_DICT) == CANDLE1


def test_to_serializable_dict():
    assert CANDLE1_DICT == CANDLE1.to_serializable_dict()


def test_from_dict():
    candle_dict = {
        "symbol": "ANX.PA",
        "open": 10,
        "high": 11,
        "low": 9,
        "close": 10,
        "volume": 300,
        "timestamp": pd.Timestamp("2020-01-01T12", tz="Europe/Paris"),
    }
    candle: Candle = Candle.from_dict(candle_dict)
    assert candle.symbol == "ANX.PA"
    assert candle.open == Decimal("10")
    assert candle.high == Decimal("11")
    assert candle.low == Decimal("9")
    assert candle.close == Decimal("10")
    assert candle.volume == 300
    assert candle.timestamp == pd.Timestamp("2020-01-01T12", tz="Europe/Paris")


def test_from_json():
    str_json = '{"symbol":"ANX.PA","open":"91.92","high":"92.0","low":"91.0","close":"92.0","volume":20,"timestamp":"2020-04-30T15:30:00.000Z"}'
    candle: Candle = Candle.from_json(str_json)
    assert candle.symbol == "ANX.PA"
    assert candle.open == Decimal("91.92")
    assert candle.high == Decimal("92.0")
    assert candle.low == Decimal("91.0")
    assert candle.close == Decimal("92.0")
    assert candle.volume == 20
    assert candle.timestamp == pd.Timestamp(
        "2020-04-30T15:30:00.000Z", tz="Europe/Paris"
    )


def test_to_json():
    str_json = '{"symbol": "IVV", "open": "25", "high": "25.5", "low": "24.8", "close": "25.3", "volume": 100, "timestamp": "2020-05-08 14:17:00+00:00"}'
    assert CANDLE1.to_json() == str_json


def test_copy():
    assert CANDLE1.copy() == CANDLE1


def test_str():
    expected_str = 'Candle(symbol="IVV",open=Decimal("25"),high=Decimal("25.5"),low=Decimal("24.8"),close=Decimal("25.3"),volume=100,timestamp=pd.Timestamp("2020-05-08 14:17:00+00:00"))'
    assert str(CANDLE1) == expected_str
