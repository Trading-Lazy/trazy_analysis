import pandas as pd
import pytest

from common.utils import (
    validate_dataframe_columns,
    build_candle_from_dict,
    build_candle_from_json_string,
    lists_equal,
)
from actionsapi.models import Candle


def test_validate_dataframe_columns_ok():
    required_columns = [
        "id",
        "timestamp",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    df = pd.DataFrame({}, columns=required_columns,)
    validate_dataframe_columns(df, required_columns)


def test_validate_dataframe_columns_empty_columns_ko():
    required_columns = [
        "id",
        "timestamp",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    df = pd.DataFrame({}, columns=[],)
    with pytest.raises(Exception):
        validate_dataframe_columns(df, required_columns)


def test_validate_dataframe_columns_different_columns_len_ko():
    required_columns = [
        "id",
        "timestamp",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    df = pd.DataFrame(
        {}, columns=["id", "timestamp", "symbol", "open", "high", "low", "close"],
    )
    with pytest.raises(Exception):
        validate_dataframe_columns(df, required_columns)


def test_validate_dataframe_columns_different_values_ko():
    required_columns = [
        "id",
        "timestamp",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    df = pd.DataFrame(
        {},
        columns=["id", "timestamp", "symbol", "open", "high", "low", "close", "other"],
    )
    with pytest.raises(Exception):
        validate_dataframe_columns(df, required_columns)


def test_build_candle_from_dict():
    candle_dict = {
        "_id": "5edc1be32e900cd0b2723a81",
        "symbol": "ANX.PA",
        "open": 10,
        "high": 11,
        "low": 9,
        "close": 10,
        "volume": 300,
        "timestamp": pd.Timestamp("2020-01-01T12", tz="Europe/Paris"),
    }
    candle: Candle = build_candle_from_dict(candle_dict)
    assert candle.symbol == "ANX.PA"
    assert candle.open == 10
    assert candle.high == 11
    assert candle.low == 9
    assert candle.close == 10
    assert candle.volume == 300
    assert candle.timestamp == pd.Timestamp("2020-01-01T12", tz="Europe/Paris")
    assert candle._id == "5edc1be32e900cd0b2723a81"


def test_build_candle_from_json_string():
    str_json = '{"_id":null,"symbol":"ANX.PA","open":91.92,"high":92.0,"low":91.0,"close":92.0,"volume":20,"timestamp":"2020-04-30T15:30:00.000Z"}'
    candle: Candle = build_candle_from_json_string(str_json)
    assert candle.symbol == "ANX.PA"
    assert candle.open == 91.92
    assert candle.high == 92.0
    assert candle.low == 91.0
    assert candle.close == 92.0
    assert candle.volume == 20
    assert candle.timestamp == pd.Timestamp(
        "2020-04-30T15:30:00.000Z", tz="Europe/Paris"
    )


#
def test_lists_equal_true():
    l1 = [5, 2, 3, 1, 4]
    l2 = [1, 2, 3, 4, 5]
    assert lists_equal(l1, l2)


def test_lists_equal_false_same_length():
    l1 = [5, 2, 3, 1, 4]
    l2 = [1, 2, 3, 3, 5]
    assert not lists_equal(l1, l2)


def test_lists_equal_false_different_length():
    l1 = [5, 2, 3, 1, 4]
    l2 = [1, 2, 3]
    assert not lists_equal(l1, l2)
