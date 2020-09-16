from datetime import datetime

import pandas as pd
import pytest
import pytz
from dateutil.parser import parse

from common.utils import lists_equal, timestamp_to_utc, validate_dataframe_columns


def test_validate_dataframe_columns_ok():
    required_columns = [
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
        "timestamp",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    df = pd.DataFrame(
        {}, columns=["timestamp", "symbol", "open", "high", "low", "close"],
    )
    with pytest.raises(Exception):
        validate_dataframe_columns(df, required_columns)


def test_validate_dataframe_columns_different_values_ko():
    required_columns = [
        "timestamp",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    df = pd.DataFrame(
        {}, columns=["timestamp", "symbol", "open", "high", "low", "close", "other"],
    )
    with pytest.raises(Exception):
        validate_dataframe_columns(df, required_columns)


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


@pytest.mark.parametrize(
    "timestamp, expected_utc_timestamp",
    [
        (
            pd.Timestamp("2020-05-08 14:16:00"),
            pd.Timestamp("2020-05-08 14:16:00+00:00"),
        ),
        (
            pd.Timestamp("2020-05-08 14:16:00+00:00"),
            pd.Timestamp("2020-05-08 14:16:00+00:00"),
        ),
        (
            pd.Timestamp("2020-05-08 16:16:00+02:00"),
            pd.Timestamp("2020-05-08 14:16:00+00:00"),
        ),
        (datetime(2020, 5, 8, 14, 16), datetime(2020, 5, 8, 14, 16, tzinfo=pytz.UTC),),
        (
            datetime(2020, 5, 8, 14, 16, tzinfo=pytz.UTC),
            datetime(2020, 5, 8, 14, 16, tzinfo=pytz.UTC),
        ),
        (
            parse("2020-05-08 16:16:00+02:00"),
            datetime(2020, 5, 8, 14, 16, tzinfo=pytz.UTC),
        ),
    ],
)
def test_timestamp_to_utc(timestamp, expected_utc_timestamp):
    timestamp_with_utc = timestamp_to_utc(timestamp)
    assert timestamp_with_utc == expected_utc_timestamp
    assert str(timestamp_with_utc) == str(expected_utc_timestamp)
    with pytest.raises(Exception):
        timestamp_to_utc(object())
