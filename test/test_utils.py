import pandas as pd
import pytest

from common.utils import validate_dataframe_columns


def test_validate_dataframe_columns_ok():
    required_columns = ["id", "timestamp", "symbol", "open", "high", "low", "close", "volume"]
    df = pd.DataFrame(
        {},
        columns=required_columns,
    )
    validate_dataframe_columns(df, required_columns)

def test_validate_dataframe_columns_empty_columns_ko():
    required_columns = ["id", "timestamp", "symbol", "open", "high", "low", "close", "volume"]
    df = pd.DataFrame(
        {},
        columns=[],
    )
    with pytest.raises(Exception):
        validate_dataframe_columns(df, required_columns)

def test_validate_dataframe_columns_different_columns_len_ko():
    required_columns = ["id", "timestamp", "symbol", "open", "high", "low", "close", "volume"]
    df = pd.DataFrame(
        {},
        columns=["id", "timestamp", "symbol", "open", "high", "low", "close"],
    )
    with pytest.raises(Exception):
        validate_dataframe_columns(df, required_columns)

def test_validate_dataframe_columns_different_values_ko():
    required_columns = ["id", "timestamp", "symbol", "open", "high", "low", "close", "volume"]
    df = pd.DataFrame(
        {},
        columns=["id", "timestamp", "symbol", "open", "high", "low", "close", "other"],
    )
    with pytest.raises(Exception):
        validate_dataframe_columns(df, required_columns)