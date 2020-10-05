from datetime import datetime

import pandas as pd
import pytz
from pandas import DatetimeIndex
from pandas.core.frame import DataFrame


def lists_equal(list1: list, list2: list) -> bool:
    return sorted(list1) == sorted(list2)


def validate_dataframe_columns(df: DataFrame, required_columns: list) -> None:
    if not lists_equal(required_columns, df.columns.tolist()):
        raise Exception(
            "The input dataframe is malformed. It must contain only columns: {} but has instead {}".format(
                required_columns, df.columns.tolist()
            )
        )


def timestamp_to_utc(timestamp):
    if isinstance(timestamp, pd.Timestamp) or isinstance(timestamp, pd.DatetimeIndex):
        if timestamp.tz is None:
            timestamp = timestamp.tz_localize("UTC")
        timestamp = timestamp.tz_convert("UTC")
    elif isinstance(timestamp, datetime):
        if timestamp.tzinfo is None or timestamp.tzinfo.utcoffset(timestamp) is None:
            timestamp = pytz.timezone("UTC").localize(timestamp)
        timestamp = timestamp.astimezone(pytz.UTC)
    else:
        raise Exception(
            "Unsupported type: {}. It should be either pandas Timestamp or datetime".format(
                type(timestamp)
            )
        )
    return timestamp
