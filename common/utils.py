import binascii
import os
import time
from datetime import datetime
from typing import Union

import pandas as pd
import pytz
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


def timestamp_to_utc(timestamp: Union[pd.Timestamp, pd.DatetimeIndex, datetime]):
    if isinstance(timestamp, pd.Timestamp) or isinstance(timestamp, pd.DatetimeIndex):
        if timestamp.tz is None:
            timestamp = timestamp.tz_localize("UTC")
        timestamp = timestamp.tz_convert("UTC")
    elif isinstance(timestamp, datetime):
        if timestamp.tzinfo is None or timestamp.tzinfo.utcoffset(timestamp) is None:
            timestamp = pytz.UTC.localize(timestamp)
        timestamp = timestamp.astimezone(pytz.UTC)
    else:
        raise Exception(
            "Unsupported type: {}. It should be either pandas Timestamp or datetime".format(
                type(timestamp)
            )
        )
    return timestamp


def generate_object_id() -> str:
    timestamp = "{:x}".format(int(time.time()))
    rest = binascii.b2a_hex(os.urandom(8)).decode("ascii")
    return timestamp + rest
