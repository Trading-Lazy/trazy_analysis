import re
from datetime import datetime, timedelta, timezone
from typing import List

import numpy as np
import pandas as pd
import pytz
import requests
from pandas import DataFrame
from pandas_market_calendars import MarketCalendar
from pandasql import sqldf
from requests import Response

from common.calendar import is_business_day, is_business_hour
from common.types import CandleDataFrame
from common.utils import timestamp_to_utc

MINUTE_IN_ONE_HOUR = 60


class TimeInterval:
    def __init__(self, interval_unit: str = None, interval_value: int = None):
        self.interval_unit: str = interval_unit
        self.interval_value: int = interval_value

    def __str__(self):
        return "{} {}".format(self.interval_value, self.interval_unit)

    @staticmethod
    def process_interval(str_interval: str):
        i = str_interval.split(" ")
        if len(i) != 2:
            raise Exception("The input string interval is malformed.")
        interval_unit: str = i[1].lower()
        interval_value: int = int(i[0])
        if interval_unit not in ["day", "minute"]:
            raise Exception("Unknown interval unit {}".format(interval_unit))
        return TimeInterval(interval_unit, interval_value)


def round_time(dt=None, time_delta=timedelta(minutes=1)):
    """Round down a datetime object to a multiple of a timedelta
    dt : datetime.datetime object, default now.
    time_delta : timedelta object, we round to a multiple of this, default 1 minute.
    """
    if dt == None:
        dt = datetime.now(timezone.utc)
    round_to = time_delta.total_seconds()
    seconds = dt.timestamp()

    # // is a floor division
    rounding = seconds // round_to * round_to

    return dt + timedelta(0, rounding - seconds, -dt.microsecond)


def ceil_time(dt=None, time_delta=timedelta(minutes=1)):
    """Round down a datetime object to a multiple of a timedelta
    dt : datetime.datetime object, default now.
    time_delta : timedelta object, we round to a multiple of this, default 1 minute.
    """
    if dt == None:
        dt = datetime.now(timezone.utc)
    if isinstance(dt, pd.Timestamp):
        return pd.Timestamp(
            ceil_time(dt.to_pydatetime(), pd.Timedelta(time_delta).to_pytimedelta())
        )
    min_datetime = datetime.min
    if dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None:
        min_datetime = datetime.min.replace(tzinfo=pytz.UTC)
    q, r = divmod(dt - min_datetime, time_delta)
    return (min_datetime + (q + 1) * time_delta) if r else dt


def find_start_interval_business_minute(
    end_timestamp: datetime,
    business_calendar: MarketCalendar,
    interval: TimeInterval,
    nb_valid_interval: int,
):
    """
    Returns the minimum start time that gives enough timespan from the end_timestamp to
    cover nb_valid_interval of business hours.
    :param end_timestamp: end timestamp of the time period
    :param business_calendar: business calendar to define the business hour
    :param interval: time interval
    :param nb_valid_interval: the number of valid business hours interval to cover
    :return:
    """
    if interval.interval_unit != "minute":
        raise Exception("Time interval not recognized")

    if MINUTE_IN_ONE_HOUR % interval.interval_value != 0:
        raise Exception("Not a valid time interval")

    max_start_offset_in_days = 365
    max_start = end_timestamp - timedelta(days=max_start_offset_in_days)
    df_business_calendar = business_calendar.schedule(
        start_date=max_start.strftime("%Y-%m-%d"),
        end_date=end_timestamp.strftime("%Y-%m-%d"),
    )

    end = round_time(
        end_timestamp,
        time_delta=timedelta(minutes=interval.interval_value),
    )
    start_timestamp = timestamp_to_utc(end)

    i = 0
    while i < nb_valid_interval and start_timestamp > max_start:
        if is_business_hour(start_timestamp, df_business_calendar=df_business_calendar):
            i += 1
        start_timestamp = start_timestamp - timedelta(minutes=interval.interval_value)

    return start_timestamp


def find_start_interval_business_date(
    end_timestamp: datetime,
    business_calendar: MarketCalendar,
    interval: TimeInterval,
    nb_valid_interval: int,
):
    """
    Returns the minimum start date that gives enough timespan from the end_timestamp to
    cover nb_valid_interval of business days.
    :param end_timestamp: end timestamp of the time period
    :param business_calendar: business calendar to define the business hour
    :param interval: time interval
    :param nb_valid_interval: the number of valid business days interval to cover
    :return:
    """
    if interval.interval_unit != "day":
        raise Exception("Time interval not recognized")

    max_start_offset_in_days = 365
    max_start = end_timestamp - timedelta(days=max_start_offset_in_days)
    df_business_calendar = business_calendar.schedule(
        start_date=max_start.strftime("%Y-%m-%d"),
        end_date=end_timestamp.strftime("%Y-%m-%d"),
    )

    td = timedelta(
        hours=end_timestamp.hour,
        minutes=end_timestamp.minute,
        seconds=end_timestamp.second,
        microseconds=end_timestamp.microsecond,
    )

    if (
        td.total_seconds() > 0
        and end_timestamp.date() in df_business_calendar.index.date
    ):
        i = 1
    else:
        i = 0
    end = end_timestamp - td
    start_timestamp = timestamp_to_utc(end)

    while i < nb_valid_interval and start_timestamp > max_start:
        start_timestamp = start_timestamp - timedelta(days=interval.interval_value)
        if is_business_day(start_timestamp, df_business_calendar=df_business_calendar):
            i += 1

    return start_timestamp


def calc_required_history_start_timestamp(
    time_unit: timedelta,
    period: int,
    business_calendar: MarketCalendar,
    end_timestamp: datetime = datetime.now(timezone.utc),
) -> datetime:
    interval_unit = "day" if time_unit.name == "D" else "minute"
    interval = TimeInterval(interval_unit, time_unit.n)

    if interval_unit == "day":
        start_timestamp = find_start_interval_business_date(
            end_timestamp, business_calendar, interval, period
        )

    elif interval_unit == "minute":
        start_timestamp = find_start_interval_business_minute(
            end_timestamp, business_calendar, interval, period - 1
        )
    return start_timestamp


def request(url: str) -> Response:
    with requests.Session() as session:
        response = session.get(url)
        return response


def resample_candle_data(
    df: CandleDataFrame,
    time_unit: timedelta,
    market_cal_df: DataFrame,
) -> CandleDataFrame:
    symbol = df.symbol
    resample_label = "right"
    if time_unit >= timedelta(days=1):
        resample_label = "left"
    df = df.resample(time_unit, label=resample_label, closed="right").agg(
        {
            "open": "first",
            "high": np.max,
            "low": np.min,
            "close": "last",
            "volume": np.sum,
        }
    )
    df_close = df["close"].ffill()
    df = df.fillna(
        {
            "open": df_close,
            "high": df_close,
            "low": df_close,
            "close": df_close,
        }
    )

    candle_dataframe = CandleDataFrame.from_dataframe(df, symbol)

    if time_unit >= timedelta(days=1):
        market_cal_df.index = timestamp_to_utc(market_cal_df.index)
        df_resampled = candle_dataframe.reindex(market_cal_df.index)
        df_resampled.index.name = "timestamp"
    else:
        s = """
            select
                d.timestamp, d.open, d.high, d.low, d.close, d.volume
            from
                candle_dataframe d join market_cal_df b on (d.timestamp >= b.market_open and d.timestamp <= 
                b.market_close)
        """
        df_resampled = sqldf(s, locals())
        df_resampled = CandleDataFrame.from_dataframe(df_resampled, symbol)
    return df_resampled


def get_or_create_nested_dict(nested_dict: dict, *keys) -> None:
    """
    Check if *keys (nested) exists in `element` (dict).
    """
    if not isinstance(nested_dict, dict):
        raise AttributeError("keys_exists() expects dict as first argument.")
    if len(keys) == 0:
        raise AttributeError("keys_exists() expects at least two arguments, one given.")

    _nested_dict = nested_dict
    for key in keys:
        try:
            _nested_dict = _nested_dict[key]
        except KeyError:
            _nested_dict[key] = {}
            _nested_dict = _nested_dict[key]


def check_type(object, allowed_types: List[type]):
    if object is None:
        return
    object_type = type(object)
    if object_type not in allowed_types:
        raise Exception(
            "data type should be one of {} not {}".format(allowed_types, object_type)
        )


def parse_timedelta_str(timedelta_str) -> timedelta:
    if "day" in timedelta_str:
        match = re.match(
            r"(?P<days>[-\d]+) day[s]*, (?P<hours>\d+):(?P<minutes>\d+):(?P<seconds>\d[\.\d+]*)",
            timedelta_str,
        )
    else:
        match = re.match(
            r"(?P<hours>\d+):(?P<minutes>\d+):(?P<seconds>\d[\.\d+]*)", timedelta_str
        )
    return timedelta(**{key: float(val) for key, val in match.groupdict().items()})
