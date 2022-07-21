import json
import os
import re
import traceback
from datetime import datetime, timedelta
from typing import List, Dict, Union

import numpy as np
import pandas as pd
import pytz
import requests
from pandas import DataFrame
from pandas_market_calendars import MarketCalendar
from pandasql import sqldf
from requests import Response

import trazy_analysis.logger
import trazy_analysis.settings
from trazy_analysis.common.calendar import is_business_day, is_business_hour
from trazy_analysis.common.constants import CONNECTION_ERROR_MESSAGE, ENCODING
from trazy_analysis.common.types import CandleDataFrame
from trazy_analysis.common.utils import timestamp_to_utc
from trazy_analysis.models.asset import Asset

MINUTE_IN_ONE_HOUR = 60
MAP_TICKER_KUCOIN_SYMBOL_LAST_UPDATE = None
UPDATE_KUCOIN_SYMBOLS_MAPPING = timedelta(days=1)
TICKER_TO_KUCOIN_SYMBOL = {}
LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
)


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
        dt = datetime.now(pytz.UTC)
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
        dt = datetime.now(pytz.UTC)
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
    end_timestamp: datetime = datetime.now(pytz.UTC),
) -> datetime:
    interval_unit = "day" if time_unit.name == "D" else "minute"
    interval = TimeInterval(interval_unit, time_unit.n)

    start_timestamp = None
    match interval_unit:
        case "day":
            start_timestamp = find_start_interval_business_date(
                end_timestamp, business_calendar, interval, period
            )
        case "minute":
            start_timestamp = find_start_interval_business_minute(
                end_timestamp, business_calendar, interval, period - 1
            )
    return start_timestamp


def request(url: str) -> Response:
    with requests.Session() as session:
        response = session.get(url)
        return response


def fill_missing_datetimes(
    df: CandleDataFrame,
    time_unit: timedelta,
) -> CandleDataFrame:
    asset = df.asset
    resample_label = "left"
    if time_unit >= timedelta(days=1):
        resample_label = "left"
    if df.empty:
        return df
    df = df.resample(time_unit, label=resample_label, closed="left").agg(
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
    return CandleDataFrame.from_dataframe(df, asset)


def resample_candle_data(
    candle_dataframe: CandleDataFrame,
    time_unit: timedelta,
    market_cal_df: DataFrame = None,
    remove_incomplete_head=True,
) -> CandleDataFrame:
    asset = candle_dataframe.asset
    initial_time_unit = candle_dataframe.time_unit
    if not candle_dataframe.empty:
        first_timestamp = candle_dataframe.get_candle(0).timestamp
    candle_dataframe = fill_missing_datetimes(df=candle_dataframe, time_unit=time_unit)
    candle_dataframe = CandleDataFrame.from_dataframe(
        candle_dataframe, Asset(symbol=asset.symbol, exchange=asset.exchange), time_unit=time_unit
    )

    if market_cal_df is None or initial_time_unit == time_unit:
        return candle_dataframe

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
        df_resampled = CandleDataFrame.from_dataframe(
            df_resampled, Asset(symbol=asset.symbol, exchange=asset.exchange), time_unit=time_unit
        )
    if (
        remove_incomplete_head
        and not candle_dataframe.empty
        and not df_resampled.empty
        and df_resampled.get_candle(0).timestamp != first_timestamp
    ):
        df_resampled.drop(df_resampled.head(1).index, inplace=True)
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


def check_type(object_or_type: object | type, allowed_types: list[type]):
    if object_or_type is None:
        return
    object_type = type(object_or_type)
    if object_type not in allowed_types:
        raise Exception(
            "data type should be one of {} not {}".format(allowed_types, object_type)
        )


def parse_timedelta_str(timedelta_str: str) -> timedelta:
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


def map_ticker_to_kucoin_symbol(ticker: str) -> str:
    now = datetime.now(tz=pytz.UTC)
    global TICKER_TO_KUCOIN_SYMBOL
    if (
        MAP_TICKER_KUCOIN_SYMBOL_LAST_UPDATE is not None
        and now - MAP_TICKER_KUCOIN_SYMBOL_LAST_UPDATE < UPDATE_KUCOIN_SYMBOLS_MAPPING
    ):
        return TICKER_TO_KUCOIN_SYMBOL
    try:
        from trazy_analysis.market_data.kucoin_data_handler import KucoinDataHandler

        response = request(KucoinDataHandler.BASE_URL_GET_TICKERS_LIST)
    except Exception as e:
        LOG.error(
            CONNECTION_ERROR_MESSAGE,
            str(e),
            traceback.format_exc(),
        )
        return []
    tickers_response: str = response.content.decode(ENCODING)
    if response:
        tickers_dict = json.loads(tickers_response)
        tickers_info = tickers_dict["data"]
        tickers_with_hyphen = [ticker_info["symbol"] for ticker_info in tickers_info]
        TICKER_TO_KUCOIN_SYMBOL = {
            ticker.replace("-", "/"): ticker for ticker in tickers_with_hyphen
        }
    else:
        LOG.error(
            "Error while getting the list of kucoin avalaible symbols for updating mapping. This happened when trying to update the mapping of the ticker %s",
            ticker,
        )
        return ticker
    return TICKER_TO_KUCOIN_SYMBOL[ticker]


def datetime_to_epoch(timestamp: datetime, unit_multiplicator: int) -> int:
    return int(timestamp.timestamp()) * unit_multiplicator


def normalize_assets(
    assets: dict[Asset, timedelta | list[timedelta]]
) -> dict[Asset, list[timedelta]]:
    assets_copy = assets.copy()
    assets_to_normalize = set()
    for asset, time_units in assets_copy.items():
        if isinstance(time_units, timedelta):
            assets_to_normalize.add(asset)
    for asset in assets_to_normalize:
        assets_copy[asset] = [assets_copy[asset]]
    return assets_copy



def all_subclasses(cls: type):
    return set(cls.__subclasses__()).union(
        [
            subsubclass
            for subclass in cls.__subclasses__()
            for subsubclass in all_subclasses(subclass)
        ]
    )
