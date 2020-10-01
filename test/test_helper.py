from datetime import datetime, timedelta
from unittest.mock import call, patch

import pandas as pd
import pytest
import pytz
from freezegun import freeze_time
from pytz import timezone
from requests import Response, Session

from common.constants import DATE_FORMAT
from common.exchange_calendar_euronext import EuronextExchangeCalendar
from common.helper import (
    TimeInterval,
    calc_required_history_start_timestamp,
    ceil_time,
    find_start_interval_business_date,
    find_start_interval_business_minute,
    request,
    resample_candle_data,
    round_time,
)
from common.types import CandleDataFrame
from strategy.strategy import euronext_cal
from test.test_candlefetcher import MARKET_CAL, SYMBOL
from test.test_historical_data_handler import STATUS_CODE_OK, URL


def test_process_interval():
    s = "1 day"
    time_interval: TimeInterval = TimeInterval.process_interval(s)
    assert time_interval.interval_unit == "day"
    assert time_interval.interval_value == 1


def test_process_interval_invalid_input():
    s = ""
    with pytest.raises(Exception):
        TimeInterval.process_interval(s)

    s = "1"
    with pytest.raises(Exception):
        TimeInterval.process_interval(s)

    s = "1 month"
    with pytest.raises(Exception):
        TimeInterval.process_interval(s)


frozen_time = datetime(2020, 5, 17, 23, 21, 34, tzinfo=pytz.UTC)


@freeze_time(frozen_time)
@pytest.mark.parametrize(
    "input, time_delta, expected_rounded_input",
    [
        (
            datetime(2020, 5, 17, 23, 50),
            timedelta(minutes=60),
            datetime(2020, 5, 17, 23),
        ),
        (
            datetime(2020, 5, 17, 23, 31),
            timedelta(minutes=30),
            datetime(2020, 5, 17, 23, 30),
        ),
        (
            datetime(2020, 5, 17, 23, 36),
            timedelta(minutes=10),
            datetime(2020, 5, 17, 23, 30),
        ),
        (None, timedelta(minutes=10), datetime(2020, 5, 17, 23, 20, tzinfo=pytz.UTC)),
        (
            pd.Timestamp("2020-05-17 23:50:00"),
            pd.offsets.Minute(60),
            pd.Timestamp("2020-05-17 23:00:00"),
        ),
        (
            pd.Timestamp("2020-05-17 23:31:00"),
            pd.offsets.Minute(30),
            pd.Timestamp("2020-05-17 23:30:00"),
        ),
        (
            pd.Timestamp("2020-05-17 23:36:00"),
            pd.offsets.Minute(10),
            pd.Timestamp("2020-05-17 23:30:00"),
        ),
    ],
)
def test_round_time(input, time_delta, expected_rounded_input):
    if input is not None:
        rounded_input = round_time(input, time_delta=time_delta)
    else:
        rounded_input = round_time(time_delta=time_delta)
    assert expected_rounded_input == rounded_input


@freeze_time(frozen_time)
@pytest.mark.parametrize(
    "input, time_delta, expected_ceiled_input",
    [
        (
            datetime(2020, 5, 17, 23, 50),
            timedelta(minutes=60),
            datetime(2020, 5, 18, 00),
        ),
        (
            datetime(2020, 5, 17, 23, 31),
            timedelta(minutes=30),
            datetime(2020, 5, 18, 00),
        ),
        (
            datetime(2020, 5, 17, 23, 36),
            timedelta(minutes=10),
            datetime(2020, 5, 17, 23, 40),
        ),
        (
            datetime(2020, 5, 17, 23, 31),
            timedelta(minutes=5),
            datetime(2020, 5, 17, 23, 35),
        ),
        (None, timedelta(minutes=10), datetime(2020, 5, 17, 23, 30, tzinfo=pytz.UTC)),
        (
            pd.Timestamp("2020-05-17 23:50:00"),
            pd.offsets.Minute(60),
            pd.Timestamp("2020-05-18 00:00:00"),
        ),
        (
            pd.Timestamp("2020-05-17 23:31:00"),
            pd.offsets.Minute(30),
            pd.Timestamp("2020-05-18 00:00:00"),
        ),
        (
            pd.Timestamp("2020-05-17 23:36:00"),
            pd.offsets.Minute(10),
            pd.Timestamp("2020-05-17 23:40:00"),
        ),
        (
            pd.Timestamp("2020-05-17 23:31:00"),
            pd.offsets.Minute(5),
            pd.Timestamp("2020-05-17 23:35:00"),
        ),
    ],
)
def test_ceil_time(input, time_delta, expected_ceiled_input):
    if input is not None:
        ceiled_input = ceil_time(input, time_delta=time_delta)
    else:
        ceiled_input = ceil_time(time_delta=time_delta)
    assert expected_ceiled_input == ceiled_input


def test_find_start_business_minute():
    business_cal = EuronextExchangeCalendar()
    input = datetime(2020, 5, 17, 23, 36, tzinfo=timezone("UTC"))
    x = find_start_interval_business_minute(
        input, business_cal, TimeInterval.process_interval("30 minute"), 2
    )
    assert x == datetime(2020, 5, 15, 14, 30, tzinfo=timezone("UTC"))


def test_find_start_business_minute_unrecognized_interval():
    business_cal = EuronextExchangeCalendar()
    input = datetime(2020, 5, 17, 23, 36, tzinfo=timezone("UTC"))
    with pytest.raises(Exception):
        find_start_interval_business_minute(
            input, business_cal, TimeInterval.process_interval("30 day"), 2
        )


def test_find_start_business_minute_invalid_time_interval():
    business_cal = EuronextExchangeCalendar()
    input = datetime(2020, 5, 17, 23, 36, tzinfo=timezone("UTC"))
    with pytest.raises(Exception):
        find_start_interval_business_minute(
            input, business_cal, TimeInterval.process_interval("17 minute"), 2
        )


def test_find_start_business_day_end_timestamp_in_business_day():
    business_cal = EuronextExchangeCalendar()
    input = datetime(2020, 5, 14, 23, 36, tzinfo=timezone("UTC"))
    x = find_start_interval_business_date(
        input, business_cal, TimeInterval.process_interval("1 day"), 2
    )
    assert x == datetime(2020, 5, 13, 0, 0, tzinfo=timezone("UTC"))


def test_find_start_business_day_end_timestamp_in_non_business_day():
    business_cal = EuronextExchangeCalendar()
    input = datetime(2020, 5, 17, 23, 36, tzinfo=timezone("UTC"))
    x = find_start_interval_business_date(
        input, business_cal, TimeInterval.process_interval("1 day"), 2
    )
    assert x == datetime(2020, 5, 14, 0, 0, tzinfo=timezone("UTC"))


def test_find_start_business_day_unrecognized_interval():
    business_cal = EuronextExchangeCalendar()
    input = datetime(2020, 5, 17, 23, 36, tzinfo=timezone("UTC"))
    with pytest.raises(Exception):
        find_start_interval_business_date(
            input, business_cal, TimeInterval.process_interval("1 minute"), 2
        )


@patch.object(Session, "get")
def test_request(get_mocked):
    get_mocked.return_value = Response()
    get_mocked.return_value.status_code = STATUS_CODE_OK
    assert request(URL).status_code == STATUS_CODE_OK
    get_mocked_calls = [call(URL)]
    get_mocked.assert_has_calls(get_mocked_calls)


def test_resample_candle_data_interval_5_minute():
    candles = {
        "timestamp": [
            "2020-05-08 14:17:00+00:00",
            "2020-05-08 14:24:00+00:00",
            "2020-05-08 14:24:56+00:00",
            "2020-05-08 14:35:00+00:00",
            "2020-05-08 14:41:00+00:00",
            "2020-05-08 14:41:58+00:00",
        ],
        "open": ["94.12", "94.07", "94.07", "94.17", "94.19", "94.19"],
        "high": ["94.15", "94.10", "94.10", "94.18", "94.22", "94.22"],
        "low": ["94.00", "93.95", "93.95", "94.05", "94.07", "94.07"],
        "close": ["94.13", "94.08", "94.08", "94.18", "94.20", "94.20"],
        "volume": [7, 91, 30, 23, 21, 7],
    }
    business_cal = MARKET_CAL.schedule(start_date="2020-05-08", end_date="2020-05-08")
    df = pd.DataFrame(
        candles, columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    df.index = pd.to_datetime(df.timestamp, format=DATE_FORMAT)
    df = df.drop(["timestamp"], axis=1)
    df = CandleDataFrame.from_dataframe(df, SYMBOL)
    df = resample_candle_data(df, pd.offsets.Minute(5), business_cal)

    expected_df_candles = {
        "timestamp": [
            "2020-05-08 14:20:00+00:00",
            "2020-05-08 14:25:00+00:00",
            "2020-05-08 14:30:00+00:00",
            "2020-05-08 14:35:00+00:00",
            "2020-05-08 14:40:00+00:00",
            "2020-05-08 14:45:00+00:00",
        ],
        "open": ["94.12", "94.07", "94.08", "94.17", "94.18", "94.19"],
        "high": ["94.15", "94.10", "94.08", "94.18", "94.18", "94.22"],
        "low": ["94.00", "93.95", "94.08", "94.05", "94.18", "94.07"],
        "close": ["94.13", "94.08", "94.08", "94.18", "94.18", "94.20"],
        "volume": [7, 121, 0, 23, 0, 28],
    }
    expected_df = pd.DataFrame(
        expected_df_candles,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    expected_df.index = pd.to_datetime(expected_df.timestamp, format=DATE_FORMAT)
    expected_df = expected_df.drop(["timestamp"], axis=1)

    assert (df == expected_df).all(axis=None)


def test_simple_resample_candle_data_interval_1_day():
    candles = {
        "timestamp": [
            "2020-05-08 14:17:00+00:00",
            "2020-05-08 14:24:00+00:00",
            "2020-05-08 14:24:56+00:00",
            "2020-05-08 14:35:00+00:00",
            "2020-05-08 14:41:00+00:00",
            "2020-05-08 14:41:58+00:00",
        ],
        "open": ["94.12", "94.07", "94.07", "94.17", "94.19", "94.19"],
        "high": ["94.15", "94.10", "94.10", "94.18", "94.22", "94.22"],
        "low": ["94.00", "93.95", "93.95", "94.05", "94.07", "94.07"],
        "close": ["94.13", "94.08", "94.08", "94.18", "94.20", "94.20"],
        "volume": [7, 91, 30, 23, 21, 7],
    }
    business_cal = MARKET_CAL.schedule(start_date="2020-05-08", end_date="2020-05-08")
    df = pd.DataFrame(
        candles, columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    df.index = pd.to_datetime(df.timestamp, format=DATE_FORMAT)
    df = df.drop(["timestamp"], axis=1)
    df = CandleDataFrame.from_dataframe(df, SYMBOL)
    df = resample_candle_data(df, pd.offsets.Day(1), business_cal)

    expected_df_candles = {
        "timestamp": ["2020-05-08 00:00:00+00:00"],
        "open": ["94.12"],
        "high": ["94.22"],
        "low": ["93.95"],
        "close": ["94.20"],
        "volume": [179],
    }
    expected_df = pd.DataFrame(
        expected_df_candles,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    expected_df.index = pd.to_datetime(expected_df.timestamp, format=DATE_FORMAT)
    expected_df = expected_df.drop(["timestamp"], axis=1)

    assert (df == expected_df).all(axis=None)


def test_simple_resample_candle_data_interval_1_day_data_spread_over_2_days():
    candles = {
        "timestamp": [
            "2020-05-08 14:17:00+00:00",
            "2020-05-08 14:24:00+00:00",
            "2020-05-08 14:24:56+00:00",
            "2020-05-08 14:35:00+00:00",
            "2020-05-11 14:41:00+00:00",
            "2020-05-11 14:41:58+00:00",
        ],
        "open": ["94.12", "94.07", "94.07", "94.17", "94.19", "94.19"],
        "high": ["94.15", "94.10", "94.10", "94.18", "94.22", "94.22"],
        "low": ["94.00", "93.95", "93.95", "94.05", "94.07", "94.07"],
        "close": ["94.13", "94.08", "94.08", "94.18", "94.20", "94.20"],
        "volume": [7, 91, 30, 23, 1, 1],
    }
    business_cal = MARKET_CAL.schedule(start_date="2020-05-08", end_date="2020-05-11")
    df = pd.DataFrame(
        candles, columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    df.index = pd.to_datetime(df.timestamp, format=DATE_FORMAT)
    df = df.drop(["timestamp"], axis=1)
    df = CandleDataFrame.from_dataframe(df, SYMBOL)
    df = resample_candle_data(df, pd.offsets.Day(1), business_cal)

    expected_df_candles = {
        "timestamp": ["2020-05-08 00:00:00+00:00", "2020-05-11 00:00:00+00:00"],
        "open": ["94.12", "94.19"],
        "high": ["94.18", "94.22"],
        "low": ["93.95", "94.07"],
        "close": ["94.18", "94.20"],
        "volume": [151, 2],
    }
    expected_df = pd.DataFrame(
        expected_df_candles,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    expected_df.index = pd.to_datetime(expected_df.timestamp, format=DATE_FORMAT)

    expected_df = expected_df.drop(["timestamp"], axis=1)
    assert (df == expected_df).all(axis=None)


def test_calc_time_range_1_day_interval():
    start = calc_required_history_start_timestamp(
        pd.offsets.Day(1),
        8,
        euronext_cal,
        datetime(2020, 5, 1, 0, 0, tzinfo=timezone("UTC")),
    )
    assert start == datetime(2020, 4, 21, 0, 0, tzinfo=timezone("UTC"))


def test_calc_time_range_1_day_interval():
    start = calc_required_history_start_timestamp(
        pd.offsets.Day(1),
        8,
        euronext_cal,
        datetime(2020, 5, 1, 0, 0, tzinfo=timezone("UTC")),
    )
    assert start == datetime(2020, 4, 21, 0, 0, tzinfo=timezone("UTC"))


def test_calc_time_range_1_day_interval_2():
    start = calc_required_history_start_timestamp(
        pd.offsets.Day(1),
        8,
        euronext_cal,
        datetime(2020, 4, 30, 0, 0, tzinfo=timezone("UTC")),
    )
    assert start == datetime(2020, 4, 20, 0, 0, tzinfo=timezone("UTC"))


def test_calc_time_range_30_minute_interval_on_business_hour():
    start = calc_required_history_start_timestamp(
        pd.offsets.Minute(30),
        4,
        euronext_cal,
        datetime(2020, 4, 30, 15, 5, tzinfo=timezone("UTC")),
    )
    assert start == datetime(2020, 4, 30, 13, 30, tzinfo=timezone("UTC"))


def test_calc_time_range_30_minute_interval_on_non_business_hour():
    start = calc_required_history_start_timestamp(
        pd.offsets.Minute(30),
        4,
        euronext_cal,
        datetime(2020, 5, 1, 12, 0, tzinfo=timezone("UTC")),
    )
    assert start == datetime(2020, 4, 30, 14, 0, tzinfo=timezone("UTC"))
