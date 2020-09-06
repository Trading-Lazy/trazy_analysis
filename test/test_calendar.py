from datetime import datetime

import pandas as pd
from pytz import timezone

from common.calendar import is_business_day, is_business_hour
from common.constants import DATE_FORMAT
from common.exchange_calendar_euronext import EuronextExchangeCalendar

euronext_cal = EuronextExchangeCalendar()


def test_euronext_calendar_holiday_mayday():
    business_cal = euronext_cal.schedule(start_date="2020-05-01", end_date="2020-05-05")
    expected_df_business_cal = {
        "timestamp": ["2020-05-04", "2020-05-05"],
        "market_open": ["2020-05-04 07:00:00+00:00", "2020-05-05 07:00:00+00:00"],
        "market_close": ["2020-05-04 15:30:00+00:00", "2020-05-05 15:30:00+00:00"],
    }
    expected_df = pd.DataFrame(
        expected_df_business_cal, columns=["timestamp", "market_open", "market_close"]
    )
    expected_df["timestamp"] = pd.to_datetime(expected_df.timestamp, format=DATE_FORMAT)
    expected_df["market_open"] = pd.to_datetime(
        expected_df["market_open"], format="%Y-%m-%d %H:%M:%S%z"
    )
    expected_df["market_close"] = pd.to_datetime(
        expected_df["market_close"], format="%Y-%m-%d %H:%M:%S%z"
    )

    expected_df = expected_df.set_index("timestamp")

    assert (business_cal == expected_df).all(axis=None)


def test_euronext_calendar_special_close():
    business_cal = euronext_cal.schedule(start_date="2020-12-24", end_date="2020-12-26")
    expected_df_business_cal = {
        "timestamp": ["2020-12-24"],
        "market_open": ["2020-12-24 08:00:00+00:00"],
        "market_close": ["2020-12-24 13:05:00+00:00"],
    }
    expected_df = pd.DataFrame(
        expected_df_business_cal, columns=["timestamp", "market_open", "market_close"]
    )
    expected_df["timestamp"] = pd.to_datetime(expected_df.timestamp, format=DATE_FORMAT)
    expected_df["market_open"] = pd.to_datetime(
        expected_df["market_open"], format="%Y-%m-%d %H:%M:%S%z"
    )
    expected_df["market_close"] = pd.to_datetime(
        expected_df["market_close"], format="%Y-%m-%d %H:%M:%S%z"
    )

    expected_df = expected_df.set_index("timestamp")

    assert (business_cal == expected_df).all(axis=None)


def test_calendar_is_business_day():
    may_day = datetime(2020, 5, 1)
    business_day = datetime(2020, 4, 30)
    assert not is_business_day(may_day, euronext_cal)
    assert is_business_day(business_day, euronext_cal)


def test_calendar_is_business_day_with_df_calendar():
    df_business_calendar = euronext_cal.schedule(
        start_date="2020-01-01", end_date="2020-10-01"
    )
    may_day = datetime(2020, 5, 1)
    business_day = datetime(2020, 4, 30)
    assert not is_business_day(may_day, df_business_calendar=df_business_calendar)
    assert is_business_day(business_day, df_business_calendar=df_business_calendar)


def test_calendar_is_business_hour():
    test_hour_neg1 = datetime(2020, 4, 30, 1, 30, 33, tzinfo=timezone("UTC"))
    test_hour_neg2 = datetime(2020, 8, 22, 15, 30, 33, tzinfo=timezone("UTC"))
    test_hour_pos = datetime(2020, 4, 30, 15, 30, tzinfo=timezone("UTC"))
    assert not is_business_hour(test_hour_neg1, business_calendar=euronext_cal)
    assert not is_business_hour(test_hour_neg2, business_calendar=euronext_cal)
    assert is_business_hour(test_hour_pos, business_calendar=euronext_cal)


def test_calendar_is_business_hour_with_calendar_df():
    business_day = euronext_cal.schedule(start_date="2020-01-01", end_date="2020-10-01")
    test_hour_neg = datetime(2020, 4, 30, 1, 30, 33, tzinfo=timezone("UTC"))
    test_hour_pos = datetime(2020, 4, 30, 15, 30, tzinfo=timezone("UTC"))
    assert not is_business_hour(test_hour_neg, df_business_calendar=business_day)
    assert is_business_hour(test_hour_pos, df_business_calendar=business_day)
