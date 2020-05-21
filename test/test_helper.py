from common.helper import TimeInterval, round_time, find_start_business_minute, find_start_business_day
from datetime import datetime, timedelta
from common.exchange_calendar_euronext import EuronextExchangeCalendar
from pytz import timezone


def test_process_interval():
    s = '1 day'
    time_interval: TimeInterval = TimeInterval.process_interval(s)
    assert time_interval.interval_unit == 'day'
    assert time_interval.interval_value == 1


def test_round_time():
    input = datetime(2020, 5, 17, 23, 50)
    rounded_input = round_time(input, time_delta=timedelta(minutes=60))
    assert(rounded_input == datetime(2020, 5, 17, 23))

    input = datetime(2020, 5, 17, 23, 31)
    rounded_input = round_time(input, time_delta=timedelta(minutes=30))
    assert(rounded_input == datetime(2020, 5, 17, 23, 30))

    input = datetime(2020, 5, 17, 23, 36)
    rounded_input = round_time(input, time_delta=timedelta(minutes=10))
    assert (rounded_input == datetime(2020, 5, 17, 23, 30))


def test_find_start_business_minute():
    business_cal = EuronextExchangeCalendar()
    input = datetime(2020, 5, 17, 23, 36, tzinfo=timezone('UTC'))
    x = find_start_business_minute(input, business_cal, TimeInterval.process_interval('30 minute'), 2)
    assert (x == datetime(2020, 5, 15, 14, 30, tzinfo=timezone('UTC')))


def test_find_start_business_day():
    business_cal = EuronextExchangeCalendar()
    input = datetime(2020, 5, 17, 23, 36, tzinfo=timezone('UTC'))
    x = find_start_business_day(input, business_cal, TimeInterval.process_interval('1 day'), 2)
    assert (x == datetime(2020, 5, 14, 0, 0, tzinfo=timezone('UTC')))