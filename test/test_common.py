import time
from datetime import date, datetime, timedelta

import numpy as np
import pytest
import pytz

from trazy_analysis.broker.common import get_rejected_order_error_message
from trazy_analysis.common.clock import LiveClock
from trazy_analysis.common.helper import get_or_create_nested_dict
from trazy_analysis.common.meta import RateLimitedSingletonMeta
from trazy_analysis.file_storage.common import concat_path
from trazy_analysis.indicators.common import get_state
from trazy_analysis.indicators.crossover import CrossoverState
from trazy_analysis.market_data.common import get_periods
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.enums import Action, Direction, OrderType
from trazy_analysis.models.order import Order


def test_concat_path_base_and_complement_empty():
    expected_concatanated_path = ""
    base_path = ""
    complement = ""
    assert expected_concatanated_path == concat_path(base_path, complement)


def test_concat_path_base_empty():
    expected_concatanated_path = "file"
    base_path = ""
    complement = "file"
    assert expected_concatanated_path == concat_path(base_path, complement)


def test_concat_path_complement_empty():
    expected_concatanated_path = "dir"
    base_path = "dir"
    complement = ""
    assert expected_concatanated_path == concat_path(base_path, complement)


def test_concat_path():
    expected_concatanated_path = "dir/file"
    base_path = "dir"
    complement = "file"
    assert expected_concatanated_path == concat_path(base_path, complement)


def test_get_or_create_nested_dict():
    keys = ["key1", "key2", "key3"]
    with pytest.raises(Exception):
        get_or_create_nested_dict(object(), *keys)

    d = {}
    with pytest.raises(Exception):
        get_or_create_nested_dict(d)

    get_or_create_nested_dict(d, *keys)
    assert "key1" in d
    assert "key2" in d["key1"]
    assert "key3" in d["key1"]["key2"]

    get_or_create_nested_dict(d, *keys, "key4")
    assert "key4" in d["key1"]["key2"]["key3"]


def test_rate_limited_singleton_meta():
    class RateLimitedSingleton(metaclass=RateLimitedSingletonMeta):
        MAX_CALLS = 3
        PERIOD = 5

    assert RateLimitedSingleton.request is not None

    rate_limited_singleton1 = RateLimitedSingleton()
    rate_limited_singleton2 = RateLimitedSingleton()
    assert rate_limited_singleton1 == rate_limited_singleton2

    dummy_url = "http://www.google.com"
    window = []
    for i in range(0, RateLimitedSingleton.MAX_CALLS + 1):
        start = time.time()
        RateLimitedSingleton.request(dummy_url)
        end = time.time()
        window.append((start, end))

    first_start = window[0][0]
    third_end = window[2][1]
    assert (third_end - first_start) < RateLimitedSingleton.PERIOD

    last_end = window[-1][1]
    assert (last_end - first_start) > RateLimitedSingleton.PERIOD


def test_get_periods():
    start = datetime.strptime("2020-06-11 20:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    end = datetime.strptime("2020-06-26 16:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    download_frame = timedelta(days=3)
    periods = get_periods(download_frame, start, end)

    expected_periods = np.array(
        [
            (date(2020, 6, 11), date(2020, 6, 13)),
            (date(2020, 6, 14), date(2020, 6, 16)),
            (date(2020, 6, 17), date(2020, 6, 19)),
            (date(2020, 6, 20), date(2020, 6, 22)),
            (date(2020, 6, 23), date(2020, 6, 25)),
            (date(2020, 6, 26), date(2020, 6, 26)),
        ]
    )
    assert (periods == expected_periods).all()


def test_get_periods_days_with_seconds():
    start = datetime.strptime("2020-06-11 20:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    end = datetime.strptime("2020-06-26 16:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    download_frame = timedelta(days=3, seconds=18000)
    periods = get_periods(download_frame, start, end)

    expected_periods = np.array(
        [
            (date(2020, 6, 11), date(2020, 6, 13)),
            (date(2020, 6, 14), date(2020, 6, 16)),
            (date(2020, 6, 17), date(2020, 6, 19)),
            (date(2020, 6, 20), date(2020, 6, 22)),
            (date(2020, 6, 23), date(2020, 6, 25)),
            (date(2020, 6, 26), date(2020, 6, 26)),
        ]
    )
    assert (periods == expected_periods).all()


def test_get_intraday_periods():
    start = datetime.strptime("2020-06-11 23:45:00+0000", "%Y-%m-%d %H:%M:%S%z")
    end = datetime.strptime("2020-06-12 00:14:59+0000", "%Y-%m-%d %H:%M:%S%z")
    download_frame = timedelta(minutes=5)
    periods = get_periods(download_frame, start, end)

    expected_periods = np.array(
        [
            [
                datetime(2020, 6, 11, 23, 45, tzinfo=pytz.UTC),
                datetime(2020, 6, 11, 23, 49, 59, tzinfo=pytz.UTC),
            ],
            [
                datetime(2020, 6, 11, 23, 50, tzinfo=pytz.UTC),
                datetime(2020, 6, 11, 23, 54, 59, tzinfo=pytz.UTC),
            ],
            [
                datetime(2020, 6, 11, 23, 55, tzinfo=pytz.UTC),
                datetime(2020, 6, 11, 23, 59, 59, tzinfo=pytz.UTC),
            ],
            [
                datetime(2020, 6, 12, 0, 0, tzinfo=pytz.UTC),
                datetime(2020, 6, 12, 0, 4, 59, tzinfo=pytz.UTC),
            ],
            [
                datetime(2020, 6, 12, 0, 5, tzinfo=pytz.UTC),
                datetime(2020, 6, 12, 0, 9, 59, tzinfo=pytz.UTC),
            ],
            [
                datetime(2020, 6, 12, 0, 10, tzinfo=pytz.UTC),
                datetime(2020, 6, 12, 0, 14, 59, tzinfo=pytz.UTC),
            ],
        ],
    )
    assert (periods == expected_periods).all()


def test_get_periods_start_date_after_end_date():
    start = datetime.strptime("2020-06-26 16:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    end = datetime.strptime("2020-06-11 20:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    download_frame = timedelta(days=3)
    periods = get_periods(download_frame, start, end)

    expected_periods = np.empty([])
    assert (periods == expected_periods).all()


def test_get_intraday_periods_start_date_after_end_date():
    start = datetime.strptime("2020-06-26 16:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    end = datetime.strptime("2020-06-11 20:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    download_frame = timedelta(minutes=5)
    periods = get_periods(download_frame, start, end)

    expected_periods = np.empty([])
    assert (periods == expected_periods).all()


def test_get_periods_single_date():
    start = datetime.strptime("2020-06-26 16:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    end = datetime.strptime("2020-06-26 20:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    download_frame = timedelta(days=3)
    periods = get_periods(download_frame, start, end)

    expected_periods = np.array([(date(2020, 6, 26), date(2020, 6, 26))])
    assert (periods == expected_periods).all()


def test_get_intraday_periods_single_datetime():
    start = datetime.strptime("2020-06-26 16:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    end = datetime.strptime("2020-06-26 16:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    download_frame = timedelta(minutes=5)
    periods = get_periods(download_frame, start, end)

    expected_periods = np.array(
        [
            [
                datetime(2020, 6, 26, 16, 0, tzinfo=pytz.UTC),
                datetime(2020, 6, 26, 16, 0, tzinfo=pytz.UTC),
            ]
        ]
    )
    assert (periods == expected_periods).all()


def test_get_state():
    assert get_state(None) == CrossoverState.IDLE
    assert get_state(0) == CrossoverState.IDLE
    assert get_state(1) == CrossoverState.POS
    assert get_state(-1) == CrossoverState.NEG


def test_get_rejected_order_error_message():
    clock = LiveClock()
    order = Order(
        asset=Asset(symbol="IFMK", exchange="IEX"),
        action=Action.BUY,
        direction=Direction.LONG,
        size=1,
        signal_id="1",
        type=OrderType.MARKET,
        clock=clock,
    )
    expected_error_message = "MARKET order (asset=IEX-IFMK, action=BUY, direction=LONG, size=1) could not be executed."
    assert get_rejected_order_error_message(order) == expected_error_message
