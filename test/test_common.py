import time
from datetime import date, timedelta

import pandas as pd
import pytest

from file_storage.common import concat_path
from indicators.common import get_state
from common.helper import get_or_create_nested_dict
from indicators.crossover import CrossoverState
from market_data.common import RateLimitedSingletonMeta, get_periods


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
    start = pd.Timestamp("2020-06-11T20:00:00+00:00")
    end = pd.Timestamp("2020-06-26T16:00:00+00:00")
    download_frame = timedelta(days=3)
    periods = get_periods(download_frame, start, end)

    expected_periods = [
        (date(2020, 6, 11), date(2020, 6, 13)),
        (date(2020, 6, 14), date(2020, 6, 16)),
        (date(2020, 6, 17), date(2020, 6, 19)),
        (date(2020, 6, 20), date(2020, 6, 22)),
        (date(2020, 6, 23), date(2020, 6, 25)),
        (date(2020, 6, 26), date(2020, 6, 26)),
    ]
    assert periods == expected_periods


def test_get_periods_start_date_after_end_date():
    start = pd.Timestamp("2020-06-26T16:00:00+00:00")
    end = pd.Timestamp("2020-06-11T20:00:00+00:00")
    download_frame = timedelta(days=3)
    periods = get_periods(download_frame, start, end)

    expected_periods = []
    assert periods == expected_periods


def test_get_periods_single_date():
    start = pd.Timestamp("2020-06-26T16:00:00+00:00")
    end = pd.Timestamp("2020-06-26T20:00:00+00:00")
    download_frame = timedelta(days=3)
    periods = get_periods(download_frame, start, end)

    expected_periods = [(date(2020, 6, 26), date(2020, 6, 26))]
    assert periods == expected_periods


def test_get_state():
    assert get_state(None) == CrossoverState.IDLE
    assert get_state(0) == CrossoverState.IDLE
    assert get_state(1) == CrossoverState.POS
    assert get_state(-1) == CrossoverState.NEG
