from datetime import datetime, timedelta

import numpy as np
import pytest
from numpy import ma
from pandas_market_calendars.exchange_calendar_eurex import EUREXExchangeCalendar

from trazy_analysis.indicators.common import PriceType
from trazy_analysis.indicators.indicator import (
    get_price_selector_function,
)
from trazy_analysis.indicators.indicators_managers import ReactiveIndicators
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.enums import ExecutionMode

SYMBOL1 = "IVV"
SYMBOL2 = "AAPL"
CANDLE1 = Candle(
    asset=Asset(symbol=SYMBOL1, exchange="IEX"),
    open=323.69,
    high=323.81,
    low=323.67,
    close=323.81,
    volume=500,
    timestamp=datetime.strptime("2020-05-06 14:24:00+0000", "%Y-%m-%d %H:%M:%S%z"),
)
CANDLE2 = Candle(
    asset=Asset(symbol=SYMBOL1, exchange="IEX"),
    open=323.81,
    high=324.21,
    low=323.81,
    close=324.10,
    volume=700,
    timestamp=datetime.strptime("2020-05-07 14:25:00+0000", "%Y-%m-%d %H:%M:%S%z"),
)
CANDLE3 = Candle(
    asset=Asset(symbol=SYMBOL1, exchange="IEX"),
    open=324.10,
    high=324.10,
    low=323.97,
    close=324.03,
    volume=400,
    timestamp=datetime.strptime("2020-05-07 14:26:00+0000", "%Y-%m-%d %H:%M:%S%z"),
)
CANDLE4 = Candle(
    asset=Asset(symbol=SYMBOL1, exchange="IEX"),
    open=324.04,
    high=324.09,
    low=323.96,
    close=323.94,
    volume=250,
    timestamp=datetime.strptime("2020-05-07 14:29:00+0000", "%Y-%m-%d %H:%M:%S%z"),
)
CANDLE5 = Candle(
    asset=Asset(symbol=SYMBOL1, exchange="IEX"),
    open=323.93,
    high=323.95,
    low=323.83,
    close=323.88,
    volume=300,
    timestamp=datetime.strptime("2020-05-07 14:31:00+0000", "%Y-%m-%d %H:%M:%S%z"),
)
CANDLE6 = Candle(
    asset=Asset(symbol=SYMBOL1, exchange="IEX"),
    open=323.88,
    high=323.90,
    low=323.75,
    close=323.79,
    volume=200,
    timestamp=datetime.strptime("2020-05-07 14:36:00+0000", "%Y-%m-%d %H:%M:%S%z"),
)
CANDLE7 = Candle(
    asset=Asset(symbol=SYMBOL1, exchange="IEX"),
    open=324.88,
    high=324.90,
    low=324.75,
    close=324.79,
    volume=400,
    timestamp=datetime.strptime("2020-05-08 14:36:00+0000", "%Y-%m-%d %H:%M:%S%z"),
)

MARKET_CAL = EUREXExchangeCalendar()
indicators = ReactiveIndicators(memoize=False, mode=ExecutionMode.LIVE)


def test_get_price_selector_function():
    assert get_price_selector_function(PriceType.OPEN)(CANDLE1) == CANDLE1.open
    assert get_price_selector_function(PriceType.HIGH)(CANDLE1) == CANDLE1.high
    assert get_price_selector_function(PriceType.LOW)(CANDLE1) == CANDLE1.low
    assert get_price_selector_function(PriceType.CLOSE)(CANDLE1) == CANDLE1.close
    with pytest.raises(Exception):
        assert get_price_selector_function(PriceType.LAST)(CANDLE1) == CANDLE1.close


def test_rolling_window_init():
    rolling_window = indicators.Indicator(size=5, dtype=float)
    assert rolling_window.size == 5
    assert rolling_window.window.size == 5
    assert rolling_window.source_dtype is None
    assert rolling_window.count() == 0


def test_rolling_window_init_prefill():
    rolling_window = indicators.Indicator(size=5)
    rolling_window.fill(array=[2, 3, 4])
    expected_array = ma.masked_array([0] * 5, mask=True)
    expected_array[-3:] = [2, 3, 4]
    assert rolling_window.insert == 0
    assert rolling_window.count() == 3
    assert (rolling_window.window[-3:] == expected_array[-3:]).all()

    rolling_window = indicators.Indicator(size=5)
    rolling_window.fill(array=[2, 3, 4, 5, 6, 7, 8])
    assert rolling_window.insert == 0
    expected_array = np.array([4, 5, 6, 7, 8], dtype=int)
    assert rolling_window.count() == 5
    assert (rolling_window.window == expected_array).all()


def test_rolling_window_push():
    rolling_window = indicators.Indicator(size=3)
    assert rolling_window.count() == 0
    assert rolling_window.insert == 0

    rolling_window.push(1)
    assert rolling_window.count() == 1
    assert rolling_window.insert == 1
    expected_array = np.empty(shape=3, dtype=int)
    expected_array[0] = 1
    assert rolling_window.window[0] == expected_array[0]

    rolling_window.push(2)
    assert rolling_window.count() == 2
    assert rolling_window.insert == 2
    expected_array = np.empty(shape=3, dtype=int)
    expected_array[0:2] = [1, 2]
    assert (rolling_window.window[0:1] == expected_array[0:1]).all()

    rolling_window.push(3)
    assert rolling_window.count() == 3
    assert rolling_window.insert == 0
    expected_array = np.empty(shape=3, dtype=int)
    expected_array[0:] = [1, 2, 3]
    assert (rolling_window.window[0:] == expected_array[0:]).all()


def test_rolling_window_filled():
    stream_data = indicators.Indicator(size=1)
    rolling_window = indicators.Indicator(source=stream_data, size=3)
    stream_data.next(1)
    assert not rolling_window.filled()
    stream_data.next(2)
    assert not rolling_window.filled()
    stream_data.next(3)
    assert rolling_window.filled()


def test_rolling_window_map():
    stream_data = indicators.Indicator(size=1)
    rolling_window = indicators.Indicator(source=stream_data, size=5)
    rolling_window.fill(array=[1, 2, 3, 4, 5])
    assert rolling_window.count() == 5
    assert rolling_window.insert == 0

    mapped_rolling_window = rolling_window.map(lambda x: 2 * x)
    assert mapped_rolling_window.count() == 5
    assert mapped_rolling_window.insert == 0
    assert (mapped_rolling_window.window == np.array([2, 4, 6, 8, 10], dtype=int)).all()

    stream_data.next(11)
    assert mapped_rolling_window.count() == 5
    assert mapped_rolling_window.insert == 1
    assert (
        mapped_rolling_window.window == np.array([22, 4, 6, 8, 10], dtype=int)
    ).all()

    rolling_window = indicators.Indicator(
        source=stream_data, transform=lambda x: 2 * x + 1, size=5
    )
    rolling_window.fill(array=[1, 2, 3, 4, 5])
    assert rolling_window.count() == 5
    assert rolling_window.insert == 0

    mapped_rolling_window = rolling_window.map(lambda x: x - 5)
    assert mapped_rolling_window.count() == 5
    assert mapped_rolling_window.insert == 0
    assert (mapped_rolling_window.window == np.array([-2, 0, 2, 4, 6], dtype=int)).all()

    stream_data.next(11)
    assert mapped_rolling_window.count() == 5
    assert mapped_rolling_window.insert == 1
    assert (mapped_rolling_window.window == np.array([18, 0, 2, 4, 6], dtype=int)).all()


def test_rolling_window_get_item_live():
    # live
    rolling_window = indicators.Indicator(size=10)
    rolling_window.fill(array=[i for i in range(0, 10)])

    # positive integers
    with pytest.raises(Exception):
        rolling_window[1]
    assert rolling_window[0] == 9
    assert rolling_window[-5] == 4

    # slices
    assert (
        rolling_window[-8:-1] == np.array([i for i in range(1, 8)], dtype=int)
    ).all()
    with pytest.raises(Exception):
        rolling_window[-3:2]
    with pytest.raises(Exception):
        rolling_window[1:-5]
    assert (rolling_window[-9:-1:2] == np.array([0, 2, 4, 6], dtype=int)).all()

    # invalid tuple type
    with pytest.raises(Exception):
        rolling_window[(1, 3)]

    # add new element to make the insert section change
    rolling_window.push(10)
    rolling_window.push(11)
    assert (
        rolling_window[-8:-1] == np.array([3, 4, 5, 6, 7, 8, 9, 10], dtype=int)
    ).all()


def test_rolling_window_get_item_not_live():
    indicators = ReactiveIndicators(memoize=False, mode=ExecutionMode.BATCH)
    rolling_window = indicators.Indicator(size=10)
    rolling_window.fill(array=[i for i in range(0, 10)])

    # positive integers
    with pytest.raises(Exception):
        rolling_window[0]

    rolling_window.push()
    assert rolling_window[0] == 9

    with pytest.raises(Exception):
        assert rolling_window[-5] == 4

    for i in range(0, 6):
        rolling_window.push()
    assert rolling_window[-5] == 4

    for i in range(0, 3):
        rolling_window.push()

    # slices
    assert (
        rolling_window[-8:-1] == np.array([i for i in range(1, 8)], dtype=int)
    ).all()
    with pytest.raises(Exception):
        rolling_window[-3:2]
    with pytest.raises(Exception):
        rolling_window[1:-5]
    assert (rolling_window[-9:-1:2] == np.array([0, 2, 4, 6], dtype=int)).all()

    # invalid tuple type
    with pytest.raises(Exception):
        rolling_window[(1, 3)]


def test_time_framed_candle_rolling_window_handle_data_1_minute_data():
    rolling_window = indicators.TimeFramedCandleIndicator(
        time_unit=timedelta(minutes=1), market_cal=MARKET_CAL, size=2
    )
    rolling_window.push(CANDLE1)
    assert rolling_window.data == CANDLE1
    rolling_window.push(CANDLE2)
    assert rolling_window.data == CANDLE2
    rolling_window.push(CANDLE3)
    assert rolling_window.data == CANDLE3


def test_time_framed_candle_rolling_window_handle_data_5_minute_data():
    rolling_window = indicators.TimeFramedCandleIndicator(
        time_unit=timedelta(minutes=5), market_cal=MARKET_CAL, size=2
    )
    rolling_window.push(CANDLE1)
    assert rolling_window.data is None
    rolling_window.push(CANDLE2)
    assert rolling_window.data is None
    rolling_window.push(CANDLE3)
    assert rolling_window.data is None
    rolling_window.push(CANDLE4)
    expected_candle1 = Candle(
        asset=Asset(symbol="IVV", exchange="IEX"),
        open=323.81,
        high=324.21,
        low=323.81,
        close=323.94,
        volume=1350,
        timestamp=datetime.strptime("2020-05-07 14:25:00+00:00", "%Y-%m-%d %H:%M:%S%z"),
    )
    assert rolling_window.data == expected_candle1
    rolling_window.push(CANDLE5)
    assert rolling_window.data == expected_candle1
    rolling_window.push(CANDLE6)
    assert rolling_window.data == Candle(
        asset=Asset(symbol="IVV", exchange="IEX"),
        open=323.93,
        high=323.95,
        low=323.83,
        close=323.88,
        volume=300,
        timestamp=datetime.strptime("2020-05-07 14:30:00+00:00", "%Y-%m-%d %H:%M:%S%z"),
    )


def test_time_framed_candle_rolling_window_handle_data_1_day_data():
    rolling_window = indicators.TimeFramedCandleIndicator(
        time_unit=timedelta(days=1), market_cal=MARKET_CAL, size=2
    )
    rolling_window.push(CANDLE1)
    assert rolling_window.data is None
    rolling_window.push(CANDLE2)
    assert rolling_window.data is None
    rolling_window.push(CANDLE3)
    assert rolling_window.data is None
    rolling_window.push(CANDLE4)
    assert rolling_window.data is None
    rolling_window.push(CANDLE5)
    assert rolling_window.data is None
    rolling_window.push(CANDLE6)
    assert rolling_window.data is None
    rolling_window.push(CANDLE7)
    assert rolling_window.data == Candle(
        asset=Asset(symbol="IVV", exchange="IEX"),
        open=323.81,
        high=324.21,
        low=323.75,
        close=323.79,
        volume=1850,
        timestamp=datetime.strptime("2020-05-07 00:00:00+00:00", "%Y-%m-%d %H:%M:%S%z"),
    )
