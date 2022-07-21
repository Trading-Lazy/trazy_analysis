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
from trazy_analysis.models.enums import IndicatorMode

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
indicators = ReactiveIndicators(memoize=False, mode=IndicatorMode.LIVE)


def test_get_price_selector_function():
    assert get_price_selector_function(PriceType.OPEN)(CANDLE1) == CANDLE1.open
    assert get_price_selector_function(PriceType.HIGH)(CANDLE1) == CANDLE1.high
    assert get_price_selector_function(PriceType.LOW)(CANDLE1) == CANDLE1.low
    assert get_price_selector_function(PriceType.CLOSE)(CANDLE1) == CANDLE1.close
    with pytest.raises(Exception):
        assert get_price_selector_function(PriceType.LAST)(CANDLE1) == CANDLE1.close


def test_indicator_init():
    indicator = indicators.Indicator(size=5, dtype=float)
    assert indicator.size == 5
    assert indicator.window.size == 5
    assert indicator.source_dtype is None
    assert indicator.count() == 0


def test_indicator_init_prefill():
    indicator = indicators.Indicator(size=5)
    indicator.fill(array=[2, 3, 4])
    expected_array = ma.masked_array([0] * 5, mask=True)
    expected_array[-3:] = [2, 3, 4]
    assert indicator.insert == 0
    assert indicator.count() == 3
    assert (indicator.window[-3:] == expected_array[-3:]).all()

    indicator = indicators.Indicator(size=5)
    indicator.fill(array=[2, 3, 4, 5, 6, 7, 8])
    assert indicator.insert == 0
    expected_array = np.array([4, 5, 6, 7, 8], dtype=int)
    assert indicator.count() == 5
    assert (indicator.window == expected_array).all()


def test_indicator_push():
    indicator = indicators.Indicator(size=3)
    assert indicator.count() == 0
    assert indicator.insert == 0

    indicator.push(1)
    assert indicator.count() == 1
    assert indicator.insert == 1
    expected_array = np.empty(shape=3, dtype=int)
    expected_array[0] = 1
    assert indicator.window[0] == expected_array[0]

    indicator.push(2)
    assert indicator.count() == 2
    assert indicator.insert == 2
    expected_array = np.empty(shape=3, dtype=int)
    expected_array[0:2] = [1, 2]
    assert (indicator.window[0:1] == expected_array[0:1]).all()

    indicator.push(3)
    assert indicator.count() == 3
    assert indicator.insert == 0
    expected_array = np.empty(shape=3, dtype=int)
    expected_array[0:] = [1, 2, 3]
    assert (indicator.window[0:] == expected_array[0:]).all()


def test_indicator_filled():
    stream_data = indicators.Indicator(size=1)
    indicator = indicators.Indicator(source=stream_data, size=3)
    stream_data.next(1)
    assert not indicator.filled()
    stream_data.next(2)
    assert not indicator.filled()
    stream_data.next(3)
    assert indicator.filled()


def test_indicator_map():
    stream_data = indicators.Indicator(size=1)
    indicator = indicators.Indicator(source=stream_data, size=5)
    indicator.fill(array=[1, 2, 3, 4, 5])
    assert indicator.count() == 5
    assert indicator.insert == 0

    mapped_indicator = indicator.map(lambda x: 2 * x)
    assert mapped_indicator.count() == 5
    assert mapped_indicator.insert == 0
    assert (mapped_indicator.window == np.array([2, 4, 6, 8, 10], dtype=int)).all()

    stream_data.next(11)
    assert mapped_indicator.count() == 5
    assert mapped_indicator.insert == 1
    assert (
        mapped_indicator.window == np.array([22, 4, 6, 8, 10], dtype=int)
    ).all()

    indicator = indicators.Indicator(
        source=stream_data, transform=lambda x: 2 * x + 1, size=5
    )
    indicator.fill(array=[1, 2, 3, 4, 5])
    assert indicator.count() == 5
    assert indicator.insert == 0

    mapped_indicator = indicator.map(lambda x: x - 5)
    assert mapped_indicator.count() == 5
    assert mapped_indicator.insert == 0
    assert (mapped_indicator.window == np.array([-2, 0, 2, 4, 6], dtype=int)).all()

    stream_data.next(11)
    assert mapped_indicator.count() == 5
    assert mapped_indicator.insert == 1
    assert (mapped_indicator.window == np.array([18, 0, 2, 4, 6], dtype=int)).all()


def test_indicator_get_item_live():
    # live
    indicator = indicators.Indicator(size=10)
    indicator.fill(array=[i for i in range(0, 10)])

    # positive integers
    with pytest.raises(Exception):
        indicator[1]
    assert indicator[0] == 9
    assert indicator[-5] == 4

    # slices
    assert (
        indicator[-8:-1] == np.array([i for i in range(1, 8)], dtype=int)
    ).all()
    with pytest.raises(Exception):
        indicator[-3:2]
    with pytest.raises(Exception):
        indicator[1:-5]
    assert (indicator[-9:-1:2] == np.array([0, 2, 4, 6], dtype=int)).all()

    # invalid tuple type
    with pytest.raises(Exception):
        indicator[(1, 3)]

    # add new element to make the insert section change
    indicator.push(10)
    indicator.push(11)
    assert (
        indicator[-8:-1] == np.array([3, 4, 5, 6, 7, 8, 9, 10], dtype=int)
    ).all()


def test_indicator_get_item_not_live():
    indicators = ReactiveIndicators(memoize=False, mode=IndicatorMode.BATCH)
    indicator = indicators.Indicator(size=10)
    indicator.fill(array=[i for i in range(0, 10)])

    # positive integers
    with pytest.raises(Exception):
        indicator[0]

    indicator.push()
    assert indicator[0] == 9

    with pytest.raises(Exception):
        assert indicator[-5] == 4

    for i in range(0, 6):
        indicator.push()
    assert indicator[-5] == 4

    for i in range(0, 3):
        indicator.push()

    # slices
    assert (
        indicator[-8:-1] == np.array([i for i in range(1, 8)], dtype=int)
    ).all()
    with pytest.raises(Exception):
        indicator[-3:2]
    with pytest.raises(Exception):
        indicator[1:-5]
    assert (indicator[-9:-1:2] == np.array([0, 2, 4, 6], dtype=int)).all()

    # invalid tuple type
    with pytest.raises(Exception):
        indicator[(1, 3)]


def test_time_framed_candle_indicator_handle_data_1_minute_data():
    indicator = indicators.TimeFramedCandleIndicator(
        time_unit=timedelta(minutes=1), market_cal=MARKET_CAL, size=2
    )
    indicator.push(CANDLE1)
    assert indicator.data == CANDLE1
    indicator.push(CANDLE2)
    assert indicator.data == CANDLE2
    indicator.push(CANDLE3)
    assert indicator.data == CANDLE3


def test_time_framed_candle_indicator_handle_data_5_minute_data():
    indicator = indicators.TimeFramedCandleIndicator(
        time_unit=timedelta(minutes=5), market_cal=MARKET_CAL, size=2
    )
    indicator.push(CANDLE1)
    assert indicator.data is None
    indicator.push(CANDLE2)
    assert indicator.data is None
    indicator.push(CANDLE3)
    assert indicator.data is None
    indicator.push(CANDLE4)
    expected_candle1 = Candle(
        asset=Asset(symbol="IVV", exchange="IEX"),
        open=323.81,
        high=324.21,
        low=323.81,
        close=323.94,
        volume=1350,
        time_unit=timedelta(minutes=5),
        timestamp=datetime.strptime("2020-05-07 14:25:00+00:00", "%Y-%m-%d %H:%M:%S%z"),
    )
    assert indicator.data == expected_candle1
    indicator.push(CANDLE5)
    assert indicator.data == expected_candle1
    indicator.push(CANDLE6)
    assert indicator.data == Candle(
        asset=Asset(symbol="IVV", exchange="IEX"),
        open=323.93,
        high=323.95,
        low=323.83,
        close=323.88,
        volume=300,
        time_unit=timedelta(minutes=5),
        timestamp=datetime.strptime("2020-05-07 14:30:00+00:00", "%Y-%m-%d %H:%M:%S%z"),
    )


def test_time_framed_candle_indicator_handle_data_1_day_data():
    indicator = indicators.TimeFramedCandleIndicator(
        time_unit=timedelta(days=1), market_cal=MARKET_CAL, size=2
    )
    indicator.push(CANDLE1)
    assert indicator.data is None
    indicator.push(CANDLE2)
    assert indicator.data is None
    indicator.push(CANDLE3)
    assert indicator.data is None
    indicator.push(CANDLE4)
    assert indicator.data is None
    indicator.push(CANDLE5)
    assert indicator.data is None
    indicator.push(CANDLE6)
    assert indicator.data is None
    indicator.push(CANDLE7)
    print(str(indicator.data))
    assert indicator.data == Candle(
        asset=Asset(symbol="IVV", exchange="IEX"),
        open=323.81,
        high=324.21,
        low=323.75,
        close=323.79,
        volume=1850,
        time_unit=timedelta(days=1),
        timestamp=datetime.strptime("2020-05-07 00:00:00+00:00", "%Y-%m-%d %H:%M:%S%z"),
    )
