from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from trazy_analysis.common.exchange_calendar_euronext import EuronextExchangeCalendar
from trazy_analysis.indicators.common import PriceType
from trazy_analysis.indicators.indicator import Indicator
from trazy_analysis.indicators.indicators_manager import IndicatorsManager
from trazy_analysis.indicators.rolling_window import (
    PriceRollingWindowManager,
    RollingWindow,
    RollingWindowManager,
    TimeFramedCandleRollingWindow,
    TimeFramedCandleRollingWindowManager,
    get_price_selector_function,
)
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle

SYMBOL1 = "IVV"
SYMBOL2 = "AAPL"
CANDLE1 = Candle(
    asset=Asset(symbol=SYMBOL1, exchange="IEX"),
    open=323.69,
    high=323.81,
    low=323.67,
    close=323.81,
    volume=500,
    timestamp=datetime.strptime("2020-05-07 14:24:00+0000", "%Y-%m-%d %H:%M:%S%z"),
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
    open=323.93,
    high=323.95,
    low=323.83,
    close=323.88,
    volume=300,
    timestamp=datetime.strptime("2020-05-07 14:31:00+0000", "%Y-%m-%d %H:%M:%S%z"),
)
CANDLE5 = Candle(
    asset=Asset(symbol=SYMBOL1, exchange="IEX"),
    open=323.88,
    high=323.90,
    low=323.75,
    close=323.79,
    volume=200,
    timestamp=datetime.strptime("2020-05-07 14:36:00+0000", "%Y-%m-%d %H:%M:%S%z"),
)
CANDLE6 = Candle(
    asset=Asset(symbol=SYMBOL1, exchange="IEX"),
    open=324.88,
    high=324.90,
    low=324.75,
    close=324.79,
    volume=400,
    timestamp=datetime.strptime("2020-05-08 14:36:00+0000", "%Y-%m-%d %H:%M:%S%z"),
)

MARKET_CAL = EuronextExchangeCalendar()


def test_get_price_selector_function():
    assert get_price_selector_function(PriceType.OPEN)(CANDLE1) == CANDLE1.open
    assert get_price_selector_function(PriceType.HIGH)(CANDLE1) == CANDLE1.high
    assert get_price_selector_function(PriceType.LOW)(CANDLE1) == CANDLE1.low
    assert get_price_selector_function(PriceType.CLOSE)(CANDLE1) == CANDLE1.close
    with pytest.raises(Exception):
        assert get_price_selector_function(PriceType.LAST)(CANDLE1) == CANDLE1.close


def test_rolling_window_stream_init():
    rolling_window = RollingWindow(size=5, preload=False)
    assert rolling_window.window.size == 5
    assert rolling_window.dtype is None
    assert rolling_window.nb_elts == 0


def test_rolling_window_stream_init_prefill():
    rolling_window = RollingWindow(size=5, dtype=int, preload=False)
    rolling_window.prefill(filling_array=[2, 3, 4])
    expected_array = np.empty(shape=5, dtype=int)
    expected_array[-3:] = [2, 3, 4]
    assert rolling_window.insert == 0
    assert rolling_window.nb_elts == 3
    assert (rolling_window.window[-3:] == expected_array[-3:]).all()

    rolling_window = RollingWindow(size=5, dtype=int, preload=False)
    rolling_window.prefill(filling_array=[2, 3, 4, 5, 6, 7, 8])
    assert rolling_window.insert == 0
    expected_array = np.array([4, 5, 6, 7, 8], dtype=int)
    assert rolling_window.nb_elts == 5
    assert (rolling_window.window == expected_array).all()


def test_rolling_window_stream_push():
    rolling_window = RollingWindow(size=3, dtype=int, preload=False)
    assert rolling_window.nb_elts == 0
    assert rolling_window.insert == 0

    rolling_window.push(1)
    assert rolling_window.nb_elts == 1
    assert rolling_window.insert == 1
    expected_array = np.empty(shape=3, dtype=int)
    expected_array[0] = 1
    assert rolling_window.window[0] == expected_array[0]

    rolling_window.push(2)
    assert rolling_window.nb_elts == 2
    assert rolling_window.insert == 2
    expected_array = np.empty(shape=3, dtype=int)
    expected_array[0:2] = [1, 2]
    assert (rolling_window.window[0:1] == expected_array[0:1]).all()

    rolling_window.push(3)
    assert rolling_window.nb_elts == 3
    assert rolling_window.insert == 0
    expected_array = np.empty(shape=3, dtype=int)
    expected_array[0:] = [1, 2, 3]
    assert (rolling_window.window[0:] == expected_array[0:]).all()


def test_rolling_window_stream_filled():
    stream_data = Indicator()
    rolling_window = RollingWindow(size=3, source_indicator=stream_data, preload=False)
    stream_data.on_next(1)
    assert not rolling_window.filled()
    stream_data.on_next(2)
    assert not rolling_window.filled()
    stream_data.on_next(3)
    assert rolling_window.filled()


def test_rolling_window_stream_map():
    stream_data = Indicator()
    rolling_window = RollingWindow(
        size=5, source_indicator=stream_data, dtype=int, preload=False
    )
    rolling_window.prefill(filling_array=[1, 2, 3, 4, 5])
    assert rolling_window.nb_elts == 5
    assert rolling_window.insert == 0

    mapped_rolling_window = rolling_window.map(lambda x: 2 * x)
    assert mapped_rolling_window.nb_elts == 5
    assert mapped_rolling_window.insert == 0
    assert (mapped_rolling_window.window == np.array([2, 4, 6, 8, 10], dtype=int)).all()

    stream_data.on_next(11)
    assert mapped_rolling_window.nb_elts == 5
    assert mapped_rolling_window.insert == 1
    assert (
        mapped_rolling_window.window == np.array([22, 4, 6, 8, 10], dtype=int)
    ).all()

    rolling_window = RollingWindow(
        size=5,
        transform=lambda x: 2 * x + 1,
        source_indicator=stream_data,
        dtype=int,
        preload=False,
    )
    rolling_window.prefill(filling_array=[1, 2, 3, 4, 5])
    assert rolling_window.nb_elts == 5
    assert rolling_window.insert == 0

    mapped_rolling_window = rolling_window.map(lambda x: x - 5)
    assert mapped_rolling_window.nb_elts == 5
    assert mapped_rolling_window.insert == 0
    assert (mapped_rolling_window.window == np.array([-2, 0, 2, 4, 6], dtype=int)).all()

    stream_data.on_next(11)
    assert mapped_rolling_window.nb_elts == 5
    assert mapped_rolling_window.insert == 1
    assert (mapped_rolling_window.window == np.array([18, 0, 2, 4, 6], dtype=int)).all()


def test_rolling_window_stream_get_item_live():
    # live
    rolling_window = RollingWindow(size=10, preload=False)
    rolling_window.prefill(filling_array=[i for i in range(0, 10)])

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
        rolling_window[-3:1]
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


def test_rolling_window_stream_get_item_not_live():
    rolling_window = RollingWindow(size=10, preload=True)
    rolling_window.prefill(filling_array=[i for i in range(0, 10)])

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
        rolling_window[-3:1]
    with pytest.raises(Exception):
        rolling_window[1:-5]
    assert (rolling_window[-9:-1:2] == np.array([0, 2, 4, 6], dtype=int)).all()

    # invalid tuple type
    with pytest.raises(Exception):
        rolling_window[(1, 3)]


def test_time_framed_candle_rolling_window_stream_handle_new_data_1_minute_data():
    rolling_window = TimeFramedCandleRollingWindow(
        size=2, time_unit=timedelta(minutes=1), market_cal=MARKET_CAL, preload=False
    )
    rolling_window.push(CANDLE1)
    assert rolling_window.data == CANDLE1
    rolling_window.push(CANDLE2)
    assert rolling_window.data == CANDLE2
    rolling_window.push(CANDLE3)
    assert rolling_window.data == CANDLE3


def test_time_framed_candle_rolling_window_stream_handle_new_data_5_minute_data():
    rolling_window = TimeFramedCandleRollingWindow(
        size=2, time_unit=timedelta(minutes=5), market_cal=MARKET_CAL, preload=False
    )
    rolling_window.push(CANDLE1)
    assert rolling_window.data is None
    rolling_window.push(CANDLE2)
    assert rolling_window.data is None
    rolling_window.push(CANDLE3)
    assert rolling_window.data == Candle(
        asset=Asset(symbol=SYMBOL1, exchange="IEX"),
        open=323.69,
        high=324.21,
        low=323.67,
        close=324.10,
        volume=1200,
        timestamp=datetime.strptime("2020-05-07 14:25:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    )
    rolling_window.push(CANDLE4)
    assert rolling_window.data == Candle(
        asset=Asset(symbol=SYMBOL1, exchange="IEX"),
        open=324.10,
        high=324.10,
        low=323.97,
        close=324.03,
        volume=400,
        timestamp=datetime.strptime("2020-05-07 14:30:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    )
    rolling_window.push(CANDLE5)
    assert rolling_window.data == Candle(
        asset=Asset(symbol=SYMBOL1, exchange="IEX"),
        open=323.93,
        high=323.95,
        low=323.83,
        close=323.88,
        volume=300,
        timestamp=datetime.strptime("2020-05-07 14:35:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    )


def test_time_framed_candle_rolling_window_stream_handle_new_data_1_day_data():
    rolling_window = TimeFramedCandleRollingWindow(
        size=2, time_unit=timedelta(days=1), market_cal=MARKET_CAL, preload=False
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
    assert rolling_window.data == Candle(
        asset=Asset(symbol="IVV", exchange="IEX"),
        open="323.69",
        high="324.21",
        low="323.67",
        close="323.79",
        volume=2100,
        timestamp=datetime.strptime("2020-05-07 00:00:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    )


def test_rolling_window_factory():
    rolling_window_factory = RollingWindowManager()
    rolling_window1 = rolling_window_factory(SYMBOL1, period=6)
    rolling_window2 = rolling_window_factory(SYMBOL1, period=9)
    rolling_window3 = rolling_window_factory(SYMBOL1, period=15)
    rolling_window4 = rolling_window_factory(SYMBOL2, period=20)
    assert id(rolling_window1) == id(rolling_window2) == id(rolling_window3)
    assert id(rolling_window1) != id(rolling_window4)


def test_time_framed_rolling_window_factory():
    indicators_manager = IndicatorsManager()
    time_framed_rolling_window_factory = TimeFramedCandleRollingWindowManager(
        indicators_manager.rolling_window_manager
    )
    time_framed_rolling_window1 = time_framed_rolling_window_factory(
        SYMBOL1, period=3, time_unit=timedelta(minutes=5)
    )
    time_framed_rolling_window2 = time_framed_rolling_window_factory(
        SYMBOL1, period=3, time_unit=timedelta(minutes=5)
    )
    time_framed_rolling_window3 = time_framed_rolling_window_factory(
        SYMBOL1, period=3, time_unit=pd.offsets.Hour(4)
    )
    time_framed_rolling_window4 = time_framed_rolling_window_factory(
        SYMBOL1, period=5, time_unit=timedelta(minutes=5)
    )
    time_framed_rolling_window5 = time_framed_rolling_window_factory(
        SYMBOL2, period=3, time_unit=timedelta(minutes=5)
    )
    assert id(time_framed_rolling_window1) == id(time_framed_rolling_window2)
    assert id(time_framed_rolling_window1) != id(time_framed_rolling_window3)
    assert id(time_framed_rolling_window1) == id(time_framed_rolling_window4)
    assert id(time_framed_rolling_window1) != id(time_framed_rolling_window5)


def test_price_rolling_window_factory():
    indicators_manager = IndicatorsManager()

    price_rolling_window_factory = PriceRollingWindowManager(
        indicators_manager.time_framed_candle_rolling_window_manager
    )
    price_rolling_window1 = price_rolling_window_factory(
        SYMBOL1, period=3, time_unit=timedelta(minutes=5), price_type=PriceType.CLOSE
    )
    price_rolling_window2 = price_rolling_window_factory(
        SYMBOL1, period=3, time_unit=timedelta(minutes=5), price_type=PriceType.CLOSE
    )
    price_rolling_window3 = price_rolling_window_factory(
        SYMBOL1, period=3, time_unit=timedelta(minutes=5), price_type=PriceType.LOW
    )
    price_rolling_window4 = price_rolling_window_factory(
        SYMBOL1, period=3, time_unit=pd.offsets.Minute(10), price_type=PriceType.CLOSE
    )
    price_rolling_window5 = price_rolling_window_factory(
        SYMBOL1, period=5, time_unit=timedelta(minutes=5), price_type=PriceType.CLOSE
    )
    price_rolling_window6 = price_rolling_window_factory(
        SYMBOL2, period=3, time_unit=timedelta(minutes=5), price_type=PriceType.CLOSE
    )
    assert id(price_rolling_window1) == id(price_rolling_window2)
    assert id(price_rolling_window1) != id(price_rolling_window3)
    assert id(price_rolling_window1) != id(price_rolling_window4)
    assert id(price_rolling_window1) == id(price_rolling_window5)
    assert id(price_rolling_window1) != id(price_rolling_window6)
