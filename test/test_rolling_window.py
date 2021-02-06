from decimal import Decimal

import pandas as pd
import pytest

from common.exchange_calendar_euronext import EuronextExchangeCalendar
from indicators.common import PriceType
from indicators.indicators import IndicatorsManager
from indicators.rolling_window import (
    PriceRollingWindowManager,
    RollingWindowManager,
    RollingWindowStream,
    TimeFramedCandleRollingWindowManager,
    TimeFramedCandleRollingWindowStream,
    get_price_selector_function,
)
from indicators.stream import StreamData
from models.candle import Candle

SYMBOL1 = "IVV"
SYMBOL2 = "AAPL"
CANDLE1 = Candle(
    symbol=SYMBOL1,
    open=Decimal("323.69"),
    high=Decimal("323.81"),
    low=Decimal("323.67"),
    close=Decimal("323.81"),
    volume=500,
    timestamp=pd.Timestamp("2020-05-07 14:24:00", tz="UTC"),
)
CANDLE2 = Candle(
    symbol=SYMBOL1,
    open=Decimal("323.81"),
    high=Decimal("324.21"),
    low=Decimal("323.81"),
    close=Decimal("324.10"),
    volume=700,
    timestamp=pd.Timestamp("2020-05-07 14:25:00", tz="UTC"),
)
CANDLE3 = Candle(
    symbol=SYMBOL1,
    open=Decimal("324.10"),
    high=Decimal("324.10"),
    low=Decimal("323.97"),
    close=Decimal("324.03"),
    volume=400,
    timestamp=pd.Timestamp("2020-05-07 14:26:00", tz="UTC"),
)
CANDLE4 = Candle(
    symbol=SYMBOL1,
    open=Decimal("323.93"),
    high=Decimal("323.95"),
    low=Decimal("323.83"),
    close=Decimal("323.88"),
    volume=300,
    timestamp=pd.Timestamp("2020-05-07 14:31:00", tz="UTC"),
)
CANDLE5 = Candle(
    symbol=SYMBOL1,
    open=Decimal("323.88"),
    high=Decimal("323.90"),
    low=Decimal("323.75"),
    close=Decimal("323.79"),
    volume=200,
    timestamp=pd.Timestamp("2020-05-07 14:36:00", tz="UTC"),
)
CANDLE6 = Candle(
    symbol=SYMBOL1,
    open=Decimal("324.88"),
    high=Decimal("324.90"),
    low=Decimal("324.75"),
    close=Decimal("324.79"),
    volume=400,
    timestamp=pd.Timestamp("2020-05-08 14:36:00", tz="UTC"),
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
    rolling_window = RollingWindowStream(period=5)
    assert rolling_window.window == [None] * 5


def test_rolling_window_stream_init_prefill():
    rolling_window = RollingWindowStream(period=5, prefill_list=[2, 3, 4])
    assert rolling_window.window == [None, None, 2, 3, 4]
    rolling_window = RollingWindowStream(period=5, prefill_list=[2, 3, 4, 5, 6, 7, 8])
    assert rolling_window.window == [4, 5, 6, 7, 8]


def test_rolling_window_stream_push():
    rolling_window = RollingWindowStream(period=3)
    rolling_window.push(1)
    assert rolling_window.window == [None, None, 1]
    rolling_window.push(2)
    assert rolling_window.window == [None, 1, 2]
    rolling_window.push(3)
    assert rolling_window.window == [1, 2, 3]
    rolling_window.push(4)
    assert rolling_window.window == [2, 3, 4]


def test_rolling_window_stream_filled():
    stream_data = StreamData()
    rolling_window = RollingWindowStream(period=3, source_data=stream_data)
    stream_data.on_next(1)
    assert not rolling_window.filled()
    stream_data.on_next(2)
    assert not rolling_window.filled()
    stream_data.on_next(3)
    assert rolling_window.filled()


def test_rolling_window_stream_map():
    stream_data = StreamData()
    rolling_window = RollingWindowStream(
        period=5, prefill_list=[1, 2, 3, 4, 5], source_data=stream_data
    )

    mapped_rolling_window = rolling_window.map(lambda x: 2 * x)
    assert mapped_rolling_window.window == [2, 4, 6, 8, 10]

    stream_data.on_next(11)
    assert mapped_rolling_window.window == [4, 6, 8, 10, 22]

    rolling_window = RollingWindowStream(
        period=5,
        prefill_list=[1, 2, 3, 4, 5],
        transform=lambda x: 2 * x + 1,
        source_data=stream_data,
    )

    mapped_rolling_window = rolling_window.map(lambda x: x - 5)
    assert mapped_rolling_window.window == [-2, 0, 2, 4, 6]

    stream_data.on_next(11)
    assert mapped_rolling_window.window == [0, 2, 4, 6, 18]


def test_rolling_window_stream_get_item():
    rolling_window = RollingWindowStream(
        period=10, prefill_list=[i for i in range(0, 10)]
    )

    # integers
    with pytest.raises(Exception):
        rolling_window[1]
    assert rolling_window[0] == 9
    assert rolling_window[-5] == 4

    # slices
    assert rolling_window[-9:-1] == [i for i in range(0, 8)]
    with pytest.raises(Exception):
        rolling_window[-3:1]
    with pytest.raises(Exception):
        rolling_window[1:-5]
    assert rolling_window[-9:-1:2] == [0, 2, 4, 6]

    # invalid tuple type
    with pytest.raises(Exception):
        rolling_window[(1, 3)]


def test_time_framed_candle_rolling_window_stream_handle_new_data_1_minute_data():
    rolling_window = TimeFramedCandleRollingWindowStream(
        period=2, time_unit=pd.offsets.Minute(1), market_cal=MARKET_CAL
    )
    rolling_window.push(CANDLE1)
    assert rolling_window.data == CANDLE1
    rolling_window.push(CANDLE2)
    assert rolling_window.data == CANDLE2
    rolling_window.push(CANDLE3)
    assert rolling_window.data == CANDLE3


def test_time_framed_candle_rolling_window_stream_handle_new_data_5_minute_data():
    rolling_window = TimeFramedCandleRollingWindowStream(
        period=2, time_unit=pd.offsets.Minute(5), market_cal=MARKET_CAL
    )
    rolling_window.push(CANDLE1)
    assert rolling_window.data is None
    rolling_window.push(CANDLE2)
    assert rolling_window.data is None
    rolling_window.push(CANDLE3)
    assert rolling_window.data == Candle(
        symbol=SYMBOL1,
        open=Decimal("323.69"),
        high=Decimal("324.21"),
        low=Decimal("323.67"),
        close=Decimal("324.10"),
        volume=1200,
        timestamp=pd.Timestamp("2020-05-07 14:25:00+00:00"),
    )
    rolling_window.push(CANDLE4)
    assert rolling_window.data == Candle(
        symbol=SYMBOL1,
        open=Decimal("324.10"),
        high=Decimal("324.10"),
        low=Decimal("323.97"),
        close=Decimal("324.03"),
        volume=400,
        timestamp=pd.Timestamp("2020-05-07 14:30:00+00:00"),
    )
    rolling_window.push(CANDLE5)
    assert rolling_window.data == Candle(
        symbol=SYMBOL1,
        open=Decimal("323.93"),
        high=Decimal("323.95"),
        low=Decimal("323.83"),
        close=Decimal("323.88"),
        volume=300,
        timestamp=pd.Timestamp("2020-05-07 14:35:00+00:00"),
    )


def test_time_framed_candle_rolling_window_stream_handle_new_data_1_day_data():
    rolling_window = TimeFramedCandleRollingWindowStream(
        period=2, time_unit=pd.offsets.Day(1), market_cal=MARKET_CAL
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
        symbol="IVV",
        open="323.69",
        high="324.21",
        low="323.67",
        close="323.79",
        volume=2100,
        timestamp=pd.Timestamp("2020-05-07 00:00:00+00:00"),
    )


def test_rolling_window_factory():
    rolling_window_factory = RollingWindowManager()
    rolling_window1 = rolling_window_factory(SYMBOL1, period=5)
    rolling_window2 = rolling_window_factory(SYMBOL1, period=5)
    rolling_window3 = rolling_window_factory(SYMBOL1, period=3)
    rolling_window4 = rolling_window_factory(SYMBOL2, period=5)
    assert id(rolling_window1) == id(rolling_window2)
    assert id(rolling_window1) != id(rolling_window3)
    assert id(rolling_window1) != id(rolling_window4)


def test_time_framed_rolling_window_factory():
    indicators_manager = IndicatorsManager()
    time_framed_rolling_window_factory = TimeFramedCandleRollingWindowManager(
        indicators_manager
    )
    time_framed_rolling_window1 = time_framed_rolling_window_factory(
        SYMBOL1, period=3, time_unit=pd.offsets.Minute(5)
    )
    time_framed_rolling_window2 = time_framed_rolling_window_factory(
        SYMBOL1, period=3, time_unit=pd.offsets.Minute(5)
    )
    time_framed_rolling_window3 = time_framed_rolling_window_factory(
        SYMBOL1, period=3, time_unit=pd.offsets.Hour(4)
    )
    time_framed_rolling_window4 = time_framed_rolling_window_factory(
        SYMBOL1, period=5, time_unit=pd.offsets.Minute(5)
    )
    time_framed_rolling_window5 = time_framed_rolling_window_factory(
        SYMBOL2, period=3, time_unit=pd.offsets.Minute(5)
    )
    assert id(time_framed_rolling_window1) == id(time_framed_rolling_window2)
    assert id(time_framed_rolling_window1) != id(time_framed_rolling_window3)
    assert id(time_framed_rolling_window1) != id(time_framed_rolling_window4)
    assert id(time_framed_rolling_window1) != id(time_framed_rolling_window5)


def test_price_rolling_window_factory():
    indicators_manager = IndicatorsManager()
    price_rolling_window_factory = PriceRollingWindowManager(indicators_manager)
    price_rolling_window1 = price_rolling_window_factory(
        SYMBOL1, period=3, time_unit=pd.offsets.Minute(5), price_type=PriceType.CLOSE
    )
    price_rolling_window2 = price_rolling_window_factory(
        SYMBOL1, period=3, time_unit=pd.offsets.Minute(5), price_type=PriceType.CLOSE
    )
    price_rolling_window3 = price_rolling_window_factory(
        SYMBOL1, period=3, time_unit=pd.offsets.Minute(5), price_type=PriceType.LOW
    )
    price_rolling_window4 = price_rolling_window_factory(
        SYMBOL1, period=3, time_unit=pd.offsets.Minute(10), price_type=PriceType.CLOSE
    )
    price_rolling_window5 = price_rolling_window_factory(
        SYMBOL1, period=5, time_unit=pd.offsets.Minute(5), price_type=PriceType.CLOSE
    )
    price_rolling_window6 = price_rolling_window_factory(
        SYMBOL2, period=3, time_unit=pd.offsets.Minute(5), price_type=PriceType.CLOSE
    )
    assert id(price_rolling_window1) == id(price_rolling_window2)
    assert id(price_rolling_window1) != id(price_rolling_window3)
    assert id(price_rolling_window1) != id(price_rolling_window4)
    assert id(price_rolling_window1) != id(price_rolling_window5)
    assert id(price_rolling_window1) != id(price_rolling_window6)
