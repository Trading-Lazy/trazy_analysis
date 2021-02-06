from decimal import Decimal

import pandas as pd

from indicators.common import PriceType
from indicators.indicators import IndicatorsManager
from indicators.rolling_window import RollingWindowStream
from indicators.sma import SmaManager, SmaStream
from indicators.stream import StreamData

SYMBOL1 = "IVV"
SYMBOL2 = "AAPL"


def test_sma_stream_handle_new_data_source_is_stream_data():
    stream_data = StreamData()
    sma = SmaStream(period=3, source_data=stream_data)
    stream_data.on_next(Decimal("7.2"))
    assert sma.data is None
    stream_data.on_next(Decimal("6.7"))
    assert sma.data is None
    stream_data.on_next(Decimal("6.3"))
    assert sma.data == Decimal("6.733333333333333333333333333")


def test_sma_stream_handle_new_data_source_is_rolling_window_stream():
    rolling_window_stream = RollingWindowStream(period=3)
    sma = SmaStream(period=3, source_data=rolling_window_stream)
    rolling_window_stream.push(Decimal("7.2"))
    assert sma.data is None
    rolling_window_stream.push(Decimal("6.7"))
    assert sma.data is None
    rolling_window_stream.push(Decimal("6.3"))
    assert sma.data == Decimal("6.733333333333333333333333333")


def test_sma_stream_handle_new_data_source_is_rolling_window_stream_with_lower_period():
    rolling_window_stream = RollingWindowStream(period=2)
    sma = SmaStream(period=3, source_data=rolling_window_stream)
    rolling_window_stream.push(Decimal("7.2"))
    assert sma.data is None
    rolling_window_stream.push(Decimal("6.7"))
    assert sma.data is None
    rolling_window_stream.push(Decimal("6.3"))
    assert sma.data == Decimal("6.733333333333333333333333333")


def test_sma_stream_handle_new_data_source_is_filled_rolling_window_stream():
    stream_data = StreamData()
    rolling_window_stream = RollingWindowStream(
        period=3,
        prefill_list=[Decimal("7.2"), Decimal("6.7"), Decimal("6.3")],
        source_data=stream_data,
    )
    sma = SmaStream(period=3, source_data=rolling_window_stream)
    assert sma.data == Decimal("6.733333333333333333333333333")
    rolling_window_stream.push(Decimal("7"))
    assert sma.data == Decimal("6.666666666666666666666666667")


def test_sma_factory():
    indicators_manager = IndicatorsManager()
    sma_factory = SmaManager(indicators_manager)
    sma1 = sma_factory(
        SYMBOL1, period=5, time_unit=pd.offsets.Minute(5), price_type=PriceType.CLOSE
    )
    sma2 = sma_factory(
        SYMBOL1, period=5, time_unit=pd.offsets.Minute(5), price_type=PriceType.CLOSE
    )
    sma3 = sma_factory(
        SYMBOL1, period=5, time_unit=pd.offsets.Minute(5), price_type=PriceType.LOW
    )
    sma4 = sma_factory(
        SYMBOL1, period=5, time_unit=pd.offsets.Minute(10), price_type=PriceType.CLOSE
    )
    sma5 = sma_factory(
        SYMBOL1, period=7, time_unit=pd.offsets.Minute(5), price_type=PriceType.CLOSE
    )
    sma6 = sma_factory(
        SYMBOL2, period=5, time_unit=pd.offsets.Minute(5), price_type=PriceType.CLOSE
    )
    assert id(sma1) == id(sma2)
    assert id(sma1) != id(sma3)
    assert id(sma1) != id(sma4)
    assert id(sma1) != id(sma5)
    assert id(sma1) != id(sma6)
