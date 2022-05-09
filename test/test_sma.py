from datetime import timedelta

import pandas as pd
import pytest

from trazy_analysis.indicators.common import PriceType
from trazy_analysis.indicators.indicator import Indicator
from trazy_analysis.indicators.indicators_manager import IndicatorsManager
from trazy_analysis.indicators.rolling_window import RollingWindow
from trazy_analysis.indicators.sma import Sma, SmaManager

SYMBOL1 = "IVV"
SYMBOL2 = "AAPL"


def test_sma_stream_handle_new_data_source_is_indicator_data():
    indicator_data = Indicator()
    sma = Sma(period=3, source_indicator=indicator_data, preload=False)
    indicator_data.push(7.2)
    assert sma.data is None
    indicator_data.push(6.7)
    assert sma.data is None
    indicator_data.push(6.3)
    assert sma.data == 6.733333333333333333333333333


def test_sma_stream_handle_new_data_source_is_rolling_window_stream():
    rolling_window_stream = RollingWindow(size=3, preload=False)
    sma = Sma(period=3, source_indicator=rolling_window_stream, preload=False)
    rolling_window_stream.push(7.2)
    assert sma.data is None
    rolling_window_stream.push(6.7)
    assert sma.data is None
    rolling_window_stream.push(6.3)
    assert sma.data == 6.733333333333333333333333333


def test_sma_stream_handle_new_data_source_is_rolling_window_stream_with_lower_period():
    indicator_data = Indicator()
    rolling_window_stream = RollingWindow(
        size=2, source_indicator=indicator_data, preload=False
    )
    sma = Sma(period=3, source_indicator=rolling_window_stream, preload=False)
    indicator_data.push(7.2)
    assert sma.data is None
    indicator_data.push(6.7)
    assert sma.data is None
    indicator_data.push(6.3)
    assert sma.data == 6.733333333333333333333333333


def test_sma_stream_handle_new_data_source_is_filled_rolling_window_stream():
    indicator_data = Indicator()
    rolling_window_stream = RollingWindow(
        size=3, source_indicator=indicator_data, preload=False
    )
    rolling_window_stream.prefill(filling_array=[7.2, 6.7, 6.3])
    sma = Sma(period=3, source_indicator=rolling_window_stream, preload=False)
    assert sma.data == pytest.approx(6.733, abs=0.01)
    indicator_data.push(7)
    assert sma.data == 6.666666666666666666666666667


def test_sma_factory():
    indicators_manager = IndicatorsManager()
    sma_factory = SmaManager(indicators_manager.price_rolling_window_manager)
    sma1 = sma_factory(
        SYMBOL1, period=5, time_unit=timedelta(minutes=5), price_type=PriceType.CLOSE
    )
    sma2 = sma_factory(
        SYMBOL1, period=5, time_unit=timedelta(minutes=5), price_type=PriceType.CLOSE
    )
    sma3 = sma_factory(
        SYMBOL1, period=5, time_unit=timedelta(minutes=5), price_type=PriceType.LOW
    )
    sma4 = sma_factory(
        SYMBOL1, period=5, time_unit=pd.offsets.Minute(10), price_type=PriceType.CLOSE
    )
    sma5 = sma_factory(
        SYMBOL1, period=7, time_unit=timedelta(minutes=5), price_type=PriceType.CLOSE
    )
    sma6 = sma_factory(
        SYMBOL2, period=5, time_unit=timedelta(minutes=5), price_type=PriceType.CLOSE
    )
    assert id(sma1) == id(sma2)
    assert id(sma1) != id(sma3)
    assert id(sma1) != id(sma4)
    assert id(sma1) != id(sma5)
    assert id(sma1) != id(sma6)
