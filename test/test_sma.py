import pytest

from trazy_analysis.indicators.indicators_managers import ReactiveIndicators
from trazy_analysis.models.enums import IndicatorMode

SYMBOL1 = "IVV"
SYMBOL2 = "AAPL"
indicators = ReactiveIndicators(memoize=False, mode=IndicatorMode.LIVE)


def test_sma_stream_handle_data_source_is_indicator_data():
    indicator_data = indicators.Indicator(size=1)
    sma = indicators.Sma(source=indicator_data, period=3)
    indicator_data.push(7.2)
    assert sma.data is None
    indicator_data.push(6.7)
    assert sma.data is None
    indicator_data.push(6.3)
    assert sma.data == 6.733333333333333333333333333


def test_sma_stream_handle_data_source_is_indicator_stream():
    indicator_stream = indicators.Indicator(size=3)
    sma = indicators.Sma(source=indicator_stream, period=3)
    indicator_stream.push(7.2)
    assert sma.data is None
    indicator_stream.push(6.7)
    assert sma.data is None
    indicator_stream.push(6.3)
    assert sma.data == 6.733333333333333333333333333


def test_sma_stream_handle_data_source_is_indicator_stream_with_lower_period():
    indicator_stream = indicators.Indicator(source=None, size=2)
    sma = indicators.Sma(source=indicator_stream, period=3)
    indicator_stream.push(7.2)
    assert sma.data is None
    indicator_stream.push(6.7)
    assert sma.data is None
    indicator_stream.push(6.3)
    assert sma.data == 6.733333333333333333333333333


def test_sma_stream_handle_data_source_is_filled_indicator_stream():
    indicator_stream = indicators.Indicator(None, size=3)
    indicator_stream.fill(array=[7.2, 6.7, 6.3])
    sma = indicators.Sma(source=indicator_stream, period=3)
    assert sma.data == pytest.approx(6.733, abs=0.01)
    indicator_stream.push(7)
    assert sma.data == 6.666666666666666666666666667
