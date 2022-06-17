from typing import List, Tuple

import numpy as np
import pytest

from trazy_analysis.feed.loader import CsvLoader
from trazy_analysis.indicators.bos import CandleBOS, Imbalance, EngulfingCandle
from trazy_analysis.indicators.indicator import Indicator
from trazy_analysis.indicators.rolling_window import RollingWindow

from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.enums import CandleDirection

BIG_DATA = [
    56281.78,
    56281.07,
    56338.77,
    56305.65,
    56330.49,
    56332.01,
    56363.13,
    56415.98,
    56263.37,
    56225.93,
    56221.6,
    56161.32,
    56121.94,
    56053.32,
    56079.39,
    56134.97,
    56099.95,
    56195.57,
    56200.0,
    56252.22,
    56349.5,
    56441.55,
    56499.02,
    56481.97,
    56615.0,
    56832.36,
    56833.74,
    56832.26,
    56713.74,
    56768.34,
    56776.41,
    56769.36,
    56735.84,
    56695.43,
    56605.0,
    56571.99,
    56510.0,
    56530.86,
    56563.44,
    56575.51,
    56599.79,
    56498.99,
    56424.0,
    56343.34,
    56378.15,
    56372.86,
    56310.05,
    56390.39,
    56423.04,
    56417.82,
]

ASSET = Asset(symbol="BTCUSDT", exchange="Binance")


@pytest.mark.parametrize(
    "three_candles, expected",
    [
        # 2 bullish candles no gap between them
        (
            [
                Candle(asset=ASSET, open=5, high=12, low=3, close=10, volume=20),
                Candle(asset=ASSET, open=10, high=17, low=8, close=15, volume=20),
            ],
            (False, 0),
        ),
        # 2 bearish candles no gap between them
        (
            [
                Candle(asset=ASSET, open=10, high=17, low=8, close=15, volume=20),
                Candle(asset=ASSET, open=5, high=12, low=3, close=10, volume=20),
            ],
            (False, 0),
        ),
        # 2 bullish candles with gap between them
        (
            [
                Candle(asset=ASSET, open=5, high=12, low=3, close=10, volume=20),
                Candle(asset=ASSET, open=15, high=20, low=13, close=18, volume=20),
            ],
            (True, 1),
        ),
        # 2 bearish candles with gap between them
        (
            [
                Candle(asset=ASSET, open=15, high=20, low=13, close=18, volume=20),
                Candle(asset=ASSET, open=5, high=12.5, low=3, close=10, volume=20),
            ],
            (True, 0.5),
        ),
        # 3 bullish candles no gap between them
        (
            [
                Candle(asset=ASSET, open=5, high=12, low=3, close=10, volume=20),
                Candle(asset=ASSET, open=10, high=17, low=8, close=15, volume=20),
                Candle(asset=ASSET, open=14, high=21, low=12, close=19, volume=20),
            ],
            (False, 0),
        ),
        # 3 bearish candles no gap between them
        (
            [
                Candle(asset=ASSET, open=14, high=21, low=12, close=19, volume=20),
                Candle(asset=ASSET, open=10, high=17, low=8, close=15, volume=20),
                Candle(asset=ASSET, open=5, high=12, low=3, close=10, volume=20),
            ],
            (False, 0),
        ),
        # 3 bullish candles with gap between first and third candles
        (
            [
                Candle(asset=ASSET, open=5, high=12, low=3, close=10, volume=20),
                Candle(asset=ASSET, open=10, high=17, low=8, close=15, volume=20),
                Candle(asset=ASSET, open=15, high=21, low=14, close=19, volume=20),
            ],
            (True, 2),
        ),
        # 3 bearish candles with gap between first and third candles
        (
            [
                Candle(asset=ASSET, open=14, high=21, low=12, close=19, volume=20),
                Candle(asset=ASSET, open=10, high=17, low=8, close=15, volume=20),
                Candle(asset=ASSET, open=2, high=9, low=1, close=7, volume=20),
            ],
            (True, 3),
        ),
    ],
)
def test_imbalance(three_candles: List[Candle], expected: Tuple[bool, float]):
    # rolling_window_stream = RollingWindow(size=3, preload=False)
    # rolling_window_stream.prefill(filling_array=three_candles)
    source = Indicator()
    imbalance = Imbalance(source_indicator=source)
    for i in range(0, len(three_candles)):
        source.push(three_candles[i])
    assert imbalance.data == expected


def test_bos_stream_handle_new_data_source_is_indicator_data():
    indicator_data = Indicator()
    bos = CandleBOS(
        comparator=np.greater,
        order=1,
        source_indicator=indicator_data,
        size=1,
        preload=False,
    )
    indicator_data.push(7.2)
    assert bos.data is False
    indicator_data.push(6.1)
    assert bos.data is False
    indicator_data.push(6.3)
    assert bos.data is False
    indicator_data.push(7.2)
    assert bos.data is False
    indicator_data.push(6.9)
    assert bos.data is False
    indicator_data.push(6.8)
    assert bos.data is True


def test_bos_stream_handle_new_data_source_is_rolling_window_stream():
    rolling_window_stream = RollingWindow(size=3, preload=False)
    bos = CandleBOS(
        comparator=np.greater,
        order=2,
        source_indicator=rolling_window_stream,
        size=1,
        preload=False,
    )
    rolling_window_stream.push(7.2)
    assert bos.data is False
    rolling_window_stream.push(6.1)
    assert bos.data is False
    rolling_window_stream.push(6.3)
    assert bos.data is False
    rolling_window_stream.push(7.2)
    assert bos.data is False
    rolling_window_stream.push(6.9)
    assert bos.data is False
    rolling_window_stream.push(6.8)
    assert bos.data is True


def test_bos_stream_handle_new_data_source_is_filled():
    indicator_data = Indicator()
    loader = CsvLoader(csv_filenames={ASSET: "test/data/bos.csv"})
    loader.load()
    candles = loader.candles[ASSET]
    rolling_window_stream = RollingWindow(
        size=candles.size, source_indicator=indicator_data, preload=False
    )
    rolling_window_stream.prefill(filling_array=candles)
    bos = CandleBOS(
        comparator=np.greater,
        order=1,
        source_indicator=rolling_window_stream,
        size=candles.size,
        preload=False,
    )
    # assert bos.window == expected_window


@pytest.mark.parametrize(
    "two_candles, direction, expected",
    [
        # 2 bullish candles overlapping
        (
            [
                Candle(asset=ASSET, open=5, high=12, low=3, close=10, volume=20),
                Candle(asset=ASSET, open=10, high=17, low=8, close=15, volume=20),
            ],
            CandleDirection.BULLISH,
            False,
        ),
        # 1 bearish candle and 1 bullish candle not engulfing the previous candle
        (
            [
                Candle(asset=ASSET, open=12, high=14, low=3, close=5, volume=20),
                Candle(asset=ASSET, open=8, high=17, low=7, close=15, volume=20),
            ],
            CandleDirection.BULLISH,
            False,
        ),
        # 1 bearish candle and 1 bullish candle engulfing the previous candle
        (
            [
                Candle(asset=ASSET, open=12, high=14, low=3, close=5, volume=20),
                Candle(asset=ASSET, open=4, high=14, low=2, close=13, volume=20),
            ],
            CandleDirection.BULLISH,
            True,
        ),
        # 1 bullish candle and 1 bearish candle engulfing the previous candle
        (
            [
                Candle(asset=ASSET, open=5, high=14, low=3, close=12, volume=20),
                Candle(asset=ASSET, open=13, high=14, low=2, close=4, volume=20),
            ],
            CandleDirection.BEARISH,
            True,
        ),
    ],
)
def test_engulfing_candle(
    two_candles: List[Candle], direction: CandleDirection, expected: bool
):
    source = Indicator()
    engulfing_candle = EngulfingCandle(direction=direction, source_indicator=source)
    for i in range(0, len(two_candles)):
        source.push(two_candles[i])
    assert engulfing_candle.data == expected
