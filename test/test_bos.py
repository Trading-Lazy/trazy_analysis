from datetime import timedelta
from typing import List, Tuple

import numpy as np
import pytest

from trazy_analysis.feed.loader import CsvLoader
from trazy_analysis.indicators.indicator import Indicator
from trazy_analysis.indicators.indicators_managers import ReactiveIndicators
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.enums import CandleDirection, IndicatorMode

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
indicators = ReactiveIndicators(memoize=False, mode=IndicatorMode.LIVE)


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
def test_imbalance(three_candles: list[Candle], expected: Tuple[bool, float]):
    source = indicators.Indicator(size=1)
    imbalance = indicators.Imbalance(source=source)
    for i in range(0, len(three_candles)):
        source.push(three_candles[i])
    assert imbalance.data == expected

def get_non_pin_bar_candle(close: float) -> Candle:
    open = close - 1
    high = close + 0.1
    low = open - 0.1
    return Candle(asset=ASSET, open=open, high=high, low=low, close=close, volume=1)


def test_candle_bos():
    indicator_data = indicators.Indicator(size=1)
    loader = CsvLoader(
        csv_filenames={ASSET: {timedelta(minutes=1): "test/data/bos.csv"}}
    )
    loader.load()
    candles = loader.candles[ASSET][timedelta(minutes=1)]
    bos = indicators.CandleBOS(comparator=np.greater, order=1, source=indicator_data, size=candles.size)
    for candle in candles:
        indicator_data.push(candle)
    expected_window = [False] * len(candles)
    expected_window[-2] = True
    expected_window = np.array(expected_window, dtype=bool)
    assert (bos.window == expected_window).all()


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
    two_candles: list[Candle], direction: CandleDirection, expected: bool
):
    source = indicators.Indicator(size=1)
    engulfing_candle = indicators.EngulfingCandle(direction=direction, source=source)
    for i in range(0, len(two_candles)):
        source.push(two_candles[i])
    assert engulfing_candle.data == expected
