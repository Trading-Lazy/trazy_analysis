from collections import deque
from datetime import datetime

import numpy as np

from trazy_analysis.feed.feed import CsvFeed, Feed
from trazy_analysis.indicators.indicator import Indicator
from trazy_analysis.indicators.level import Peak, ResistanceLevels, TightTradingRange
from trazy_analysis.indicators.rolling_window import RollingWindow

from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle

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

TTR_CANDLES = [
    Candle(
        asset=Asset(symbol="BTC/USDT", exchange="BINANCE"),
        open=49205.00,
        high=49205.00,
        low=49120.82,
        close=49135.32,
        volume=25.25826,
        timestamp=datetime.strptime("2021-12-04 21:03:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    ),
    Candle(
        asset=Asset(symbol="BTC/USDT", exchange="BINANCE"),
        open=49135.26,
        high=49210.64,
        low=49135.26,
        close=49181.5,
        volume=24.54924,
        timestamp=datetime.strptime("2021-12-04 21:03:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    ),
    Candle(
        asset=Asset(symbol="BTC/USDT", exchange="BINANCE"),
        open=49181.49,
        high=49198.81,
        low=49100.01,
        close=49107.0,
        volume=20.07389,
        timestamp=datetime.strptime("2021-12-04 21:04:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    ),
    Candle(
        asset=Asset(symbol="BTC/USDT", exchange="BINANCE"),
        open=49107.0,
        high=49169.05,
        low=49104.1,
        close=49166.55,
        volume=22.484460000000002,
        timestamp=datetime.strptime("2021-12-04 21:05:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    ),
    Candle(
        asset=Asset(symbol="BTC/USDT", exchange="BINANCE"),
        open=49166.55,
        high=49174.84,
        low=49116.44,
        close=49144.08,
        volume=21.12189,
        timestamp=datetime.strptime("2021-12-04 21:06:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    ),
    Candle(
        asset=Asset(symbol="BTC/USDT", exchange="BINANCE"),
        open=49144.47,
        high=49160.37,
        low=49105.87,
        close=49121.02,
        volume=21.91763,
        timestamp=datetime.strptime("2021-12-04 21:07:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    ),
    Candle(
        asset=Asset(symbol="BTC/USDT", exchange="BINANCE"),
        open=49121.01,
        high=49182.0,
        low=49080.01,
        close=49162.41,
        volume=22.84226,
        timestamp=datetime.strptime("2021-12-04 21:08:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    ),
    Candle(
        asset=Asset(symbol="BTC/USDT", exchange="BINANCE"),
        open=49162.41,
        high=49198.12,
        low=49108.85,
        close=49108.86,
        volume=16.63171,
        timestamp=datetime.strptime("2021-12-04 21:09:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    ),
    Candle(
        asset=Asset(symbol="BTC/USDT", exchange="BINANCE"),
        open=49108.85,
        high=49179.6,
        low=49102.01,
        close=49158.39,
        volume=22.02259,
        timestamp=datetime.strptime("2021-12-04 21:10:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    ),
    Candle(
        asset=Asset(symbol="BTC/USDT", exchange="BINANCE"),
        open=49158.38,
        high=49160.19,
        low=48928.01,
        close=49020.81,
        volume=83.29263,
        timestamp=datetime.strptime("2021-12-04 21:11:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    ),
]


def test_peak_stream_handle_new_data_source_is_indicator_data():
    indicator_data = Indicator()
    peak = Peak(
        comparator=np.greater,
        order=2,
        size=1,
        source_indicator=indicator_data,
        preload=False,
    )
    indicator_data.push(7.2)
    assert peak.data is False
    indicator_data.push(6.1)
    assert peak.data is False
    indicator_data.push(6.3)
    assert peak.data is False
    indicator_data.push(7.2)
    assert peak.data is False
    indicator_data.push(6.9)
    assert peak.data is False
    indicator_data.push(6.8)
    assert peak.data is True


def test_peak_stream_handle_new_data_source_is_rolling_window_stream():
    rolling_window_stream = RollingWindow(size=3, preload=False)
    peak = Peak(
        comparator=np.greater,
        order=2,
        size=1,
        source_indicator=rolling_window_stream,
        preload=False,
    )
    rolling_window_stream.push(7.2)
    assert peak.data is False
    rolling_window_stream.push(6.1)
    assert peak.data is False
    rolling_window_stream.push(6.3)
    assert peak.data is False
    rolling_window_stream.push(7.2)
    assert peak.data is False
    rolling_window_stream.push(6.9)
    assert peak.data is False
    rolling_window_stream.push(6.8)
    assert peak.data is True


def test_peak_stream_handle_new_data_source_is_filled_small_data():
    indicator_data = Indicator()
    rolling_window_stream = RollingWindow(
        size=3, source_indicator=indicator_data, preload=False
    )
    rolling_window_stream.prefill(filling_array=[7.2, 7.5, 2.3])
    peak = Peak(
        comparator=np.greater,
        order=1,
        size=1,
        source_indicator=rolling_window_stream,
        preload=False,
    )
    assert peak.data == True


def test_peak_stream_handle_new_data_source_is_filled_big_data():
    order = 2
    rolling_window_stream = RollingWindow(size=len(BIG_DATA), preload=False)
    rolling_window_stream.prefill(filling_array=BIG_DATA)
    peak = Peak(
        comparator=np.greater_equal,
        order=order,
        size=len(BIG_DATA),
        source_indicator=rolling_window_stream,
    )
    peaks = list(peak.window[order:])
    peaks_indexes = []
    for peak_index, peak_value in enumerate(peaks):
        if peaks[peak_index]:
            peaks_indexes.append(peak_index)

    expected_indexes = [7, 26, 30, 40]
    assert peaks_indexes == expected_indexes


def test_resistance():
    exchange = "BINANCE"
    exchange_asset = Asset(symbol="BTC/USDT", exchange=exchange)
    events = deque()
    feed: Feed = CsvFeed(
        {exchange_asset: f"test/data/btc_usdt.csv"},
        events,
    )
    df = feed.candle_dataframes[exchange_asset]
    candles = df.to_candles()
    rolling_window_stream = RollingWindow(
        size=len(candles), idtype=Candle, preload=False
    )

    r = ResistanceLevels(
        accuracy=2,
        order=2,
        size=1,
        source_indicator=rolling_window_stream,
    )

    index = 0
    high_sum = 0
    low_sum = 0
    count = 0
    for candle in candles:
        count += 1
        high_sum += candle.high
        low_sum += candle.low
        high_avg = high_sum / count
        low_avg = low_sum / count
        rolling_window_stream.push(candle)
        index += 1
        if index == 60:
            break


def test_tight_trading_range():
    exchange = "BINANCE"
    exchange_asset = Asset(symbol="BTC/USDT", exchange=exchange)
    events = deque()
    feed: Feed = CsvFeed(
        {exchange_asset: f"test/data/btc_usdt_tight_trading_range.csv"},
        events,
    )
    df = feed.candle_dataframes[exchange_asset]
    rolling_window_stream = RollingWindow(
        size=len(TTR_CANDLES), idtype=Candle, preload=False
    )
    t = TightTradingRange(
        size=10, min_overlaps=10, source_indicator=rolling_window_stream
    )

    for candle in TTR_CANDLES:
        rolling_window_stream.push(candle)
    for interval in t.trading_ranges:
        data = interval.data
