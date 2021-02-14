import threading
from datetime import datetime
from unittest.mock import patch

import numpy as np

from candles_queue.fake_queue import FakeQueue
from common.types import CandleDataFrame
from db_storage.mongodb_storage import MongoDbStorage
from feed.feed import (
    CsvFeed,
    ExternalStorageFeed,
    Feed,
    HistoricalFeed,
    LiveFeed,
    OfflineFeed,
    PandasFeed,
)
from market_data.historical.tiingo_historical_data_handler import (
    TiingoHistoricalDataHandler,
)
from market_data.live.tiingo_live_data_handler import TiingoLiveDataHandler
from models.candle import Candle
from settings import DATABASE_NAME

DB_STORAGE = MongoDbStorage(DATABASE_NAME)
QUEUE_NAME = "candles"

AAPL_SYMBOL = "AAPL"
AAPL_CANDLES1 = np.array(
    [
        Candle(
            symbol=AAPL_SYMBOL,
            open=355.15,
            high=355.15,
            low=353.74,
            close=353.84,
            volume=3254,
            timestamp=datetime.strptime(
                "2020-06-18 13:30:00+0000", "%Y-%m-%d %H:%M:%S%z"
            ),
        ),
        Candle(
            symbol=AAPL_SYMBOL,
            open=354.28,
            high=354.96,
            low=353.96,
            close=354.78,
            volume=2324,
            timestamp=datetime.strptime(
                "2020-06-18 13:31:00+0000", "%Y-%m-%d %H:%M:%S%z"
            ),
        ),
    ],
    dtype=Candle,
)

AAPL_CANDLES2 = np.array(
    [
        Candle(
            symbol=AAPL_SYMBOL,
            open=354.78,
            high=354.95,
            low=354.74,
            close=354.90,
            volume=2534,
            timestamp=datetime.strptime(
                "2020-06-18 13:32:00+0000", "%Y-%m-%d %H:%M:%S%z"
            ),
        ),
        Candle(
            symbol=AAPL_SYMBOL,
            open=354.90,
            high=355.06,
            low=354.86,
            close=355.04,
            volume=2234,
            timestamp=datetime.strptime(
                "2020-06-18 13:33:00+0000", "%Y-%m-%d %H:%M:%S%z"
            ),
        ),
    ],
    dtype=Candle,
)
AAPL_CANDLES = np.concatenate([AAPL_CANDLES1, AAPL_CANDLES2])
AAPL_CANDLE_DATAFRAME = CandleDataFrame.from_candle_list(
    symbol=AAPL_SYMBOL, candles=AAPL_CANDLES
)

GOOGL_SYMBOL = "GOOGL"
GOOGL_CANDLES1 = np.array(
    [
        Candle(
            symbol=GOOGL_SYMBOL,
            open=354.92,
            high=355.32,
            low=354.09,
            close=354.09,
            volume=1123,
            timestamp=datetime.strptime(
                "2020-06-18 13:32:00+0000", "%Y-%m-%d %H:%M:%S%z"
            ),
        ),
        Candle(
            symbol=GOOGL_SYMBOL,
            open=354.25,
            high=354.59,
            low=354.14,
            close=354.59,
            volume=2613,
            timestamp=datetime.strptime(
                "2020-06-18 13:33:00+0000", "%Y-%m-%d %H:%M:%S%z"
            ),
        ),
    ],
    dtype=Candle,
)
GOOGL_CANDLES2 = np.array(
    [
        Candle(
            symbol=GOOGL_SYMBOL,
            open=354.62,
            high=354.92,
            low=354.54,
            close=354.75,
            volume=2113,
            timestamp=datetime.strptime(
                "2020-06-18 13:34:00+0000", "%Y-%m-%d %H:%M:%S%z"
            ),
        ),
        Candle(
            symbol=GOOGL_SYMBOL,
            open=354.78,
            high=354.95,
            low=354.47,
            close=354.76,
            volume=1326,
            timestamp=datetime.strptime(
                "2020-06-18 13:35:00+0000", "%Y-%m-%d %H:%M:%S%z"
            ),
        ),
    ],
    dtype=Candle,
)
GOOGL_CANDLES = np.concatenate([GOOGL_CANDLES1, GOOGL_CANDLES2])
GOOGL_CANDLE_DATAFRAME = CandleDataFrame.from_candle_list(
    symbol=GOOGL_SYMBOL, candles=GOOGL_CANDLES
)

SYMBOL = "IVV"
CANDLES = np.array(
    [
        Candle(
            symbol=SYMBOL,
            open=94.12,
            high=94.15,
            low=94.00,
            close=94.13,
            volume=7,
            timestamp=datetime.strptime(
                "2020-05-08 14:17:00+0000", "%Y-%m-%d %H:%M:%S%z"
            ),
        ),
        Candle(
            symbol=SYMBOL,
            open=94.07,
            high=94.10,
            low=93.95,
            close=94.08,
            volume=91,
            timestamp=datetime.strptime(
                "2020-05-08 14:24:00+0000", "%Y-%m-%d %H:%M:%S%z"
            ),
        ),
        Candle(
            symbol=SYMBOL,
            open=94.07,
            high=94.10,
            low=93.95,
            close=94.08,
            volume=30,
            timestamp=datetime.strptime(
                "2020-05-08 14:24:56+0000", "%Y-%m-%d %H:%M:%S%z"
            ),
        ),
        Candle(
            symbol=SYMBOL,
            open=94.17,
            high=94.18,
            low=94.05,
            close=94.18,
            volume=23,
            timestamp=datetime.strptime(
                "2020-05-08 14:35:00+0000", "%Y-%m-%d %H:%M:%S%z"
            ),
        ),
        Candle(
            symbol=SYMBOL,
            open=94.19,
            high=94.22,
            low=94.07,
            close=94.20,
            volume=21,
            timestamp=datetime.strptime(
                "2020-05-08 14:41:00+0000", "%Y-%m-%d %H:%M:%S%z"
            ),
        ),
        Candle(
            symbol=SYMBOL,
            open=94.19,
            high=94.22,
            low=94.07,
            close=94.20,
            volume=7,
            timestamp=datetime.strptime(
                "2020-05-08 14:41:58+0000", "%Y-%m-%d %H:%M:%S%z"
            ),
        ),
    ],
    dtype=Candle,
)


def test_feed():
    candles_queue = FakeQueue(QUEUE_NAME)

    feed = Feed(
        candles_queue,
        {AAPL_SYMBOL: AAPL_CANDLES1, GOOGL_SYMBOL: GOOGL_CANDLES1},
        seconds=1,
    )

    aapl_idx = 0
    googl_idx = 0
    checks = True
    key_lock = threading.Lock()

    def callback(candle: Candle):
        symbol = candle.symbol
        with key_lock:
            nonlocal aapl_idx
            nonlocal googl_idx
            nonlocal checks
            if symbol == AAPL_SYMBOL:
                if candle != AAPL_CANDLES1[aapl_idx]:
                    checks = False
                aapl_idx += 1
            else:
                if candle != GOOGL_CANDLES1[googl_idx]:
                    checks = False
                googl_idx += 1

    candles_queue.add_consumer_no_retry(callback)

    feed.start()
    assert checks


@patch(
    "market_data.live.tiingo_live_data_handler.TiingoLiveDataHandler.request_ticker_lastest_candles"
)
def test_live_feed(request_ticker_lastest_candles_mocked):
    candles_queue = FakeQueue(QUEUE_NAME)

    request_ticker_lastest_candles_mocked.side_effect = [
        AAPL_CANDLES1,
        GOOGL_CANDLES1,
        AAPL_CANDLES2,
        GOOGL_CANDLES2,
    ]
    tiingo_live_data_handler = TiingoLiveDataHandler()
    live_feed = LiveFeed(
        [AAPL_SYMBOL], candles_queue, tiingo_live_data_handler, seconds=1
    )

    aapl_idx = 0
    googl_idx = 0
    checks = True
    key_lock = threading.Lock()

    def callback(candle: Candle):
        symbol = candle.symbol
        with key_lock:
            nonlocal aapl_idx
            nonlocal googl_idx
            nonlocal checks
            if symbol == AAPL_SYMBOL:
                if candle != AAPL_CANDLES[aapl_idx]:
                    checks = False
                aapl_idx += 1
            else:
                if candle != GOOGL_CANDLES[googl_idx]:
                    checks = False
                googl_idx += 1
            if aapl_idx == len(AAPL_CANDLES) and googl_idx == len(GOOGL_CANDLES):
                live_feed.stop()

    candles_queue.add_consumer_no_retry(callback)

    live_feed.start()
    assert checks


def test_offline_feed():
    candles_queue = FakeQueue(QUEUE_NAME)

    offline_feed = OfflineFeed(
        candles_queue, {AAPL_SYMBOL: AAPL_CANDLES1, GOOGL_SYMBOL: GOOGL_CANDLES1}
    )

    aapl_idx = 0
    googl_idx = 0
    checks = True
    key_lock = threading.Lock()

    def callback(candle: Candle):
        symbol = candle.symbol
        with key_lock:
            nonlocal aapl_idx
            nonlocal googl_idx
            nonlocal checks
            if symbol == AAPL_SYMBOL:
                if candle != AAPL_CANDLES1[aapl_idx]:
                    checks = False
                aapl_idx += 1
            else:
                if candle != GOOGL_CANDLES1[googl_idx]:
                    checks = False
                googl_idx += 1

    candles_queue.add_consumer_no_retry(callback)

    offline_feed.start()
    assert checks


@patch(
    "market_data.historical.historical_data_handler.HistoricalDataHandler.request_ticker_data_in_range"
)
def test_historical_feed(request_ticker_data_in_range_mocked):
    candles_queue = FakeQueue(QUEUE_NAME)
    tiingo_historical_data_handler = TiingoHistoricalDataHandler()
    start = datetime.strptime("2020-06-11 20:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    end = datetime.strptime("2020-06-26 16:00:00+0000", "%Y-%m-%d %H:%M:%S%z")

    request_ticker_data_in_range_mocked.side_effect = [
        (
            AAPL_CANDLE_DATAFRAME,
            set(),
            {},
        ),
        (
            GOOGL_CANDLE_DATAFRAME,
            set(),
            {},
        ),
    ]
    historical_feed = HistoricalFeed(
        [AAPL_SYMBOL, GOOGL_SYMBOL],
        candles_queue,
        tiingo_historical_data_handler,
        start,
        end,
    )

    aapl_idx = 0
    googl_idx = 0
    checks = True
    key_lock = threading.Lock()

    def callback(candle: Candle):
        symbol = candle.symbol
        with key_lock:
            nonlocal aapl_idx
            nonlocal googl_idx
            nonlocal checks
            if symbol == AAPL_SYMBOL:
                if candle != AAPL_CANDLES[aapl_idx]:
                    checks = False
                aapl_idx += 1
            else:
                if candle != GOOGL_CANDLES[googl_idx]:
                    checks = False
                googl_idx += 1
            if aapl_idx == len(AAPL_CANDLES) and googl_idx == len(GOOGL_CANDLES):
                historical_feed.stop()

    candles_queue.add_consumer_no_retry(callback)

    historical_feed.start()
    assert checks


def test_pandas_feed():
    candles_queue = FakeQueue(QUEUE_NAME)
    candle_dataframes = [
        CandleDataFrame.from_candle_list(symbol=AAPL_SYMBOL, candles=AAPL_CANDLES),
        CandleDataFrame.from_candle_list(symbol=GOOGL_SYMBOL, candles=GOOGL_CANDLES),
    ]
    pandas_feed = PandasFeed(candle_dataframes, candles_queue)

    aapl_idx = 0
    googl_idx = 0
    checks = True
    key_lock = threading.Lock()

    def callback(candle: Candle):
        symbol = candle.symbol
        with key_lock:
            nonlocal aapl_idx
            nonlocal googl_idx
            nonlocal checks
            if symbol == AAPL_SYMBOL:
                if candle != AAPL_CANDLES[aapl_idx]:
                    checks = False
                aapl_idx += 1
            else:
                if candle != GOOGL_CANDLES[googl_idx]:
                    checks = False
                googl_idx += 1
            if aapl_idx == len(AAPL_CANDLES) and googl_idx == len(GOOGL_CANDLES):
                pandas_feed.stop()

    candles_queue.add_consumer_no_retry(callback)

    pandas_feed.start()
    assert checks


def test_csv_feed():
    candles_queue = FakeQueue(QUEUE_NAME)

    csv_feed = CsvFeed(
        {
            AAPL_SYMBOL: "test/data/aapl_candles.csv",
            GOOGL_SYMBOL: "test/data/googl_candles.csv",
        },
        candles_queue,
    )

    aapl_idx = 0
    googl_idx = 0
    checks = True
    key_lock = threading.Lock()

    def callback(candle: Candle):
        symbol = candle.symbol
        with key_lock:
            nonlocal aapl_idx
            nonlocal googl_idx
            nonlocal checks
            if symbol == AAPL_SYMBOL:
                if candle != AAPL_CANDLES[aapl_idx]:
                    checks = False
                aapl_idx += 1
            else:
                if candle != GOOGL_CANDLES[googl_idx]:
                    checks = False
                googl_idx += 1
            if aapl_idx == len(AAPL_CANDLES) and googl_idx == len(GOOGL_CANDLES):
                csv_feed.stop()

    candles_queue.add_consumer_no_retry(callback)

    csv_feed.start()
    assert checks


@patch("file_storage.meganz_file_storage.MegaNzFileStorage.get_file_content")
def test_external_storage_feed(get_file_content_mocked):
    DB_STORAGE.clean_all_candles()
    for candle in CANDLES:
        DB_STORAGE.add_candle(candle)

    get_file_content_mocked.side_effect = [
        (
            "timestamp,open,high,low,close,volume\n"
            "2020-05-08 14:11:00+00:00,94.15,95.15,93.74,93.84,32\n"
            "2020-05-08 14:12:00+00:00,94.28,94.96,93.96,94.78,23\n"
            "2020-05-08 14:13:00+00:00,94.92,95.32,94.09,94.09,11\n"
            "2020-05-08 14:14:00+00:00,94.25,94.59,94.14,94.59,26\n"
            "2020-05-08 14:15:00+00:00,94.22,94.26,93.95,93.98,11\n"
        )
    ]
    candles_queue = FakeQueue(QUEUE_NAME)

    external_storage_feed = ExternalStorageFeed(
        symbols=[SYMBOL],
        candles_queue=candles_queue,
        start=datetime.strptime("2020-05-08 14:12:00+0000", "%Y-%m-%d %H:%M:%S%z"),
        end=datetime.strptime("2020-05-08 14:49:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    )

    idx = 0
    checks = True
    key_lock = threading.Lock()

    def callback(candle: Candle):
        symbol = candle.symbol
        with key_lock:
            nonlocal idx
            nonlocal checks
            assert symbol == SYMBOL
            if candle != CANDLES[idx]:
                checks = False
            idx += 1
            if idx == len(CANDLES):
                external_storage_feed.stop()

    candles_queue.add_consumer_no_retry(callback)

    external_storage_feed.start()
    assert checks
