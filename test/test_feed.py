import json
import threading
from decimal import Decimal
from unittest.mock import patch

import pandas as pd

from candles_queue.simple_queue import SimpleQueue
from common.types import CandleDataFrame
from db_storage.mongodb_storage import MongoDbStorage
from feed.feed import (
    CsvFeed,
    Feed,
    HistoricalFeed,
    LiveFeed,
    PandasFeed,
    OfflineFeed)
from market_data.historical.tiingo_historical_data_handler import (
    TiingoHistoricalDataHandler,
)
from market_data.live.tiingo_live_data_handler import TiingoLiveDataHandler
from models.candle import Candle
from settings import DATABASE_NAME

DB_STORAGE = MongoDbStorage(DATABASE_NAME)
QUEUE_NAME = "candles"

AAPL_SYMBOL = "AAPL"
AAPL_CANDLES1 = [
    Candle(
        symbol=AAPL_SYMBOL,
        open=Decimal("355.15"),
        high=Decimal("355.15"),
        low=Decimal("353.74"),
        close=Decimal("353.84"),
        volume=3254,
        timestamp=pd.Timestamp("2020-06-18 13:30:00+00:00"),
    ),
    Candle(
        symbol=AAPL_SYMBOL,
        open=Decimal("354.28"),
        high=Decimal("354.96"),
        low=Decimal("353.96"),
        close=Decimal("354.78"),
        volume=2324,
        timestamp=pd.Timestamp("2020-06-18 13:31:00+00:00"),
    ),
]

AAPL_CANDLES2 = [
    Candle(
        symbol=AAPL_SYMBOL,
        open=Decimal("354.78"),
        high=Decimal("354.95"),
        low=Decimal("354.74"),
        close=Decimal("354.90"),
        volume=2534,
        timestamp=pd.Timestamp("2020-06-18 13:32:00+00:00"),
    ),
    Candle(
        symbol=AAPL_SYMBOL,
        open=Decimal("354.90"),
        high=Decimal("355.06"),
        low=Decimal("354.86"),
        close=Decimal("355.04"),
        volume=2234,
        timestamp=pd.Timestamp("2020-06-18 13:33:00+00:00"),
    ),
]
AAPL_CANDLES = AAPL_CANDLES1 + AAPL_CANDLES2
AAPL_CANDLE_DATAFRAME = CandleDataFrame.from_candle_list(symbol=AAPL_SYMBOL, candles=AAPL_CANDLES)

GOOGL_SYMBOL = "GOOGL"
GOOGL_CANDLES1 = [
    Candle(
        symbol=GOOGL_SYMBOL,
        open=Decimal("354.92"),
        high=Decimal("355.32"),
        low=Decimal("354.09"),
        close=Decimal("354.09"),
        volume=1123,
        timestamp=pd.Timestamp("2020-06-18 13:32:00+00:00"),
    ),
    Candle(
        symbol=GOOGL_SYMBOL,
        open=Decimal("354.25"),
        high=Decimal("354.59"),
        low=Decimal("354.14"),
        close=Decimal("354.59"),
        volume=2613,
        timestamp=pd.Timestamp("2020-06-18 13:33:00+00:00"),
    ),
]
GOOGL_CANDLES2 = [
    Candle(
        symbol=GOOGL_SYMBOL,
        open=Decimal("354.62"),
        high=Decimal("354.92"),
        low=Decimal("354.54"),
        close=Decimal("354.75"),
        volume=2113,
        timestamp=pd.Timestamp("2020-06-18 13:34:00+00:00"),
    ),
    Candle(
        symbol=GOOGL_SYMBOL,
        open=Decimal("354.78"),
        high=Decimal("354.95"),
        low=Decimal("354.47"),
        close=Decimal("354.76"),
        volume=1326,
        timestamp=pd.Timestamp("2020-06-18 13:35:00+00:00"),
    ),
]
GOOGL_CANDLES = GOOGL_CANDLES1 + GOOGL_CANDLES2
GOOGL_CANDLE_DATAFRAME = CandleDataFrame.from_candle_list(symbol=GOOGL_SYMBOL, candles=GOOGL_CANDLES)


def test_feed():
    candles_queue = SimpleQueue(QUEUE_NAME)

    feed = Feed(
        candles_queue, {AAPL_SYMBOL: AAPL_CANDLES1, GOOGL_SYMBOL: GOOGL_CANDLES1}
    )

    aapl_idx = 0
    googl_idx = 0
    checks = True
    key_lock = threading.Lock()

    def callback(json_str: str):
        json_dict = json.loads(json_str)
        symbol = json_dict["symbol"]
        with key_lock:
            nonlocal aapl_idx
            nonlocal googl_idx
            nonlocal checks
            if symbol == AAPL_SYMBOL:
                if json_str != AAPL_CANDLES1[aapl_idx].to_json():
                    checks = False
                aapl_idx += 1
            else:
                if json_str != GOOGL_CANDLES1[googl_idx].to_json():
                    checks = False
                googl_idx += 1

    candles_queue.add_consumer_no_retry(callback)

    feed.start()
    assert checks


@patch(
    "market_data.live.tiingo_live_data_handler.TiingoLiveDataHandler.request_ticker_lastest_candles"
)
def test_live_feed(request_ticker_lastest_candles_mocked):
    candles_queue = SimpleQueue(QUEUE_NAME)

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

    def callback(json_str: str):
        json_dict = json.loads(json_str)
        symbol = json_dict["symbol"]
        with key_lock:
            nonlocal aapl_idx
            nonlocal googl_idx
            nonlocal checks
            if symbol == AAPL_SYMBOL:
                if json_str != AAPL_CANDLES[aapl_idx].to_json():
                    checks = False
                aapl_idx += 1
            else:
                if json_str != GOOGL_CANDLES[googl_idx].to_json():
                    checks = False
                googl_idx += 1
            if aapl_idx == len(AAPL_CANDLES) and googl_idx == len(GOOGL_CANDLES):
                live_feed.stop()

    candles_queue.add_consumer_no_retry(callback)

    live_feed.start()
    assert checks


def test_offline_feed():
    candles_queue = SimpleQueue(QUEUE_NAME)

    offline_feed = OfflineFeed(
        candles_queue, {AAPL_SYMBOL: AAPL_CANDLES1, GOOGL_SYMBOL: GOOGL_CANDLES1}
    )

    aapl_idx = 0
    googl_idx = 0
    checks = True
    key_lock = threading.Lock()

    def callback(json_str: str):
        json_dict = json.loads(json_str)
        symbol = json_dict["symbol"]
        with key_lock:
            nonlocal aapl_idx
            nonlocal googl_idx
            nonlocal checks
            if symbol == AAPL_SYMBOL:
                if json_str != AAPL_CANDLES1[aapl_idx].to_json():
                    checks = False
                aapl_idx += 1
            else:
                if json_str != GOOGL_CANDLES1[googl_idx].to_json():
                    checks = False
                googl_idx += 1

    candles_queue.add_consumer_no_retry(callback)

    offline_feed.start()
    assert checks


@patch(
    "market_data.historical.historical_data_handler.HistoricalDataHandler.request_ticker_data_in_range"
)
def test_historical_feed(request_ticker_data_in_range_mocked):
    candles_queue = SimpleQueue(QUEUE_NAME)
    tiingo_historical_data_handler = TiingoHistoricalDataHandler()
    start = pd.Timestamp("2020-06-11T20:00:00+00:00")
    end = pd.Timestamp("2020-06-26T16:00:00+00:00")

    request_ticker_data_in_range_mocked.side_effect = [
        (AAPL_CANDLE_DATAFRAME, set(), {},),
        (GOOGL_CANDLE_DATAFRAME, set(), {},),
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

    def callback(json_str: str):
        json_dict = json.loads(json_str)
        symbol = json_dict["symbol"]
        with key_lock:
            nonlocal aapl_idx
            nonlocal googl_idx
            nonlocal checks
            if symbol == AAPL_SYMBOL:
                if json_str != AAPL_CANDLES[aapl_idx].to_json():
                    checks = False
                aapl_idx += 1
            else:
                if json_str != GOOGL_CANDLES[googl_idx].to_json():
                    checks = False
                googl_idx += 1
            if aapl_idx == len(AAPL_CANDLES) and googl_idx == len(GOOGL_CANDLES):
                historical_feed.stop()

    candles_queue.add_consumer_no_retry(callback)

    historical_feed.start()
    assert checks


def test_pandas_feed():
    candles_queue = SimpleQueue(QUEUE_NAME)
    candle_dataframes = [
        CandleDataFrame.from_candle_list(symbol=AAPL_SYMBOL, candles=AAPL_CANDLES),
        CandleDataFrame.from_candle_list(symbol=GOOGL_SYMBOL, candles=GOOGL_CANDLES),
    ]
    pandas_feed = PandasFeed(candle_dataframes, candles_queue)

    aapl_idx = 0
    googl_idx = 0
    checks = True
    key_lock = threading.Lock()

    def callback(json_str: str):
        json_dict = json.loads(json_str)
        symbol = json_dict["symbol"]
        with key_lock:
            nonlocal aapl_idx
            nonlocal googl_idx
            nonlocal checks
            if symbol == AAPL_SYMBOL:
                if json_str != AAPL_CANDLES[aapl_idx].to_json():
                    checks = False
                aapl_idx += 1
            else:
                if json_str != GOOGL_CANDLES[googl_idx].to_json():
                    checks = False
                googl_idx += 1
            if aapl_idx == len(AAPL_CANDLES) and googl_idx == len(GOOGL_CANDLES):
                pandas_feed.stop()

    candles_queue.add_consumer_no_retry(callback)

    pandas_feed.start()
    assert checks


def test_csv_feed():
    candles_queue = SimpleQueue(QUEUE_NAME)

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

    def callback(json_str: str):
        json_dict = json.loads(json_str)
        symbol = json_dict["symbol"]
        with key_lock:
            nonlocal aapl_idx
            nonlocal googl_idx
            nonlocal checks
            if symbol == AAPL_SYMBOL:
                if json_str != AAPL_CANDLES[aapl_idx].to_json():
                    checks = False
                aapl_idx += 1
            else:
                if json_str != GOOGL_CANDLES[googl_idx].to_json():
                    checks = False
                googl_idx += 1
            if aapl_idx == len(AAPL_CANDLES) and googl_idx == len(GOOGL_CANDLES):
                csv_feed.stop()

    candles_queue.add_consumer_no_retry(callback)

    csv_feed.start()
    assert checks
