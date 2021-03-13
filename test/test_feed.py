from collections import deque
from datetime import datetime, timedelta
from unittest.mock import patch

import numpy as np

from common.types import CandleDataFrame
from db_storage.mongodb_storage import MongoDbStorage
from feed.feed import (
    CsvFeed,
    ExternalStorageFeed,
    Feed,
    HistoricalFeed,
    LiveFeed,
    PandasFeed,
)
from file_storage.meganz_file_storage import MegaNzFileStorage
from market_data.historical.tiingo_historical_data_handler import (
    TiingoHistoricalDataHandler,
)
from market_data.live.tiingo_live_data_handler import TiingoLiveDataHandler
from models.candle import Candle
from models.event import MarketDataEndEvent, MarketDataEvent
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

FILE_STORAGE = MegaNzFileStorage()


def test_feed():
    events = deque()
    candles = {AAPL_SYMBOL: AAPL_CANDLES1, GOOGL_SYMBOL: GOOGL_CANDLES1}
    feed = Feed(
        events=events,
        candles=candles,
    )

    for i in range(0, 3):
        feed.update_latest_data()

    events_list = list(events)
    assert len(events_list) == 6
    assert isinstance(events_list[0], MarketDataEvent)
    assert events_list[0].candle == AAPL_CANDLES1[0]
    assert isinstance(events_list[1], MarketDataEvent)
    assert events_list[1].candle == GOOGL_CANDLES1[0]
    assert isinstance(events_list[2], MarketDataEvent)
    assert events_list[2].candle == AAPL_CANDLES1[1]
    assert isinstance(events_list[3], MarketDataEvent)
    assert events_list[3].candle == GOOGL_CANDLES1[1]
    assert isinstance(events_list[4], MarketDataEndEvent)
    assert events_list[4].symbol == AAPL_SYMBOL
    assert isinstance(events_list[5], MarketDataEndEvent)
    assert events_list[5].symbol == GOOGL_SYMBOL


@patch(
    "market_data.live.tiingo_live_data_handler.TiingoLiveDataHandler.request_ticker_lastest_candles"
)
def test_live_feed(request_ticker_lastest_candles_mocked):
    request_ticker_lastest_candles_mocked.side_effect = [
        AAPL_CANDLES1,
        GOOGL_CANDLES1,
        AAPL_CANDLES2,
        GOOGL_CANDLES2,
    ]
    tiingo_live_data_handler = TiingoLiveDataHandler()
    events = deque()
    live_feed = LiveFeed([AAPL_SYMBOL], events, tiingo_live_data_handler)

    for i in range(0, 4):
        live_feed.update_latest_data()

    events_list = list(events)
    assert len(events_list) == 8
    assert isinstance(events_list[0], MarketDataEvent)
    assert events_list[0].candle == AAPL_CANDLES1[0]
    assert isinstance(events_list[1], MarketDataEvent)
    assert events_list[1].candle == AAPL_CANDLES1[1]
    assert isinstance(events_list[2], MarketDataEvent)
    assert events_list[2].candle == GOOGL_CANDLES1[0]
    assert isinstance(events_list[3], MarketDataEvent)
    assert events_list[3].candle == GOOGL_CANDLES1[1]
    assert isinstance(events_list[4], MarketDataEvent)
    assert events_list[4].candle == AAPL_CANDLES2[0]
    assert isinstance(events_list[5], MarketDataEvent)
    assert events_list[5].candle == AAPL_CANDLES2[1]
    assert isinstance(events_list[6], MarketDataEvent)
    assert events_list[6].candle == GOOGL_CANDLES2[0]
    assert isinstance(events_list[7], MarketDataEvent)
    assert events_list[7].candle == GOOGL_CANDLES2[1]


@patch(
    "market_data.historical.historical_data_handler.HistoricalDataHandler.request_ticker_data_in_range"
)
def test_historical_feed(request_ticker_data_in_range_mocked):
    events = deque()
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
        events,
        tiingo_historical_data_handler,
        start,
        end,
    )

    for i in range(0, len(AAPL_CANDLES) + 1):
        historical_feed.update_latest_data()

    events_list = list(events)
    assert len(events_list) == 10
    assert isinstance(events_list[0], MarketDataEvent)
    assert events_list[0].candle == AAPL_CANDLES1[0]
    assert isinstance(events_list[1], MarketDataEvent)
    assert events_list[1].candle == GOOGL_CANDLES1[0]
    assert isinstance(events_list[2], MarketDataEvent)
    assert events_list[2].candle == AAPL_CANDLES1[1]
    assert isinstance(events_list[3], MarketDataEvent)
    assert events_list[3].candle == GOOGL_CANDLES1[1]
    assert isinstance(events_list[4], MarketDataEvent)
    assert events_list[4].candle == AAPL_CANDLES2[0]
    assert isinstance(events_list[5], MarketDataEvent)
    assert events_list[5].candle == GOOGL_CANDLES2[0]
    assert isinstance(events_list[6], MarketDataEvent)
    assert events_list[6].candle == AAPL_CANDLES2[1]
    assert isinstance(events_list[7], MarketDataEvent)
    assert events_list[7].candle == GOOGL_CANDLES2[1]
    assert isinstance(events_list[8], MarketDataEndEvent)
    assert events_list[8].symbol == AAPL_SYMBOL
    assert isinstance(events_list[9], MarketDataEndEvent)
    assert events_list[9].symbol == GOOGL_SYMBOL


def test_pandas_feed():
    events = deque()
    candle_dataframes = [
        CandleDataFrame.from_candle_list(symbol=AAPL_SYMBOL, candles=AAPL_CANDLES),
        CandleDataFrame.from_candle_list(symbol=GOOGL_SYMBOL, candles=GOOGL_CANDLES),
    ]
    pandas_feed = PandasFeed(candle_dataframes, events)

    for i in range(0, len(AAPL_CANDLES) + 1):
        pandas_feed.update_latest_data()

    events_list = list(events)
    assert len(events_list) == 10
    assert isinstance(events_list[0], MarketDataEvent)
    assert events_list[0].candle == AAPL_CANDLES1[0]
    assert isinstance(events_list[1], MarketDataEvent)
    assert events_list[1].candle == GOOGL_CANDLES1[0]
    assert isinstance(events_list[2], MarketDataEvent)
    assert events_list[2].candle == AAPL_CANDLES1[1]
    assert isinstance(events_list[3], MarketDataEvent)
    assert events_list[3].candle == GOOGL_CANDLES1[1]
    assert isinstance(events_list[4], MarketDataEvent)
    assert events_list[4].candle == AAPL_CANDLES2[0]
    assert isinstance(events_list[5], MarketDataEvent)
    assert events_list[5].candle == GOOGL_CANDLES2[0]
    assert isinstance(events_list[6], MarketDataEvent)
    assert events_list[6].candle == AAPL_CANDLES2[1]
    assert isinstance(events_list[7], MarketDataEvent)
    assert events_list[7].candle == GOOGL_CANDLES2[1]
    assert isinstance(events_list[8], MarketDataEndEvent)
    assert events_list[8].symbol == AAPL_SYMBOL
    assert isinstance(events_list[9], MarketDataEndEvent)
    assert events_list[9].symbol == GOOGL_SYMBOL


def test_csv_feed():
    events = deque()
    csv_feed = CsvFeed(
        events=events,
        csv_filenames={
            AAPL_SYMBOL: "test/data/aapl_candles.csv",
            GOOGL_SYMBOL: "test/data/googl_candles.csv",
        },
    )

    for i in range(0, len(AAPL_CANDLES) + 1):
        csv_feed.update_latest_data()

    events_list = list(events)
    assert len(events_list) == 10
    assert isinstance(events_list[0], MarketDataEvent)
    assert events_list[0].candle == AAPL_CANDLES1[0]
    assert isinstance(events_list[1], MarketDataEvent)
    assert events_list[1].candle == GOOGL_CANDLES1[0]
    assert isinstance(events_list[2], MarketDataEvent)
    assert events_list[2].candle == AAPL_CANDLES1[1]
    assert isinstance(events_list[3], MarketDataEvent)
    assert events_list[3].candle == GOOGL_CANDLES1[1]
    assert isinstance(events_list[4], MarketDataEvent)
    assert events_list[4].candle == AAPL_CANDLES2[0]
    assert isinstance(events_list[5], MarketDataEvent)
    assert events_list[5].candle == GOOGL_CANDLES2[0]
    assert isinstance(events_list[6], MarketDataEvent)
    assert events_list[6].candle == AAPL_CANDLES2[1]
    assert isinstance(events_list[7], MarketDataEvent)
    assert events_list[7].candle == GOOGL_CANDLES2[1]
    assert isinstance(events_list[8], MarketDataEndEvent)
    assert events_list[8].symbol == AAPL_SYMBOL
    assert isinstance(events_list[9], MarketDataEndEvent)
    assert events_list[9].symbol == GOOGL_SYMBOL


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
    events = deque()
    external_storage_feed = ExternalStorageFeed(
        symbols=[SYMBOL],
        events=events,
        time_unit=timedelta(minutes=1),
        start=datetime.strptime("2020-05-08 14:12:00+0000", "%Y-%m-%d %H:%M:%S%z"),
        end=datetime.strptime("2020-05-08 14:17:00+0000", "%Y-%m-%d %H:%M:%S%z"),
        db_storage=DB_STORAGE,
        file_storage=FILE_STORAGE,
    )
    file_content_valid_candles = np.array(
        [
            Candle(
                symbol=SYMBOL,
                open=94.28,
                high=94.96,
                low=93.96,
                close=94.78,
                volume=23,
                timestamp=datetime.strptime(
                    "2020-05-08 14:12:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
            Candle(
                symbol=SYMBOL,
                open=94.92,
                high=95.32,
                low=94.09,
                close=94.09,
                volume=11,
                timestamp=datetime.strptime(
                    "2020-05-08 14:13:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
            Candle(
                symbol=SYMBOL,
                open=94.25,
                high=94.59,
                low=94.14,
                close=94.59,
                volume=26,
                timestamp=datetime.strptime(
                    "2020-05-08 14:14:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
            Candle(
                symbol=SYMBOL,
                open=94.22,
                high=94.26,
                low=93.95,
                close=93.98,
                volume=11,
                timestamp=datetime.strptime(
                    "2020-05-08 14:15:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
            Candle(
                symbol="IVV",
                open=93.98,
                high=93.98,
                low=93.98,
                close=93.98,
                volume=0,
                timestamp=datetime.strptime(
                    "2020-05-08 14:16:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
        ],
        dtype=Candle,
    )
    candles_valid_candles_count = 1
    for i in range(
        0, candles_valid_candles_count + len(file_content_valid_candles) + 1
    ):
        external_storage_feed.update_latest_data()

    events_list = list(events)
    assert len(events_list) == 7
    assert isinstance(events_list[0], MarketDataEvent)
    assert events_list[0].candle == file_content_valid_candles[0]
    assert isinstance(events_list[1], MarketDataEvent)
    assert events_list[1].candle == file_content_valid_candles[1]
    assert isinstance(events_list[2], MarketDataEvent)
    assert events_list[2].candle == file_content_valid_candles[2]
    assert isinstance(events_list[3], MarketDataEvent)
    assert events_list[3].candle == file_content_valid_candles[3]
    assert isinstance(events_list[4], MarketDataEvent)
    assert events_list[4].candle == file_content_valid_candles[4]
    assert isinstance(events_list[5], MarketDataEvent)
    assert events_list[5].candle == CANDLES[0]
    assert isinstance(events_list[6], MarketDataEndEvent)
    assert events_list[6].symbol == SYMBOL
