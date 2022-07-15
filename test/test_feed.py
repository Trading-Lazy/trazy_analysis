from collections import deque
from datetime import datetime, timedelta
from unittest.mock import patch

import numpy as np

from trazy_analysis.common.types import CandleDataFrame
from trazy_analysis.db_storage.mongodb_storage import MongoDbStorage
from trazy_analysis.feed.feed import (
    CsvFeed,
    ExternalStorageFeed,
    Feed,
    HistoricalFeed,
    LiveFeed,
    PandasFeed,
)
from trazy_analysis.file_storage.meganz_file_storage import MegaNzFileStorage
from trazy_analysis.market_data.historical.tiingo_historical_data_handler import (
    TiingoHistoricalDataHandler,
)
from trazy_analysis.market_data.live.tiingo_live_data_handler import (
    TiingoLiveDataHandler,
)
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.event import MarketDataEndEvent, MarketDataEvent
from trazy_analysis.settings import DATABASE_NAME

DB_STORAGE = MongoDbStorage(DATABASE_NAME)
QUEUE_NAME = "candles"

EXCHANGE = "IEX"
AAPL_SYMBOL = "AAPL"
AAPL_ASSET = Asset(symbol=AAPL_SYMBOL, exchange=EXCHANGE)
AAPL_CANDLES1 = np.array(
    [
        Candle(
            asset=AAPL_ASSET,
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
            asset=AAPL_ASSET,
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
            asset=AAPL_ASSET,
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
            asset=AAPL_ASSET,
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
    asset=AAPL_ASSET, candles=AAPL_CANDLES
)

GOOGL_SYMBOL = "GOOGL"
GOOGL_ASSET = Asset(symbol=GOOGL_SYMBOL, exchange=EXCHANGE)
GOOGL_CANDLES1 = np.array(
    [
        Candle(
            asset=GOOGL_ASSET,
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
            asset=GOOGL_ASSET,
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
            asset=GOOGL_ASSET,
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
            asset=GOOGL_ASSET,
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
    asset=GOOGL_ASSET, candles=GOOGL_CANDLES
)

IVV_SYMBOL = "IVV"
IVV_ASSET = Asset(symbol=IVV_SYMBOL, exchange=EXCHANGE)
CANDLES = np.array(
    [
        Candle(
            asset=IVV_ASSET,
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
            asset=IVV_ASSET,
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
            asset=IVV_ASSET,
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
            asset=IVV_ASSET,
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
            asset=IVV_ASSET,
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
            asset=IVV_ASSET,
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
    time_unit = timedelta(minutes=1)
    candles = {
        AAPL_ASSET: {time_unit: AAPL_CANDLES1},
        GOOGL_ASSET: {time_unit: GOOGL_CANDLES1},
    }
    feed = Feed(events=events, candles=candles)

    for i in range(0, 5):
        feed.update_latest_data()

    events_list = list(events)
    assert len(events_list) == 5

    assert events_list[0].candles[AAPL_ASSET][time_unit][0] == AAPL_CANDLES1[0]
    assert isinstance(events_list[0], MarketDataEvent)
    assert events_list[1].candles[AAPL_ASSET][time_unit][0] == AAPL_CANDLES1[1]
    assert isinstance(events_list[1], MarketDataEvent)
    assert events_list[2].candles[GOOGL_ASSET][time_unit][0] == GOOGL_CANDLES1[0]
    assert isinstance(events_list[2], MarketDataEvent)
    assert events_list[3].candles[GOOGL_ASSET][time_unit][0] == GOOGL_CANDLES1[1]
    assert isinstance(events_list[3], MarketDataEvent)

    assert events_list[4].assets == set([GOOGL_ASSET, AAPL_ASSET])
    assert isinstance(events_list[4], MarketDataEndEvent)


@patch(
    "trazy_analysis.market_data.live.tiingo_live_data_handler.TiingoLiveDataHandler.request_ticker_lastest_candles"
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
    live_feed = LiveFeed(
        assets={AAPL_ASSET: timedelta(minutes=1), GOOGL_ASSET: timedelta(minutes=1)},
        live_data_handlers={"iex": tiingo_live_data_handler},
        events=events,
    )

    for i in range(0, 2):
        live_feed.update_latest_data()

    events_list = list(events)
    assert len(events_list) == 2

    time_unit = timedelta(minutes=1)
    assert isinstance(events_list[0], MarketDataEvent)
    assert events_list[0].candles[AAPL_ASSET][time_unit][0] == AAPL_CANDLES1[0]
    assert isinstance(events_list[0], MarketDataEvent)
    assert events_list[0].candles[AAPL_ASSET][time_unit][1] == AAPL_CANDLES1[1]
    assert isinstance(events_list[0], MarketDataEvent)
    assert events_list[0].candles[GOOGL_ASSET][time_unit][0] == GOOGL_CANDLES1[0]
    assert isinstance(events_list[0], MarketDataEvent)
    assert events_list[0].candles[GOOGL_ASSET][time_unit][1] == GOOGL_CANDLES1[1]

    assert isinstance(events_list[1], MarketDataEvent)
    assert events_list[1].candles[AAPL_ASSET][time_unit][0] == AAPL_CANDLES2[0]
    assert isinstance(events_list[1], MarketDataEvent)
    assert events_list[1].candles[AAPL_ASSET][time_unit][1] == AAPL_CANDLES2[1]
    assert isinstance(events_list[1], MarketDataEvent)
    assert events_list[1].candles[GOOGL_ASSET][time_unit][0] == GOOGL_CANDLES2[0]
    assert isinstance(events_list[1], MarketDataEvent)
    assert events_list[1].candles[GOOGL_ASSET][time_unit][1] == GOOGL_CANDLES2[1]


@patch(
    "trazy_analysis.market_data.historical.historical_data_handler.HistoricalDataHandler.request_ticker_data_in_range"
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
    time_unit = timedelta(minutes=1)
    historical_feed = HistoricalFeed(
        {AAPL_ASSET: time_unit, GOOGL_ASSET: time_unit},
        {EXCHANGE.lower(): tiingo_historical_data_handler},
        start,
        end,
        events,
    )

    for i in range(0, 8):
        historical_feed.update_latest_data()

    events_list = list(events)
    assert len(events_list) == 8

    assert isinstance(events_list[0], MarketDataEvent)
    assert events_list[0].candles[AAPL_ASSET][time_unit][0] == AAPL_CANDLES1[0]
    assert isinstance(events_list[1], MarketDataEvent)
    assert events_list[1].candles[AAPL_ASSET][time_unit][0] == AAPL_CANDLES1[1]

    assert isinstance(events_list[2], MarketDataEvent)
    assert events_list[2].candles[AAPL_ASSET][time_unit][0] == AAPL_CANDLES2[0]
    assert isinstance(events_list[2], MarketDataEvent)
    assert events_list[2].candles[GOOGL_ASSET][time_unit][0] == GOOGL_CANDLES1[0]

    assert isinstance(events_list[3], MarketDataEvent)
    assert events_list[3].candles[AAPL_ASSET][time_unit][0] == AAPL_CANDLES2[1]
    assert isinstance(events_list[3], MarketDataEvent)
    assert events_list[3].candles[GOOGL_ASSET][time_unit][0] == GOOGL_CANDLES1[1]

    assert isinstance(events_list[4], MarketDataEvent)
    assert events_list[4].candles[GOOGL_ASSET][time_unit][0] == GOOGL_CANDLES2[0]
    assert isinstance(events_list[5], MarketDataEvent)
    assert events_list[5].candles[GOOGL_ASSET][time_unit][0] == GOOGL_CANDLES2[1]

    assert events_list[7].assets == set([AAPL_ASSET, GOOGL_ASSET])
    assert isinstance(events_list[7], MarketDataEndEvent)


def test_pandas_feed():
    events = deque()
    candle_dataframes = [
        CandleDataFrame.from_candle_list(asset=AAPL_ASSET, candles=AAPL_CANDLES),
        CandleDataFrame.from_candle_list(asset=GOOGL_ASSET, candles=GOOGL_CANDLES),
    ]
    pandas_feed = PandasFeed(candle_dataframes, events)

    for i in range(0, 8):
        pandas_feed.update_latest_data()

    events_list = list(events)
    assert len(events_list) == 8

    time_unit = timedelta(minutes=1)
    assert isinstance(events_list[0], MarketDataEvent)
    assert events_list[0].candles[AAPL_ASSET][time_unit][0] == AAPL_CANDLES1[0]
    assert isinstance(events_list[1], MarketDataEvent)
    assert events_list[1].candles[AAPL_ASSET][time_unit][0] == AAPL_CANDLES1[1]

    assert isinstance(events_list[2], MarketDataEvent)
    assert events_list[2].candles[AAPL_ASSET][time_unit][0] == AAPL_CANDLES2[0]
    assert isinstance(events_list[2], MarketDataEvent)
    assert events_list[2].candles[GOOGL_ASSET][time_unit][0] == GOOGL_CANDLES1[0]

    assert isinstance(events_list[3], MarketDataEvent)
    assert events_list[3].candles[AAPL_ASSET][time_unit][0] == AAPL_CANDLES2[1]
    assert isinstance(events_list[3], MarketDataEvent)
    assert events_list[3].candles[GOOGL_ASSET][time_unit][0] == GOOGL_CANDLES1[1]

    assert isinstance(events_list[4], MarketDataEvent)
    assert events_list[4].candles[GOOGL_ASSET][time_unit][0] == GOOGL_CANDLES2[0]
    assert isinstance(events_list[5], MarketDataEvent)
    assert events_list[5].candles[GOOGL_ASSET][time_unit][0] == GOOGL_CANDLES2[1]

    assert events_list[7].assets == {AAPL_ASSET, GOOGL_ASSET}
    assert isinstance(events_list[7], MarketDataEndEvent)


def test_csv_feed():
    events = deque()
    time_unit = timedelta(minutes=1)
    csv_feed = CsvFeed(
        csv_filenames={
            AAPL_ASSET: {time_unit: "test/data/aapl_candles.csv"},
            GOOGL_ASSET: {time_unit: "test/data/googl_candles.csv"},
        },
        events=events,
    )

    for i in range(0, 8):
        csv_feed.update_latest_data()

    events_list = list(events)
    assert len(events_list) == 8

    assert isinstance(events_list[0], MarketDataEvent)
    assert events_list[0].candles[AAPL_ASSET][time_unit][0] == AAPL_CANDLES1[0]
    assert isinstance(events_list[1], MarketDataEvent)
    assert events_list[1].candles[AAPL_ASSET][time_unit][0] == AAPL_CANDLES1[1]

    assert isinstance(events_list[2], MarketDataEvent)
    assert events_list[2].candles[AAPL_ASSET][time_unit][0] == AAPL_CANDLES2[0]
    assert isinstance(events_list[2], MarketDataEvent)
    assert events_list[2].candles[GOOGL_ASSET][time_unit][0] == GOOGL_CANDLES1[0]

    assert isinstance(events_list[3], MarketDataEvent)
    assert events_list[3].candles[AAPL_ASSET][time_unit][0] == AAPL_CANDLES2[1]
    assert isinstance(events_list[3], MarketDataEvent)
    assert events_list[3].candles[GOOGL_ASSET][time_unit][0] == GOOGL_CANDLES1[1]

    assert isinstance(events_list[4], MarketDataEvent)
    assert events_list[4].candles[GOOGL_ASSET][time_unit][0] == GOOGL_CANDLES2[0]
    assert isinstance(events_list[5], MarketDataEvent)
    assert events_list[5].candles[GOOGL_ASSET][time_unit][0] == GOOGL_CANDLES2[1]

    assert events_list[7].assets == {AAPL_ASSET, GOOGL_ASSET}
    assert isinstance(events_list[7], MarketDataEndEvent)


@patch(
    "trazy_analysis.file_storage.meganz_file_storage.MegaNzFileStorage.get_file_content"
)
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
    time_unit = timedelta(minutes=1)
    external_storage_feed = ExternalStorageFeed(
        assets={IVV_ASSET: time_unit},
        start=datetime.strptime("2020-05-08 14:12:00+0000", "%Y-%m-%d %H:%M:%S%z"),
        end=datetime.strptime("2020-05-08 14:17:00+0000", "%Y-%m-%d %H:%M:%S%z"),
        events=events,
        db_storage=DB_STORAGE,
        file_storage=FILE_STORAGE,
    )
    file_content_valid_candles = np.array(
        [
            Candle(
                asset=IVV_ASSET,
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
                asset=IVV_ASSET,
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
                asset=IVV_ASSET,
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
                asset=IVV_ASSET,
                open=94.22,
                high=94.26,
                low=93.95,
                close=93.98,
                volume=11,
                timestamp=datetime.strptime(
                    "2020-05-08 14:15:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
        ],
        dtype=Candle,
    )
    candles_valid_candles_count = 1
    for i in range(
        0, candles_valid_candles_count + len(file_content_valid_candles) + 2
    ):
        external_storage_feed.update_latest_data()

    events_list = list(events)
    assert len(events_list) == 7

    assert isinstance(events_list[0], MarketDataEvent)
    assert (
        events_list[0].candles[IVV_ASSET][time_unit][0] == file_content_valid_candles[0]
    )
    assert isinstance(events_list[1], MarketDataEvent)
    assert (
        events_list[1].candles[IVV_ASSET][time_unit][0] == file_content_valid_candles[1]
    )
    assert isinstance(events_list[2], MarketDataEvent)
    assert (
        events_list[2].candles[IVV_ASSET][time_unit][0] == file_content_valid_candles[2]
    )
    assert isinstance(events_list[3], MarketDataEvent)
    assert (
        events_list[3].candles[IVV_ASSET][time_unit][0] == file_content_valid_candles[3]
    )
    assert isinstance(events_list[4], MarketDataEvent)
    print(str(events_list[4].candles[IVV_ASSET][time_unit][0]))
    assert events_list[4].candles[IVV_ASSET][time_unit][0] == Candle(
        asset=IVV_ASSET,
        open=93.98,
        high=93.98,
        low=93.98,
        close=93.98,
        volume=0,
        timestamp=datetime.strptime("2020-05-08 14:16:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    )
    assert isinstance(events_list[5], MarketDataEvent)
    assert events_list[5].candles[IVV_ASSET][time_unit][0] == CANDLES[0]
    assert isinstance(events_list[6], MarketDataEndEvent)
    assert events_list[6].assets == {IVV_ASSET}
