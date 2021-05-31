from datetime import datetime, timedelta
from unittest.mock import call, patch

import numpy as np
import pandas as pd

from common.constants import DATE_FORMAT
from common.exchange_calendar_euronext import EuronextExchangeCalendar
from common.types import CandleDataFrame
from db_storage.mongodb_storage import MongoDbStorage
from file_storage.common import DATASETS_DIR, DONE_DIR
from file_storage.meganz_file_storage import MegaNzFileStorage
from models.asset import Asset
from models.candle import Candle
from settings import DATABASE_NAME
from strategy.candlefetcher import CandleFetcher
from test.tools.tools import compare_candles_list

SYMBOL = "IVV"
EXCHANGE = "IEX"
IVV_ASSET = Asset(symbol=SYMBOL, exchange=EXCHANGE)
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

DB_STORAGE = MongoDbStorage(DATABASE_NAME)
FILE_STORAGE = MegaNzFileStorage()
MARKET_CAL = EuronextExchangeCalendar()
CANDLE_FETCHER = CandleFetcher(DB_STORAGE, FILE_STORAGE, MARKET_CAL)


def test_query_candles():
    DB_STORAGE.clean_all_candles()
    for candle in CANDLES:
        DB_STORAGE.add_candle(candle)
    start, end = CANDLES[0].timestamp, CANDLES[-1].timestamp
    candles = CANDLE_FETCHER.query_candles(IVV_ASSET, start, end)
    DB_STORAGE.clean_all_candles()
    assert compare_candles_list(candles, CANDLES)


def test_fetch_candle_db_data():
    DB_STORAGE.clean_all_candles()
    for candle in CANDLES:
        DB_STORAGE.add_candle(candle)
    start = CANDLES[0].timestamp
    end = CANDLES[-1].timestamp
    df = CANDLE_FETCHER.fetch_candle_db_data(IVV_ASSET, start, end)
    expected_df_candles = {
        "timestamp": [
            "2020-05-08 14:17:00+00:00",
            "2020-05-08 14:24:00+00:00",
            "2020-05-08 14:24:56+00:00",
            "2020-05-08 14:35:00+00:00",
            "2020-05-08 14:41:00+00:00",
            "2020-05-08 14:41:58+00:00",
        ],
        "open": [
            "94.12",
            "94.07",
            "94.07",
            "94.17",
            "94.19",
            "94.19",
        ],
        "high": [
            "94.15",
            "94.1",
            "94.1",
            "94.18",
            "94.22",
            "94.22",
        ],
        "low": ["94.0", "93.95", "93.95", "94.05", "94.07", "94.07"],
        "close": [
            "94.13",
            "94.08",
            "94.08",
            "94.18",
            "94.2",
            "94.2",
        ],
        "volume": [7, 91, 30, 23, 21, 7],
    }
    expected_df = pd.DataFrame(
        expected_df_candles,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    expected_df.index = pd.to_datetime(expected_df.timestamp, format=DATE_FORMAT)
    expected_df = expected_df.drop(["timestamp"], axis=1)
    assert (df == expected_df).all(axis=None)
    DB_STORAGE.clean_all_candles()


@patch("file_storage.meganz_file_storage.MegaNzFileStorage.get_file_content")
def test_fetch_historical_data(get_file_content_mocked):
    get_file_content_mocked.side_effect = [
        (
            "timestamp,open,high,low,close,volume\n"
            "2020-06-17 09:30:00+00:00,355.15,355.15,353.74,353.84,3254.0\n"
            "2020-06-17 09:31:00+00:00,354.28,354.96,353.96,354.78,2324.0\n"
            "2020-06-17 09:32:00+00:00,354.92,355.32,354.09,354.09,1123.0\n"
            "2020-06-17 09:33:00+00:00,354.25,354.59,354.14,354.59,2613.0\n"
            "2020-06-17 09:34:00+00:00,354.22,354.26,353.95,353.98,1186.0\n"
        ),
        "",
        (
            "timestamp,open,high,low,close,volume\n"
            "2020-06-19 09:30:00+00:00,355.15,355.15,353.74,353.84,3254.0\n"
            "2020-06-19 09:31:00+00:00,354.28,354.96,353.96,354.78,2324.0\n"
            "2020-06-19 09:32:00+00:00,354.92,355.32,354.09,354.09,1123.0\n"
            "2020-06-19 09:33:00+00:00,354.25,354.59,354.14,354.59,2613.0\n"
            "2020-06-19 09:34:00+00:00,354.22,354.26,353.95,353.98,1186.0\n"
        ),
    ]

    start = datetime.strptime("2020-06-17 09:33:00+0000", "%Y-%m-%d %H:%M:%S%z")
    end = datetime.strptime("2020-06-19 09:32:00+0000", "%Y-%m-%d %H:%M:%S%z")
    df = CANDLE_FETCHER.fetch_candle_historical_data(IVV_ASSET, start, end)

    expected_df_candles = {
        "timestamp": [
            "2020-06-17 09:33:00+00:00",
            "2020-06-17 09:34:00+00:00",
            "2020-06-19 09:30:00+00:00",
            "2020-06-19 09:31:00+00:00",
            "2020-06-19 09:32:00+00:00",
        ],
        "open": ["354.25", "354.22", "355.15", "354.28", "354.92"],
        "high": ["354.59", "354.26", "355.15", "354.96", "355.32"],
        "low": ["354.14", "353.95", "353.74", "353.96", "354.09"],
        "close": ["354.59", "353.98", "353.84", "354.78", "354.09"],
        "volume": [2613, 1186, 3254, 2324, 1123],
    }
    expected_df = pd.DataFrame(
        expected_df_candles,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    expected_df.index = pd.to_datetime(expected_df.timestamp, format=DATE_FORMAT)
    expected_df = expected_df.drop(["timestamp"], axis=1)
    assert (df == expected_df).all(axis=None)

    dates_str = ["20200617", "20200618", "20200619"]
    get_file_content_mocked_calls = [
        call(
            "{}/{}/{}/{}_{}.csv".format(
                DATASETS_DIR, dates_str[0], DONE_DIR, IVV_ASSET.key(), dates_str[0]
            )
        ),
        call(
            "{}/{}/{}/{}_{}.csv".format(
                DATASETS_DIR, dates_str[1], DONE_DIR, IVV_ASSET.key(), dates_str[1]
            )
        ),
        call(
            "{}/{}/{}/{}_{}.csv".format(
                DATASETS_DIR, dates_str[2], DONE_DIR, IVV_ASSET.key(), dates_str[2]
            )
        ),
    ]
    get_file_content_mocked.assert_has_calls(get_file_content_mocked_calls)


@patch("file_storage.meganz_file_storage.MegaNzFileStorage.get_file_content")
def test_fetch_no_historical_data(get_file_content_mocked):
    DB_STORAGE.clean_all_candles()
    for candle in CANDLES:
        DB_STORAGE.add_candle(candle)

    get_file_content_mocked.return_value = ""

    df = CANDLE_FETCHER.fetch(
        IVV_ASSET,
        timedelta(minutes=5),
        datetime.strptime("2020-05-06 14:12:00+0000", "%Y-%m-%d %H:%M:%S%z"),
        datetime.strptime("2020-05-08 14:49:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    )

    expected_df_candles = np.array(
        [
            Candle(
                asset=IVV_ASSET,
                open=94.12,
                high=94.15,
                low=94.00,
                close=94.13,
                volume=7,
                timestamp=datetime.strptime(
                    "2020-05-08 14:20:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
            Candle(
                asset=IVV_ASSET,
                open=94.07,
                high=94.10,
                low=93.95,
                close=94.08,
                volume=121,
                timestamp=datetime.strptime(
                    "2020-05-08 14:25:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
            Candle(
                asset=IVV_ASSET,
                open=94.08,
                high=94.08,
                low=94.08,
                close=94.08,
                volume=0,
                timestamp=datetime.strptime(
                    "2020-05-08 14:30:00+0000", "%Y-%m-%d %H:%M:%S%z"
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
                open=94.18,
                high=94.18,
                low=94.18,
                close=94.18,
                volume=0,
                timestamp=datetime.strptime(
                    "2020-05-08 14:40:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
            Candle(
                asset=IVV_ASSET,
                open=94.19,
                high=94.22,
                low=94.07,
                close=94.20,
                volume=28,
                timestamp=datetime.strptime(
                    "2020-05-08 14:45:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
        ],
        dtype=Candle,
    )
    expected_df = CandleDataFrame.from_candle_list(
        asset=IVV_ASSET, candles=expected_df_candles
    )
    assert (df == expected_df).all(axis=None)

    date_strs = ["20200506", "20200507", "20200508"]
    get_file_content_mocked_calls = [
        call(
            "{}/{}/{}/{}_{}.csv".format(
                DATASETS_DIR, date_strs[0], DONE_DIR, IVV_ASSET.key(), date_strs[0]
            )
        ),
        call(
            "{}/{}/{}/{}_{}.csv".format(
                DATASETS_DIR, date_strs[1], DONE_DIR, IVV_ASSET.key(), date_strs[1]
            )
        ),
        call(
            "{}/{}/{}/{}_{}.csv".format(
                DATASETS_DIR, date_strs[2], DONE_DIR, IVV_ASSET.key(), date_strs[2]
            )
        ),
    ]
    get_file_content_mocked.assert_has_calls(get_file_content_mocked_calls)

    DB_STORAGE.clean_all_candles()


@patch("file_storage.meganz_file_storage.MegaNzFileStorage.get_file_content")
def test_fetch_no_db_data(get_file_content_mocked):
    DB_STORAGE.clean_all_candles()

    get_file_content_mocked.side_effect = [
        (
            "timestamp,open,high,low,close,volume\n"
            "2020-06-11 14:11:00+00:00,94.15,95.15,93.74,93.84,32\n"
            "2020-06-11 14:12:00+00:00,94.28,94.96,93.96,94.78,23\n"
            "2020-06-11 14:13:00+00:00,94.92,95.32,94.09,94.09,11\n"
            "2020-06-11 14:14:00+00:00,94.25,94.59,94.14,94.59,26\n"
            "2020-06-11 14:17:00+00:00,94.22,94.26,93.95,93.98,11\n"
        )
    ]

    df = CANDLE_FETCHER.fetch(
        IVV_ASSET,
        timedelta(minutes=5),
        datetime.strptime("2020-06-11 14:12:00+0000", "%Y-%m-%d %H:%M:%S%z"),
        datetime.strptime("2020-06-11 14:49:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    )

    expected_df_candles = np.array(
        [
            Candle(
                asset=IVV_ASSET,
                open=94.28,
                high=95.32,
                low=93.96,
                close=94.59,
                volume=60,
                timestamp=datetime.strptime(
                    "2020-06-11 14:15:00+0000", "%Y-%m-%d %H:%M:%S%z"
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
                    "2020-06-11 14:20:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
        ],
        dtype=Candle,
    )
    expected_df = CandleDataFrame.from_candle_list(
        IVV_ASSET, candles=expected_df_candles
    )
    assert (df == expected_df).all(axis=None)

    date_str = "20200611"
    get_file_content_mocked_calls = [
        call(
            "{}/{}/{}/{}_{}.csv".format(
                DATASETS_DIR, date_str, DONE_DIR, IVV_ASSET.key(), date_str
            )
        )
    ]
    get_file_content_mocked.assert_has_calls(get_file_content_mocked_calls)


@patch("file_storage.meganz_file_storage.MegaNzFileStorage.get_file_content")
def test_fetch(get_file_content_mocked):
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

    df = CANDLE_FETCHER.fetch(
        IVV_ASSET,
        timedelta(minutes=5),
        datetime.strptime("2020-05-08 14:12:00+0000", "%Y-%m-%d %H:%M:%S%z"),
        datetime.strptime("2020-05-08 14:49:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    )

    expected_df_candles = np.array(
        [
            Candle(
                asset=IVV_ASSET,
                open=94.28,
                high=95.32,
                low=93.95,
                close=93.98,
                volume=71,
                timestamp=datetime.strptime(
                    "2020-05-08 14:15:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
            Candle(
                asset=IVV_ASSET,
                open=94.12,
                high=94.15,
                low=94.00,
                close=94.13,
                volume=7,
                timestamp=datetime.strptime(
                    "2020-05-08 14:20:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
            Candle(
                asset=IVV_ASSET,
                open=94.07,
                high=94.10,
                low=93.95,
                close=94.08,
                volume=121,
                timestamp=datetime.strptime(
                    "2020-05-08 14:25:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
            Candle(
                asset=IVV_ASSET,
                open=94.08,
                high=94.08,
                low=94.08,
                close=94.08,
                volume=0,
                timestamp=datetime.strptime(
                    "2020-05-08 14:30:00+0000", "%Y-%m-%d %H:%M:%S%z"
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
                open=94.18,
                high=94.18,
                low=94.18,
                close=94.18,
                volume=0,
                timestamp=datetime.strptime(
                    "2020-05-08 14:40:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
            Candle(
                asset=IVV_ASSET,
                open=94.19,
                high=94.22,
                low=94.07,
                close=94.20,
                volume=28,
                timestamp=datetime.strptime(
                    "2020-05-08 14:45:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
        ],
        dtype=Candle,
    )
    expected_df = CandleDataFrame.from_candle_list(
        IVV_ASSET, candles=expected_df_candles
    )
    assert (df == expected_df).all(axis=None)

    date_str = "20200508"
    get_file_content_mocked_calls = [
        call(
            "{}/{}/{}/{}_{}.csv".format(
                DATASETS_DIR, date_str, DONE_DIR, IVV_ASSET.key(), date_str
            )
        )
    ]
    get_file_content_mocked.assert_has_calls(get_file_content_mocked_calls)

    DB_STORAGE.clean_all_candles()


@patch("file_storage.meganz_file_storage.MegaNzFileStorage.get_file_content")
def test_fetch_1_day_offset(get_file_content_mocked):
    DB_STORAGE.clean_all_candles()
    for candle in CANDLES:
        DB_STORAGE.add_candle(candle)

    get_file_content_mocked.side_effect = [
        (
            "timestamp,open,high,low,close,volume\n"
            "2020-05-07 14:11:00+00:00,94.15,95.15,93.74,93.84,32\n"
            "2020-05-07 14:12:00+00:00,94.28,94.96,93.96,94.78,23\n"
            "2020-05-07 14:13:00+00:00,94.92,95.32,94.09,94.09,11\n"
            "2020-05-07 14:14:00+00:00,94.25,94.59,94.14,94.59,26\n"
        ),
        (
            "timestamp,open,high,low,close,volume\n"
            "2020-05-08 14:15:00+00:00,94.22,94.26,93.95,93.98,11\n"
        ),
    ]

    df = CANDLE_FETCHER.fetch(
        IVV_ASSET,
        pd.offsets.Day(1),
        datetime.strptime("2020-05-07 14:12:00+0000", "%Y-%m-%d %H:%M:%S%z"),
        datetime.strptime("2020-05-08 14:49:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    )

    expected_df_candles = np.array(
        [
            Candle(
                asset=IVV_ASSET,
                open=94.28,
                high=95.32,
                low=93.96,
                close=94.59,
                volume=60,
                timestamp=datetime.strptime(
                    "2020-05-07 00:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
            Candle(
                asset=IVV_ASSET,
                open=94.22,
                high=94.26,
                low=93.95,
                close=94.20,
                volume=190,
                timestamp=datetime.strptime(
                    "2020-05-08 00:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
        ],
        dtype=Candle,
    )
    expected_df = CandleDataFrame.from_candle_list(
        asset=IVV_ASSET, candles=expected_df_candles
    )
    assert (df == expected_df).all(axis=None)

    date_str = "20200507"
    get_file_content_mocked_calls = [
        call(
            "{}/{}/{}/{}_{}.csv".format(
                DATASETS_DIR, date_str, DONE_DIR, IVV_ASSET.key(), date_str
            )
        )
    ]
    get_file_content_mocked.assert_has_calls(get_file_content_mocked_calls)

    DB_STORAGE.clean_all_candles()


def test_fetch_none_db_storage_none_file_storage():
    candle_fetcher = CandleFetcher(
        db_storage=None, file_storage=None, market_cal=MARKET_CAL
    )

    df = candle_fetcher.fetch(
        SYMBOL,
        timedelta(minutes=5),
        datetime.strptime("2020-05-08 14:12:00+0000", "%Y-%m-%d %H:%M:%S%z"),
        datetime.strptime("2020-05-08 14:49:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    )

    expected_df = CandleDataFrame.from_candle_list(
        asset=IVV_ASSET, candles=np.array([], dtype=Candle)
    )
    assert (df == expected_df).all(axis=None)
