from decimal import Decimal
from unittest.mock import call, patch

import pandas as pd

from actionsapi.models import Candle
from common.exchange_calendar_euronext import EuronextExchangeCalendar
from historical_data.common import DATASETS_DIR, DONE_DIR
from strategy.candlefetcher import CandleFetcher
from strategy.constants import DATE_FORMAT
from test.tools.tools import clean_candles_in_db, compare_candles_list

EURONEXT_CAL = EuronextExchangeCalendar()

SYMBOL = "ANX.PA"
CANDLES = [
    Candle(
        _id=1,
        symbol=SYMBOL,
        open=Decimal("94.1200"),
        high=Decimal("94.1500"),
        low=Decimal("94.0000"),
        close=Decimal("94.1300"),
        volume=7,
        timestamp=pd.Timestamp("2020-05-08 14:17:00", tz="UTC"),
    ),
    Candle(
        _id=2,
        symbol=SYMBOL,
        open=Decimal("94.0700"),
        high=Decimal("94.1000"),
        low=Decimal("93.9500"),
        close=Decimal("94.0800"),
        volume=91,
        timestamp=pd.Timestamp("2020-05-08 14:24:00", tz="UTC"),
    ),
    Candle(
        _id=3,
        symbol=SYMBOL,
        open=Decimal("94.0700"),
        high=Decimal("94.1000"),
        low=Decimal("93.9500"),
        close=Decimal("94.0800"),
        volume=0,
        timestamp=pd.Timestamp("2020-05-08 14:24:56", tz="UTC"),
    ),
    Candle(
        _id=4,
        symbol=SYMBOL,
        open=Decimal("94.1700"),
        high=Decimal("94.1800"),
        low=Decimal("94.0500"),
        close=Decimal("94.1800"),
        volume=0,
        timestamp=pd.Timestamp("2020-05-08 14:35:00", tz="UTC"),
    ),
    Candle(
        _id=5,
        symbol=SYMBOL,
        open=Decimal("94.1900"),
        high=Decimal("94.2200"),
        low=Decimal("94.0700"),
        close=Decimal("94.2000"),
        volume=0,
        timestamp=pd.Timestamp("2020-05-08 14:41:00", tz="UTC"),
    ),
    Candle(
        _id=6,
        symbol=SYMBOL,
        open=Decimal("94.1900"),
        high=Decimal("94.2200"),
        low=Decimal("94.0700"),
        close=Decimal("94.2000"),
        volume=7,
        timestamp=pd.Timestamp("2020-05-08 14:41:58", tz="UTC"),
    ),
]


def test_query_candles():
    clean_candles_in_db()
    for candle in CANDLES:
        candle.save()
    start, end = CANDLES[0].timestamp, CANDLES[-1].timestamp
    candles = CandleFetcher.query_candles(SYMBOL, start, end)
    assert compare_candles_list(candles, CANDLES)
    clean_candles_in_db()


def test_resample_candle_data_interval_5_minute():
    candles = {
        "timestamp": [
            "2020-05-08 14:17:00+00:00",
            "2020-05-08 14:24:00+00:00",
            "2020-05-08 14:24:56+00:00",
            "2020-05-08 14:35:00+00:00",
            "2020-05-08 14:41:00+00:00",
            "2020-05-08 14:41:58+00:00",
        ],
        "open": [94.1200, 94.0700, 94.0700, 94.1700, 94.1900, 94.1900],
        "high": [94.1500, 94.1000, 94.1000, 94.1800, 94.2200, 94.2200],
        "low": [94.0000, 93.9500, 93.9500, 94.0500, 94.0700, 94.0700],
        "close": [94.1300, 94.0800, 94.0800, 94.1800, 94.2000, 94.2000],
        "volume": [7, 91, 0, 0, 0, 0],
    }
    business_cal = EURONEXT_CAL.schedule(start_date="2020-05-08", end_date="2020-05-08")
    df = pd.DataFrame(
        candles, columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    df.index = pd.to_datetime(df.timestamp, format=DATE_FORMAT)
    df = df.drop(["timestamp"], axis=1)
    df = CandleFetcher.resample_candle_data(df, pd.offsets.Minute(5), business_cal)

    expected_df_candles = {
        "timestamp": [
            "2020-05-08 14:20:00+00:00",
            "2020-05-08 14:25:00+00:00",
            "2020-05-08 14:30:00+00:00",
            "2020-05-08 14:35:00+00:00",
            "2020-05-08 14:40:00+00:00",
            "2020-05-08 14:45:00+00:00",
        ],
        "open": [94.12, 94.07, 94.07, 94.17, 94.17, 94.19],
        "high": [94.15, 94.10, 94.10, 94.18, 94.18, 94.22],
        "low": [94.00, 93.95, 93.95, 94.05, 94.05, 94.07],
        "close": [94.13, 94.08, 94.08, 94.18, 94.18, 94.20],
        "volume": [7, 91, 0, 0, 0, 0],
    }
    expected_df = pd.DataFrame(
        expected_df_candles,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    expected_df.index = pd.to_datetime(expected_df.timestamp, format=DATE_FORMAT)
    expected_df = expected_df.drop(["timestamp"], axis=1)

    assert (df == expected_df).all(axis=None)


def test_simple_resample_candle_data_interval_1_day():
    candles = {
        "timestamp": [
            "2020-05-08 14:17:00+00:00",
            "2020-05-08 14:24:00+00:00",
            "2020-05-08 14:24:56+00:00",
            "2020-05-08 14:35:00+00:00",
            "2020-05-08 14:41:00+00:00",
            "2020-05-08 14:41:58+00:00",
        ],
        "open": [94.1200, 94.0700, 94.0700, 94.1700, 94.1900, 94.1900],
        "high": [94.1500, 94.1000, 94.1000, 94.1800, 94.2200, 94.2200],
        "low": [94.0000, 93.9500, 93.9500, 94.0500, 94.0700, 94.0700],
        "close": [94.1300, 94.0800, 94.0800, 94.1800, 94.2000, 94.2000],
        "volume": [7, 91, 0, 0, 0, 0],
    }
    business_cal = EURONEXT_CAL.schedule(start_date="2020-05-08", end_date="2020-05-08")
    df = pd.DataFrame(
        candles, columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    df.index = pd.to_datetime(df.timestamp, format=DATE_FORMAT)
    df = df.drop(["timestamp"], axis=1)
    df = CandleFetcher.resample_candle_data(df, pd.offsets.Day(1), business_cal)

    expected_df_candles = {
        "timestamp": ["2020-05-08"],
        "open": [94.12],
        "high": [94.22],
        "low": [93.95],
        "close": [94.20],
        "volume": [98],
    }
    expected_df = pd.DataFrame(
        expected_df_candles,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    expected_df.index = pd.to_datetime(expected_df.timestamp, format=DATE_FORMAT)
    expected_df = expected_df.drop(["timestamp"], axis=1)

    assert (df == expected_df).all(axis=None)


def test_simple_resample_candle_data_interval_1_day_data_spread_over_2_days():
    candles = {
        "timestamp": [
            "2020-05-08 14:17:00+00:00",
            "2020-05-08 14:24:00+00:00",
            "2020-05-08 14:24:56+00:00",
            "2020-05-08 14:35:00+00:00",
            "2020-05-11 14:41:00+00:00",
            "2020-05-11 14:41:58+00:00",
        ],
        "open": [94.1200, 94.0700, 94.0700, 94.1700, 94.1900, 94.1900],
        "high": [94.1500, 94.1000, 94.1000, 94.1800, 94.2200, 94.2200],
        "low": [94.0000, 93.9500, 93.9500, 94.0500, 94.0700, 94.0700],
        "close": [94.1300, 94.0800, 94.0800, 94.1800, 94.2000, 94.2000],
        "volume": [7, 91, 0, 0, 1, 1],
    }
    business_cal = EURONEXT_CAL.schedule(start_date="2020-05-08", end_date="2020-05-11")
    df = pd.DataFrame(
        candles, columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    df.index = pd.to_datetime(df.timestamp, format=DATE_FORMAT)
    df = df.drop(["timestamp"], axis=1)
    df = CandleFetcher.resample_candle_data(df, pd.offsets.Day(1), business_cal)

    expected_df_candles = {
        "timestamp": ["2020-05-08", "2020-05-11"],
        "open": [94.12, 94.19],
        "high": [94.18, 94.22],
        "low": [93.95, 94.07],
        "close": [94.18, 94.20],
        "volume": [98, 2],
    }
    expected_df = pd.DataFrame(
        expected_df_candles,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    expected_df.index = pd.to_datetime(expected_df.timestamp, format=DATE_FORMAT)

    expected_df = expected_df.drop(["timestamp"], axis=1)
    # assert (df == expected_df).all(axis=None)


def test_fetch_candle_db_data():
    clean_candles_in_db()
    for candle in CANDLES:
        candle.save()
    start = CANDLES[0].timestamp
    end = CANDLES[-1].timestamp
    df = CandleFetcher.fetch_candle_db_data(SYMBOL, start, end)
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
            "94.12000",
            "94.07000",
            "94.07000",
            "94.17000",
            "94.19000",
            "94.19000",
        ],
        "high": [
            "94.15000",
            "94.10000",
            "94.10000",
            "94.18000",
            "94.22000",
            "94.22000",
        ],
        "low": ["94.00000", "93.95000", "93.95000", "94.05000", "94.07000", "94.07000"],
        "close": [
            "94.13000",
            "94.08000",
            "94.08000",
            "94.18000",
            "94.20000",
            "94.20000",
        ],
        "volume": [7, 91, 0, 0, 0, 7],
    }
    expected_df = pd.DataFrame(
        expected_df_candles,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    expected_df.index = pd.to_datetime(expected_df.timestamp, format=DATE_FORMAT)
    expected_df = expected_df.drop(["timestamp"], axis=1)
    assert (df == expected_df).all(axis=None)
    clean_candles_in_db()


@patch("historical_data.meganz_storage.MegaNzStorage.get_file_content")
def test_fetch_historical_data(get_file_content_mocked):
    get_file_content_mocked.side_effect = [
        (
            "date,open,high,low,close,volume\n"
            "2020-06-17 09:30:00+00:00,355.15,355.15,353.74,353.84,3254.0\n"
            "2020-06-17 09:31:00+00:00,354.28,354.96,353.96,354.78,2324.0\n"
            "2020-06-17 09:32:00+00:00,354.92,355.32,354.09,354.09,1123.0\n"
            "2020-06-17 09:33:00+00:00,354.25,354.59,354.14,354.59,2613.0\n"
            "2020-06-17 09:34:00+00:00,354.22,354.26,353.95,353.98,1186.0\n"
        ),
        "",
        (
            "date,open,high,low,close,volume\n"
            "2020-06-19 09:30:00+00:00,355.15,355.15,353.74,353.84,3254.0\n"
            "2020-06-19 09:31:00+00:00,354.28,354.96,353.96,354.78,2324.0\n"
            "2020-06-19 09:32:00+00:00,354.92,355.32,354.09,354.09,1123.0\n"
            "2020-06-19 09:33:00+00:00,354.25,354.59,354.14,354.59,2613.0\n"
            "2020-06-19 09:34:00+00:00,354.22,354.26,353.95,353.98,1186.0\n"
        ),
    ]

    start = pd.Timestamp("2020-06-17 09:33:00", tz="UTC")
    end = pd.Timestamp("2020-06-19 09:32:00", tz="UTC")
    df = CandleFetcher.fetch_candle_historical_data(SYMBOL, start, end)

    expected_df_candles = {
        "timestamp": [
            "2020-06-17 09:33:00+00:00",
            "2020-06-17 09:34:00+00:00",
            "2020-06-19 09:30:00+00:00",
            "2020-06-19 09:31:00+00:00",
            "2020-06-19 09:32:00+00:00",
        ],
        "open": [354.25, 354.22, 355.15, 354.28, 354.92],
        "high": [354.59, 354.26, 355.15, 354.96, 355.32],
        "low": [354.14, 353.95, 353.74, 353.96, 354.09],
        "close": [354.59, 353.98, 353.84, 354.78, 354.09],
        "volume": [2613.0, 1186.0, 3254.0, 2324.0, 1123.0],
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
                DATASETS_DIR, dates_str[0], DONE_DIR, SYMBOL, dates_str[0]
            )
        ),
        call(
            "{}/{}/{}/{}_{}.csv".format(
                DATASETS_DIR, dates_str[1], DONE_DIR, SYMBOL, dates_str[1]
            )
        ),
        call(
            "{}/{}/{}/{}_{}.csv".format(
                DATASETS_DIR, dates_str[2], DONE_DIR, SYMBOL, dates_str[2]
            )
        ),
    ]
    get_file_content_mocked.assert_has_calls(get_file_content_mocked_calls)


@patch("historical_data.meganz_storage.MegaNzStorage.get_file_content")
def test_fetch(get_file_content_mocked):
    clean_candles_in_db()
    for candle in CANDLES:
        candle.save()

    get_file_content_mocked.side_effect = [
        (
            "date,open,high,low,close,volume\n"
            "2020-05-08 14:11:00+00:00,94.15,95.15,93.74,93.84,32\n"
            "2020-05-08 14:12:00+00:00,94.28,94.96,93.96,94.78,23\n"
            "2020-05-08 14:13:00+00:00,94.92,95.32,94.09,94.09,11\n"
            "2020-05-08 14:14:00+00:00,94.25,94.59,94.14,94.59,26\n"
            "2020-05-08 14:15:00+00:00,94.22,94.26,93.95,93.98,11\n"
        )
    ]

    df = CandleFetcher.fetch(
        SYMBOL,
        pd.offsets.Minute(5),
        EuronextExchangeCalendar(),
        pd.Timestamp("2020-05-08 14:12:00", tz="UTC"),
        pd.Timestamp("2020-05-08 14:51:00", tz="UTC"),
    )

    expected_df_candles = {
        "timestamp": [
            "2020-05-08 14:15:00+00:00",
            "2020-05-08 14:20:00+00:00",
            "2020-05-08 14:25:00+00:00",
            "2020-05-08 14:30:00+00:00",
            "2020-05-08 14:35:00+00:00",
            "2020-05-08 14:40:00+00:00",
            "2020-05-08 14:45:00+00:00",
        ],
        "open": [94.28, 94.12, 94.07, 94.07, 94.17, 94.17, 94.19],
        "high": [95.32, 94.15, 94.1, 94.1, 94.18, 94.18, 94.22],
        "low": [93.95, 94.0, 93.95, 93.95, 94.05, 94.05, 94.07],
        "close": [93.98, 94.13, 94.08, 94.08, 94.18, 94.18, 94.2],
        "volume": [71, 7, 91, 0, 0, 0, 7],
    }
    expected_df = pd.DataFrame(
        expected_df_candles,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    expected_df.index = pd.to_datetime(expected_df.timestamp, format=DATE_FORMAT)
    expected_df = expected_df.drop(["timestamp"], axis=1)
    assert (df == expected_df).all(axis=None)

    date_str = "20200508"
    get_file_content_mocked_calls = [
        call(
            "{}/{}/{}/{}_{}.csv".format(
                DATASETS_DIR, date_str, DONE_DIR, SYMBOL, date_str
            )
        )
    ]
    get_file_content_mocked.assert_has_calls(get_file_content_mocked_calls)

    clean_candles_in_db()
