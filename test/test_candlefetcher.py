from datetime import datetime
from decimal import Decimal

import pandas as pd

from strategy.candlefetcher import CandleFetcher
from strategy.constants import DATE_FORMAT
from actionsapi.models import Candle
from common.exchange_calendar_euronext import EuronextExchangeCalendar
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
        timestamp=pd.Timestamp("2020-05-08 14:17:00", tz='UTC'),
    ),
    Candle(
        _id=2,
        symbol=SYMBOL,
        open=Decimal("94.0700"),
        high=Decimal("94.1000"),
        low=Decimal("93.9500"),
        close=Decimal("94.0800"),
        volume=91,
        timestamp=pd.Timestamp("2020-05-08 14:24:00", tz='UTC'),
    ),
    Candle(
        _id=3,
        symbol=SYMBOL,
        open=Decimal("94.0700"),
        high=Decimal("94.1000"),
        low=Decimal("93.9500"),
        close=Decimal("94.0800"),
        volume=0,
        timestamp=pd.Timestamp("2020-05-08 14:24:56", tz='UTC'),
    ),
    Candle(
        _id=4,
        symbol=SYMBOL,
        open=Decimal("94.1700"),
        high=Decimal("94.1800"),
        low=Decimal("94.0500"),
        close=Decimal("94.1800"),
        volume=0,
        timestamp=pd.Timestamp("2020-05-08 14:35:00", tz='UTC'),
    ),
    Candle(
        _id=5,
        symbol=SYMBOL,
        open=Decimal("94.1900"),
        high=Decimal("94.2200"),
        low=Decimal("94.0700"),
        close=Decimal("94.2000"),
        volume=0,
        timestamp=pd.Timestamp("2020-05-08 14:41:00", tz='UTC'),
    ),
    Candle(
        _id=6,
        symbol=SYMBOL,
        open=Decimal("94.1900"),
        high=Decimal("94.2200"),
        low=Decimal("94.0700"),
        close=Decimal("94.2000"),
        volume=7,
        timestamp=pd.Timestamp("2020-05-08 14:41:58", tz='UTC'),
    ),
]


def test_get_candles_from_db():
    clean_candles_in_db()
    for candle in CANDLES:
        candle.save()
    start, end = CANDLES[0].timestamp, CANDLES[-1].timestamp
    candles = CandleFetcher.get_candles_from_db(SYMBOL, start, end)
    assert compare_candles_list(candles, CANDLES)
    clean_candles_in_db()


def test_resample_candle_data_interval_5_minute():
    candles = {
        "timestamp": [
            datetime.strptime("2020-05-08 14:17:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:24:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:24:56", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:35:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:41:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:41:58", DATE_FORMAT),
        ],
        "symbol": ["ANX.PA", "ANX.PA", "ANX.PA", "ANX.PA", "ANX.PA", "ANX.PA"],
        "open": [94.1200, 94.0700, 94.0700, 94.1700, 94.1900, 94.1900],
        "high": [94.1500, 94.1000, 94.1000, 94.1800, 94.2200, 94.2200],
        "low": [94.0000, 93.9500, 93.9500, 94.0500, 94.0700, 94.0700],
        "close": [94.1300, 94.0800, 94.0800, 94.1800, 94.2000, 94.2000],
        "volume": [7, 91, 0, 0, 0, 0],
    }
    business_cal = EURONEXT_CAL.schedule(start_date="2020-05-08", end_date="2020-05-08")
    df = pd.DataFrame(candles,
                      columns=['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume'])
    df = CandleFetcher.resample_candle_data(df, pd.offsets.Minute(5), business_cal)

    expected_df_candles = {
        "timestamp": [
            datetime.strptime("2020-05-08 14:20:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:25:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:30:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:35:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:40:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:45:00", DATE_FORMAT),
        ],
        "symbol": ["ANX.PA", "ANX.PA", "ANX.PA", "ANX.PA", "ANX.PA", "ANX.PA"],
        "open": [94.12, 94.07, 94.07, 94.17, 94.17, 94.19],
        "high": [94.15, 94.10, 94.10, 94.18, 94.18, 94.22],
        "low": [94.00, 93.95, 93.95, 94.05, 94.05, 94.07],
        "close": [94.13, 94.08, 94.08, 94.18, 94.18, 94.20],
        "volume": [7, 91, 0, 0, 0, 0],
    }
    expected_df = pd.DataFrame(
        expected_df_candles,
        columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"],
    )
    expected_df.index = pd.to_datetime(expected_df.timestamp, format=DATE_FORMAT)
    expected_df = expected_df.drop(["timestamp"], axis=1)

    assert (df == expected_df).all(axis=None)


def test_simple_resample_candle_data_interval_1_day():
    candles = {
        'timestamp': ['2020-05-08 14:17:00', '2020-05-08 14:24:00', '2020-05-08 14:24:56',
                      '2020-05-08 14:35:00', '2020-05-08 14:41:00', '2020-05-08 14:41:58'],
        'symbol': ['ANX.PA', 'ANX.PA', 'ANX.PA', 'ANX.PA', 'ANX.PA', 'ANX.PA'],
        'open': [94.1200, 94.0700, 94.0700, 94.1700, 94.1900, 94.1900],
        'high': [94.1500, 94.1000, 94.1000, 94.1800, 94.2200, 94.2200],
        'low': [94.0000, 93.9500, 93.9500, 94.0500, 94.0700, 94.0700],
        'close': [94.1300, 94.0800, 94.0800, 94.1800, 94.2000, 94.2000],
        'volume': [7, 91, 0, 0, 0, 0]
    }
    business_cal = EURONEXT_CAL.schedule(start_date="2020-05-08", end_date="2020-05-08")
    df = pd.DataFrame(candles,
                      columns=['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume'])
    df = CandleFetcher.resample_candle_data(df, pd.offsets.Day(1), business_cal)

    expected_df_candles = {
        'timestamp': ['2020-05-08'],
        'symbol': ['ANX.PA'],
        'open': [94.12],
        'high': [94.22],
        'low': [93.95],
        'close': [94.20],
        'volume': [98]
    }
    expected_df = pd.DataFrame(expected_df_candles,
                               columns=['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume'])
    expected_df.index = pd.to_datetime(expected_df.timestamp, format=DATE_FORMAT)

    expected_df = expected_df.drop(['timestamp'], axis=1)
    assert ((df == expected_df).all(axis=None))


def test_simple_resample_candle_data_interval_1_day_data_spread_over_2_days():
    candles = {
        'timestamp': ['2020-05-08 14:17:00', '2020-05-08 14:24:00', '2020-05-08 14:24:56',
                      '2020-05-08 14:35:00', '2020-05-11 14:41:00', '2020-05-11 14:41:58'],
        'symbol': ['ANX.PA', 'ANX.PA', 'ANX.PA', 'ANX.PA', 'ANX.PA', 'ANX.PA'],
        'open': [94.1200, 94.0700, 94.0700, 94.1700, 94.1900, 94.1900],
        'high': [94.1500, 94.1000, 94.1000, 94.1800, 94.2200, 94.2200],
        'low': [94.0000, 93.9500, 93.9500, 94.0500, 94.0700, 94.0700],
        'close': [94.1300, 94.0800, 94.0800, 94.1800, 94.2000, 94.2000],
        'volume': [7, 91, 0, 0, 1, 1]
    }
    business_cal = EURONEXT_CAL.schedule(start_date="2020-05-08", end_date="2020-05-11")
    df = pd.DataFrame(candles,
                      columns=['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume'])
    df = CandleFetcher.resample_candle_data(df, pd.offsets.Day(1), business_cal)

    expected_df_candles = {
        'timestamp': ['2020-05-08','2020-05-11'],
        'symbol': ['ANX.PA','ANX.PA'],
        'open': [94.12, 94.19],
        'high': [94.18, 94.22],
        'low': [93.95, 94.07],
        'close': [94.18, 94.20],
        'volume': [98, 2]
    }
    expected_df = pd.DataFrame(expected_df_candles,
                               columns=['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume'])
    expected_df.index = pd.to_datetime(expected_df.timestamp, format=DATE_FORMAT)

    expected_df = expected_df.drop(['timestamp'], axis=1)
    assert ((df == expected_df).all(axis=None))


def test_fetch():
    expected_df_candles = {
        "timestamp": [
            datetime.strptime("2020-05-08 14:20:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:25:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:30:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:35:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:40:00", DATE_FORMAT),
            datetime.strptime("2020-05-08 14:45:00", DATE_FORMAT),
        ],
        "symbol": ["ANX.PA", "ANX.PA", "ANX.PA", "ANX.PA", "ANX.PA", "ANX.PA"],
        "open": [94.12, 94.07, 94.07, 94.17, 94.17, 94.19],
        "high": [94.15, 94.10, 94.10, 94.18, 94.18, 94.22],
        "low": [94.00, 93.95, 93.95, 94.05, 94.05, 94.07],
        "close": [94.13, 94.08, 94.08, 94.18, 94.18, 94.20],
        "volume": [7, 91, 0, 0, 0, 7],
    }
    expected_df = pd.DataFrame(
        expected_df_candles,
        columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"],
    )
    expected_df = expected_df.set_index(['timestamp'])

    clean_candles_in_db()
    for candle in CANDLES:
        candle.save()
    start, end = CANDLES[0].timestamp, CANDLES[-1].timestamp
    df = CandleFetcher.fetch(SYMBOL, pd.offsets.Minute(5), EURONEXT_CAL, start, end)
    clean_candles_in_db()

    assert ((df == expected_df).all(axis=None))
