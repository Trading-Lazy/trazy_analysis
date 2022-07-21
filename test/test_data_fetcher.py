from datetime import datetime, timedelta
from unittest.mock import call, patch

import numpy as np
import pandas as pd
import pytz
from pandas_market_calendars.exchange_calendar_eurex import EUREXExchangeCalendar

from trazy_analysis.common.constants import DATE_FORMAT
from trazy_analysis.common.types import CandleDataFrame
from trazy_analysis.db_storage.mongodb_storage import MongoDbStorage
from trazy_analysis.file_storage.common import DATASETS_DIR, DONE_DIR
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle
from trazy_analysis.settings import DATABASE_NAME
from trazy_analysis.market_data.data_fetcher import ExternalStorageFetcher
from trazy_analysis.test.tools.tools import compare_candles_list

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
MARKET_CAL = EUREXExchangeCalendar()
CANDLE_FETCHER = ExternalStorageFetcher(db_storage=DB_STORAGE, market_cal=MARKET_CAL)


def test_query_candles():
    DB_STORAGE.clean_all_candles()
    for candle in CANDLES:
        DB_STORAGE.add_candle(candle)
    start, end = CANDLES[0].timestamp, CANDLES[-1].timestamp
    candles = CANDLE_FETCHER.query_candles(IVV_ASSET, timedelta(minutes=1), start, end)
    DB_STORAGE.clean_all_candles()
    assert compare_candles_list(candles, CANDLES)


def test_fetch_candle_db_data():
    DB_STORAGE.clean_all_candles()
    for candle in CANDLES:
        DB_STORAGE.add_candle(candle)
    start = CANDLES[0].timestamp
    end = CANDLES[-1].timestamp
    df = CANDLE_FETCHER.fetch_candle_db_data(
        IVV_ASSET, timedelta(minutes=1), start, end
    )
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
            94.12,
            94.07,
            94.07,
            94.17,
            94.19,
            94.19,
        ],
        "high": [
            94.15,
            94.1,
            94.1,
            94.18,
            94.22,
            94.22,
        ],
        "low": [94.0, 93.95, 93.95, 94.05, 94.07, 94.07],
        "close": [
            94.13,
            94.08,
            94.08,
            94.18,
            94.2,
            94.2,
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


def test_fetch_none_db_storage_none_file_storage():
    candle_fetcher = ExternalStorageFetcher(
        db_storage=None, file_storage=None, market_cal=MARKET_CAL
    )

    df = candle_fetcher.fetch(
        SYMBOL,
        timedelta(minutes=1),
        datetime.strptime("2020-05-08 14:12:00+0000", "%Y-%m-%d %H:%M:%S%z"),
        datetime.strptime("2020-05-08 14:49:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    ).rescale(time_unit=timedelta(minutes=5), market_cal=MARKET_CAL)

    expected_df = CandleDataFrame.from_candle_list(
        asset=IVV_ASSET, candles=np.array([], dtype=Candle)
    )
    assert (df == expected_df).all(axis=None)


def test_data_fetcher_multi_fetch():
    start = datetime(2022, 6, 8, 0, 0, 0, 0, tzinfo=pytz.UTC)
    end = datetime(2022, 6, 9, 0, 0, 0, 0, tzinfo=pytz.UTC)
    btc = Asset(symbol="BTCUSDT", exchange="BINANCE")
    timeframe = timedelta(minutes=1)
    CandleDataFrame.multi_fetch(
        {btc: timeframe, btc: timedelta(minutes=5)}, start, end
    )
