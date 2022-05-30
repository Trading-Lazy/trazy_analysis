from datetime import datetime
from unittest.mock import MagicMock, call, patch

import ccxt
import numpy as np
import pandas as pd
import pytz

from trazy_analysis.common.ccxt_connector import CcxtConnector
from trazy_analysis.common.types import CandleDataFrame
from trazy_analysis.market_data.historical.ccxt_historical_data_handler import (
    CcxtHistoricalDataHandler,
)
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle

BTC_SYMBOL = "BTCUSDT"
EXCHANGE = "BINANCE"
ASSET = Asset(symbol=BTC_SYMBOL, exchange=EXCHANGE)
URL = "trazy.com"
STATUS_CODE_OK = 200
STATUS_CODE_KO = 404
TOKEN = "my_token"

ETH_SYMBOL = "ETH"
ETH_ASSET = Asset(symbol=ETH_SYMBOL, exchange=EXCHANGE)
ETH_CANDLES1 = np.array(
    [
        Candle(asset=ETH_ASSET, open=355.15, high=355.15, low=353.74, close=353.84, volume=3254,
               timestamp=datetime.strptime(
                   "2020-06-11 13:30:00+0000", "%Y-%m-%d %H:%M:%S%z"
               )),
        Candle(asset=ETH_ASSET, open=354.28, high=354.96, low=353.96, close=354.78, volume=2324,
               timestamp=datetime.strptime(
                   "2020-06-13 13:31:00+0000", "%Y-%m-%d %H:%M:%S%z"
               )),
    ],
    dtype=Candle,
)
ETH_CANDLE_DATAFRAME1 = CandleDataFrame.from_candle_list(asset=ETH_ASSET, candles=ETH_CANDLES1)

ETH_CANDLES2 = np.array(
    [
        Candle(asset=ETH_ASSET, open=354.92, high=355.32, low=354.09, close=354.09, volume=1123,
               timestamp=datetime.strptime(
                   "2020-06-15 13:32:00+0000", "%Y-%m-%d %H:%M:%S%z"
               )),
    ],
    dtype=Candle,
)
ETH_CANDLE_DATAFRAME2 = CandleDataFrame.from_candle_list(asset=ETH_ASSET, candles=ETH_CANDLES2)
ETH_CANDLES3 = np.array(
    [
        Candle(asset=ETH_ASSET, open=354.25, high=354.59, low=354.14, close=354.59, volume=2613,
               timestamp=datetime.strptime(
                   "2020-06-17 13:33:00+0000", "%Y-%m-%d %H:%M:%S%z"
               )),
        Candle(asset=ETH_ASSET, open=354.22, high=354.26, low=353.95, close=353.98, volume=1186,
               timestamp=datetime.strptime(
                   "2020-06-19 13:34:00+0000", "%Y-%m-%d %H:%M:%S%z"
               )),
        Candle(asset=ETH_ASSET, open=354.13, high=354.26, low=353.01, close=353.30, volume=1536,
               timestamp=datetime.strptime(
                   "2020-06-19 13:35:00+0000", "%Y-%m-%d %H:%M:%S%z"
               )),
    ],
    dtype=Candle,
)
ETH_CANDLE_DATAFRAME3 = CandleDataFrame.from_candle_list(asset=ETH_ASSET, candles=ETH_CANDLES3)

ETH_CANDLES = np.concatenate([ETH_CANDLES1, ETH_CANDLES2, ETH_CANDLES3])
ETH_CANDLE_DATAFRAME = CandleDataFrame.from_candle_list(asset=ETH_ASSET, candles=ETH_CANDLES)

FETCH_OHLCV_RETURN_VALUE = [
    [
        1626393480000,
        "7922.99000000",
        "7936.99000000",
        "7919.84000000",
        "7935.61000000",
        "53.68618000",
        1626393539999,
        "425682.91899601",
        230,
        "45.80239200",
        "363185.75390966",
        "0",
    ],
    [
        1626393540000,
        "7935.54000000",
        "7954.99000000",
        "7930.09000000",
        "7945.67000000",
        "39.59533500",
        1626393599999,
        "314494.40146720",
        274,
        "35.04922700",
        "278402.89612295",
        "0",
    ],
    [
        1626393600000,
        "7950.00000000",
        "7954.98000000",
        "7946.00000000",
        "7948.00000000",
        "28.71729500",
        1626393659999,
        "228316.83190434",
        195,
        "18.86619400",
        "149992.40120906",
        "0",
    ],
    [
        1626393660000,
        "7950.26000000",
        "7959.72000000",
        "7950.00000000",
        "7957.00000000",
        "56.88972200",
        1626393719999,
        "452590.86434942",
        245,
        "27.29523700",
        "217157.17977072",
        "0",
    ],
    [
        1626393720000,
        "7957.00000000",
        "7979.00000000",
        "7942.35000000",
        "7978.89000000",
        "75.47576000",
        1626393779999,
        "600929.81756139",
        374,
        "49.81860800",
        "396780.28925758",
        "0",
    ],
]

FETCH_OHLCV_TICKER_IN_RANGE_RETURN_VALUE1 = [
    [
        1592573580000,  # 2020-06-19 13:33:00+00:00
        "7922.99000000",
        "7936.99000000",
        "7919.84000000",
        "7935.61000000",
        "53.68618000",
        1592573639999,  # 2020-06-19 13:33:59.999
        "425682.91899601",
        230,
        "45.80239200",
        "363185.75390966",
        "0",
    ],
    [
        1592573640000,  # 2020-06-19 13:34:00+00:00
        "7935.54000000",
        "7954.99000000",
        "7930.09000000",
        "7945.67000000",
        "39.59533500",
        1592573699999,  # 2020-06-19 13:34:59.999
        "314494.40146720",
        274,
        "35.04922700",
        "278402.89612295",
        "0",
    ],
]

FETCH_OHLCV_TICKER_IN_RANGE_RETURN_VALUE2 = [
    [
        1591882260000,  # 2020-06-11 13:31:00+00:00
        "7922.97000000",
        "7936.97000000",
        "7919.86000000",
        "7935.62000000",
        "53.68618000",
        1591882319999,  # 2020-06-11 13:31:59.999
        "425682.91899601",
        230,
        "45.80239200",
        "363185.75390966",
        "0",
    ],
    [
        1592573520000,  # 2020-06-19 13:32:00+00:00
        "7935.54000000",
        "7954.99000000",
        "7930.09000000",
        "7945.67000000",
        "39.59533500",
        1592573579999,  # 2020-06-19 13:32:59.999
        "314494.40146720",
        274,
        "35.04922700",
        "278402.89612295",
        "0",
    ],
]

EXCHANGES_API_KEYS = {
    "binance": {
        "key": None,
        "secret": None,
        "password": None,
    }
}


def test_parse_ticker_data():
    groups_df = CcxtHistoricalDataHandler.group_ticker_data_by_date(
        ASSET, FETCH_OHLCV_RETURN_VALUE
    )
    expected_dates = ["20210715", "20210716"]
    expected_dfs = [
        pd.DataFrame(
            {
                "timestamp": ["2021-07-15 23:58:00+00:00", "2021-07-15 23:59:00+00:00"],
                "open": ["7922.99", "7935.54"],
                "high": ["7936.99", "7954.99"],
                "low": ["7919.84", "7930.09"],
                "close": ["7935.61", "7945.67"],
                "volume": [53.68618, 39.595335],
            }
        ),
        pd.DataFrame(
            {
                "timestamp": [
                    "2021-07-16 00:00:00+00:00",
                    "2021-07-16 00:01:00+00:00",
                    "2021-07-16 00:02:00+00:00",
                ],
                "open": ["7950.0", "7950.26", "7957.0"],
                "high": ["7954.98", "7959.72", "7979.0"],
                "low": ["7946.0", "7950.0", "7942.35"],
                "close": ["7948.0", "7957.0", "7978.89"],
                "volume": [28.717295, 56.889722, 75.47576],
            }
        ),
    ]
    for expected_df in expected_dfs:
        expected_df.index = pd.to_datetime(expected_df.timestamp)
        expected_df.drop(["timestamp"], axis=1, inplace=True)
    idx = 0
    for date_str, group_df in groups_df:
        assert date_str == expected_dates[idx]
        assert (group_df == expected_dfs[idx]).all(axis=None)
        idx += 1


@patch("ccxt.binance.__init__")
def test_request_ticker_data_in_range(binance_init_mocked):
    binance_init_mocked.return_value = None
    ccxt.binance.fetchOHLCV = MagicMock()
    ccxt.binance.fetchOHLCV.side_effect = [
        FETCH_OHLCV_TICKER_IN_RANGE_RETURN_VALUE1,
        FETCH_OHLCV_TICKER_IN_RANGE_RETURN_VALUE2,
        [],
    ]

    start = datetime(2020, 6, 11, 13, 31, tzinfo=pytz.UTC)
    end = datetime(2020, 6, 19, 13, 34, tzinfo=pytz.UTC)
    expected_candle_dataframe = CandleDataFrame.from_candle_list(asset=ETH_ASSET, candles=np.array(
        [
            Candle(asset=ETH_ASSET, open=7935.62, high=7935.62, low=7935.62, close=7935.62, volume=0.0,
                   timestamp=datetime.strptime(
                       "2020-06-13 13:32:00+0000", "%Y-%m-%d %H:%M:%S%z"
                   )),
            Candle(asset=ETH_ASSET, open=7935.62, high=7935.62, low=7935.62, close=7935.62, volume=0.0,
                   timestamp=datetime.strptime(
                       "2020-06-15 13:33:00+0000", "%Y-%m-%d %H:%M:%S%z"
                   )),
        ],
        dtype=Candle,
    ))

    ccxt_connector = CcxtConnector(exchanges_api_keys=EXCHANGES_API_KEYS)
    historical_data_handler = CcxtHistoricalDataHandler(ccxt_connector)

    (
        candle_dataframe,
        none_response_periods,
        error_response_periods,
    ) = historical_data_handler.request_ticker_data_in_range(ETH_ASSET, start, end)

    timestamps = [candle.timestamp for candle in expected_candle_dataframe.to_candles()]
    assert (expected_candle_dataframe == candle_dataframe.loc[timestamps]).all(
        axis=None
    )


@patch("pandas.DataFrame.to_csv")
@patch(
    "trazy_analysis.market_data.historical.ccxt_historical_data_handler.CcxtHistoricalDataHandler.request_ticker_data_in_range"
)
@patch("ccxt.binance.__init__")
def test_save_ticker_data_in_csv(
    binance_init_mocked, request_ticker_data_in_range_mocked, to_csv_mocked
):
    binance_init_mocked.return_value = None
    request_ticker_data_in_range_mocked.return_value = (
        ETH_CANDLE_DATAFRAME,
        set(),
        {},
    )
    csv_filename = "aapl_candles.csv"
    start = datetime.strptime("2020-06-11 13:31:00+0000", "%Y-%m-%d %H:%M:%S%z")
    end = datetime.strptime("2020-06-19 13:34:00+0000", "%Y-%m-%d %H:%M:%S%z")

    ccxt_connector = CcxtConnector(exchanges_api_keys=EXCHANGES_API_KEYS)
    historical_data_handler = CcxtHistoricalDataHandler(ccxt_connector)

    historical_data_handler.save_ticker_data_in_csv(ETH_ASSET, csv_filename, start, end)

    request_ticker_data_in_range_mocked_calls = [call(ETH_ASSET, start, end)]
    request_ticker_data_in_range_mocked.assert_has_calls(
        request_ticker_data_in_range_mocked_calls
    )

    to_csv_mocked_calls = [call(csv_filename, ",")]
    to_csv_mocked.assert_has_calls(to_csv_mocked_calls)
