from datetime import date, datetime, timedelta
from unittest.mock import call, patch

import numpy as np
import pandas as pd
from requests import Response

from common.types import CandleDataFrame
from market_data.historical.historical_data_handler import HistoricalDataHandler
from market_data.historical.iex_cloud_historical_data_handler import (
    IexCloudHistoricalDataHandler,
)
from market_data.historical.tiingo_historical_data_handler import (
    TiingoHistoricalDataHandler,
)
from models.candle import Candle

SYMBOL = "IVV"
URL = "trazy.com"
STATUS_CODE_OK = 200
STATUS_CODE_KO = 404
TOKEN = "my_token"

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
                "2020-06-11 13:30:00+0000", "%Y-%m-%d %H:%M:%S%z"
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
                "2020-06-13 13:31:00+0000", "%Y-%m-%d %H:%M:%S%z"
            ),
        ),
    ],
    dtype=Candle,
)
AAPL_CANDLE_DATAFRAME1 = CandleDataFrame.from_candle_list(
    symbol=AAPL_SYMBOL, candles=AAPL_CANDLES1
)

AAPL_CANDLES2 = np.array(
    [
        Candle(
            symbol=AAPL_SYMBOL,
            open=354.92,
            high=355.32,
            low=354.09,
            close=354.09,
            volume=1123,
            timestamp=datetime.strptime(
                "2020-06-15 13:32:00+0000", "%Y-%m-%d %H:%M:%S%z"
            ),
        ),
    ],
    dtype=Candle,
)
AAPL_CANDLE_DATAFRAME2 = CandleDataFrame.from_candle_list(
    symbol=AAPL_SYMBOL, candles=AAPL_CANDLES2
)
AAPL_CANDLES3 = np.array(
    [
        Candle(
            symbol=AAPL_SYMBOL,
            open=354.25,
            high=354.59,
            low=354.14,
            close=354.59,
            volume=2613,
            timestamp=datetime.strptime(
                "2020-06-17 13:33:00+0000", "%Y-%m-%d %H:%M:%S%z"
            ),
        ),
        Candle(
            symbol=AAPL_SYMBOL,
            open=354.22,
            high=354.26,
            low=353.95,
            close=353.98,
            volume=1186,
            timestamp=datetime.strptime(
                "2020-06-19 13:34:00+0000", "%Y-%m-%d %H:%M:%S%z"
            ),
        ),
        Candle(
            symbol=AAPL_SYMBOL,
            open=354.13,
            high=354.26,
            low=353.01,
            close=353.30,
            volume=1536,
            timestamp=datetime.strptime(
                "2020-06-19 13:35:00+0000", "%Y-%m-%d %H:%M:%S%z"
            ),
        ),
    ],
    dtype=Candle,
)
AAPL_CANDLE_DATAFRAME3 = CandleDataFrame.from_candle_list(
    symbol=AAPL_SYMBOL, candles=AAPL_CANDLES3
)

AAPL_CANDLES = np.concatenate([AAPL_CANDLES1, AAPL_CANDLES2, AAPL_CANDLES3])
AAPL_CANDLE_DATAFRAME = CandleDataFrame.from_candle_list(
    symbol=AAPL_SYMBOL, candles=AAPL_CANDLES
)


def test_parse_ticker_data_tiingo():
    data = (
        "date,open,high,low,close,volume\n"
        "2020-06-17 09:30:00-04:00,355.15,355.15,353.74,353.84,3254.0\n"
        "2020-06-18 09:31:00-04:00,354.28,354.96,353.96,354.78,2324.0\n"
        "2020-06-18 09:32:00-04:00,354.92,355.32,354.09,354.09,1123.0\n"
        "2020-06-19 09:33:00-04:00,354.25,354.59,354.14,354.59,2613.0\n"
        "2020-06-19 09:34:00-04:00,354.22,354.26,353.95,353.98,1186.0\n"
    )
    groups_df = TiingoHistoricalDataHandler.group_ticker_data_by_date(SYMBOL, data)
    expected_dates = ["20200617", "20200618", "20200619"]
    expected_dfs = [
        pd.DataFrame(
            {
                "timestamp": ["2020-06-17 13:30:00+00:00"],
                "open": ["355.15"],
                "high": ["355.15"],
                "low": ["353.74"],
                "close": ["353.84"],
                "volume": [3254],
            }
        ),
        pd.DataFrame(
            {
                "timestamp": ["2020-06-18 13:31:00+00:00", "2020-06-18 13:32:00+00:00"],
                "open": ["354.28", "354.92"],
                "high": ["354.96", "355.32"],
                "low": ["353.96", "354.09"],
                "close": ["354.78", "354.09"],
                "volume": [2324, 1123],
            }
        ),
        pd.DataFrame(
            {
                "timestamp": ["2020-06-19 13:33:00+00:00", "2020-06-19 13:34:00+00:00"],
                "open": ["354.25", "354.22"],
                "high": ["354.59", "354.26"],
                "low": ["354.14", "353.95"],
                "close": ["354.59", "353.98"],
                "volume": [2613, 1186],
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


def test_parse_ticker_data_iex():
    data = (
        "date,minute,label,high,low,open,close,average,volume,notional,numberOfTrades,symbol\n"
        "2020-06-17,09:30,09:30 AM,192.94,192.6,192.855,192.83,192.804,1362,262599.24,19,AAPL\n"
        "2020-06-18,09:31,09:31 AM,193.27,192.89,192.94,192.9,193.121,2345,452869.29,31,AAPL\n"
        "2020-06-18,09:32,09:32 AM,192.6,192.3,192.6,192.3,192.519,1350,259901,15,AAPL\n"
        "2020-06-19,09:33,09:33 AM,192.46,192.22,192.22,192.29,192.372,756,145432.96,11,AAPL\n"
        "2020-06-19,09:34,09:34 AM,192.89,192.32,192.67,192.89,192.596,1660,319709.6,19,AAPL"
    )
    groups_df = IexCloudHistoricalDataHandler.group_ticker_data_by_date(SYMBOL, data)
    expected_dates = ["20200617", "20200618", "20200619"]
    expected_dfs = [
        pd.DataFrame(
            {
                "timestamp": ["2020-06-17 13:30:00+00:00"],
                "open": ["192.855"],
                "high": ["192.94"],
                "low": ["192.6"],
                "close": ["192.83"],
                "volume": [1362],
            }
        ),
        pd.DataFrame(
            {
                "timestamp": ["2020-06-18 13:31:00+00:00", "2020-06-18 13:32:00+00:00"],
                "open": ["192.94", "192.6"],
                "high": ["193.27", "192.6"],
                "low": ["192.89", "192.3"],
                "close": ["192.9", "192.3"],
                "volume": [2345, 1350],
            }
        ),
        pd.DataFrame(
            {
                "timestamp": ["2020-06-19 13:33:00+00:00", "2020-06-19 13:34:00+00:00"],
                "open": ["192.22", "192.67"],
                "high": ["192.46", "192.89"],
                "low": ["192.22", "192.32"],
                "close": ["192.29", "192.89"],
                "volume": [756, 1660],
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


@patch(
    "market_data.historical.tiingo_historical_data_handler.TiingoHistoricalDataHandler.request"
)
@patch("requests.Response.content")
def test_get_tickers_success_tiingo(content_mocked, request_mocked):
    request_mocked.return_value = Response()
    request_mocked.return_value.status_code = STATUS_CODE_OK
    tickers_response = """[
        {
            "last": 86.22,
            "bidPrice": 0.0,
            "quoteTimestamp": "2020-06-26T12:00:08.031020218-04:00",
            "mid": null,
            "high": 87.52,
            "timestamp": "2020-06-26T12:07:42.694741256-04:00",
            "tngoLast": 86.22,
            "lastSize": 2,
            "open": 87.23,
            "askSize": 100,
            "ticker": "AAPL",
            "askPrice": 93.9,
            "low": 85.98,
            "volume": 12021,
            "prevClose": 87.26,
            "bidSize": 0,
            "lastSaleTimestamp": "2020-06-26T12:07:42.694741256-04:00"
        },
        {
            "last": 11.16,
            "bidPrice": 10.95,
            "quoteTimestamp": "2020-06-26T12:07:56.271171994-04:00",
            "mid": 11.94,
            "high": 11.65,
            "timestamp": "2020-06-26T12:07:56.271171994-04:00",
            "tngoLast": 11.16,
            "lastSize": 100,
            "open": 11.65,
            "askSize": 1000,
            "ticker": "GOOGL",
            "askPrice": 12.93,
            "low": 11.015,
            "volume": 106111,
            "prevClose": 11.83,
            "bidSize": 100,
            "lastSaleTimestamp": "2020-06-26T12:07:56.212498617-04:00"
        },
        {
            "last": 0.055,
            "bidPrice": null,
            "quoteTimestamp": "2020-06-25T20:00:00+00:00",
            "mid": null,
            "open": 0.055,
            "timestamp": "2020-06-25T20:00:00+00:00",
            "tngoLast": 0.055,
            "lastSize": null,
            "askSize": null,
            "ticker": "AMZN",
            "askPrice": null,
            "low": 0.055,
            "volume": 0,
            "prevClose": 0.055,
            "bidSize": null,
            "lastSaleTimestamp": "2020-06-25T20:00:00+00:00",
            "high": 0.055
        }]"""
    request_mocked.return_value.content = str.encode(tickers_response)
    TiingoHistoricalDataHandler.API_TOKEN = TOKEN
    expected_parsed_response = ["AAPL", "GOOGL", "AMZN"]
    assert TiingoHistoricalDataHandler.get_tickers_list() == expected_parsed_response
    expected_url = "https://api.tiingo.com/iex?token=my_token"
    request_mocked_calls = [call(expected_url)]
    request_mocked.assert_has_calls(request_mocked_calls)


@patch(
    "market_data.historical.iex_cloud_historical_data_handler.IexCloudHistoricalDataHandler.request"
)
@patch("requests.Response.content")
def test_get_tickers_success_iex(content_mocked, request_mocked):
    request_mocked.return_value = Response()
    request_mocked.return_value.status_code = STATUS_CODE_OK
    tickers_response = """[
    {
        "symbol": "AAPL",
        "date": "2020-06-22",
        "isEnabled": true
    },
    {
        "symbol": "GOOGL",
        "date": "2020-06-22",
        "isEnabled": true
    },
    {
        "symbol": "AMZN",
        "date": "2020-06-22",
        "isEnabled": true
    }]"""
    request_mocked.return_value.content = str.encode(tickers_response)
    IexCloudHistoricalDataHandler.API_TOKEN = TOKEN
    expected_parsed_response = np.array(["AAPL", "GOOGL", "AMZN"], dtype="U6")
    assert (
        IexCloudHistoricalDataHandler.get_tickers_list() == expected_parsed_response
    ).all()
    expected_url = (
        "https://cloud.iexapis.com/stable/ref-data/iex/symbols?token=my_token"
    )
    request_mocked_calls = [call(expected_url)]
    request_mocked.assert_has_calls(request_mocked_calls)


@patch(
    "market_data.historical.tiingo_historical_data_handler.TiingoHistoricalDataHandler.request"
)
def test_get_tickers_failure_tiingo(request_mocked):
    request_mocked.return_value = Response()
    request_mocked.return_value.status_code = 404
    TiingoHistoricalDataHandler.API_TOKEN = TOKEN
    expected_parsed_response = np.empty([], dtype="U6")
    assert TiingoHistoricalDataHandler.get_tickers_list() == expected_parsed_response
    expected_url = "https://api.tiingo.com/iex?token=my_token"
    request_mocked_calls = [call(expected_url)]
    request_mocked.assert_has_calls(request_mocked_calls)


@patch(
    "market_data.historical.iex_cloud_historical_data_handler.IexCloudHistoricalDataHandler.request"
)
def test_get_tickers_failure_iex(request_mocked):
    request_mocked.return_value = Response()
    request_mocked.return_value.status_code = 404
    IexCloudHistoricalDataHandler.API_TOKEN = TOKEN
    expected_parsed_response = np.empty([], dtype="U6")
    assert IexCloudHistoricalDataHandler.get_tickers_list() == expected_parsed_response
    expected_url = (
        "https://cloud.iexapis.com/stable/ref-data/iex/symbols?token=my_token"
    )
    request_mocked_calls = [call(expected_url)]
    request_mocked.assert_has_calls(request_mocked_calls)


@patch("market_data.historical.historical_data_handler.request")
def test_request_ticker_data_tiingo(request_mocked):
    ticker = "aapl"
    period = (date(1996, 4, 13), date(1996, 5, 13))
    TiingoHistoricalDataHandler.API_TOKEN = TOKEN
    request_mocked.return_value = Response()
    request_mocked.return_value.status_code = STATUS_CODE_OK
    assert (
        type(TiingoHistoricalDataHandler.request_ticker_data(ticker, period))
        == Response
    )
    expected_url = (
        "https://api.tiingo.com/iex/aapl/prices?"
        "startDate=1996-04-13&"
        "endDate=1996-05-13&"
        "format=csv&columns=open,high,low,close,volume&"
        "resampleFreq=1min&"
        "token=my_token"
    )
    request_mocked_calls = [call(expected_url)]
    request_mocked.assert_has_calls(request_mocked_calls)


@patch("market_data.historical.historical_data_handler.request")
def test_request_ticker_data_iex(request_mocked):
    ticker = "aapl"
    period = (date(1996, 4, 13), date(1996, 4, 13))
    IexCloudHistoricalDataHandler.API_TOKEN = TOKEN
    request_mocked.return_value = Response()
    request_mocked.return_value.status_code = STATUS_CODE_OK
    assert (
        type(IexCloudHistoricalDataHandler.request_ticker_data(ticker, period))
        == Response
    )
    expected_url = (
        "https://cloud.iexapis.com/stable/stock/aapl/chart/date/19960413?"
        "format=csv&"
        "token=my_token&"
        "chartIEXOnly=true"
    )
    request_mocked_calls = [call(expected_url)]
    request_mocked.assert_has_calls(request_mocked_calls)


@patch(
    "market_data.historical.historical_data_handler.HistoricalDataHandler.request_ticker_data"
)
@patch("requests.Response.content")
def test_request_ticker_data_for_period_tiingo(
    content_mocked, request_ticker_data_mocked
):
    request_ticker_data_mocked.return_value = Response()
    request_ticker_data_mocked.return_value.status_code = STATUS_CODE_OK
    ticker_data = (
        "date,open,high,low,close,volume\n"
        "2020-06-18 09:31:00-04:00,354.28,354.96,353.96,354.78,2324\n"
        "2020-06-18 09:32:00-04:00,354.92,355.32,354.09,354.09,1123\n"
        "2020-06-19 09:33:00-04:00,354.25,354.59,354.14,354.59,2613\n"
    )
    request_ticker_data_mocked.return_value.content = str.encode(ticker_data)

    expected_candles = np.array(
        [
            Candle(
                symbol=SYMBOL,
                open=354.28,
                high=354.96,
                low=353.96,
                close=354.78,
                volume=2324,
                timestamp=datetime.strptime(
                    "2020-06-18 13:31:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
            Candle(
                symbol=SYMBOL,
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
                symbol=SYMBOL,
                open=354.25,
                high=354.59,
                low=354.14,
                close=354.59,
                volume=2613,
                timestamp=datetime.strptime(
                    "2020-06-19 13:33:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
        ],
        dtype=Candle,
    )
    none_response_periods = set()
    error_response_periods = {}
    candle_dataframe = TiingoHistoricalDataHandler.request_ticker_data_for_period(
        SYMBOL,
        (date(2020, 6, 18), date(2020, 6, 19)),
        none_response_periods,
        error_response_periods,
    )
    assert (expected_candles == candle_dataframe.to_candles()).all()
    assert none_response_periods == set()
    assert error_response_periods == {}


@patch(
    "market_data.historical.historical_data_handler.HistoricalDataHandler.request_ticker_data"
)
@patch("requests.Response.content")
def test_request_ticker_data_for_period_ticker_data_is_none_tiingo(
    content_mocked, request_ticker_data_mocked
):
    request_ticker_data_mocked.return_value = Response()
    request_ticker_data_mocked.return_value.status_code = STATUS_CODE_OK
    ticker_data = ",open,high,low,close,volume\n"
    request_ticker_data_mocked.return_value.content = str.encode(ticker_data)

    expected_candles = np.empty(shape=0, dtype=Candle)
    none_response_periods = set()
    error_response_periods = {}
    candle_dataframe = TiingoHistoricalDataHandler.request_ticker_data_for_period(
        SYMBOL,
        (date(2020, 6, 18), date(2020, 6, 19)),
        none_response_periods,
        error_response_periods,
    )
    assert (expected_candles == candle_dataframe.to_candles()).all()
    assert none_response_periods == {(date(2020, 6, 18), date(2020, 6, 19))}
    assert error_response_periods == {}


@patch(
    "market_data.historical.historical_data_handler.HistoricalDataHandler.request_ticker_data"
)
@patch("requests.Response.content")
def test_request_ticker_data_for_period_error_tiingo(
    content_mocked, request_ticker_data_mocked
):
    request_ticker_data_mocked.return_value = Response()
    request_ticker_data_mocked.return_value.status_code = STATUS_CODE_KO
    error_response = "Not Found"
    request_ticker_data_mocked.return_value.content = str.encode(error_response)

    expected_candles = np.empty(shape=0, dtype=Candle)
    none_response_periods = set()
    error_response_periods = {}
    candle_dataframe = TiingoHistoricalDataHandler.request_ticker_data_for_period(
        SYMBOL,
        (date(2020, 6, 18), date(2020, 6, 19)),
        none_response_periods,
        error_response_periods,
    )
    assert (expected_candles == candle_dataframe.to_candles()).all()
    assert none_response_periods == set()
    assert error_response_periods == {
        (date(2020, 6, 18), date(2020, 6, 19)): "404: Not Found"
    }


@patch(
    "market_data.historical.historical_data_handler.HistoricalDataHandler.request_ticker_data"
)
@patch("requests.Response.content")
def test_request_ticker_data_for_period_iex(content_mocked, request_ticker_data_mocked):
    request_ticker_data_mocked.return_value = Response()
    request_ticker_data_mocked.return_value.status_code = STATUS_CODE_OK
    ticker_data = (
        "date,minute,label,high,low,open,close,average,volume,notional,numberOfTrades,symbol\n"
        "2019-06-18,09:31,09:31 AM,193.27,192.89,192.94,192.9,193.121,2345,452869.29,31,IVV\n"
        "2019-06-18,09:32,09:32 AM,192.6,192.3,192.6,192.3,192.519,1350,259901,15,IVV\n"
        "2019-06-19,09:33,09:33 AM,192.46,192.22,192.22,192.29,192.372,756,145432.96,11,IVV"
    )
    request_ticker_data_mocked.return_value.content = str.encode(ticker_data)

    expected_candles = np.array(
        [
            Candle(
                symbol=SYMBOL,
                open=192.94,
                high=193.27,
                low=192.89,
                close=192.9,
                volume=2345,
                timestamp=datetime.strptime(
                    "2019-06-18 13:31:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
            Candle(
                symbol=SYMBOL,
                open=192.6,
                high=192.6,
                low=192.3,
                close=192.3,
                volume=1350,
                timestamp=datetime.strptime(
                    "2019-06-18 13:32:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
            Candle(
                symbol=SYMBOL,
                open=192.22,
                high=192.46,
                low=192.22,
                close=192.29,
                volume=756,
                timestamp=datetime.strptime(
                    "2019-06-19 13:33:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
        ],
        dtype=Candle,
    )
    none_response_periods = set()
    error_response_periods = {}
    candle_dataframe = IexCloudHistoricalDataHandler.request_ticker_data_for_period(
        SYMBOL,
        (date(2020, 6, 18), date(2020, 6, 19)),
        none_response_periods,
        error_response_periods,
    )
    assert (expected_candles == candle_dataframe.to_candles()).all()
    assert none_response_periods == set()
    assert error_response_periods == {}


@patch(
    "market_data.historical.historical_data_handler.HistoricalDataHandler.request_ticker_data"
)
@patch("requests.Response.content")
def test_request_ticker_data_for_period_ticker_data_is_none_iex(
    content_mocked, request_ticker_data_mocked
):
    request_ticker_data_mocked.return_value = Response()
    request_ticker_data_mocked.return_value.status_code = STATUS_CODE_OK
    ticker_data = ""
    request_ticker_data_mocked.return_value.content = str.encode(ticker_data)

    expected_candles = np.empty(shape=0, dtype=Candle)
    none_response_periods = set()
    error_response_periods = {}
    candle_dataframe = IexCloudHistoricalDataHandler.request_ticker_data_for_period(
        SYMBOL,
        (date(2020, 6, 18), date(2020, 6, 19)),
        none_response_periods,
        error_response_periods,
    )
    assert (expected_candles == candle_dataframe.to_candles()).all()
    assert none_response_periods == {(date(2020, 6, 18), date(2020, 6, 19))}
    assert error_response_periods == {}


@patch(
    "market_data.historical.historical_data_handler.HistoricalDataHandler.request_ticker_data"
)
@patch("requests.Response.content")
def test_request_ticker_data_for_period_error_iex(
    content_mocked, request_ticker_data_mocked
):
    request_ticker_data_mocked.return_value = Response()
    request_ticker_data_mocked.return_value.status_code = STATUS_CODE_KO
    error_response = "Not Found"
    request_ticker_data_mocked.return_value.content = str.encode(error_response)

    expected_candles = np.empty(shape=0, dtype=Candle)
    none_response_periods = set()
    error_response_periods = {}
    candle_dataframe = IexCloudHistoricalDataHandler.request_ticker_data_for_period(
        SYMBOL,
        (date(2020, 6, 18), date(2020, 6, 19)),
        none_response_periods,
        error_response_periods,
    )
    assert (expected_candles == candle_dataframe.to_candles()).all()
    assert none_response_periods == set()
    assert error_response_periods == {
        (date(2020, 6, 18), date(2020, 6, 19)): "404: Not Found"
    }


@patch(
    "market_data.historical.historical_data_handler.HistoricalDataHandler.request_ticker_data_for_period"
)
def test_request_ticker_data_from_periods(request_ticker_data_for_period_mocked):
    expected_candles = np.concatenate([AAPL_CANDLES1, AAPL_CANDLES2, AAPL_CANDLES3])
    expected_candle_dataframe = CandleDataFrame.from_candle_list(
        symbol=AAPL_SYMBOL, candles=expected_candles
    )
    request_ticker_data_for_period_mocked.side_effect = [
        AAPL_CANDLE_DATAFRAME1,
        AAPL_CANDLE_DATAFRAME2,
        AAPL_CANDLE_DATAFRAME3,
    ]
    periods = np.array(
        [
            (date(2020, 6, 11), date(2020, 6, 13)),
            (date(2020, 6, 14), date(2020, 6, 16)),
            (date(2020, 6, 17), date(2020, 6, 19)),
        ],
        dtype=datetime,
    )
    (
        candle_dataframe,
        none_response_periods,
        error_response_periods,
    ) = HistoricalDataHandler.request_ticker_data_from_periods(SYMBOL, periods)

    assert (expected_candle_dataframe == candle_dataframe).all(axis=None)
    assert none_response_periods == set()
    assert error_response_periods == {}

    request_ticker_data_for_period_mocked_calls = [
        call(SYMBOL, periods[0], set(), {}),
        call(SYMBOL, periods[1], set(), {}),
        call(SYMBOL, periods[2], set(), {}),
    ]
    for i, call_arg_tuple in enumerate(
        request_ticker_data_for_period_mocked.call_args_list
    ):
        call_arg = call_arg_tuple[0]
        symbol = call_arg[0]
        assert symbol == SYMBOL
        period = call_arg[1]
        assert (periods[i] == period).all()
        none_response_periods = call_arg[2]
        assert none_response_periods == set()
        error_response_periods = call_arg[3]
        assert error_response_periods == {}


@patch(
    "market_data.historical.historical_data_handler.HistoricalDataHandler.request_ticker_data_from_periods"
)
def test_request_ticker_data_in_range(request_ticker_data_from_periods_mocked):
    request_ticker_data_from_periods_mocked.return_value = (
        AAPL_CANDLE_DATAFRAME,
        {(date(2020, 6, 14), date(2020, 6, 16))},
        {(date(2020, 6, 11), date(2020, 6, 13)): "400: Bad Request"},
    )
    start = datetime.strptime("2020-06-11 13:31:00+0000", "%Y-%m-%d %H:%M:%S%z")
    end = datetime.strptime("2020-06-19 13:34:00+0000", "%Y-%m-%d %H:%M:%S%z")
    expected_candle_dataframe = CandleDataFrame.from_candle_list(
        symbol=AAPL_SYMBOL, candles=AAPL_CANDLES[1:-1]
    )
    TiingoHistoricalDataHandler.MAX_DOWNLOAD_FRAME = timedelta(days=3)
    (
        candle_dataframe,
        none_response_periods,
        error_response_periods,
    ) = TiingoHistoricalDataHandler.request_ticker_data_in_range(
        AAPL_SYMBOL, start, end
    )

    assert (expected_candle_dataframe == candle_dataframe).all(axis=None)
    assert none_response_periods == {(date(2020, 6, 14), date(2020, 6, 16))}
    assert error_response_periods == {
        (date(2020, 6, 11), date(2020, 6, 13)): "400: Bad Request"
    }

    expected_periods = np.array(
        [
            (date(2020, 6, 11), date(2020, 6, 13)),
            (date(2020, 6, 14), date(2020, 6, 16)),
            (date(2020, 6, 17), date(2020, 6, 19)),
        ]
    )
    assert len(request_ticker_data_from_periods_mocked.call_args_list) == 1
    call_args = request_ticker_data_from_periods_mocked.call_args_list[0]
    symbol = call_args[0][0]
    assert symbol == AAPL_SYMBOL
    periods = call_args[0][1]
    assert (periods == expected_periods).all()


@patch("pandas.DataFrame.to_csv")
@patch(
    "market_data.historical.historical_data_handler.HistoricalDataHandler.request_ticker_data_in_range"
)
def test_save_ticker_data_in_range(request_ticker_data_in_range_mocked, to_csv_mocked):
    request_ticker_data_in_range_mocked.return_value = (
        AAPL_CANDLE_DATAFRAME,
        set(),
        {},
    )
    csv_filename = "aapl_candles.csv"
    start = datetime.strptime("2020-06-11 13:31:00+0000", "%Y-%m-%d %H:%M:%S%z")
    end = datetime.strptime("2020-06-19 13:34:00+0000", "%Y-%m-%d %H:%M:%S%z")
    TiingoHistoricalDataHandler.MAX_DOWNLOAD_FRAME = timedelta(days=3)
    TiingoHistoricalDataHandler.save_ticker_data_in_range(
        AAPL_SYMBOL, csv_filename, start, end
    )

    request_ticker_data_in_range_mocked_calls = [call(AAPL_SYMBOL, start, end)]
    request_ticker_data_in_range_mocked.assert_has_calls(
        request_ticker_data_in_range_mocked_calls
    )

    to_csv_mocked_calls = [call(csv_filename, ",")]
    to_csv_mocked.assert_has_calls(to_csv_mocked_calls)
