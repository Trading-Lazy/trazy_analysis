from datetime import date
from unittest.mock import call, patch

import pandas as pd
from requests import Response, Session

from historical_data.historical_data_api_access import HistoricalDataApiAccess
from historical_data.iex_cloud_api_access import IexCloudApiAccess
from historical_data.tiingo_api_access import TiingoApiAccess

URL = "trazy.com"
STATUS_CODE_OK = 200
STATUS_CODE_KO = 404
CONTENT = "YOLO"
TOKEN = "my_token"


def test_process_row():
    columns_values = {
        "date": [
            "2020-05-08 09:30:00-04:00",
            "2020-05-09 09:30:00-04:00",
            "2020-05-10 09:30:00-04:00",
            "2020-05-11 09:30:00-04:00",
            "2020-05-12 09:30:00-04:00",
            "2020-05-13 09:30:00-04:00",
        ]
    }
    df = pd.DataFrame(columns_values, columns=["date"])
    df.index = pd.to_datetime(df.date)
    df = df.drop(["date"], axis=1)
    expected_processed_row = "20200510"
    assert expected_processed_row == IexCloudApiAccess.process_row(
        df, pd.Timestamp("2020-05-10 09:30:00-04:00", tz="UTC")
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
    groups_df = TiingoApiAccess.parse_ticker_data(data)
    expected_dates = ["20200617", "20200618", "20200619"]
    expected_dfs = [
        pd.DataFrame(
            {
                "date": ["2020-06-17 13:30:00+00:00"],
                "open": [355.15],
                "high": [355.15],
                "low": [353.74],
                "close": [353.84],
                "volume": [3254.0],
            }
        ),
        pd.DataFrame(
            {
                "date": ["2020-06-18 13:31:00+00:00", "2020-06-18 13:32:00+00:00"],
                "open": [354.28, 354.92],
                "high": [354.96, 355.32],
                "low": [353.96, 354.09],
                "close": [354.78, 354.09,],
                "volume": [2324.0, 1123.0],
            }
        ),
        pd.DataFrame(
            {
                "date": ["2020-06-19 13:33:00+00:00", "2020-06-19 13:34:00+00:00"],
                "open": [354.25, 354.22],
                "high": [354.59, 354.26],
                "low": [354.14, 353.95],
                "close": [354.59, 353.98],
                "volume": [2613.0, 1186.0],
            }
        ),
    ]
    for expected_df in expected_dfs:
        expected_df.index = pd.to_datetime(expected_df.date)
        expected_df.drop(["date"], axis=1, inplace=True)
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
    groups_df = IexCloudApiAccess.parse_ticker_data(data)
    expected_dates = ["20200617", "20200618", "20200619"]
    expected_dfs = [
        pd.DataFrame(
            {
                "date": ["2020-06-17 13:30:00+00:00"],
                "high": [192.94],
                "low": [192.6],
                "open": [192.855],
                "close": [192.83],
                "volume": [1362],
            }
        ),
        pd.DataFrame(
            {
                "date": ["2020-06-18 13:31:00+00:00", "2020-06-18 13:32:00+00:00"],
                "high": [193.27, 192.60],
                "low": [192.89, 192.30],
                "open": [192.94, 192.60],
                "close": [192.9, 192.3],
                "volume": [2345, 1350],
            }
        ),
        pd.DataFrame(
            {
                "date": ["2020-06-19 13:33:00+00:00", "2020-06-19 13:34:00+00:00"],
                "high": [192.46, 192.89],
                "low": [192.22, 192.32],
                "open": [192.22, 192.67],
                "close": [192.29, 192.89],
                "volume": [756, 1660],
            }
        ),
    ]
    for expected_df in expected_dfs:
        expected_df.index = pd.to_datetime(expected_df.date)
        expected_df.drop(["date"], axis=1, inplace=True)
    idx = 0
    for date_str, group_df in groups_df:
        assert date_str == expected_dates[idx]
        assert (group_df == expected_dfs[idx]).all(axis=None)
        idx += 1


@patch.object(Session, "get")
def test_request(get_mocked):
    get_mocked.return_value = Response()
    get_mocked.return_value.status_code = STATUS_CODE_OK
    assert HistoricalDataApiAccess.request(URL).status_code == STATUS_CODE_OK
    get_mocked_calls = [call(URL)]
    get_mocked.assert_has_calls(get_mocked_calls)


@patch("historical_data.tiingo_api_access.TiingoApiAccess.request")
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
    TiingoApiAccess.API_TOKEN = TOKEN
    expected_parsed_response = ["AAPL", "GOOGL", "AMZN"]
    assert TiingoApiAccess.get_tickers() == expected_parsed_response
    expected_url = "https://api.tiingo.com/iex?token=my_token"
    request_mocked_calls = [call(expected_url)]
    request_mocked.assert_has_calls(request_mocked_calls)


@patch("historical_data.iex_cloud_api_access.IexCloudApiAccess.request")
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
    IexCloudApiAccess.API_TOKEN = TOKEN
    expected_parsed_response = ["AAPL", "GOOGL", "AMZN"]
    assert IexCloudApiAccess.get_tickers() == expected_parsed_response
    expected_url = (
        "https://cloud.iexapis.com/stable/ref-data/iex/symbols?token=my_token"
    )
    request_mocked_calls = [call(expected_url)]
    request_mocked.assert_has_calls(request_mocked_calls)


@patch("historical_data.tiingo_api_access.TiingoApiAccess.request")
def test_get_tickers_failure_tiingo(request_mocked):
    request_mocked.return_value = Response()
    request_mocked.return_value.status_code = 404
    TiingoApiAccess.API_TOKEN = TOKEN
    expected_parsed_response = []
    assert TiingoApiAccess.get_tickers() == expected_parsed_response
    expected_url = "https://api.tiingo.com/iex?token=my_token"
    request_mocked_calls = [call(expected_url)]
    request_mocked.assert_has_calls(request_mocked_calls)


@patch("historical_data.iex_cloud_api_access.IexCloudApiAccess.request")
def test_get_tickers_failure_iex(request_mocked):
    request_mocked.return_value = Response()
    request_mocked.return_value.status_code = 404
    IexCloudApiAccess.API_TOKEN = TOKEN
    expected_parsed_response = []
    assert IexCloudApiAccess.get_tickers() == expected_parsed_response
    expected_url = (
        "https://cloud.iexapis.com/stable/ref-data/iex/symbols?token=my_token"
    )
    request_mocked_calls = [call(expected_url)]
    request_mocked.assert_has_calls(request_mocked_calls)


@patch("historical_data.tiingo_api_access.TiingoApiAccess.request")
def test_request_ticker_data_tiingo(request_mocked):
    ticker = "aapl"
    period = (date(1996, 4, 13), date(1996, 5, 13))
    TiingoApiAccess.API_TOKEN = TOKEN
    request_mocked.return_value = Response()
    request_mocked.return_value.status_code = STATUS_CODE_OK
    assert type(TiingoApiAccess.request_ticker_data(ticker, period)) == Response
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


@patch("historical_data.iex_cloud_api_access.IexCloudApiAccess.request")
def test_request_ticker_data_iex(request_mocked):
    ticker = "aapl"
    period = (date(1996, 4, 13), date(1996, 4, 13))
    IexCloudApiAccess.API_TOKEN = TOKEN
    request_mocked.return_value = Response()
    request_mocked.return_value.status_code = STATUS_CODE_OK
    assert type(IexCloudApiAccess.request_ticker_data(ticker, period)) == Response
    expected_url = (
        "https://cloud.iexapis.com/stable/stock/aapl/chart/date/19960413?"
        "format=csv&"
        "token=my_token&"
        "chartIEXOnly=true"
    )
    request_mocked_calls = [call(expected_url)]
    request_mocked.assert_has_calls(request_mocked_calls)
