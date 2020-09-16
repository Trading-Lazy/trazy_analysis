from decimal import Decimal
from unittest.mock import call, patch

import pandas as pd
from freezegun import freeze_time
from requests import Response

from market_data.live.live_data_handler import LiveDataHandler
from market_data.live.tiingo_live_data_handler import TiingoLiveDataHandler
from models.candle import Candle

SYMBOL = "AAPL"
TOKEN = "abcde"
STATUS_CODE_OK = 200
STATUS_CODE_KO = 404


@patch("market_data.live.live_data_handler.request")
@freeze_time("2020-06-18")
def test_request_ticker_latest_data_tiingo(request_mocked):
    TiingoLiveDataHandler.API_TOKEN = TOKEN
    request_mocked.return_value = Response()
    assert type(TiingoLiveDataHandler.request_ticker_latest_data(SYMBOL)) == Response
    expected_url = (
        "https://api.tiingo.com/iex/AAPL/prices?"
        "startDate=2020-06-18&"
        "endDate=2020-06-18&"
        "format=csv&columns=open,high,low,close,volume&"
        "resampleFreq=1min&"
        "token=abcde"
    )
    request_mocked_calls = [call(expected_url)]
    request_mocked.assert_has_calls(request_mocked_calls)


@patch("market_data.live.live_data_handler.LiveDataHandler.request_ticker_latest_data")
@patch("requests.Response.content")
def test_request_ticker_lastest_candles_tiingo(
    content_mocked, request_ticker_latest_data_mocked
):
    request_ticker_latest_data_mocked.return_value = Response()
    request_ticker_latest_data_mocked.return_value.status_code = STATUS_CODE_OK
    ticker_data = (
        "date,open,high,low,close,volume\n"
        "2020-06-17 09:30:00-04:00,355.15,355.15,353.74,353.84,3254.0\n"
        "2020-06-18 09:31:00-04:00,354.28,354.96,353.96,354.78,2324.0\n"
        "2020-06-18 09:32:00-04:00,354.92,355.32,354.09,354.09,1123.0\n"
        "2020-06-19 09:33:00-04:00,354.25,354.59,354.14,354.59,2613.0\n"
        "2020-06-19 09:34:00-04:00,354.22,354.26,353.95,353.98,1186.0\n"
    )
    request_ticker_latest_data_mocked.return_value.content = str.encode(ticker_data)

    expected_candles = [
        Candle(
            symbol="AAPL",
            open=Decimal("354.92"),
            high=Decimal("355.32"),
            low=Decimal("354.09"),
            close=Decimal("354.09"),
            volume=1123,
            timestamp=pd.Timestamp("2020-06-18 13:32:00+00:00"),
        ),
        Candle(
            symbol="AAPL",
            open=Decimal("354.25"),
            high=Decimal("354.59"),
            low=Decimal("354.14"),
            close=Decimal("354.59"),
            volume=2613,
            timestamp=pd.Timestamp("2020-06-19 13:33:00+00:00"),
        ),
        Candle(
            symbol="AAPL",
            open=Decimal("354.22"),
            high=Decimal("354.26"),
            low=Decimal("353.95"),
            close=Decimal("353.98"),
            volume=1186,
            timestamp=pd.Timestamp("2020-06-19 13:34:00+00:00"),
        ),
    ]
    candles = TiingoLiveDataHandler.request_ticker_lastest_candles(SYMBOL, nb_candles=3)
    assert expected_candles == candles


@patch("market_data.live.live_data_handler.LiveDataHandler.request_ticker_latest_data")
@patch("requests.Response.content")
def test_request_ticker_lastest_candles_not_enough_available_data_tiingo(
    content_mocked, request_ticker_latest_data_mocked
):
    request_ticker_latest_data_mocked.return_value = Response()
    request_ticker_latest_data_mocked.return_value.status_code = STATUS_CODE_OK
    ticker_data = (
        "date,open,high,low,close,volume\n"
        "2020-06-17 09:30:00-04:00,355.15,355.15,353.74,353.84,3254.0\n"
        "2020-06-18 09:31:00-04:00,354.28,354.96,353.96,354.78,2324.0\n"
        "2020-06-18 09:32:00-04:00,354.92,355.32,354.09,354.09,1123.0\n"
        "2020-06-19 09:33:00-04:00,354.25,354.59,354.14,354.59,2613.0\n"
        "2020-06-19 09:34:00-04:00,354.22,354.26,353.95,353.98,1186.0\n"
    )
    request_ticker_latest_data_mocked.return_value.content = str.encode(ticker_data)

    expected_candles = [
        Candle(
            symbol="AAPL",
            open=Decimal("355.15"),
            high=Decimal("355.15"),
            low=Decimal("353.74"),
            close=Decimal("353.84"),
            volume=3254,
            timestamp=pd.Timestamp("2020-06-17 13:30:00+00:00"),
        ),
        Candle(
            symbol="AAPL",
            open=Decimal("354.28"),
            high=Decimal("354.96"),
            low=Decimal("353.96"),
            close=Decimal("354.78"),
            volume=2324,
            timestamp=pd.Timestamp("2020-06-18 13:31:00+00:00"),
        ),
        Candle(
            symbol="AAPL",
            open=Decimal("354.92"),
            high=Decimal("355.32"),
            low=Decimal("354.09"),
            close=Decimal("354.09"),
            volume=1123,
            timestamp=pd.Timestamp("2020-06-18 13:32:00+00:00"),
        ),
        Candle(
            symbol="AAPL",
            open=Decimal("354.25"),
            high=Decimal("354.59"),
            low=Decimal("354.14"),
            close=Decimal("354.59"),
            volume=2613,
            timestamp=pd.Timestamp("2020-06-19 13:33:00+00:00"),
        ),
        Candle(
            symbol="AAPL",
            open=Decimal("354.22"),
            high=Decimal("354.26"),
            low=Decimal("353.95"),
            close=Decimal("353.98"),
            volume=1186,
            timestamp=pd.Timestamp("2020-06-19 13:34:00+00:00"),
        ),
    ]
    candles = TiingoLiveDataHandler.request_ticker_lastest_candles(SYMBOL, nb_candles=7)
    assert expected_candles == candles


@patch("market_data.live.live_data_handler.LiveDataHandler.request_ticker_latest_data")
@patch("requests.Response.content")
def test_request_ticker_lastest_candles_ticker_data_is_none_tiingo(
    content_mocked, request_ticker_latest_data_mocked
):
    request_ticker_latest_data_mocked.return_value = Response()
    request_ticker_latest_data_mocked.return_value.status_code = STATUS_CODE_OK
    ticker_data = ",open,high,low,close,volume\n"
    request_ticker_latest_data_mocked.return_value.content = str.encode(ticker_data)

    expected_candles = []
    candles = TiingoLiveDataHandler.request_ticker_lastest_candles(SYMBOL, nb_candles=7)
    assert expected_candles == candles


@patch("market_data.live.live_data_handler.LiveDataHandler.request_ticker_latest_data")
@patch("requests.Response.content")
def test_request_ticker_lastest_candles_error_tiingo(
    content_mocked, request_ticker_latest_data_mocked
):
    request_ticker_latest_data_mocked.return_value = Response()
    request_ticker_latest_data_mocked.return_value.status_code = STATUS_CODE_KO
    error_response = "Not Found"
    request_ticker_latest_data_mocked.return_value.content = str.encode(error_response)

    expected_candles = []
    candles = TiingoLiveDataHandler.request_ticker_lastest_candles(SYMBOL, nb_candles=7)
    assert expected_candles == candles


@patch(
    "market_data.live.live_data_handler.LiveDataHandler.request_ticker_lastest_candles"
)
def test_request_ticker_lastest_candle(request_ticker_lastest_candles_mocked):
    request_ticker_lastest_candles_mocked.return_value = [
        Candle(
            symbol="AAPL",
            open=Decimal("354.92"),
            high=Decimal("355.32"),
            low=Decimal("354.09"),
            close=Decimal("354.09"),
            volume=1123,
            timestamp=pd.Timestamp("2020-06-18 13:32:00+00:00"),
        ),
        Candle(
            symbol="AAPL",
            open=Decimal("354.25"),
            high=Decimal("354.59"),
            low=Decimal("354.14"),
            close=Decimal("354.59"),
            volume=2613,
            timestamp=pd.Timestamp("2020-06-19 13:33:00+00:00"),
        ),
        Candle(
            symbol="AAPL",
            open=Decimal("354.22"),
            high=Decimal("354.26"),
            low=Decimal("353.95"),
            close=Decimal("353.98"),
            volume=1186,
            timestamp=pd.Timestamp("2020-06-19 13:34:00+00:00"),
        ),
    ]

    expected_candle = Candle(
        symbol="AAPL",
        open=Decimal("354.22"),
        high=Decimal("354.26"),
        low=Decimal("353.95"),
        close=Decimal("353.98"),
        volume=1186,
        timestamp=pd.Timestamp("2020-06-19 13:34:00+00:00"),
    )
    assert expected_candle == LiveDataHandler.request_ticker_lastest_candle(SYMBOL)

    request_ticker_lastest_candles_mocked_calls = [call(SYMBOL)]
    request_ticker_lastest_candles_mocked.assert_has_calls(
        request_ticker_lastest_candles_mocked_calls
    )


@patch(
    "market_data.live.live_data_handler.LiveDataHandler.request_ticker_lastest_candles"
)
def test_request_ticker_lastest_candle_no_available_candle(
    request_ticker_lastest_candles_mocked,
):
    request_ticker_lastest_candles_mocked.return_value = []
    assert LiveDataHandler.request_ticker_lastest_candle(SYMBOL) is None

    request_ticker_lastest_candles_mocked_calls = [call(SYMBOL)]
    request_ticker_lastest_candles_mocked.assert_has_calls(
        request_ticker_lastest_candles_mocked_calls
    )
