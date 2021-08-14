from datetime import datetime
from unittest.mock import call, patch

import numpy as np
from freezegun import freeze_time
from requests import Response

from trazy_analysis.market_data.live.live_data_handler import LiveDataHandler
from trazy_analysis.market_data.live.tiingo_live_data_handler import (
    TiingoLiveDataHandler,
)
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle

SYMBOL = "AAPL"
EXCHANGE = "IEX"
ASSET = Asset(symbol=SYMBOL, exchange=EXCHANGE)
TOKEN = "abcde"
STATUS_CODE_OK = 200
STATUS_CODE_KO = 404


@patch("trazy_analysis.market_data.live.live_data_handler.request")
@freeze_time("2020-06-18")
def test_request_ticker_latest_data_tiingo(request_mocked):
    TiingoLiveDataHandler.API_TOKEN = TOKEN
    request_mocked.return_value = Response()
    assert type(TiingoLiveDataHandler.request_ticker_latest_data(ASSET)) == Response
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


@patch(
    "trazy_analysis.market_data.live.live_data_handler.LiveDataHandler.request_ticker_latest_data"
)
@patch("requests.Response.content")
def test_request_ticker_lastest_candles_tiingo(
    content_mocked, request_ticker_latest_data_mocked
):
    request_ticker_latest_data_mocked.return_value = Response()
    request_ticker_latest_data_mocked.return_value.status_code = STATUS_CODE_OK
    ticker_data = (
        "date,open,high,low,close,volume\n"
        "2020-06-17 15:59:00-04:00,355.15,355.15,353.74,353.84,3254.0\n"
        "2020-06-17 16:00:00-04:00,354.28,354.96,353.96,354.78,2324.0\n"
        "2020-06-18 09:30:00-04:00,354.92,355.32,354.09,354.09,1123.0\n"
        "2020-06-18 09:31:00-04:00,354.25,354.59,354.14,354.59,2613.0\n"
        "2020-06-18 09:32:00-04:00,354.22,354.26,353.95,353.98,1186.0\n"
    )
    request_ticker_latest_data_mocked.return_value.content = str.encode(ticker_data)

    expected_candles = np.array(
        [
            Candle(
                asset=ASSET,
                open=354.92,
                high=355.32,
                low=354.09,
                close=354.09,
                volume=1123,
                timestamp=datetime.strptime(
                    "2020-06-18 13:30:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
            Candle(
                asset=ASSET,
                open=354.25,
                high=354.59,
                low=354.14,
                close=354.59,
                volume=2613,
                timestamp=datetime.strptime(
                    "2020-06-18 13:31:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
            Candle(
                asset=ASSET,
                open=354.22,
                high=354.26,
                low=353.95,
                close=353.98,
                volume=1186,
                timestamp=datetime.strptime(
                    "2020-06-18 13:32:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
        ],
        dtype=Candle,
    )
    candles = TiingoLiveDataHandler.request_ticker_lastest_candles(ASSET, nb_candles=3)
    assert (expected_candles == candles).all()


@patch(
    "trazy_analysis.market_data.live.live_data_handler.LiveDataHandler.request_ticker_latest_data"
)
@patch("requests.Response.content")
def test_request_ticker_lastest_candles_not_enough_available_data_tiingo(
    content_mocked, request_ticker_latest_data_mocked
):
    request_ticker_latest_data_mocked.return_value = Response()
    request_ticker_latest_data_mocked.return_value.status_code = STATUS_CODE_OK
    ticker_data = (
        "date,open,high,low,close,volume\n"
        "2020-06-17 15:59:00-04:00,355.15,355.15,353.74,353.84,3254.0\n"
        "2020-06-17 16:00:00-04:00,354.28,354.96,353.96,354.78,2324.0\n"
        "2020-06-18 09:30:00-04:00,354.92,355.32,354.09,354.09,1123.0\n"
        "2020-06-18 09:31:00-04:00,354.25,354.59,354.14,354.59,2613.0\n"
        "2020-06-18 09:32:00-04:00,354.22,354.26,353.95,353.98,1186.0\n"
    )
    request_ticker_latest_data_mocked.return_value.content = str.encode(ticker_data)

    expected_candles = np.array(
        [
            Candle(
                asset=ASSET,
                open=355.15,
                high=355.15,
                low=353.74,
                close=353.84,
                volume=3254,
                timestamp=datetime.strptime(
                    "2020-06-17 19:59:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
            Candle(
                asset=ASSET,
                open=354.28,
                high=354.96,
                low=353.96,
                close=354.78,
                volume=2324,
                timestamp=datetime.strptime(
                    "2020-06-17 20:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
            Candle(
                asset=ASSET,
                open=354.92,
                high=355.32,
                low=354.09,
                close=354.09,
                volume=1123,
                timestamp=datetime.strptime(
                    "2020-06-18 13:30:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
            Candle(
                asset=ASSET,
                open=354.25,
                high=354.59,
                low=354.14,
                close=354.59,
                volume=2613,
                timestamp=datetime.strptime(
                    "2020-06-18 13:31:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
            Candle(
                asset=ASSET,
                open=354.22,
                high=354.26,
                low=353.95,
                close=353.98,
                volume=1186,
                timestamp=datetime.strptime(
                    "2020-06-18 13:32:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
        ],
        dtype=Candle,
    )
    candles = TiingoLiveDataHandler.request_ticker_lastest_candles(ASSET, nb_candles=7)
    assert (expected_candles == candles).all()


@patch(
    "trazy_analysis.market_data.live.live_data_handler.LiveDataHandler.request_ticker_latest_data"
)
@patch("requests.Response.content")
def test_request_ticker_lastest_candles_ticker_data_is_none_tiingo(
    content_mocked, request_ticker_latest_data_mocked
):
    request_ticker_latest_data_mocked.return_value = Response()
    request_ticker_latest_data_mocked.return_value.status_code = STATUS_CODE_OK
    ticker_data = ",open,high,low,close,volume\n"
    request_ticker_latest_data_mocked.return_value.content = str.encode(ticker_data)

    expected_candles = np.empty(shape=0, dtype=Candle)
    candles = TiingoLiveDataHandler.request_ticker_lastest_candles(ASSET, nb_candles=7)
    assert (expected_candles == candles).all()


@patch(
    "trazy_analysis.market_data.live.live_data_handler.LiveDataHandler.request_ticker_latest_data"
)
@patch("requests.Response.content")
def test_request_ticker_lastest_candles_error_tiingo(
    content_mocked, request_ticker_latest_data_mocked
):
    request_ticker_latest_data_mocked.return_value = Response()
    request_ticker_latest_data_mocked.return_value.status_code = STATUS_CODE_KO
    error_response = "Not Found"
    request_ticker_latest_data_mocked.return_value.content = str.encode(error_response)

    expected_candles = np.empty(shape=0, dtype=Candle)
    candles = TiingoLiveDataHandler.request_ticker_lastest_candles(ASSET, nb_candles=7)
    assert (expected_candles == candles).all()


@patch(
    "trazy_analysis.market_data.live.live_data_handler.LiveDataHandler.request_ticker_lastest_candles"
)
def test_request_ticker_lastest_candle(request_ticker_lastest_candles_mocked):
    request_ticker_lastest_candles_mocked.return_value = np.array(
        [
            Candle(
                asset=ASSET,
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
                asset=ASSET,
                open=354.25,
                high=354.59,
                low=354.14,
                close=354.59,
                volume=2613,
                timestamp=datetime.strptime(
                    "2020-06-19 13:33:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
            Candle(
                asset=ASSET,
                open=354.22,
                high=354.26,
                low=353.95,
                close=353.98,
                volume=1186,
                timestamp=datetime.strptime(
                    "2020-06-19 13:34:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
        ],
        dtype=Candle,
    )

    expected_candle = Candle(
        asset=ASSET,
        open=354.22,
        high=354.26,
        low=353.95,
        close=353.98,
        volume=1186,
        timestamp=datetime.strptime("2020-06-19 13:34:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    )
    assert expected_candle == LiveDataHandler.request_ticker_lastest_candle(SYMBOL)

    request_ticker_lastest_candles_mocked_calls = [call(SYMBOL)]
    request_ticker_lastest_candles_mocked.assert_has_calls(
        request_ticker_lastest_candles_mocked_calls
    )


@patch(
    "trazy_analysis.market_data.live.live_data_handler.LiveDataHandler.request_ticker_lastest_candles"
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
