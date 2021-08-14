from datetime import datetime
from unittest.mock import call, patch

import numpy as np

from trazy_analysis.common.ccxt_connector import CcxtConnector
from trazy_analysis.market_data.live.ccxt_live_data_handler import (
    CcxtLiveDataHandler,
)
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle

SYMBOL = "BTC/USDT"
EXCHANGE = "BINANCE"
ASSET = Asset(symbol=SYMBOL, exchange=EXCHANGE)
TOKEN = "abcde"

EXCHANGES_API_KEYS = {
    "BINANCE": {
        "key": None,
        "secret": None,
        "password": None,
    }
}


@patch("ccxt.binance.fetch_ohlcv")
def test_request_ticker_lastest_candles(fetch_ohlcv_mocked):
    raw_candles = [
        [1592423940000, 355.15, 355.15, 353.74, 353.84, 3254.0],
        [1592424000000, 354.28, 354.96, 353.96, 354.78, 2324.0],
        [1592487000000, 354.92, 355.32, 354.09, 354.09, 1123.0],
        [1592487060000, 354.25, 354.59, 354.14, 354.59, 2613.0],
        [1592487120000, 354.22, 354.26, 353.95, 353.98, 1186.0],
    ]
    fetch_ohlcv_mocked.return_value = raw_candles

    ccxt_connector = CcxtConnector(exchanges_api_keys=EXCHANGES_API_KEYS)
    ccxt_live_data_handler = CcxtLiveDataHandler(ccxt_connector)

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
    candles = ccxt_live_data_handler.request_ticker_lastest_candles(ASSET, nb_candles=3)
    print([str(candle) for candle in expected_candles])
    print([str(candle) for candle in candles])
    assert (expected_candles == candles).all()


@patch("ccxt.binance.fetch_ohlcv")
def test_request_ticker_lastest_candles_not_enough_available_data(
    fetch_ohlcv_mocked,
):
    raw_candles = [
        [1592423940000, 355.15, 355.15, 353.74, 353.84, 3254.0],
        [1592424000000, 354.28, 354.96, 353.96, 354.78, 2324.0],
        [1592424060000, 354.92, 355.32, 354.09, 354.09, 1123.0],
        [1592424120000, 354.25, 354.59, 354.14, 354.59, 2613.0],
        [1592424180000, 354.22, 354.26, 353.95, 353.98, 1186.0],
    ]
    fetch_ohlcv_mocked.return_value = raw_candles

    ccxt_connector = CcxtConnector(exchanges_api_keys=EXCHANGES_API_KEYS)
    ccxt_live_data_handler = CcxtLiveDataHandler(ccxt_connector)

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
                    "2020-06-17 20:01:00+0000", "%Y-%m-%d %H:%M:%S%z"
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
                    "2020-06-17 20:02:00+0000", "%Y-%m-%d %H:%M:%S%z"
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
                    "2020-06-17 20:03:00+0000", "%Y-%m-%d %H:%M:%S%z"
                ),
            ),
        ],
        dtype=Candle,
    )
    candles = ccxt_live_data_handler.request_ticker_lastest_candles(ASSET, nb_candles=7)
    assert (expected_candles == candles).all()


@patch("ccxt.binance.fetch_ohlcv")
def test_request_ticker_lastest_candles_ticker_data_is_none(
    fetch_ohlcv_mocked,
):
    raw_candles = []
    fetch_ohlcv_mocked.return_value = raw_candles

    ccxt_connector = CcxtConnector(exchanges_api_keys=EXCHANGES_API_KEYS)
    ccxt_live_data_handler = CcxtLiveDataHandler(ccxt_connector)

    expected_candles = np.empty(shape=0, dtype=Candle)
    candles = ccxt_live_data_handler.request_ticker_lastest_candles(ASSET, nb_candles=7)
    assert (expected_candles == candles).all()


@patch("ccxt.binance.fetch_ohlcv")
def test_request_ticker_lastest_candles_error(fetch_ohlcv_mocked):
    fetch_ohlcv_mocked.side_effect = [Exception()]

    ccxt_connector = CcxtConnector(exchanges_api_keys=EXCHANGES_API_KEYS)
    ccxt_live_data_handler = CcxtLiveDataHandler(ccxt_connector)

    expected_candles = np.empty(shape=0, dtype=Candle)
    candles = ccxt_live_data_handler.request_ticker_lastest_candles(ASSET, nb_candles=7)
    assert (expected_candles == candles).all()


@patch(
    "trazy_analysis.market_data.live.ccxt_live_data_handler.CcxtLiveDataHandler.request_ticker_lastest_candles"
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

    ccxt_connector = CcxtConnector(exchanges_api_keys=EXCHANGES_API_KEYS)
    ccxt_live_data_handler = CcxtLiveDataHandler(ccxt_connector)
    assert expected_candle == ccxt_live_data_handler.request_ticker_lastest_candle(
        ticker=ASSET
    )

    request_ticker_lastest_candles_mocked_calls = [call(ASSET)]
    request_ticker_lastest_candles_mocked.assert_has_calls(
        request_ticker_lastest_candles_mocked_calls
    )


@patch(
    "trazy_analysis.market_data.live.ccxt_live_data_handler.CcxtLiveDataHandler.request_ticker_lastest_candles"
)
def test_request_ticker_lastest_candle_no_available_candle(
    request_ticker_lastest_candles_mocked,
):
    request_ticker_lastest_candles_mocked.return_value = []
    ccxt_connector = CcxtConnector(exchanges_api_keys=EXCHANGES_API_KEYS)
    ccxt_live_data_handler = CcxtLiveDataHandler(ccxt_connector)
    assert ccxt_live_data_handler.request_ticker_lastest_candle(ASSET) is None

    request_ticker_lastest_candles_mocked_calls = [call(ASSET)]
    request_ticker_lastest_candles_mocked.assert_has_calls(
        request_ticker_lastest_candles_mocked_calls
    )
