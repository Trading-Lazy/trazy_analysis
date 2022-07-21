from unittest.mock import MagicMock, call, patch

import ccxt
import numpy as np
import pandas as pd

from trazy_analysis.common.ccxt_connector import CcxtConnector
from trazy_analysis.market_data.ccxt_data_handler import CcxtDataHandler
from trazy_analysis.market_data.historical.ccxt_historical_data_handler import (
    CcxtHistoricalDataHandler,
)
from trazy_analysis.models.asset import Asset
from trazy_analysis.settings import BINANCE_API_KEY, BINANCE_API_SECRET

SYMBOL = "BTCUSDT"
EXCHANGE = "CCXT"
ASSET = Asset(symbol=SYMBOL, exchange=EXCHANGE)

FETCH_TICKERS_RETURN_VALUE = {
    "ETH/BTC": {
        "symbol": "ETH/BTC",
        "timestamp": 1626382743219,
        "datetime": "2021-07-15T20:59:03.219Z",
        "high": 0.062159,
        "low": 0.059686,
        "bid": 0.060676,
        "bidVolume": 5.258,
        "ask": 0.060684,
        "askVolume": 2.51,
        "vwap": 0.06047155,
        "open": 0.06065,
        "close": 0.060684,
        "last": 0.060684,
        "previousClose": 0.060653,
        "change": 3.4e-05,
        "percentage": 0.056,
        "average": None,
        "baseVolume": 107664.159,
        "quoteVolume": 6510.61852716,
        "info": {
            "symbol": "ETHBTC",
            "priceChange": "0.00003400",
            "priceChangePercent": "0.056",
            "weightedAvgPrice": "0.06047155",
            "prevClosePrice": "0.06065300",
            "lastPrice": "0.06068400",
            "lastQty": "0.09700000",
            "bidPrice": "0.06067600",
            "bidQty": "5.25800000",
            "askPrice": "0.06068400",
            "askQty": "2.51000000",
            "openPrice": "0.06065000",
            "highPrice": "0.06215900",
            "lowPrice": "0.05968600",
            "volume": "107664.15900000",
            "quoteVolume": "6510.61852716",
            "openTime": "1626296343219",
            "closeTime": "1626382743219",
            "firstId": "282959708",
            "lastId": "283121261",
            "count": "161554",
        },
    },
    "LTC/BTC": {
        "symbol": "LTC/BTC",
        "timestamp": 1626382741762,
        "datetime": "2021-07-15T20:59:01.762Z",
        "high": 0.004023,
        "low": 0.003906,
        "bid": 0.003945,
        "bidVolume": 15.0,
        "ask": 0.003946,
        "askVolume": 111.81,
        "vwap": 0.00395397,
        "open": 0.004005,
        "close": 0.003947,
        "last": 0.003947,
        "previousClose": 0.004007,
        "change": -5.8e-05,
        "percentage": -1.448,
        "average": None,
        "baseVolume": 68056.01,
        "quoteVolume": 269.09162529,
        "info": {
            "symbol": "LTCBTC",
            "priceChange": "-0.00005800",
            "priceChangePercent": "-1.448",
            "weightedAvgPrice": "0.00395397",
            "prevClosePrice": "0.00400700",
            "lastPrice": "0.00394700",
            "lastQty": "0.03000000",
            "bidPrice": "0.00394500",
            "bidQty": "15.00000000",
            "askPrice": "0.00394600",
            "askQty": "111.81000000",
            "openPrice": "0.00400500",
            "highPrice": "0.00402300",
            "lowPrice": "0.00390600",
            "volume": "68056.01000000",
            "quoteVolume": "269.09162529",
            "openTime": "1626296341762",
            "closeTime": "1626382741762",
            "firstId": "67267564",
            "lastId": "67289751",
            "count": "22188",
        },
    },
}


def test_ticker_data_is_none():
    data = []
    assert CcxtHistoricalDataHandler.ticker_data_is_none(data)


def test_ticker_data_to_dataframe():
    data = [
        [
            1523577600000,
            "7922.99000000",
            "7936.99000000",
            "7919.84000000",
            "7935.61000000",
            "53.68618000",
            1523577659999,
            "425682.91899601",
            230,
            "45.80239200",
            "363185.75390966",
            "0",
        ],
        [
            1523577660000,
            "7935.54000000",
            "7954.99000000",
            "7930.09000000",
            "7945.67000000",
            "39.59533500",
            1523577719999,
            "314494.40146720",
            274,
            "35.04922700",
            "278402.89612295",
            "0",
        ],
        [
            1523577720000,
            "7950.00000000",
            "7954.98000000",
            "7946.00000000",
            "7948.00000000",
            "28.71729500",
            1523577779999,
            "228316.83190434",
            195,
            "18.86619400",
            "149992.40120906",
            "0",
        ],
        [
            1523577780000,
            "7950.26000000",
            "7959.72000000",
            "7950.00000000",
            "7957.00000000",
            "56.88972200",
            1523577839999,
            "452590.86434942",
            245,
            "27.29523700",
            "217157.17977072",
            "0",
        ],
        [
            1523577840000,
            "7957.00000000",
            "7979.00000000",
            "7942.35000000",
            "7978.89000000",
            "75.47576000",
            1523577899999,
            "600929.81756139",
            374,
            "49.81860800",
            "396780.28925758",
            "0",
        ],
    ]
    df = CcxtHistoricalDataHandler.ticker_data_to_dataframe(ASSET, data)

    expected_df_columns_values = {
        "date": [
            "2018-04-13 00:00:00+00:00",
            "2018-04-13 00:01:00+00:00",
            "2018-04-13 00:02:00+00:00",
            "2018-04-13 00:03:00+00:00",
            "2018-04-13 00:04:00+00:00",
        ],
        "open": [7922.99, 7935.54, 7950.0, 7950.26, 7957.0],
        "high": [7936.99, 7954.99, 7954.98, 7959.72, 7979.0],
        "low": [7919.84, 7930.09, 7946.0, 7950.0, 7942.35],
        "close": [7935.61, 7945.67, 7948.0, 7957.0, 7978.89],
        "volume": [53.68618, 39.595335, 28.717295, 56.889722, 75.47576],
    }
    expected_df = pd.DataFrame(
        expected_df_columns_values,
        columns=["date", "open", "high", "low", "close", "volume"],
    )
    expected_df.index = pd.to_datetime(expected_df.date)
    expected_df = expected_df.drop(["date"], axis=1)
    assert (df == expected_df).all(axis=None)


@patch("ccxt.binance.has")
@patch("ccxt.binance.__init__")
def test_get_tickers_list_success(
    binance_init_mocked,
    has_mocked,
):
    binance_init_mocked.return_value = None
    ccxt.binance.fetchTickers = MagicMock()
    ccxt.binance.fetchTickers.return_value = FETCH_TICKERS_RETURN_VALUE
    has_mocked.__contains__.return_value = True
    has_mocked.__getitem__.return_value = True

    exchanges_api_keys = {
        "BINANCE": {
            "key": BINANCE_API_KEY,
            "secret": BINANCE_API_SECRET,
            "password": None,
        }
    }

    ccxt_connector = CcxtConnector(exchanges_api_keys=exchanges_api_keys)
    ccxt_data_handler = CcxtDataHandler(ccxt_connector)

    assert (
        ccxt_data_handler.get_tickers_list("BINANCE") == ["ETH/BTC", "LTC/BTC"]
    ).all()

    binance_init_mocked_calls = [
        call(
            {
                "apiKey": BINANCE_API_KEY,
                "secret": BINANCE_API_SECRET,
                "password": None,
            }
        )
    ]
    binance_init_mocked.assert_has_calls(binance_init_mocked_calls)


@patch("ccxt.binance.has")
@patch("ccxt.binance.__init__")
def test_get_tickers_list_failure(
    binance_init_mocked,
    has_mocked,
):
    binance_init_mocked.return_value = None
    ccxt.binance.fetchTickers = MagicMock()
    ccxt.binance.fetchTickers.side_effect = Exception(
        "This exception has been raised for testing purpose"
    )
    has_mocked.__contains__.return_value = False
    has_mocked.__getitem__.return_value = True

    exchanges_api_keys = {
        "BINANCE": {
            "key": BINANCE_API_KEY,
            "secret": BINANCE_API_SECRET,
            "password": None,
        }
    }

    ccxt_connector = CcxtConnector(exchanges_api_keys=exchanges_api_keys)
    ccxt_data_handler = CcxtDataHandler(ccxt_connector)

    assert ccxt_data_handler.get_tickers_list("BINANCE") == np.empty([], dtype="U20")

    binance_init_mocked_calls = [
        call(
            {
                "apiKey": BINANCE_API_KEY,
                "secret": BINANCE_API_SECRET,
                "password": None,
            }
        )
    ]
    binance_init_mocked.assert_has_calls(binance_init_mocked_calls)
