import json
from datetime import datetime

import numpy as np
from freezegun import freeze_time

from trazy_analysis.market_data.live.tiingo_live_crypto_data_handler import (
    TiingoLiveCryptoDataHandler,
)
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle

SYMBOL = "BTCUSD"
EXCHANGE = "BINANCE"
ASSET = Asset(symbol=SYMBOL, exchange=EXCHANGE)
TOKEN = "abcde"
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


def test_parse_ticker_latest_data_points():
    data = json.dumps(
        [
            {
                "ticker": "btcusd",
                "baseCurrency": "btc",
                "quoteCurrency": "usd",
                "priceData": [
                    {
                        "low": 17041.08813324084,
                        "open": 17042.224796462127,
                        "close": 17067.0207267784,
                        "tradesDone": 283.0,
                        "date": "2017-12-13T00:00:00+00:00",
                        "volume": 44.99116858,
                        "high": 17068.325134477574,
                        "volumeNotional": 767865.206676841,
                    },
                    {
                        "low": 16898.50928968564,
                        "open": 16912.399791210508,
                        "close": 16900.749140139273,
                        "tradesDone": 414.0,
                        "date": "2017-12-13T00:01:00+00:00",
                        "volume": 85.58077693,
                        "high": 16935.48154468411,
                        "volumeNotional": 1446379.2421121483,
                    },
                    {
                        "low": 17032.290027982737,
                        "open": 17035.623180823583,
                        "close": 17059.25902662951,
                        "tradesDone": 267.0,
                        "date": "2017-12-13T00:02:00+00:00",
                        "volume": 54.42578747,
                        "high": 17060.86908710398,
                        "volumeNotional": 928463.6061790168,
                    },
                    {
                        "low": 16974.70938582292,
                        "open": 16978.25334442074,
                        "close": 16995.146302196226,
                        "tradesDone": 319.0,
                        "date": "2017-12-13T00:03:00+00:00",
                        "volume": 112.06246923,
                        "high": 17011.583797102718,
                        "volumeNotional": 1904518.059549213,
                    },
                    {
                        "low": 17044.220843317977,
                        "open": 17112.281365604864,
                        "close": 17134.611038665582,
                        "tradesDone": 718.0,
                        "date": "2017-12-13T00:04:00+00:00",
                        "volume": 253.65318281000003,
                        "high": 17278.87984139109,
                        "volumeNotional": 4346248.626168885,
                    },
                    {
                        "low": 17234.716552011258,
                        "open": 17238.0199449088,
                        "close": 17300.679665902073,
                        "tradesDone": 451.0,
                        "date": "2017-12-13T00:05:00+00:00",
                        "volume": 69.91745618999998,
                        "high": 17324.83886467445,
                        "volumeNotional": 1209619.5125979318,
                    },
                ],
            }
        ]
    )
    expected_candles = np.array(
        [
            Candle(asset=ASSET, open=17042.224796462127, high=17068.325134477574, low=17041.08813324084,
                   close=17067.0207267784, volume=44.99116858, timestamp=datetime.strptime(
                    "2017-12-13T00:00:00+0000", DATETIME_FORMAT
                )),
            Candle(asset=ASSET, open=16912.399791210508, high=16935.48154468411, low=16898.50928968564,
                   close=16900.749140139273, volume=85.58077693, timestamp=datetime.strptime(
                    "2017-12-13T00:01:00+0000", DATETIME_FORMAT
                )),
            Candle(asset=ASSET, open=17035.623180823583, high=17060.86908710398, low=17032.290027982737,
                   close=17059.25902662951, volume=54.42578747, timestamp=datetime.strptime(
                    "2017-12-13T00:02:00+0000", DATETIME_FORMAT
                )),
            Candle(asset=ASSET, open=16978.25334442074, high=17011.583797102718, low=16974.70938582292,
                   close=16995.146302196226, volume=112.06246923, timestamp=datetime.strptime(
                    "2017-12-13T00:03:00+0000", DATETIME_FORMAT
                )),
            Candle(asset=ASSET, open=17112.281365604864, high=17278.87984139109, low=17044.220843317977,
                   close=17134.611038665582, volume=253.65318281000003, timestamp=datetime.strptime(
                    "2017-12-13T00:04:00+0000", DATETIME_FORMAT
                )),
            Candle(asset=ASSET, open=17238.0199449088, high=17324.83886467445, low=17234.716552011258,
                   close=17300.679665902073, volume=69.91745618999998, timestamp=datetime.strptime(
                    "2017-12-13T00:05:00+0000", DATETIME_FORMAT
                )),
        ],
        dtype=Candle,
    )
    assert (
        expected_candles
        == TiingoLiveCryptoDataHandler.parse_ticker_latest_data(ASSET, data)
    ).all()


@freeze_time("2020-06-18")
def test_generate_ticker_latest_data_points_url():
    TiingoLiveCryptoDataHandler.API_TOKEN = TOKEN
    expected_url = (
        "https://api.tiingo.com/tiingo/crypto/prices?tickers=BTCUSD&"
        "startDate=2020-06-18&"
        "endDate=2020-06-19&"
        "resampleFreq=1min&"
        "token=abcde"
    )
    assert expected_url == TiingoLiveCryptoDataHandler.generate_ticker_latest_data_url(
        SYMBOL
    )
