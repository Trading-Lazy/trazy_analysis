import json
from datetime import datetime

import numpy as np
import pytz
from freezegun import freeze_time

from trazy_analysis.market_data.live.binance_live_data_handler import (
    BinanceLiveDataHandler,
)
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle

SYMBOL = "BTC/USDT"
EXCHANGE = "BINANCE"
ASSET = Asset(symbol=SYMBOL, exchange=EXCHANGE)
TOKEN = "abcde"


def test_parse_ticker_latest_data_points():
    data = json.dumps(
        [
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
    )
    expected_candles = np.array(
        [
            Candle(
                asset=Asset(symbol="BTC/USDT", exchange="BINANCE"),
                open=7922.99,
                high=7936.99,
                low=7919.84,
                close=7935.61,
                volume=53.68618,
                timestamp=datetime(2018, 4, 13, tzinfo=pytz.UTC),
            ),
            Candle(
                asset=Asset(symbol="BTC/USDT", exchange="BINANCE"),
                open=7935.54,
                high=7954.99,
                low=7930.09,
                close=7945.67,
                volume=39.595335,
                timestamp=datetime(2018, 4, 13, 0, 1, tzinfo=pytz.UTC),
            ),
            Candle(
                asset=Asset(symbol="BTC/USDT", exchange="BINANCE"),
                open=7950.0,
                high=7954.98,
                low=7946.0,
                close=7948.0,
                volume=28.717295,
                timestamp=datetime(2018, 4, 13, 0, 2, tzinfo=pytz.UTC),
            ),
            Candle(
                asset=Asset(symbol="BTC/USDT", exchange="BINANCE"),
                open=7950.26,
                high=7959.72,
                low=7950.0,
                close=7957.0,
                volume=56.889722,
                timestamp=datetime(2018, 4, 13, 0, 3, tzinfo=pytz.UTC),
            ),
            Candle(
                asset=Asset(symbol="BTC/USDT", exchange="BINANCE"),
                open=7957.0,
                high=7979.0,
                low=7942.35,
                close=7978.89,
                volume=75.47576,
                timestamp=datetime(2018, 4, 13, 0, 4, tzinfo=pytz.UTC),
            ),
        ],
        dtype=Candle,
    )
    assert (
        expected_candles == BinanceLiveDataHandler.parse_ticker_latest_data(ASSET, data)
    ).all()


@freeze_time("2018-04-14 00:00:00+00:00")
def test_generate_ticker_latest_data_points_url():
    BinanceLiveDataHandler.API_TOKEN = TOKEN
    expected_url = (
        "https://api.binance.com/api/v3/klines?"
        "symbol=BTCUSDT&"
        "interval=1m&"
        "startTime=1523662200000&"
        "endTime=1523664000000&"
        "limit=1000"
    )
    assert expected_url == BinanceLiveDataHandler.generate_ticker_latest_data_url(
        SYMBOL
    )
