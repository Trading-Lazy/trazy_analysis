import json
from datetime import datetime

import numpy as np
import pytz
from freezegun import freeze_time

from trazy_analysis.market_data.live.kucoin_live_data_handler import (
    KucoinLiveDataHandler,
)
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle

SYMBOL = "BTC/USDT"
EXCHANGE = "IEX"
ASSET = Asset(symbol=SYMBOL, exchange=EXCHANGE)
TOKEN = "abcde"


def test_parse_ticker_latest_data_points():
    data = json.dumps(
        {
            "code": "200000",
            "data": [
                [
                    "1589929980",
                    "9724.1",
                    "9724.9",
                    "9725",
                    "9724",
                    "0.15736457",
                    "1530.276009862",
                ],
                [
                    "1589929920",
                    "9728.9",
                    "9724.1",
                    "9728.9",
                    "9723.4",
                    "0.79528422",
                    "7734.768300982",
                ],
                [
                    "1589929860",
                    "9734",
                    "9729.7",
                    "9734",
                    "9729.7",
                    "1.24719652",
                    "12135.843512373",
                ],
                [
                    "1589929800",
                    "9731.1",
                    "9734",
                    "9735.9",
                    "9731.1",
                    "1.31664004",
                    "12815.907322113",
                ],
                [
                    "1589929740",
                    "9734.9",
                    "9731.7",
                    "9736",
                    "9730.5",
                    "0.41095301",
                    "4000.27909583",
                ],
            ],
        }
    )
    expected_candles = np.array(
        [
            Candle(asset=ASSET, open=9734.9, high=9736.0, low=9730.5, close=9731.7, volume=4000.27909583,
                   timestamp=datetime(2020, 5, 19, 23, 9, 0, tzinfo=pytz.UTC)),
            Candle(asset=ASSET, open=9731.1, high=9735.9, low=9731.1, close=9734.0, volume=12815.907322113,
                   timestamp=datetime(2020, 5, 19, 23, 10, 0, tzinfo=pytz.UTC)),
            Candle(asset=ASSET, open=9734.0, high=9734.0, low=9729.7, close=9729.7, volume=12135.843512373,
                   timestamp=datetime(2020, 5, 19, 23, 11, 0, tzinfo=pytz.UTC)),
            Candle(asset=ASSET, open=9728.9, high=9728.9, low=9723.4, close=9724.1, volume=7734.768300982,
                   timestamp=datetime(2020, 5, 19, 23, 12, 0, tzinfo=pytz.UTC)),
            Candle(asset=ASSET, open=9724.1, high=9725.0, low=9724.0, close=9724.9, volume=1530.276009862,
                   timestamp=datetime(2020, 5, 19, 23, 13, 0, tzinfo=pytz.UTC)),
        ],
        dtype=Candle,
    )
    assert (
        expected_candles == KucoinLiveDataHandler.parse_ticker_latest_data(ASSET, data)
    ).all()


@freeze_time("2021-04-14 00:00:00+00:00")
def test_generate_ticker_latest_data_points_url():
    expected_url = (
        "https://api.kucoin.com/api/v1/market/candles?"
        "symbol=BTC-USDT&"
        "type=1min&"
        "startAt=1618356600&"
        "endAt=1618358400"
    )
    assert expected_url == KucoinLiveDataHandler.generate_ticker_latest_data_url(ASSET)
