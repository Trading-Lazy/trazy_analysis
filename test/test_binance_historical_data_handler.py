from datetime import datetime

import pytz

from trazy_analysis.market_data.historical.binance_historical_data_handler import (
    BinanceHistoricalDataHandler,
)
from trazy_analysis.models.asset import Asset


def test_generate_ticker_data_url():
    ticker = Asset(symbol="XRPEUR", exchange="BINANCE")
    period = (
        datetime(2018, 4, 13, tzinfo=pytz.UTC),
        datetime(2018, 4, 13, 23, 59, 59, tzinfo=pytz.UTC),
    )
    BinanceHistoricalDataHandler.API_TOKEN = "abcde"
    expected_url = (
        "https://api.binance.com/api/v3/klines?"
        "symbol=XRPEUR&"
        "interval=1m&"
        "startTime=1523577600000&"
        "endTime=1523663999000&"
        "limit=1000"
    )
    assert expected_url == BinanceHistoricalDataHandler.generate_ticker_data_url(
        ticker, period
    )
