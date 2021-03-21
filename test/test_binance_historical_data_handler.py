from datetime import date, datetime, timezone

from market_data.historical.binance_historical_data_handler import (
    BinanceHistoricalDataHandler,
)


def test_generate_ticker_data_url():
    ticker = "XRPEUR"
    period = (
        datetime(2018, 4, 13, tzinfo=timezone.utc),
        datetime(2018, 4, 13, 23, 59, 59, tzinfo=timezone.utc),
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
