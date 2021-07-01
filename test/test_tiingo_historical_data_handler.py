from datetime import date

from market_data.historical.tiingo_historical_data_handler import (
    TiingoHistoricalDataHandler,
)
from models.asset import Asset


def test_generate_ticker_data_url():
    ticker = Asset(symbol="aapl", exchange="IEX")
    period = (date(1996, 4, 13), date(1996, 5, 13))
    TiingoHistoricalDataHandler.API_TOKEN = "abcde"
    expected_url = (
        "https://api.tiingo.com/iex/aapl/prices?"
        "startDate=1996-04-13&"
        "endDate=1996-05-13&"
        "format=csv&columns=open,high,low,close,volume&"
        "resampleFreq=1min&"
        "token=abcde"
    )
    assert expected_url == TiingoHistoricalDataHandler.generate_ticker_data_url(
        ticker, period
    )
