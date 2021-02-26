from datetime import date

from market_data.historical.tiingo_historical_crypto_data_handler import (
    TiingoHistoricalCryptoDataHandler,
)


def test_generate_ticker_data_url():
    ticker = "btcusd"
    period = (date(1996, 4, 13), date(1996, 5, 13))
    TiingoHistoricalCryptoDataHandler.API_TOKEN = "abcde"
    expected_url = (
        "https://api.tiingo.com/tiingo/crypto/prices?tickers=btcusd&"
        "startDate=1996-04-13&"
        "endDate=1996-05-13&"
        "resampleFreq=1min&"
        "token=abcde"
    )
    assert expected_url == TiingoHistoricalCryptoDataHandler.generate_ticker_data_url(
        ticker, period
    )
