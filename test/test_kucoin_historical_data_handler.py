from datetime import date

from trazy_analysis.market_data.historical.kucoin_historical_data_handler import (
    KucoinHistoricalDataHandler,
)
from trazy_analysis.models.asset import Asset


def test_generate_ticker_data_url():
    ticker = Asset(symbol="XRP/USDT", exchange="BINANCE")
    period = (
        date(2018, 4, 13),
        date(2018, 4, 13),
    )
    expected_url = (
        "https://api.kucoin.com/api/v1/market/candles?"
        "symbol=XRP-USDT&"
        "type=1min&"
        "startAt=1523577600&"
        "endAt=1523663999"
    )
    assert expected_url == KucoinHistoricalDataHandler.generate_ticker_data_url(
        ticker, period
    )
