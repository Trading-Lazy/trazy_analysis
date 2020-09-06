from decimal import Decimal

import pandas as pd
from freezegun import freeze_time

from market_data.live.tiingo_live_data_handler import TiingoLiveDataHandler
from models.candle import Candle

SYMBOL = "AAPL"
TOKEN = "abcde"


def test_parse_ticker_latest_data_points():
    data = (
        "date,open,high,low,close,volume\n"
        "2020-06-17 09:30:00-04:00,355.15,355.15,353.74,353.84,3254\n"
        "2020-06-17 09:31:00-04:00,354.28,354.96,353.96,354.78,2324\n"
        "2020-06-17 09:32:00-04:00,354.92,355.32,354.09,354.09,1123\n"
        "2020-06-17 09:33:00-04:00,354.25,354.59,354.14,354.59,2613\n"
        "2020-06-17 09:34:00-04:00,354.22,354.26,353.95,353.98,1186\n"
    )
    expected_candles = [
        Candle(
            symbol="AAPL",
            open=Decimal("355.15"),
            high=Decimal("355.15"),
            low=Decimal("353.74"),
            close=Decimal("353.84"),
            volume=3254,
            timestamp=pd.Timestamp("2020-06-17 13:30:00+00:00"),
        ),
        Candle(
            symbol="AAPL",
            open=Decimal("354.28"),
            high=Decimal("354.96"),
            low=Decimal("353.96"),
            close=Decimal("354.78"),
            volume=2324,
            timestamp=pd.Timestamp("2020-06-17 13:31:00+00:00"),
        ),
        Candle(
            symbol="AAPL",
            open=Decimal("354.92"),
            high=Decimal("355.32"),
            low=Decimal("354.09"),
            close=Decimal("354.09"),
            volume=1123,
            timestamp=pd.Timestamp("2020-06-17 13:32:00+00:00"),
        ),
        Candle(
            symbol="AAPL",
            open=Decimal("354.25"),
            high=Decimal("354.59"),
            low=Decimal("354.14"),
            close=Decimal("354.59"),
            volume=2613,
            timestamp=pd.Timestamp("2020-06-17 13:33:00+00:00"),
        ),
        Candle(
            symbol="AAPL",
            open=Decimal("354.22"),
            high=Decimal("354.26"),
            low=Decimal("353.95"),
            close=Decimal("353.98"),
            volume=1186,
            timestamp=pd.Timestamp("2020-06-17 13:34:00+00:00"),
        ),
    ]
    assert expected_candles == TiingoLiveDataHandler.parse_ticker_latest_data(
        SYMBOL, data
    )


@freeze_time("2020-06-18")
def test_generate_ticker_latest_data_points_url():
    TiingoLiveDataHandler.API_TOKEN = TOKEN
    expected_url = (
        "https://api.tiingo.com/iex/AAPL/prices?"
        "startDate=2020-06-18&"
        "endDate=2020-06-18&"
        "format=csv&columns=open,high,low,close,volume&"
        "resampleFreq=1min&"
        "token=abcde"
    )
    assert expected_url == TiingoLiveDataHandler.generate_ticker_latest_data_url(SYMBOL)
