import pandas as pd

from trazy_analysis.market_data.historical.tiingo_historical_data_handler import (
    TiingoHistoricalDataHandler,
)
from trazy_analysis.models.asset import Asset

ASSET = Asset(exchange="IEX", symbol="IVV")


def test_parse_get_tickers_response():
    tickers_response = """[
    {
        "last": 86.22,
        "bidPrice": 0.0,
        "quoteTimestamp": "2020-06-26T12:00:08.031020218-04:00",
        "mid": null,
        "high": 87.52,
        "timestamp": "2020-06-26T12:07:42.694741256-04:00",
        "tngoLast": 86.22,
        "lastSize": 2,
        "open": 87.23,
        "askSize": 100,
        "ticker": "AAPL",
        "askPrice": 93.9,
        "low": 85.98,
        "volume": 12021,
        "prevClose": 87.26,
        "bidSize": 0,
        "lastSaleTimestamp": "2020-06-26T12:07:42.694741256-04:00"
    },
    {
        "last": 11.16,
        "bidPrice": 10.95,
        "quoteTimestamp": "2020-06-26T12:07:56.271171994-04:00",
        "mid": 11.94,
        "high": 11.65,
        "timestamp": "2020-06-26T12:07:56.271171994-04:00",
        "tngoLast": 11.16,
        "lastSize": 100,
        "open": 11.65,
        "askSize": 1000,
        "ticker": "GOOGL",
        "askPrice": 12.93,
        "low": 11.015,
        "volume": 106111,
        "prevClose": 11.83,
        "bidSize": 100,
        "lastSaleTimestamp": "2020-06-26T12:07:56.212498617-04:00"
    },
    {
        "last": 0.055,
        "bidPrice": null,
        "quoteTimestamp": "2020-06-25T20:00:00+00:00",
        "mid": null,
        "open": 0.055,
        "timestamp": "2020-06-25T20:00:00+00:00",
        "tngoLast": 0.055,
        "lastSize": null,
        "askSize": null,
        "ticker": "AMZN",
        "askPrice": null,
        "low": 0.055,
        "volume": 0,
        "prevClose": 0.055,
        "bidSize": null,
        "lastSaleTimestamp": "2020-06-25T20:00:00+00:00",
        "high": 0.055
    }]"""
    expected_parsed_response = ["AAPL", "GOOGL", "AMZN"]
    assert (
        TiingoHistoricalDataHandler.parse_get_tickers_response(tickers_response)
        == expected_parsed_response
    )


def test_ticker_data_is_none():
    data = ",open,high,low,close,volume"
    assert TiingoHistoricalDataHandler.ticker_data_is_none(data)


def test_ticker_data_to_dataframe():
    data = (
        "date,open,high,low,close,volume\n"
        "2020-06-17 09:30:00-04:00,355.15,355.15,353.74,353.84,3254\n"
        "2020-06-17 09:31:00-04:00,354.28,354.96,353.96,354.78,2324\n"
        "2020-06-17 09:32:00-04:00,354.92,355.32,354.09,354.09,1123\n"
        "2020-06-17 09:33:00-04:00,354.25,354.59,354.14,354.59,2613\n"
        "2020-06-17 09:34:00-04:00,354.22,354.26,353.95,353.98,1186\n"
    )
    df = TiingoHistoricalDataHandler.ticker_data_to_dataframe(ASSET, data)

    expected_df_columns_values = {
        "date": [
            "2020-06-17 13:30:00+00:00",
            "2020-06-17 13:31:00+00:00",
            "2020-06-17 13:32:00+00:00",
            "2020-06-17 13:33:00+00:00",
            "2020-06-17 13:34:00+00:00",
        ],
        "open": ["355.15", "354.28", "354.92", "354.25", "354.22"],
        "high": ["355.15", "354.96", "355.32", "354.59", "354.26"],
        "low": ["353.74", "353.96", "354.09", "354.14", "353.95"],
        "close": ["353.84", "354.78", "354.09", "354.59", "353.98"],
        "volume": [3254, 2324, 1123, 2613, 1186],
    }
    expected_df = pd.DataFrame(
        expected_df_columns_values,
        columns=["date", "open", "high", "low", "close", "volume"],
    )
    expected_df.index = pd.to_datetime(expected_df.date)
    expected_df = expected_df.drop(["date"], axis=1)
    assert (df == expected_df).all(axis=None)
