from datetime import date

import numpy as np
import pandas as pd
from pandas._libs.tslibs.timestamps import Timestamp

from market_data.historical.iex_cloud_historical_data_handler import (
    IexCloudHistoricalDataHandler,
)

SYMBOL = "IVV"


def test_generate_ticker_url():
    ticker = "aapl"
    period = (date(1996, 4, 13), date(1996, 5, 13))
    IexCloudHistoricalDataHandler.API_TOKEN = "abcde"
    expected_url = (
        "https://cloud.iexapis.com/stable/stock/aapl/chart/date/19960413?"
        "format=csv&"
        "token=abcde&"
        "chartIEXOnly=true"
    )
    assert expected_url == IexCloudHistoricalDataHandler.generate_ticker_data_url(
        ticker, period
    )


def test_parse_get_tickers_response():
    tickers_response = """[
    {
        "symbol": "AAPL",
        "date": "2020-06-22",
        "isEnabled": true
    },
    {
        "symbol": "GOOGL",
        "date": "2020-06-22",
        "isEnabled": true
    },
    {
        "symbol": "AMZN",
        "date": "2020-06-22",
        "isEnabled": true
    }]"""
    expected_parsed_response = np.array(["AAPL", "GOOGL", "AMZN"], dtype="U6")
    assert (
        IexCloudHistoricalDataHandler.parse_get_tickers_response(tickers_response)
        == expected_parsed_response
    ).all()


def test_ticker_data_is_none():
    data = ""
    assert IexCloudHistoricalDataHandler.ticker_data_is_none(data)


def test_ticker_data_to_dataframe():
    data = (
        "date,minute,label,high,low,open,close,average,volume,notional,numberOfTrades,symbol\n"
        "2019-06-17,09:30,09:30 AM,192.94,192.6,192.855,192.83,192.804,1362,262599.24,19,AAPL\n"
        "2019-06-17,09:31,09:31 AM,193.27,192.89,192.94,192.9,193.121,2345,452869.29,31,AAPL\n"
        "2019-06-17,09:32,09:32 AM,192.6,192.3,192.6,192.3,192.519,1350,259901,15,AAPL\n"
        "2019-06-17,09:33,09:33 AM,192.46,192.22,192.22,192.29,192.372,756,145432.96,11,AAPL\n"
        "2019-06-17,09:34,09:34 AM,192.89,192.32,192.67,192.89,192.596,1660,319709.6,19,AAPL"
    )
    df = IexCloudHistoricalDataHandler.ticker_data_to_dataframe(SYMBOL, data)

    expected_df_columns_values = {
        "date": [
            Timestamp("2019-06-17 13:30:00+00:00"),
            Timestamp("2019-06-17 13:31:00+00:00"),
            Timestamp("2019-06-17 13:32:00+00:00"),
            Timestamp("2019-06-17 13:33:00+00:00"),
            Timestamp("2019-06-17 13:34:00+00:00"),
        ],
        "open": ["192.855", "192.94", "192.6", "192.22", "192.67"],
        "high": ["192.94", "193.27", "192.6", "192.46", "192.89"],
        "low": ["192.6", "192.89", "192.3", "192.22", "192.32"],
        "close": ["192.83", "192.9", "192.3", "192.29", "192.89"],
        "volume": [1362, 2345, 1350, 756, 1660],
    }
    expected_df = pd.DataFrame(
        expected_df_columns_values,
        columns=["date", "open", "high", "low", "close", "volume"],
    )
    expected_df.index = pd.to_datetime(expected_df.date)
    expected_df = expected_df.drop(["date"], axis=1)
    assert (df == expected_df).all(axis=None)
