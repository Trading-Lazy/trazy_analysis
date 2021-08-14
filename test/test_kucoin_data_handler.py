import json

import pandas as pd

from trazy_analysis.market_data.historical.kucoin_historical_data_handler import (
    KucoinHistoricalDataHandler,
)
from trazy_analysis.models.asset import Asset

SYMBOL = "BTC-USDT"
EXCHANGE = "BINANCE"
ASSET = Asset(symbol=SYMBOL, exchange=EXCHANGE)


def test_parse_get_tickers_response():
    tickers_response = json.dumps(
        {
            "code": "200000",
            "data": [
                {
                    "symbol": "ETH-BTC",
                    "name": "ETH-BTC",
                    "baseCurrency": "ETH",
                    "quoteCurrency": "BTC",
                    "feeCurrency": "BTC",
                    "market": "BTC",
                    "baseMinSize": "0.0001",
                    "quoteMinSize": "0.00001",
                    "baseMaxSize": "10000000000",
                    "quoteMaxSize": "999999999",
                    "baseIncrement": "0.0000001",
                    "quoteIncrement": "0.00000001",
                    "priceIncrement": "0.000001",
                    "priceLimitRate": "0.1",
                    "isMarginEnabled": True,
                    "enableTrading": True,
                },
                {
                    "symbol": "LTC-BTC",
                    "name": "LTC-BTC",
                    "baseCurrency": "LTC",
                    "quoteCurrency": "BTC",
                    "feeCurrency": "BTC",
                    "market": "BTC",
                    "baseMinSize": "0.001",
                    "quoteMinSize": "0.00001",
                    "baseMaxSize": "10000000000",
                    "quoteMaxSize": "99999999",
                    "baseIncrement": "0.000001",
                    "quoteIncrement": "0.00000001",
                    "priceIncrement": "0.000001",
                    "priceLimitRate": "0.1",
                    "isMarginEnabled": True,
                    "enableTrading": True,
                },
            ],
        }
    )
    expected_parsed_response = ["ETHBTC", "LTCBTC"]
    assert (
        KucoinHistoricalDataHandler.parse_get_tickers_response(tickers_response)
        == expected_parsed_response
    )


def test_ticker_data_is_none():
    data = json.dumps({"code": "200000", "data": []})
    assert KucoinHistoricalDataHandler.ticker_data_is_none(data)


def test_ticker_data_to_dataframe():
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
    df = KucoinHistoricalDataHandler.ticker_data_to_dataframe(ASSET, data)

    expected_df_columns_values = {
        "date": [
            "2020-05-19 23:09:00+00:00",
            "2020-05-19 23:10:00+00:00",
            "2020-05-19 23:11:00+00:00",
            "2020-05-19 23:12:00+00:00",
            "2020-05-19 23:13:00+00:00",
        ],
        "open": ["9734.9", "9731.1", "9734.0", "9728.9", "9724.1"],
        "high": ["9736.0", "9735.9", "9734.0", "9728.9", "9725.0"],
        "low": ["9730.5", "9731.1", "9729.7", "9723.4", "9724.0"],
        "close": ["9731.7", "9734.0", "9729.7", "9724.1", "9724.9"],
        "volume": [
            4000.27909583,
            12815.907322113,
            12135.843512373,
            7734.768300982,
            1530.276009862,
        ],
    }
    expected_df = pd.DataFrame(
        expected_df_columns_values,
        columns=["date", "open", "high", "low", "close", "volume"],
    )
    expected_df.index = pd.to_datetime(expected_df.date)
    expected_df = expected_df.drop(["date"], axis=1)
    assert (df == expected_df).all(axis=None)


# def test_queries():
#     KucoinHistoricalDataHandler.save_ticker_data_in_csv(
#         "BTCUSDT", "test/data/btcusdt.csv", start=datetime(2021, 3, 23, 0, 0, 0, tzinfo=pytz.UTC)
#     )
