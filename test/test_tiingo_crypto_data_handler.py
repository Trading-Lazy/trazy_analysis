import json

import pandas as pd

from market_data.historical.tiingo_historical_crypto_data_handler import (
    TiingoHistoricalCryptoDataHandler,
)

SYMBOL = "IVV"


def test_parse_get_tickers_response():
    tickers_response = json.dumps(
        {
            "timezone": "UTC",
            "serverTime": 1614031563159,
            "rateLimits": [
                {
                    "rateLimitType": "REQUEST_WEIGHT",
                    "interval": "MINUTE",
                    "intervalNum": 1,
                    "limit": 1200,
                },
                {
                    "rateLimitType": "ORDERS",
                    "interval": "SECOND",
                    "intervalNum": 10,
                    "limit": 100,
                },
                {
                    "rateLimitType": "ORDERS",
                    "interval": "DAY",
                    "intervalNum": 1,
                    "limit": 200000,
                },
            ],
            "exchangeFilters": [],
            "symbols": [
                {
                    "symbol": "ETHBTC",
                    "status": "TRADING",
                    "baseAsset": "ETH",
                    "baseAssetPrecision": 8,
                    "quoteAsset": "BTC",
                    "quotePrecision": 8,
                    "quoteAssetPrecision": 8,
                    "baseCommissionPrecision": 8,
                    "quoteCommissionPrecision": 8,
                    "orderTypes": [
                        "LIMIT",
                        "LIMIT_MAKER",
                        "MARKET",
                        "STOP_LOSS_LIMIT",
                        "TAKE_PROFIT_LIMIT",
                    ],
                    "icebergAllowed": True,
                    "ocoAllowed": True,
                    "quoteOrderQtyMarketAllowed": True,
                    "isSpotTradingAllowed": True,
                    "isMarginTradingAllowed": True,
                    "filters": [
                        {
                            "filterType": "PRICE_FILTER",
                            "minPrice": "0.00000100",
                            "maxPrice": "100000.00000000",
                            "tickSize": "0.00000100",
                        },
                        {
                            "filterType": "PERCENT_PRICE",
                            "multiplierUp": "5",
                            "multiplierDown": "0.2",
                            "avgPriceMins": 5,
                        },
                        {
                            "filterType": "LOT_SIZE",
                            "minQty": "0.00100000",
                            "maxQty": "100000.00000000",
                            "stepSize": "0.00100000",
                        },
                        {
                            "filterType": "MIN_NOTIONAL",
                            "minNotional": "0.00010000",
                            "applyToMarket": True,
                            "avgPriceMins": 5,
                        },
                        {"filterType": "ICEBERG_PARTS", "limit": 10},
                        {
                            "filterType": "MARKET_LOT_SIZE",
                            "minQty": "0.00000000",
                            "maxQty": "2224.64501250",
                            "stepSize": "0.00000000",
                        },
                        {"filterType": "MAX_NUM_ORDERS", "maxNumOrders": 200},
                        {"filterType": "MAX_NUM_ALGO_ORDERS", "maxNumAlgoOrders": 5},
                    ],
                    "permissions": ["SPOT", "MARGIN"],
                },
                {
                    "symbol": "LTCBTC",
                    "status": "TRADING",
                    "baseAsset": "LTC",
                    "baseAssetPrecision": 8,
                    "quoteAsset": "BTC",
                    "quotePrecision": 8,
                    "quoteAssetPrecision": 8,
                    "baseCommissionPrecision": 8,
                    "quoteCommissionPrecision": 8,
                    "orderTypes": [
                        "LIMIT",
                        "LIMIT_MAKER",
                        "MARKET",
                        "STOP_LOSS_LIMIT",
                        "TAKE_PROFIT_LIMIT",
                    ],
                    "icebergAllowed": True,
                    "ocoAllowed": True,
                    "quoteOrderQtyMarketAllowed": True,
                    "isSpotTradingAllowed": True,
                    "isMarginTradingAllowed": True,
                    "filters": [
                        {
                            "filterType": "PRICE_FILTER",
                            "minPrice": "0.00000100",
                            "maxPrice": "100000.00000000",
                            "tickSize": "0.00000100",
                        },
                        {
                            "filterType": "PERCENT_PRICE",
                            "multiplierUp": "5",
                            "multiplierDown": "0.2",
                            "avgPriceMins": 5,
                        },
                        {
                            "filterType": "LOT_SIZE",
                            "minQty": "0.01000000",
                            "maxQty": "100000.00000000",
                            "stepSize": "0.01000000",
                        },
                        {
                            "filterType": "MIN_NOTIONAL",
                            "minNotional": "0.00010000",
                            "applyToMarket": True,
                            "avgPriceMins": 5,
                        },
                        {"filterType": "ICEBERG_PARTS", "limit": 10},
                        {
                            "filterType": "MARKET_LOT_SIZE",
                            "minQty": "0.00000000",
                            "maxQty": "9497.61790972",
                            "stepSize": "0.00000000",
                        },
                        {"filterType": "MAX_NUM_ORDERS", "maxNumOrders": 200},
                        {"filterType": "MAX_NUM_ALGO_ORDERS", "maxNumAlgoOrders": 5},
                    ],
                    "permissions": ["SPOT", "MARGIN"],
                },
            ],
        }
    )
    expected_parsed_response = ["ETHBTC", "LTCBTC"]
    assert (
        TiingoHistoricalCryptoDataHandler.parse_get_tickers_response(tickers_response)
        == expected_parsed_response
    )


def test_ticker_data_is_none():
    data = json.dumps([])
    assert TiingoHistoricalCryptoDataHandler.ticker_data_is_none(data)
    data = json.dumps(
        [
            {
                "ticker": "btcusd",
                "baseCurrency": "btc",
                "quoteCurrency": "usd",
                "priceData": [],
            }
        ]
    )
    assert TiingoHistoricalCryptoDataHandler.ticker_data_is_none(data)


def test_ticker_data_to_dataframe():
    data = json.dumps(
        [
            {
                "ticker": "btcusd",
                "baseCurrency": "btc",
                "quoteCurrency": "usd",
                "priceData": [
                    {
                        "low": 17041.08813324084,
                        "open": 17042.224796462127,
                        "close": 17067.0207267784,
                        "tradesDone": 283.0,
                        "date": "2017-12-13T00:00:00+00:00",
                        "volume": 44.99116858,
                        "high": 17068.325134477574,
                        "volumeNotional": 767865.206676841,
                    },
                    {
                        "low": 16898.50928968564,
                        "open": 16912.399791210508,
                        "close": 16900.749140139273,
                        "tradesDone": 414.0,
                        "date": "2017-12-13T00:01:00+00:00",
                        "volume": 85.58077693,
                        "high": 16935.48154468411,
                        "volumeNotional": 1446379.2421121483,
                    },
                    {
                        "low": 17032.290027982737,
                        "open": 17035.623180823583,
                        "close": 17059.25902662951,
                        "tradesDone": 267.0,
                        "date": "2017-12-13T00:02:00+00:00",
                        "volume": 54.42578747,
                        "high": 17060.86908710398,
                        "volumeNotional": 928463.6061790168,
                    },
                    {
                        "low": 16974.70938582292,
                        "open": 16978.25334442074,
                        "close": 16995.146302196226,
                        "tradesDone": 319.0,
                        "date": "2017-12-13T00:03:00+00:00",
                        "volume": 112.06246923,
                        "high": 17011.583797102718,
                        "volumeNotional": 1904518.059549213,
                    },
                    {
                        "low": 17044.220843317977,
                        "open": 17112.281365604864,
                        "close": 17134.611038665582,
                        "tradesDone": 718.0,
                        "date": "2017-12-13T00:04:00+00:00",
                        "volume": 253.65318281000003,
                        "high": 17278.87984139109,
                        "volumeNotional": 4346248.626168885,
                    },
                    {
                        "low": 17234.716552011258,
                        "open": 17238.0199449088,
                        "close": 17300.679665902073,
                        "tradesDone": 451.0,
                        "date": "2017-12-13T00:05:00+00:00",
                        "volume": 69.91745618999998,
                        "high": 17324.83886467445,
                        "volumeNotional": 1209619.5125979318,
                    },
                ],
            }
        ]
    )
    df = TiingoHistoricalCryptoDataHandler.ticker_data_to_dataframe(SYMBOL, data)
    expected_df_columns_values = {
        "timestamp": [
            "2017-12-13T00:00:00+00:00",
            "2017-12-13T00:01:00+00:00",
            "2017-12-13T00:02:00+00:00",
            "2017-12-13T00:03:00+00:00",
            "2017-12-13T00:04:00+00:00",
            "2017-12-13T00:05:00+00:00",
        ],
        "open": [
            "17042.224796462127",
            "16912.399791210508",
            "17035.623180823583",
            "16978.25334442074",
            "17112.281365604864",
            "17238.0199449088",
        ],
        "high": [
            "17068.325134477574",
            "16935.48154468411",
            "17060.86908710398",
            "17011.583797102718",
            "17278.87984139109",
            "17324.83886467445",
        ],
        "low": [
            "17041.08813324084",
            "16898.50928968564",
            "17032.290027982737",
            "16974.70938582292",
            "17044.220843317977",
            "17234.716552011258",
        ],
        "close": [
            "17067.0207267784",
            "16900.749140139273",
            "17059.25902662951",
            "16995.146302196226",
            "17134.611038665582",
            "17300.679665902073",
        ],
        "volume": [
            44.99116858,
            85.58077693,
            54.42578747,
            112.06246923,
            253.65318281000003,
            69.91745618999998,
        ],
    }
    expected_df = pd.DataFrame(
        expected_df_columns_values,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    expected_df.index = pd.to_datetime(expected_df.timestamp)
    expected_df = expected_df.drop(["timestamp"], axis=1)
    assert (df == expected_df).all(axis=None)
