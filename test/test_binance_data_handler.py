import json

import pandas as pd

from market_data.historical.binance_historical_data_handler import (
    BinanceHistoricalDataHandler,
)
from market_data.historical.tiingo_historical_crypto_data_handler import (
    TiingoHistoricalCryptoDataHandler,
)

SYMBOL = "BTCUSDT"


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
    data = "[]"
    assert BinanceHistoricalDataHandler.ticker_data_is_none(data)


def test_ticker_data_to_dataframe():
    data = json.dumps(
        [
            [
                1523577600000,
                "7922.99000000",
                "7936.99000000",
                "7919.84000000",
                "7935.61000000",
                "53.68618000",
                1523577659999,
                "425682.91899601",
                230,
                "45.80239200",
                "363185.75390966",
                "0",
            ],
            [
                1523577660000,
                "7935.54000000",
                "7954.99000000",
                "7930.09000000",
                "7945.67000000",
                "39.59533500",
                1523577719999,
                "314494.40146720",
                274,
                "35.04922700",
                "278402.89612295",
                "0",
            ],
            [
                1523577720000,
                "7950.00000000",
                "7954.98000000",
                "7946.00000000",
                "7948.00000000",
                "28.71729500",
                1523577779999,
                "228316.83190434",
                195,
                "18.86619400",
                "149992.40120906",
                "0",
            ],
            [
                1523577780000,
                "7950.26000000",
                "7959.72000000",
                "7950.00000000",
                "7957.00000000",
                "56.88972200",
                1523577839999,
                "452590.86434942",
                245,
                "27.29523700",
                "217157.17977072",
                "0",
            ],
            [
                1523577840000,
                "7957.00000000",
                "7979.00000000",
                "7942.35000000",
                "7978.89000000",
                "75.47576000",
                1523577899999,
                "600929.81756139",
                374,
                "49.81860800",
                "396780.28925758",
                "0",
            ],
        ]
    )
    df = BinanceHistoricalDataHandler.ticker_data_to_dataframe(SYMBOL, data)

    expected_df_columns_values = {
        "date": [
            "2018-04-13 00:00:00+00:00",
            "2018-04-13 00:01:00+00:00",
            "2018-04-13 00:02:00+00:00",
            "2018-04-13 00:03:00+00:00",
            "2018-04-13 00:04:00+00:00",
        ],
        "open": ["7922.99", "7935.54", "7950.0", "7950.26", "7957.0"],
        "high": ["7936.99", "7954.99", "7954.98", "7959.72", "7979.0"],
        "low": ["7919.84", "7930.09", "7946.0", "7950.0", "7942.35"],
        "close": ["7935.61", "7945.67", "7948.0", "7957.0", "7978.89"],
        "volume": [53.68618, 39.595335, 28.717295, 56.889722, 75.47576],
    }
    expected_df = pd.DataFrame(
        expected_df_columns_values,
        columns=["date", "open", "high", "low", "close", "volume"],
    )
    expected_df.index = pd.to_datetime(expected_df.date)
    expected_df = expected_df.drop(["date"], axis=1)
    assert (df == expected_df).all(axis=None)
