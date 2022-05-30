from collections import deque
from datetime import datetime
from decimal import Decimal
from unittest.mock import call, patch

import pytest
from freezegun import freeze_time
from pytz import timezone

from trazy_analysis.broker.binance_fee_model import BinanceFeeModel
from trazy_analysis.broker.ccxt_broker import CcxtBroker
from trazy_analysis.broker.ccxt_parser import CcxtBinanceParser
from trazy_analysis.common.ccxt_connector import CcxtConnector
from trazy_analysis.common.clock import LiveClock
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.enums import Action, Direction, OrderType
from trazy_analysis.models.order import Order
from trazy_analysis.portfolio.portfolio_event import PortfolioEvent
from trazy_analysis.position.position import Position

INITIAL_CASH = "51.07118"

EXCHANGE = "binance"
SYMBOL1 = "ETH/EUR"
ASSET1 = Asset(symbol=SYMBOL1, exchange=EXCHANGE)
SYMBOL2 = "XRP/EUR"
ASSET2 = Asset(symbol=SYMBOL2, exchange=EXCHANGE)
SYMBOL3 = "SXP/EUR"
ASSET3 = Asset(symbol=SYMBOL3, exchange=EXCHANGE)

ORDER_ID1 = "c8741cfa-6170-42c9-b952-e915bc614b36"
ORDER_ID2 = "d52d385d-f346-4f0f-88cb-6e845b5dfa75"
ORDER_ID3 = "4f3ff98b-89b5-41d5-ad65-ecad0624053c"

ORDERS_RETURN_VALUE = [
    {
        "symbol": "XRPEUR",
        "orderId": 131247881,
        "orderListId": -1,
        "clientOrderId": "web_33927a4da37b4b12aa42092d274ed008",
        "price": "0.00000000",
        "origQty": "29.50000000",
        "executedQty": "29.50000000",
        "cummulativeQuoteQty": "9.97749000",
        "status": "FILLED",
        "timeInForce": "GTC",
        "type": "MARKET",
        "side": "BUY",
        "stopPrice": "0.00000000",
        "icebergQty": "0.00000000",
        "time": 1614080757873,
        "updateTime": 1614080757873,
        "isWorking": True,
        "origQuoteOrderQty": "10.00000000",
    }
]

TIMESTAMP = datetime.strptime("2021-02-08 16:01:14+0000", "%Y-%m-%d %H:%M:%S%z")

FETCH_BALANCE_RETURN_VALUE = {
    "info": {
        "makerCommission": "10",
        "takerCommission": "10",
        "buyerCommission": "0",
        "sellerCommission": "0",
        "canTrade": True,
        "canWithdraw": True,
        "canDeposit": True,
        "updateTime": "1628724393245",
        "accountType": "SPOT",
        "balances": [
            {"asset": "ETH", "free": "0.15067000", "locked": "0.00000000"},
            {"asset": "EUR", "free": INITIAL_CASH, "locked": "0.00000000"},
            {"asset": "SXP", "free": "0.00000000", "locked": "0.00000000"},
            {"asset": "BTC", "free": "0.00000000", "locked": "0.00000000"},
            {"asset": "LINK", "free": "0.00000000", "locked": "0.00000000"},
            {"asset": "XRP", "free": "-29.47050000", "locked": "0.00000000"},
        ],
        "permissions": ["SPOT"],
    }
}

GET_ACCOUNT_RETURN_VALUE_CRYPTO_DUST = {
    "makerCommission": 10,
    "takerCommission": 10,
    "buyerCommission": 0,
    "sellerCommission": 0,
    "canTrade": True,
    "canWithdraw": True,
    "canDeposit": True,
    "updateTime": 1614080757873,
    "accountType": "SPOT",
    "balances": [
        {"asset": "ETH", "free": "0.15067000", "locked": "0.00000000"},
        {"asset": "EUR", "free": INITIAL_CASH, "locked": "0.00000000"},
        {"asset": "SXP", "free": "0.000570000", "locked": "0.00000000"},
        {"asset": "BTC", "free": "0.00000000", "locked": "0.00000000"},
        {"asset": "LINK", "free": "0.00000000", "locked": "0.00000000"},
        {"asset": "XRP", "free": "-29.47050000", "locked": "0.00000000"},
    ],
    "permissions": ["SPOT"],
}

FETCH_TICKERS_RETURN_VALUE = {
    "ETH/EUR": {
        "symbol": "ETH/EUR",
        "timestamp": 1628892381964,
        "datetime": "2021-08-13T22:06:21.964Z",
        "high": 2829.0,
        "low": 2586.6,
        "bid": 2821.26,
        "bidVolume": 0.10923,
        "ask": 2821.86,
        "askVolume": 0.191,
        "vwap": 2761.6289569,
        "open": 2630.61,
        "close": 2821.24,
        "last": 2821.24,
        "previousClose": 2630.61,
        "change": 190.63,
        "percentage": 7.247,
        "average": None,
        "baseVolume": 31735.0375,
        "quoteVolume": 87640398.5082394,
        "info": {
            "symbol": "ETHEUR",
            "priceChange": "190.63000000",
            "priceChangePercent": "7.247",
            "weightedAvgPrice": "2761.62895690",
            "prevClosePrice": "2630.61000000",
            "lastPrice": "1353.12000000",
            "lastQty": "0.02635000",
            "bidPrice": "2821.26000000",
            "bidQty": "0.10923000",
            "askPrice": "2821.86000000",
            "askQty": "0.19100000",
            "openPrice": "2630.61000000",
            "highPrice": "2829.00000000",
            "lowPrice": "2586.60000000",
            "volume": "31735.03750000",
            "quoteVolume": "87640398.50823940",
            "openTime": "1628805981964",
            "closeTime": "1628892381964",
            "firstId": "41617763",
            "lastId": "41733987",
            "count": "116225",
        },
    },
    "SXP/EUR": {
        "symbol": "SXP/EUR",
        "timestamp": 1628892377889,
        "datetime": "2021-08-13T22:06:17.889Z",
        "high": 3.108,
        "low": 2.699,
        "bid": 3.031,
        "bidVolume": 85.139,
        "ask": 3.039,
        "askVolume": 687.687,
        "vwap": 2.99538943,
        "open": 2.78,
        "close": 3.031,
        "last": 3.031,
        "previousClose": 2.787,
        "change": 0.251,
        "percentage": 9.029,
        "average": None,
        "baseVolume": 313219.848,
        "quoteVolume": 938215.42263,
        "info": {
            "symbol": "SXPEUR",
            "priceChange": "0.25100000",
            "priceChangePercent": "9.029",
            "weightedAvgPrice": "2.99538943",
            "prevClosePrice": "2.78700000",
            "lastPrice": "1.90400000",
            "lastQty": "5.69500000",
            "bidPrice": "3.03100000",
            "bidQty": "85.13900000",
            "askPrice": "3.03900000",
            "askQty": "687.68700000",
            "openPrice": "2.78000000",
            "highPrice": "3.10800000",
            "lowPrice": "2.69900000",
            "volume": "313219.84800000",
            "quoteVolume": "938215.42263000",
            "openTime": "1628805977889",
            "closeTime": "1628892377889",
            "firstId": "2151223",
            "lastId": "2154660",
            "count": "3438",
        },
    },
    "BTC/EUR": {
        "symbol": "BTC/EUR",
        "timestamp": 1628892381875,
        "datetime": "2021-08-13T22:06:21.875Z",
        "high": 41130.45,
        "low": 37986.06,
        "bid": 40855.58,
        "bidVolume": 0.01,
        "ask": 40858.93,
        "askVolume": 0.018495,
        "vwap": 39918.83787865,
        "open": 38303.08,
        "close": 40858.93,
        "last": 40858.93,
        "previousClose": 38302.81,
        "change": 2555.85,
        "percentage": 6.673,
        "average": None,
        "baseVolume": 1752.722318,
        "quoteVolume": 69966638.05853376,
        "info": {
            "symbol": "BTCEUR",
            "priceChange": "2555.85000000",
            "priceChangePercent": "6.673",
            "weightedAvgPrice": "39918.83787865",
            "prevClosePrice": "38302.81000000",
            "lastPrice": "40825.94000000",
            "lastQty": "0.00025500",
            "bidPrice": "40855.58000000",
            "bidQty": "0.01000000",
            "askPrice": "40858.93000000",
            "askQty": "0.01849500",
            "openPrice": "38303.08000000",
            "highPrice": "41130.45000000",
            "lowPrice": "37986.06000000",
            "volume": "1752.72231800",
            "quoteVolume": "69966638.05853376",
            "openTime": "1628805981875",
            "closeTime": "1628892381875",
            "firstId": "60363393",
            "lastId": "60446412",
            "count": "83020",
        },
    },
    "LINK/EUR": {
        "symbol": "LINK/EUR",
        "timestamp": 1628892380351,
        "datetime": "2021-08-13T22:06:20.351Z",
        "high": 23.389,
        "low": 20.921,
        "bid": 23.193,
        "bidVolume": 302.37,
        "ask": 23.226,
        "askVolume": 210.0,
        "vwap": 22.73365405,
        "open": 21.282,
        "close": 23.2,
        "last": 23.2,
        "previousClose": 21.321,
        "change": 1.918,
        "percentage": 9.012,
        "average": None,
        "baseVolume": 64651.276,
        "quoteVolume": 1469759.742567,
        "info": {
            "symbol": "LINKEUR",
            "priceChange": "1.91800000",
            "priceChangePercent": "9.012",
            "weightedAvgPrice": "22.73365405",
            "prevClosePrice": "21.32100000",
            "lastPrice": "22.55000000",
            "lastQty": "100.00000000",
            "bidPrice": "23.19300000",
            "bidQty": "302.37000000",
            "askPrice": "23.22600000",
            "askQty": "210.00000000",
            "openPrice": "21.28200000",
            "highPrice": "23.38900000",
            "lowPrice": "20.92100000",
            "volume": "64651.27600000",
            "quoteVolume": "1469759.74256700",
            "openTime": "1628805980351",
            "closeTime": "1628892380351",
            "firstId": "2705052",
            "lastId": "2709469",
            "count": "4418",
        },
    },
    "XRP/EUR": {
        "symbol": "XRP/EUR",
        "timestamp": 1628892382015,
        "datetime": "2021-08-13T22:06:22.015Z",
        "high": 0.9206,
        "low": 0.799,
        "bid": 0.9072,
        "bidVolume": 7983.97,
        "ask": 0.9088,
        "askVolume": 10355.03,
        "vwap": 0.88061526,
        "open": 0.8264,
        "close": 0.9078,
        "last": 0.9078,
        "previousClose": 0.8264,
        "change": 0.0814,
        "percentage": 9.85,
        "average": None,
        "baseVolume": 13665862.51,
        "quoteVolume": 12034367.062063,
        "info": {
            "symbol": "XRPEUR",
            "priceChange": "0.08140000",
            "priceChangePercent": "9.850",
            "weightedAvgPrice": "0.88061526",
            "prevClosePrice": "0.82640000",
            "lastPrice": "0.39100000",
            "lastQty": "3357.79000000",
            "bidPrice": "0.90720000",
            "bidQty": "7983.97000000",
            "askPrice": "0.90880000",
            "askQty": "10355.03000000",
            "openPrice": "0.82640000",
            "highPrice": "0.92060000",
            "lowPrice": "0.79900000",
            "volume": "13665862.51000000",
            "quoteVolume": "12034367.06206300",
            "openTime": "1628805982015",
            "closeTime": "1628892382015",
            "firstId": "14758960",
            "lastId": "14778629",
            "count": "19670",
        },
    },
}

FETCH_MY_TRADES_SIDE_EFFECT = [
    [
        {
            "info": {
                "symbol": "XRPEUR",
                "id": "6160298",
                "orderId": "131247881",
                "orderListId": "-1",
                "price": "0.33822000",
                "qty": "29.50000000",
                "quoteQty": "9.97749000",
                "commission": "0.02950000",
                "commissionAsset": "BNB",
                "time": "1614080757873",
                "isBuyer": True,
                "isMaker": False,
                "isBestMatch": True,
            },
            "timestamp": 1628898947607,
            "datetime": "2021-08-13T23:55:47.607Z",
            "symbol": "XRP/EUR",
            "id": "14782367",
            "order": "325503814",
            "type": None,
            "side": "buy",
            "takerOrMaker": "taker",
            "price": 0.9331,
            "amount": 15.1,
            "cost": 14.08981,
            "fee": {"cost": 3.006e-05, "currency": "BNB"},
        },
        {
            "info": {
                "symbol": "XRPEUR",
                "id": "6217956",
                "orderId": "132141706",
                "orderListId": -1,
                "price": "0.38805000",
                "qty": "29.40000000",
                "quoteQty": "11.40867000",
                "commission": "0.00000000",
                "commissionAsset": "EUR",
                "time": "1614115784610",
                "isBuyer": False,
                "isMaker": True,
                "isBestMatch": True,
            },
            "timestamp": 1628898947607,
            "datetime": "2021-08-13T23:55:47.607Z",
            "symbol": "XRP/EUR",
            "id": "14782367",
            "order": "325503814",
            "type": None,
            "side": "buy",
            "takerOrMaker": "taker",
            "price": 0.9331,
            "amount": 15.1,
            "cost": 14.08981,
            "fee": {"cost": 3.006e-05, "currency": "BNB"},
        },
    ],
    [
        {
            "info": {
                "symbol": "ETHEUR",
                "id": 6360297,
                "orderId": 131249805,
                "orderListId": -1,
                "price": "40825.94000000",
                "qty": "0.000367",
                "quoteQty": "14.98311998",
                "commission": "0.02950000",
                "commissionAsset": "XRP",
                "time": 1614080759873,
                "isBuyer": True,
                "isMaker": False,
                "isBestMatch": True,
            },
            "timestamp": 1628898947607,
            "datetime": "2021-08-13T23:55:47.607Z",
            "symbol": "ETH/EUR",
            "id": "14782367",
            "order": "325503814",
            "type": None,
            "side": "buy",
            "takerOrMaker": "taker",
            "price": 0.9331,
            "amount": 15.1,
            "cost": 14.08981,
            "fee": {"cost": 3.006e-05, "currency": "BNB"},
        },
        {
            "info": {
                "symbol": "ETHEUR",
                "id": 6360397,
                "orderId": 132149502,
                "orderListId": -1,
                "price": "43825.94000000",
                "qty": "0.000367",
                "quoteQty": "16,08411998",
                "commission": "0.00000000",
                "commissionAsset": "EUR",
                "time": 1614115789610,
                "isBuyer": False,
                "isMaker": True,
                "isBestMatch": True,
            },
            "timestamp": 1628898947607,
            "datetime": "2021-08-13T23:55:47.607Z",
            "symbol": "ETH/EUR",
            "id": "14782367",
            "order": "325503814",
            "type": None,
            "side": "buy",
            "takerOrMaker": "taker",
            "price": 0.9331,
            "amount": 15.1,
            "cost": 14.08981,
            "fee": {"cost": 3.006e-05, "currency": "BNB"},
        },
    ],
]

FETCH_MARKETS_RETURN_VALUE = [
    {
        "id": "ETHEUR",
        "lowercaseId": "etheur",
        "symbol": "ETH/EUR",
        "base": "ETH",
        "quote": "EUR",
        "baseId": "ETH",
        "quoteId": "EUR",
        "info": {
            "symbol": "ETHEUR",
            "status": "TRADING",
            "baseAsset": "ETH",
            "baseAssetPrecision": 8,
            "quoteAsset": "EUR",
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
                    "minPrice": "0.01000000",
                    "maxPrice": "100000.00000000",
                    "tickSize": "0.01000000",
                },
                {
                    "filterType": "PERCENT_PRICE",
                    "multiplierUp": "5",
                    "multiplierDown": "0.2",
                    "avgPriceMins": 5,
                },
                {
                    "filterType": "LOT_SIZE",
                    "minQty": "0.00001000",
                    "maxQty": "90000.00000000",
                    "stepSize": "0.00001000",
                },
                {
                    "filterType": "MIN_NOTIONAL",
                    "minNotional": "10.00000000",
                    "applyToMarket": True,
                    "avgPriceMins": 5,
                },
                {"filterType": "ICEBERG_PARTS", "limit": 10},
                {
                    "filterType": "MARKET_LOT_SIZE",
                    "minQty": "0.00000000",
                    "maxQty": "828.06989296",
                    "stepSize": "0.00000000",
                },
                {"filterType": "MAX_NUM_ORDERS", "maxNumOrders": 200},
                {"filterType": "MAX_NUM_ALGO_ORDERS", "maxNumAlgoOrders": 5},
            ],
            "permissions": ["SPOT", "MARGIN"],
        },
    },
    {
        "id": "SXPEUR",
        "lowercaseId": "sxpeur",
        "symbol": "SXP/EUR",
        "base": "SXP",
        "quote": "EUR",
        "baseId": "SXP",
        "quoteId": "EUR",
        "info": {
            "symbol": "SXPEUR",
            "status": "TRADING",
            "baseAsset": "SXP",
            "baseAssetPrecision": 8,
            "quoteAsset": "EUR",
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
            "isMarginTradingAllowed": False,
            "filters": [
                {
                    "filterType": "PRICE_FILTER",
                    "minPrice": "0.00100000",
                    "maxPrice": "10000.00000000",
                    "tickSize": "0.00100000",
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
                    "maxQty": "90000.00000000",
                    "stepSize": "0.00100000",
                },
                {
                    "filterType": "MIN_NOTIONAL",
                    "minNotional": "10.00000000",
                    "applyToMarket": True,
                    "avgPriceMins": 5,
                },
                {"filterType": "ICEBERG_PARTS", "limit": 10},
                {
                    "filterType": "MARKET_LOT_SIZE",
                    "minQty": "0.00000000",
                    "maxQty": "39252.48498122",
                    "stepSize": "0.00000000",
                },
                {"filterType": "MAX_NUM_ORDERS", "maxNumOrders": 200},
                {"filterType": "MAX_NUM_ALGO_ORDERS", "maxNumAlgoOrders": 5},
            ],
            "permissions": ["SPOT"],
        },
    },
    {
        "id": "BTCEUR",
        "lowercaseId": "btceur",
        "symbol": "BTC/EUR",
        "base": "BTC",
        "quote": "EUR",
        "baseId": "BTC",
        "quoteId": "EUR",
        "info": {
            "symbol": "BTCEUR",
            "status": "TRADING",
            "baseAsset": "BTC",
            "baseAssetPrecision": 8,
            "quoteAsset": "EUR",
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
                    "minPrice": "0.01000000",
                    "maxPrice": "1000000.00000000",
                    "tickSize": "0.01000000",
                },
                {
                    "filterType": "PERCENT_PRICE",
                    "multiplierUp": "5",
                    "multiplierDown": "0.2",
                    "avgPriceMins": 5,
                },
                {
                    "filterType": "LOT_SIZE",
                    "minQty": "0.00000100",
                    "maxQty": "9000.00000000",
                    "stepSize": "0.00000100",
                },
                {
                    "filterType": "MIN_NOTIONAL",
                    "minNotional": "10.00000000",
                    "applyToMarket": True,
                    "avgPriceMins": 5,
                },
                {"filterType": "ICEBERG_PARTS", "limit": 10},
                {
                    "filterType": "MARKET_LOT_SIZE",
                    "minQty": "0.00000000",
                    "maxQty": "60.51747899",
                    "stepSize": "0.00000000",
                },
                {"filterType": "MAX_NUM_ORDERS", "maxNumOrders": 200},
                {"filterType": "MAX_NUM_ALGO_ORDERS", "maxNumAlgoOrders": 5},
            ],
            "permissions": ["SPOT", "MARGIN"],
        },
    },
    {
        "id": "LINKEUR",
        "lowercaseId": "linkeur",
        "symbol": "LINK/EUR",
        "base": "LINK",
        "quote": "EUR",
        "baseId": "LINK",
        "quoteId": "EUR",
        "info": {
            "symbol": "LINKEUR",
            "status": "TRADING",
            "baseAsset": "LINK",
            "baseAssetPrecision": 8,
            "quoteAsset": "EUR",
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
            "isMarginTradingAllowed": False,
            "filters": [
                {
                    "filterType": "PRICE_FILTER",
                    "minPrice": "0.00100000",
                    "maxPrice": "10000.00000000",
                    "tickSize": "0.00100000",
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
                    "maxQty": "90000.00000000",
                    "stepSize": "0.00100000",
                },
                {
                    "filterType": "MIN_NOTIONAL",
                    "minNotional": "10.00000000",
                    "applyToMarket": True,
                    "avgPriceMins": 5,
                },
                {"filterType": "ICEBERG_PARTS", "limit": 10},
                {
                    "filterType": "MARKET_LOT_SIZE",
                    "minQty": "0.00000000",
                    "maxQty": "9067.64303220",
                    "stepSize": "0.00000000",
                },
                {"filterType": "MAX_NUM_ORDERS", "maxNumOrders": 200},
                {"filterType": "MAX_NUM_ALGO_ORDERS", "maxNumAlgoOrders": 5},
            ],
            "permissions": ["SPOT"],
        },
    },
    {
        "id": "XRPEUR",
        "lowercaseId": "xrpeur",
        "symbol": "XRP/EUR",
        "base": "XRP",
        "quote": "EUR",
        "baseId": "XRP",
        "quoteId": "EUR",
        "info": {
            "symbol": "XRPEUR",
            "status": "TRADING",
            "baseAsset": "XRP",
            "baseAssetPrecision": 8,
            "quoteAsset": "EUR",
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
            "isMarginTradingAllowed": False,
            "filters": [
                {
                    "filterType": "PRICE_FILTER",
                    "minPrice": "0.00001000",
                    "maxPrice": "1000.00000000",
                    "tickSize": "0.00001000",
                },
                {
                    "filterType": "PERCENT_PRICE",
                    "multiplierUp": "5",
                    "multiplierDown": "0.2",
                    "avgPriceMins": 5,
                },
                {
                    "filterType": "LOT_SIZE",
                    "minQty": "0.10000000",
                    "maxQty": "9000000.00000000",
                    "stepSize": "0.10000000",
                },
                {
                    "filterType": "MIN_NOTIONAL",
                    "minNotional": "10.00000000",
                    "applyToMarket": True,
                    "avgPriceMins": 5,
                },
                {"filterType": "ICEBERG_PARTS", "limit": 10},
                {
                    "filterType": "MARKET_LOT_SIZE",
                    "minQty": "0.00000000",
                    "maxQty": "566558.22647585",
                    "stepSize": "0.00000000",
                },
                {"filterType": "MAX_NUM_ORDERS", "maxNumOrders": 200},
                {"filterType": "MAX_NUM_ALGO_ORDERS", "maxNumAlgoOrders": 5},
            ],
            "permissions": ["SPOT"],
        },
    },
]

MARKET_BUY_ORDER_RESPONSE = {
    "info": {
        "symbol": "SXPEUR",
        "orderId": "134562464",
        "orderListId": -1,
        "clientOrderId": "bOxEy1MFegaDEqkiQhF3Xg",
        "transactTime": "1614237068817",
        "price": "0.00000000",
        "origQty": "27.00000000",
        "executedQty": "27.00000000",
        "cummulativeQuoteQty": "10.44144000",
        "status": "FILLED",
        "timeInForce": "GTC",
        "type": "MARKET",
        "side": "BUY",
        "fills": [
            {
                "price": "0.38672000",
                "qty": "27.00000000",
                "commission": "0.02700000",
                "commissionAsset": "SXP",
                "tradeId": "6324526",
            }
        ],
    },
    "id": "325548521",
    "clientOrderId": "x-R4BD3S82cdd983f2c5f6500e6bbaee",
    "timestamp": 1628903667542,
    "datetime": "2021-08-14T01:14:27.542Z",
    "lastTradeTimestamp": None,
    "symbol": "SXP/EUR",
    "type": "market",
    "timeInForce": "GTC",
    "postOnly": False,
    "side": "buy",
    "price": 0.9363999999999999,
    "stopPrice": None,
    "amount": 15.97,
    "cost": 14.954308,
    "average": 0.9363999999999999,
    "filled": 15.97,
    "remaining": 0.0,
    "status": "closed",
    "fee": {"cost": 3.199e-05, "currency": "BNB"},
    "trades": [
        {
            "info": {
                "price": "0.93640000",
                "qty": "15.97000000",
                "commission": "0.00003199",
                "commissionAsset": "BNB",
                "tradeId": "14784137",
            },
            "timestamp": None,
            "datetime": None,
            "symbol": "SXP/EUR",
            "id": None,
            "order": None,
            "type": None,
            "side": None,
            "takerOrMaker": None,
            "price": 0.9364,
            "amount": 15.97,
            "cost": 14.954308,
            "fee": {"cost": 3.199e-05, "currency": "BNB"},
        }
    ],
    "fees": [{"cost": 3.199e-05, "currency": "BNB"}],
}

MARKET_BUY_ORDER_RESPONSE_ORDER_NOT_FILLED = {
    "info": {
        "symbol": "SXPEUR",
        "orderId": "134562468",
        "orderListId": -1,
        "clientOrderId": "bOxEy1MFegaDEqkiQhF3Xh",
        "transactTime": "1614237068827",
        "price": "0.00000000",
        "origQty": "27.00000000",
        "executedQty": "27.00000000",
        "cummulativeQuoteQty": "10.44144000",
        "status": "NEW",
        "timeInForce": "GTC",
        "type": "MARKET",
        "side": "BUY",
        "fills": [
            {
                "price": "0.38672000",
                "qty": "27.00000000",
                "commission": "0.02700000",
                "commissionAsset": "SXP",
                "tradeId": "6324529",
            }
        ],
    },
    "id": "325548521",
    "clientOrderId": "x-R4BD3S82cdd983f2c5f6500e6bbaee",
    "timestamp": 1628903667542,
    "datetime": "2021-08-14T01:14:27.542Z",
    "lastTradeTimestamp": None,
    "symbol": "SXP/EUR",
    "type": "market",
    "timeInForce": "GTC",
    "postOnly": False,
    "side": "buy",
    "price": 0.9363999999999999,
    "stopPrice": None,
    "amount": 15.97,
    "cost": 14.954308,
    "average": 0.9363999999999999,
    "filled": 15.97,
    "remaining": 0.0,
    "status": "closed",
    "fee": {"cost": 3.199e-05, "currency": "BNB"},
    "trades": [
        {
            "info": {
                "price": "0.93640000",
                "qty": "15.97000000",
                "commission": "0.00003199",
                "commissionAsset": "BNB",
                "tradeId": "14784137",
            },
            "timestamp": None,
            "datetime": None,
            "symbol": "SXP/EUR",
            "id": None,
            "order": None,
            "type": None,
            "side": None,
            "takerOrMaker": None,
            "price": 0.9364,
            "amount": 15.97,
            "cost": 14.954308,
            "fee": {"cost": 3.199e-05, "currency": "BNB"},
        }
    ],
    "fees": [{"cost": 3.199e-05, "currency": "BNB"}],
}

MARKET_SELL_ORDER_RESPONSE = {
    "info": {
        "symbol": "SXPEUR",
        "orderId": "134791954",
        "orderListId": -1,
        "clientOrderId": "KcFWfDoNXNhKnJGiIjUQJn",
        "transactTime": "1614248929811",
        "price": "0.00000000",
        "origQty": "26.70000000",
        "executedQty": "26.70000000",
        "cummulativeQuoteQty": "10.10301300",
        "status": "FILLED",
        "timeInForce": "GTC",
        "type": "MARKET",
        "side": "SELL",
        "fills": [
            {
                "price": "0.37839000",
                "qty": "26.70000000",
                "commission": "0.01010301",
                "commissionAsset": "EUR",
                "tradeId": "6334009",
            }
        ],
    },
    "id": "325548521",
    "clientOrderId": "x-R4BD3S82cdd983f2c5f6500e6bbaee",
    "timestamp": 1628903667542,
    "datetime": "2021-08-14T01:14:27.542Z",
    "lastTradeTimestamp": None,
    "symbol": "SXP/EUR",
    "type": "market",
    "timeInForce": "GTC",
    "postOnly": False,
    "side": "buy",
    "price": 0.9363999999999999,
    "stopPrice": None,
    "amount": 15.97,
    "cost": 14.954308,
    "average": 0.9363999999999999,
    "filled": 15.97,
    "remaining": 0.0,
    "status": "closed",
    "fee": {"cost": 3.199e-05, "currency": "BNB"},
    "trades": [
        {
            "info": {
                "price": "0.93640000",
                "qty": "15.97000000",
                "commission": "0.00003199",
                "commissionAsset": "BNB",
                "tradeId": "14784137",
            },
            "timestamp": None,
            "datetime": None,
            "symbol": "SXP/EUR",
            "id": None,
            "order": None,
            "type": None,
            "side": None,
            "takerOrMaker": None,
            "price": 0.9364,
            "amount": 15.97,
            "cost": 14.954308,
            "fee": {"cost": 3.199e-05, "currency": "BNB"},
        }
    ],
    "fees": [{"cost": 3.199e-05, "currency": "BNB"}],
}

MARKET_SELL_ORDER_RESPONSE_NOT_FILLED = {
    "info": {
        "symbol": "SXPEUR",
        "orderId": "134791962",
        "orderListId": -1,
        "clientOrderId": "KcFWfDoNXNhKnJGiIjUQJp",
        "transactTime": "1614248929911",
        "price": "0.00000000",
        "origQty": "26.70000000",
        "executedQty": "26.70000000",
        "cummulativeQuoteQty": "10.10301300",
        "status": "NEW",
        "timeInForce": "GTC",
        "type": "MARKET",
        "side": "SELL",
        "fills": [
            {
                "price": "0.37839000",
                "qty": "26.70000000",
                "commission": "0.01010301",
                "commissionAsset": "EUR",
                "tradeId": "6334018",
            }
        ],
    },
    "id": "325548521",
    "clientOrderId": "x-R4BD3S82cdd983f2c5f6500e6bbaee",
    "timestamp": 1628903667542,
    "datetime": "2021-08-14T01:14:27.542Z",
    "lastTradeTimestamp": None,
    "symbol": "SXP/EUR",
    "type": "market",
    "timeInForce": "GTC",
    "postOnly": False,
    "side": "buy",
    "price": 0.9363999999999999,
    "stopPrice": None,
    "amount": 15.97,
    "cost": 14.954308,
    "average": 0.9363999999999999,
    "filled": 15.97,
    "remaining": 0.0,
    "status": "closed",
    "fee": {"cost": 3.199e-05, "currency": "BNB"},
    "trades": [
        {
            "info": {
                "price": "0.93640000",
                "qty": "15.97000000",
                "commission": "0.00003199",
                "commissionAsset": "BNB",
                "tradeId": "14784137",
            },
            "timestamp": None,
            "datetime": None,
            "symbol": "SXP/EUR",
            "id": None,
            "order": None,
            "type": None,
            "side": None,
            "takerOrMaker": None,
            "price": 0.9364,
            "amount": 15.97,
            "cost": 14.954308,
            "fee": {"cost": 3.199e-05, "currency": "BNB"},
        }
    ],
    "fees": [{"cost": 3.199e-05, "currency": "BNB"}],
}

LIMIT_BUY_ORDER_RESPONSE = {
    "info": {
        "symbol": "SXPEUR",
        "orderId": "134879100",
        "orderListId": -1,
        "clientOrderId": "2rPZy0qJCcuX0r2fVjG5s7",
        "transactTime": "1614254004034",
        "price": "0.35340000",
        "origQty": "29.00000000",
        "executedQty": "0.00000000",
        "cummulativeQuoteQty": "0.00000000",
        "status": "NEW",
        "timeInForce": "GTC",
        "type": "LIMIT",
        "side": "BUY",
        "fills": [],
    },
    "id": "325548521",
    "clientOrderId": "x-R4BD3S82cdd983f2c5f6500e6bbaee",
    "timestamp": 1628903667542,
    "datetime": "2021-08-14T01:14:27.542Z",
    "lastTradeTimestamp": None,
    "symbol": "SXP/EUR",
    "type": "market",
    "timeInForce": "GTC",
    "postOnly": False,
    "side": "buy",
    "price": 0.9363999999999999,
    "stopPrice": None,
    "amount": 15.97,
    "cost": 14.954308,
    "average": 0.9363999999999999,
    "filled": 15.97,
    "remaining": 0.0,
    "status": "closed",
    "fee": {"cost": 3.199e-05, "currency": "BNB"},
    "trades": [
        {
            "info": {
                "price": "0.93640000",
                "qty": "15.97000000",
                "commission": "0.00003199",
                "commissionAsset": "BNB",
                "tradeId": "14784137",
            },
            "timestamp": None,
            "datetime": None,
            "symbol": "SXP/EUR",
            "id": None,
            "order": None,
            "type": None,
            "side": None,
            "takerOrMaker": None,
            "price": 0.9364,
            "amount": 15.97,
            "cost": 14.954308,
            "fee": {"cost": 3.199e-05, "currency": "BNB"},
        }
    ],
    "fees": [{"cost": 3.199e-05, "currency": "BNB"}],
}

LIMIT_SELL_ORDER_RESPONSE = {
    "info": {
        "symbol": "XRPEUR",
        "orderId": "134912358",
        "orderListId": -1,
        "clientOrderId": "UH2GvNOuXQtBFLDLc6fr1b",
        "transactTime": "1614255967741",
        "price": "0.45340000",
        "origQty": "26.30000000",
        "executedQty": "0.00000000",
        "cummulativeQuoteQty": "0.00000000",
        "status": "NEW",
        "timeInForce": "GTC",
        "type": "LIMIT",
        "side": "SELL",
        "fills": [],
    },
    "id": "325548521",
    "clientOrderId": "x-R4BD3S82cdd983f2c5f6500e6bbaee",
    "timestamp": 1628903667542,
    "datetime": "2021-08-14T01:14:27.542Z",
    "lastTradeTimestamp": None,
    "symbol": "SXP/EUR",
    "type": "market",
    "timeInForce": "GTC",
    "postOnly": False,
    "side": "buy",
    "price": 0.9363999999999999,
    "stopPrice": None,
    "amount": 15.97,
    "cost": 14.954308,
    "average": 0.9363999999999999,
    "filled": 15.97,
    "remaining": 0.0,
    "status": "closed",
    "fee": {"cost": 3.199e-05, "currency": "BNB"},
    "trades": [
        {
            "info": {
                "price": "0.93640000",
                "qty": "15.97000000",
                "commission": "0.00003199",
                "commissionAsset": "BNB",
                "tradeId": "14784137",
            },
            "timestamp": None,
            "datetime": None,
            "symbol": "SXP/EUR",
            "id": None,
            "order": None,
            "type": None,
            "side": None,
            "takerOrMaker": None,
            "price": 0.9364,
            "amount": 15.97,
            "cost": 14.954308,
            "fee": {"cost": 3.199e-05, "currency": "BNB"},
        }
    ],
    "fees": [{"cost": 3.199e-05, "currency": "BNB"}],
}


@patch("trazy_analysis.broker.ccxt_broker.CcxtBroker.update_transactions")
@patch("trazy_analysis.broker.ccxt_broker.CcxtBroker.update_balances_and_positions")
@patch("trazy_analysis.broker.ccxt_broker.CcxtBroker.update_price")
@patch("ccxt.binance.fetch_markets")
def test_lot_size_info(
    fetch_markets_mocked,
    update_price_mocked,
    update_balances_mocked,
    update_transactions_mocked,
):
    fetch_markets_mocked.return_value = FETCH_MARKETS_RETURN_VALUE
    clock = LiveClock()
    events = deque()
    exchanges_api_keys = {
        "BINANCE": {
            "key": None,
            "secret": None,
            "password": None,
        }
    }
    parsers = {"binance": CcxtBinanceParser}
    ccxt_connector = CcxtConnector(
        exchanges_api_keys=exchanges_api_keys, parsers=parsers
    )
    ccxt_broker = CcxtBroker(clock=clock, events=events, ccxt_connector=ccxt_connector)
    assert ccxt_broker.lot_size == {
        Asset(symbol="BTC/EUR", exchange=EXCHANGE): 0.000001,
        Asset(symbol="ETH/EUR", exchange=EXCHANGE): 0.00001,
        Asset(symbol="LINK/EUR", exchange=EXCHANGE): 0.001,
        Asset(symbol="SXP/EUR", exchange=EXCHANGE): 0.001,
        Asset(symbol="XRP/EUR", exchange=EXCHANGE): 0.1,
    }

    assert isinstance(ccxt_broker.lot_size_last_update, datetime)
    timestamp = ccxt_broker.lot_size_last_update

    # Call again to see if the value is cached
    ccxt_broker.update_lot_size_info()
    assert ccxt_broker.lot_size_last_update == timestamp


@patch("trazy_analysis.broker.ccxt_broker.CcxtBroker.update_transactions")
@patch("trazy_analysis.broker.ccxt_broker.CcxtBroker.update_balances_and_positions")
@patch("ccxt.binance.fetch_tickers")
@patch("ccxt.binance.fetch_markets")
def test_update_price(
    fetch_markets_mocked,
    fetch_tickers_mocked,
    update_balances_mocked,
    update_transactions_mocked,
):
    fetch_markets_mocked.return_value = FETCH_MARKETS_RETURN_VALUE
    fetch_tickers_mocked.return_value = FETCH_TICKERS_RETURN_VALUE
    clock = LiveClock()
    events = deque()
    exchanges_api_keys = {
        "BINANCE": {
            "key": None,
            "secret": None,
            "password": None,
        }
    }
    parsers = {"binance": CcxtBinanceParser}
    ccxt_connector = CcxtConnector(
        exchanges_api_keys=exchanges_api_keys, parsers=parsers
    )
    ccxt_broker = CcxtBroker(clock=clock, events=events, ccxt_connector=ccxt_connector)
    assert ccxt_broker.last_prices == {
        Asset(symbol="BTC/EUR", exchange=EXCHANGE): 40825.94,
        Asset(symbol="ETH/EUR", exchange=EXCHANGE): 1353.12,
        Asset(symbol="LINK/EUR", exchange=EXCHANGE): 22.55,
        Asset(symbol="SXP/EUR", exchange=EXCHANGE): 1.904,
        Asset(symbol="XRP/EUR", exchange=EXCHANGE): 0.391,
    }

    assert isinstance(ccxt_broker.price_last_update, datetime)
    timestamp = ccxt_broker.price_last_update

    # Call again to see if the value is cached
    ccxt_broker.update_price()
    assert ccxt_broker.price_last_update == timestamp


@patch("trazy_analysis.broker.ccxt_broker.CcxtBroker.update_transactions")
@patch("ccxt.binance.fetch_balance")
@patch("ccxt.binance.fetch_tickers")
@patch("ccxt.binance.fetch_markets")
def test_update_balances_and_positions(
    fetch_markets_mocked,
    fetch_tickers_mocked,
    fetch_balance_mocked,
    update_transactions_mocked,
):
    fetch_markets_mocked.return_value = FETCH_MARKETS_RETURN_VALUE
    fetch_tickers_mocked.return_value = FETCH_TICKERS_RETURN_VALUE
    fetch_balance_mocked.return_value = FETCH_BALANCE_RETURN_VALUE
    clock = LiveClock()
    events = deque()
    exchanges_api_keys = {
        "BINANCE": {
            "key": None,
            "secret": None,
            "password": None,
        }
    }
    parsers = {"binance": CcxtBinanceParser}
    ccxt_connector = CcxtConnector(
        exchanges_api_keys=exchanges_api_keys, parsers=parsers
    )
    ccxt_broker = CcxtBroker(clock=clock, events=events, ccxt_connector=ccxt_connector)

    assert ccxt_broker.cash_balances == {
        "binance": {
            "EUR": float(INITIAL_CASH),
            "USDT": 0.0,
        }
    }
    assert isinstance(ccxt_broker.balances_last_update, datetime)

    price1 = 1353.12000000
    buy_size1 = 0.15067000
    sell_size1 = 0
    expected_position1 = Position(
        asset=ASSET1,
        price=price1,
        buy_size=buy_size1,
        sell_size=sell_size1,
        direction=Direction.LONG,
    )

    price2 = 0.39100000
    buy_size2 = 0
    sell_size2 = -29.47050000
    expected_position2 = Position(
        asset=ASSET2,
        price=price2,
        buy_size=buy_size2,
        sell_size=sell_size2,
        direction=Direction.SHORT,
    )

    expected_positions = {
        ASSET2: {Direction.SHORT: expected_position2},
        ASSET1: {Direction.LONG: expected_position1},
    }

    assert ccxt_broker.portfolio.pos_handler.positions == expected_positions

    # Call again to see if the value is cached
    timestamp = ccxt_broker.balances_last_update
    ccxt_broker.update_balances_and_positions()
    assert ccxt_broker.balances_last_update == timestamp

    assert ccxt_broker.get_cash_balance() == {
        "binance": {
            "EUR": float(INITIAL_CASH),
            "USDT": 0.0,
        }
    }

    assert ccxt_broker.get_cash_balance("EUR") == float(INITIAL_CASH)

    with pytest.raises(ValueError):
        assert ccxt_broker.get_cash_balance("GBP")

    fetch_balance_mocked.assert_has_calls([call()])
    fetch_tickers_mocked.assert_has_calls([call()])


@patch("trazy_analysis.broker.ccxt_broker.CcxtBroker.update_transactions")
@patch("ccxt.binance.fetch_balance")
@patch("ccxt.binance.fetch_tickers")
@patch("ccxt.binance.fetch_markets")
def test_has_opened_position(
    fetch_markets_mocked,
    fetch_tickers_mocked,
    fetch_balance_mocked,
    update_transactions_mocked,
):
    fetch_markets_mocked.return_value = FETCH_MARKETS_RETURN_VALUE
    fetch_tickers_mocked.return_value = FETCH_TICKERS_RETURN_VALUE
    fetch_balance_mocked.return_value = FETCH_BALANCE_RETURN_VALUE

    clock = LiveClock()
    events = deque()
    exchanges_api_keys = {
        "BINANCE": {
            "key": None,
            "secret": None,
            "password": None,
        }
    }
    parsers = {"binance": CcxtBinanceParser}
    ccxt_connector = CcxtConnector(
        exchanges_api_keys=exchanges_api_keys, parsers=parsers
    )
    ccxt_broker = CcxtBroker(clock=clock, events=events, ccxt_connector=ccxt_connector)

    assert ccxt_broker.has_opened_position(ASSET1, Direction.LONG)
    assert not ccxt_broker.has_opened_position(ASSET1, Direction.SHORT)
    assert ccxt_broker.has_opened_position(ASSET2, Direction.SHORT)
    assert not ccxt_broker.has_opened_position(ASSET2, Direction.LONG)
    assert not ccxt_broker.has_opened_position(ASSET3, Direction.LONG)
    assert not ccxt_broker.has_opened_position(ASSET3, Direction.SHORT)


@patch("trazy_analysis.common.clock.LiveClock.current_time")
@patch("ccxt.binance.fetch_my_trades")
@patch("ccxt.binance.fetch_balance")
@patch("ccxt.binance.fetch_tickers")
@patch("ccxt.binance.fetch_markets")
@freeze_time("2021-02-22 11:45:57+00:00")
def test_update_transactions(
    fetch_markets_mocked,
    fetch_tickers_mocked,
    fetch_balance_mocked,
    fetch_my_trades_mocked,
    current_time_mocked,
):
    fetch_markets_mocked.return_value = FETCH_MARKETS_RETURN_VALUE
    fetch_tickers_mocked.return_value = FETCH_TICKERS_RETURN_VALUE
    fetch_balance_mocked.return_value = FETCH_BALANCE_RETURN_VALUE
    fetch_my_trades_mocked.side_effect = FETCH_MY_TRADES_SIDE_EFFECT

    nb_current_time_calls = 11

    current_time_mocked.side_effect = [
        datetime(
            year=2021,
            month=2,
            day=8,
            hour=15,
            minute=i,
            second=0,
            tzinfo=timezone("UTC"),
        )
        for i in range(1, nb_current_time_calls)
    ] + [
        datetime(
            year=2021,
            month=2,
            day=8,
            hour=15,
            minute=10,
            second=5,
            tzinfo=timezone("UTC"),
        )
    ]

    clock = LiveClock()
    events = deque()
    exchanges_api_keys = {
        "BINANCE": {
            "key": None,
            "secret": None,
            "password": None,
        }
    }
    parsers = {"binance": CcxtBinanceParser}
    ccxt_connector = CcxtConnector(
        exchanges_api_keys=exchanges_api_keys, parsers=parsers
    )
    ccxt_broker = CcxtBroker(clock=clock, events=events, ccxt_connector=ccxt_connector)
    assert isinstance(ccxt_broker.transactions_last_update, datetime)

    # Call again to see if the value is cached
    timestamp = ccxt_broker.transactions_last_update
    ccxt_broker.update_transactions()
    assert ccxt_broker.transactions_last_update == timestamp

    assert len(ccxt_broker.portfolio.history) == 4
    assert ccxt_broker.portfolio.history[0] == PortfolioEvent(
        timestamp=datetime.strptime(
            "2021-02-23 11:45:57.873000+0000", "%Y-%m-%d %H:%M:%S.%f%z"
        ),
        type="symbol_transaction",
        description="BUY LONG 29.5 BINANCE-XRPEUR-0:01:00 0.33822 23/02/2021",
        debit=10.006990000000002,
        credit=0.0,
        balance=51.07118,
    )
    assert ccxt_broker.portfolio.history[1] == PortfolioEvent(
        timestamp=datetime.strptime(
            "2021-02-23 21:29:44.610000+0000", "%Y-%m-%d %H:%M:%S.%f%z"
        ),
        type="symbol_transaction",
        description="SELL LONG 29.4 BINANCE-XRPEUR-0:01:00 0.38805 23/02/2021",
        debit=0.0,
        credit=11.41,
        balance=51.07,
    )
    assert ccxt_broker.portfolio.history[2] == PortfolioEvent(
        timestamp=datetime.strptime(
            "2021-02-23 11:45:59.873000+0000", "%Y-%m-%d %H:%M:%S.%f%z"
        ),
        type="symbol_transaction",
        description="BUY LONG 0.000367 BINANCE-ETHEUR-0:01:00 40825.94 23/02/2021",
        debit=15.01261998,
        credit=0.0,
        balance=51.07118,
    )
    assert ccxt_broker.portfolio.history[3] == PortfolioEvent(
        timestamp=datetime.strptime(
            "2021-02-23 21:29:49.610000+0000", "%Y-%m-%d %H:%M:%S.%f%z"
        ),
        type="symbol_transaction",
        description="SELL LONG 0.000367 BINANCE-ETHEUR-0:01:00 43825.94 23/02/2021",
        debit=0.0,
        credit=16.08,
        balance=51.07,
    )

    fetch_balance_mocked.assert_has_calls([call()])
    fetch_tickers_mocked.assert_has_calls([call()])

    fetch_my_trades_mocked_calls = [
        call(since=1612796880000, symbol="XRP/EUR"),
        call(since=1612796880000, symbol="ETH/EUR"),
    ]
    fetch_my_trades_mocked.assert_has_calls(
        fetch_my_trades_mocked_calls, any_order=True
    )


@patch("ccxt.binance.create_order")
@patch("ccxt.binance.fetch_my_trades")
@patch("ccxt.binance.fetch_balance")
@patch("ccxt.binance.fetch_tickers")
@patch("ccxt.binance.fetch_markets")
def test_execute_market_order(
    fetch_markets_mocked,
    fetch_tickers_mocked,
    fetch_balance_mocked,
    fetch_my_trades_mocked,
    create_order_mocked,
):
    fetch_markets_mocked.return_value = FETCH_MARKETS_RETURN_VALUE
    fetch_tickers_mocked.return_value = FETCH_TICKERS_RETURN_VALUE
    fetch_balance_mocked.return_value = FETCH_BALANCE_RETURN_VALUE
    fetch_my_trades_mocked.side_effect = FETCH_MY_TRADES_SIDE_EFFECT
    create_order_mocked.side_effect = [
        MARKET_BUY_ORDER_RESPONSE,
        MARKET_BUY_ORDER_RESPONSE_ORDER_NOT_FILLED,
        Exception(),
        MARKET_SELL_ORDER_RESPONSE,
        MARKET_SELL_ORDER_RESPONSE_NOT_FILLED,
    ]

    clock = LiveClock()
    events = deque()
    exchanges_api_keys = {
        "BINANCE": {
            "key": None,
            "secret": None,
            "password": None,
        }
    }
    parsers = {"binance": CcxtBinanceParser}
    ccxt_connector = CcxtConnector(
        exchanges_api_keys=exchanges_api_keys, parsers=parsers
    )
    ccxt_broker = CcxtBroker(clock=clock, events=events, ccxt_connector=ccxt_connector)

    # test buy orders
    buy_order = Order(
        asset=ASSET3,
        action=Action.BUY,
        direction=Direction.LONG,
        size=15.97654,
        signal_id="1",
        type=OrderType.MARKET,
        clock=clock,
    )

    assert ccxt_broker.currency_pairs_traded == {
        Asset(symbol="XRP/EUR", exchange=EXCHANGE),
        Asset(symbol="ETH/EUR", exchange=EXCHANGE),
    }
    ccxt_broker.execute_order(buy_order)
    assert buy_order.order_id == "134562464"
    assert ccxt_broker.currency_pairs_traded == {
        Asset(symbol="XRP/EUR", exchange=EXCHANGE),
        Asset(symbol="ETH/EUR", exchange=EXCHANGE),
        Asset(symbol="SXP/EUR", exchange=EXCHANGE),
    }
    # Order submitted to the broker but not filled yet
    ccxt_broker.execute_order(buy_order)
    assert ccxt_broker.open_orders_ids == {"binance": {"134562468"}}

    # test ccxt buyorder Exception
    ccxt_broker.execute_order(buy_order)

    # test sell orders
    sell_order = Order(
        asset=ASSET3,
        action=Action.SELL,
        direction=Direction.LONG,
        size=26.77654,
        signal_id="1",
        type=OrderType.MARKET,
        clock=clock,
    )

    assert ccxt_broker.currency_pairs_traded == {
        Asset(symbol="XRP/EUR", exchange=EXCHANGE),
        Asset(symbol="ETH/EUR", exchange=EXCHANGE),
        Asset(symbol="SXP/EUR", exchange=EXCHANGE),
    }
    ccxt_broker.execute_order(sell_order)
    assert sell_order.order_id == "134791954"
    assert ccxt_broker.currency_pairs_traded == {
        Asset(symbol="XRP/EUR", exchange=EXCHANGE),
        Asset(symbol="ETH/EUR", exchange=EXCHANGE),
        Asset(symbol="SXP/EUR", exchange=EXCHANGE),
    }
    # Order submitted to the broker but not filled yet
    ccxt_broker.execute_order(sell_order)
    assert ccxt_broker.open_orders_ids == {"binance": {"134562468", "134791962"}}

    # check mock calls
    create_order_mocked_calls = [
        call(amount=Decimal("15.976"), side="buy", symbol="SXP/EUR", type="market"),
        call(amount=Decimal("15.976"), side="buy", symbol="SXP/EUR", type="market"),
        call(amount=Decimal("15.976"), side="buy", symbol="SXP/EUR", type="market"),
        call(amount=Decimal("26.776"), side="sell", symbol="SXP/EUR", type="market"),
        call(amount=Decimal("26.776"), side="sell", symbol="SXP/EUR", type="market"),
    ]
    create_order_mocked.assert_has_calls(create_order_mocked_calls)


@patch("ccxt.binance.create_order")
@patch("ccxt.binance.fetch_my_trades")
@patch("ccxt.binance.fetch_balance")
@patch("ccxt.binance.fetch_tickers")
@patch("ccxt.binance.fetch_markets")
def test_execute_limit_order(
    fetch_markets_mocked,
    fetch_tickers_mocked,
    fetch_balance_mocked,
    fetch_my_trades_mocked,
    create_order_mocked,
):
    fetch_markets_mocked.return_value = FETCH_MARKETS_RETURN_VALUE
    fetch_tickers_mocked.return_value = FETCH_TICKERS_RETURN_VALUE
    fetch_balance_mocked.return_value = FETCH_BALANCE_RETURN_VALUE
    fetch_my_trades_mocked.side_effect = FETCH_MY_TRADES_SIDE_EFFECT
    create_order_mocked.side_effect = [
        LIMIT_BUY_ORDER_RESPONSE,
        Exception(),
        LIMIT_SELL_ORDER_RESPONSE,
    ]

    clock = LiveClock()
    events = deque()
    exchanges_api_keys = {
        "BINANCE": {
            "key": None,
            "secret": None,
            "password": None,
        }
    }
    parsers = {"binance": CcxtBinanceParser}
    ccxt_connector = CcxtConnector(
        exchanges_api_keys=exchanges_api_keys, parsers=parsers
    )
    ccxt_broker = CcxtBroker(clock=clock, events=events, ccxt_connector=ccxt_connector)

    # test buy orders
    buy_order = Order(
        asset=ASSET1,
        action=Action.BUY,
        direction=Direction.LONG,
        size=26.97654,
        signal_id="1",
        type=OrderType.LIMIT,
        limit=0.3534,
        clock=clock,
    )

    assert ccxt_broker.currency_pairs_traded == {
        Asset(symbol="XRP/EUR", exchange=EXCHANGE),
        Asset(symbol="ETH/EUR", exchange=EXCHANGE),
    }
    ccxt_broker.execute_order(buy_order)
    assert buy_order.order_id == "134879100"
    assert ccxt_broker.currency_pairs_traded == {
        Asset(symbol="XRP/EUR", exchange=EXCHANGE),
        Asset(symbol="ETH/EUR", exchange=EXCHANGE),
    }
    assert ccxt_broker.open_orders_ids == {"binance": {"134879100"}}

    # test ccxt buyorder Exception
    ccxt_broker.execute_order(buy_order)

    # test sell orders
    sell_order = Order(
        asset=ASSET1,
        action=Action.SELL,
        direction=Direction.LONG,
        size=26.77654,
        signal_id="1",
        type=OrderType.LIMIT,
        limit=0.4534,
        clock=clock,
    )

    assert ccxt_broker.currency_pairs_traded == {
        Asset(symbol="XRP/EUR", exchange=EXCHANGE),
        Asset(symbol="ETH/EUR", exchange=EXCHANGE),
    }
    ccxt_broker.execute_order(sell_order)
    assert sell_order.order_id == "134912358"
    assert ccxt_broker.currency_pairs_traded == {
        Asset(symbol="XRP/EUR", exchange=EXCHANGE),
        Asset(symbol="ETH/EUR", exchange=EXCHANGE),
    }
    assert ccxt_broker.open_orders_ids == {"binance": {"134912358", "134879100"}}

    # check mock calls
    create_order_mocked_calls = [
        call(
            amount=Decimal("26.97654"),
            price=0.3534,
            side="buy",
            symbol="ETH/EUR",
            type="limit",
        ),
        call(
            amount=Decimal("26.97654"),
            price=0.3534,
            side="buy",
            symbol="ETH/EUR",
            type="limit",
        ),
        call(
            amount=Decimal("0.15067"),
            price=0.4534,
            side="sell",
            symbol="ETH/EUR",
            type="limit",
        ),
    ]
    create_order_mocked.assert_has_calls(create_order_mocked_calls)


@patch("trazy_analysis.broker.ccxt_broker.CcxtBroker.execute_market_order")
@patch("ccxt.binance.fetch_my_trades")
@patch("ccxt.binance.fetch_balance")
@patch("ccxt.binance.fetch_tickers")
@patch("ccxt.binance.fetch_markets")
def test_execute_stop_order(
    fetch_markets_mocked,
    fetch_tickers_mocked,
    fetch_balance_mocked,
    fetch_my_trades_mocked,
    execute_market_order_mocked,
):
    fetch_markets_mocked.return_value = FETCH_MARKETS_RETURN_VALUE
    fetch_tickers_mocked.return_value = FETCH_TICKERS_RETURN_VALUE
    fetch_balance_mocked.return_value = FETCH_BALANCE_RETURN_VALUE
    fetch_my_trades_mocked.side_effect = FETCH_MY_TRADES_SIDE_EFFECT

    clock = LiveClock()
    events = deque()
    exchanges_api_keys = {
        "BINANCE": {
            "key": None,
            "secret": None,
            "password": None,
        }
    }
    parsers = {"binance": CcxtBinanceParser}
    ccxt_connector = CcxtConnector(
        exchanges_api_keys=exchanges_api_keys, parsers=parsers
    )
    ccxt_broker = CcxtBroker(clock=clock, events=events, ccxt_connector=ccxt_connector)

    # stop order sell
    stop_order_sell = Order(
        asset=ASSET2,
        action=Action.SELL,
        direction=Direction.LONG,
        size=1,
        signal_id="1",
        stop=0.20,
        type=OrderType.STOP,
        clock=clock,
    )
    ccxt_broker.execute_order(stop_order_sell)
    assert len(ccxt_broker.open_orders) == 1
    assert ccxt_broker.open_orders.popleft() == stop_order_sell

    ccxt_broker.last_prices[ASSET2] = 0.18
    ccxt_broker.execute_order(stop_order_sell)
    assert len(ccxt_broker.open_orders) == 0

    # stop order buy
    stop_order_buy = Order(
        asset=ASSET2,
        action=Action.BUY,
        direction=Direction.LONG,
        size=1,
        signal_id="1",
        stop=0.45,
        type=OrderType.STOP,
        clock=clock,
    )
    ccxt_broker.execute_order(stop_order_buy)
    assert len(ccxt_broker.open_orders) == 1
    assert ccxt_broker.open_orders.popleft() == stop_order_buy

    ccxt_broker.last_prices[ASSET2] = 0.46
    ccxt_broker.execute_order(stop_order_buy)
    assert len(ccxt_broker.open_orders) == 0

    execute_market_order_mocked_calls = [call(stop_order_sell), call(stop_order_buy)]
    execute_market_order_mocked.assert_has_calls(execute_market_order_mocked_calls)


@patch("trazy_analysis.broker.ccxt_broker.CcxtBroker.execute_market_order")
@patch("ccxt.binance.fetch_my_trades")
@patch("ccxt.binance.fetch_balance")
@patch("ccxt.binance.fetch_tickers")
@patch("ccxt.binance.fetch_markets")
def test_execute_target_order(
    fetch_markets_mocked,
    fetch_tickers_mocked,
    fetch_balance_mocked,
    fetch_my_trades_mocked,
    execute_market_order_mocked,
):
    fetch_markets_mocked.return_value = FETCH_MARKETS_RETURN_VALUE
    fetch_tickers_mocked.return_value = FETCH_TICKERS_RETURN_VALUE
    fetch_balance_mocked.return_value = FETCH_BALANCE_RETURN_VALUE
    fetch_my_trades_mocked.side_effect = FETCH_MY_TRADES_SIDE_EFFECT

    clock = LiveClock()
    events = deque()
    exchanges_api_keys = {
        "BINANCE": {
            "key": None,
            "secret": None,
            "password": None,
        }
    }
    parsers = {"binance": CcxtBinanceParser}
    ccxt_connector = CcxtConnector(
        exchanges_api_keys=exchanges_api_keys, parsers=parsers
    )
    ccxt_broker = CcxtBroker(clock=clock, events=events, ccxt_connector=ccxt_connector)

    # target order sell
    target_order_sell = Order(
        asset=ASSET2,
        action=Action.SELL,
        direction=Direction.LONG,
        size=1,
        signal_id="1",
        target=1.01,
        type=OrderType.TARGET,
        clock=clock,
    )
    ccxt_broker.execute_order(target_order_sell)
    assert len(ccxt_broker.open_orders) == 1
    assert ccxt_broker.open_orders.popleft() == target_order_sell

    ccxt_broker.last_prices[ASSET2] = 1.02
    ccxt_broker.execute_order(target_order_sell)
    assert len(ccxt_broker.open_orders) == 0

    # target order buy
    target_order_buy = Order(
        asset=ASSET2,
        action=Action.BUY,
        direction=Direction.LONG,
        size=1,
        signal_id="1",
        target=1.01,
        type=OrderType.TARGET,
        clock=clock,
    )
    ccxt_broker.last_prices[ASSET2] = 1.13
    ccxt_broker.execute_order(target_order_buy)
    assert len(ccxt_broker.open_orders) == 1
    assert ccxt_broker.open_orders.popleft() == target_order_buy

    ccxt_broker.last_prices[ASSET2] = 0.87
    ccxt_broker.execute_order(target_order_buy)
    assert len(ccxt_broker.open_orders) == 0

    execute_market_order_mocked_calls = [
        call(target_order_sell),
        call(target_order_buy),
    ]
    execute_market_order_mocked.assert_has_calls(execute_market_order_mocked_calls)


@patch("trazy_analysis.broker.ccxt_broker.CcxtBroker.execute_market_order")
@patch("ccxt.binance.fetch_my_trades")
@patch("ccxt.binance.fetch_balance")
@patch("ccxt.binance.fetch_tickers")
@patch("ccxt.binance.fetch_markets")
def test_execute_trailing_stop_order_sell(
    fetch_markets_mocked,
    fetch_tickers_mocked,
    fetch_balance_mocked,
    fetch_my_trades_mocked,
    execute_market_order_mocked,
):
    fetch_markets_mocked.return_value = FETCH_MARKETS_RETURN_VALUE
    fetch_tickers_mocked.return_value = FETCH_TICKERS_RETURN_VALUE
    fetch_balance_mocked.return_value = FETCH_BALANCE_RETURN_VALUE
    fetch_my_trades_mocked.side_effect = FETCH_MY_TRADES_SIDE_EFFECT

    clock = LiveClock()
    events = deque()
    exchanges_api_keys = {
        "BINANCE": {
            "key": None,
            "secret": None,
            "password": None,
        }
    }
    parsers = {"binance": CcxtBinanceParser}
    ccxt_connector = CcxtConnector(
        exchanges_api_keys=exchanges_api_keys, parsers=parsers
    )
    ccxt_broker = CcxtBroker(clock=clock, events=events, ccxt_connector=ccxt_connector)

    trailing_stop_order = Order(
        asset=ASSET2,
        action=Action.SELL,
        direction=Direction.LONG,
        size=1,
        signal_id="1",
        stop_pct=0.05,
        type=OrderType.TRAILING_STOP,
        clock=clock,
    )
    ccxt_broker.last_prices[ASSET2] = 1.11
    ccxt_broker.execute_order(trailing_stop_order)
    assert len(ccxt_broker.open_orders) == 1
    assert ccxt_broker.open_orders.popleft() == trailing_stop_order
    assert trailing_stop_order.order_id is not None
    assert trailing_stop_order.stop == 1.11 - 1.11 * trailing_stop_order.stop_pct

    ccxt_broker.last_prices[ASSET2] = 1.15
    ccxt_broker.execute_order(trailing_stop_order)
    assert len(ccxt_broker.open_orders) == 1
    assert ccxt_broker.open_orders.popleft() == trailing_stop_order
    assert trailing_stop_order.stop == 1.15 - 1.15 * trailing_stop_order.stop_pct

    ccxt_broker.last_prices[ASSET2] = 1.09
    ccxt_broker.execute_order(trailing_stop_order)
    assert len(ccxt_broker.open_orders) == 0

    execute_market_order_mocked_calls = [call(trailing_stop_order)]
    execute_market_order_mocked.assert_has_calls(execute_market_order_mocked_calls)


@patch("trazy_analysis.broker.ccxt_broker.CcxtBroker.execute_market_order")
@patch("ccxt.binance.fetch_my_trades")
@patch("ccxt.binance.fetch_balance")
@patch("ccxt.binance.fetch_tickers")
@patch("ccxt.binance.fetch_markets")
def test_execute_trailing_stop_order_buy(
    fetch_markets_mocked,
    fetch_tickers_mocked,
    fetch_balance_mocked,
    fetch_my_trades_mocked,
    execute_market_order_mocked,
):
    fetch_markets_mocked.return_value = FETCH_MARKETS_RETURN_VALUE
    fetch_tickers_mocked.return_value = FETCH_TICKERS_RETURN_VALUE
    fetch_balance_mocked.return_value = FETCH_BALANCE_RETURN_VALUE
    fetch_my_trades_mocked.side_effect = FETCH_MY_TRADES_SIDE_EFFECT

    clock = LiveClock()
    events = deque()
    exchanges_api_keys = {
        "BINANCE": {
            "key": None,
            "secret": None,
            "password": None,
        }
    }
    parsers = {"binance": CcxtBinanceParser}
    ccxt_connector = CcxtConnector(
        exchanges_api_keys=exchanges_api_keys, parsers=parsers
    )
    ccxt_broker = CcxtBroker(clock=clock, events=events, ccxt_connector=ccxt_connector)

    trailing_stop_order = Order(
        asset=ASSET2,
        action=Action.BUY,
        direction=Direction.LONG,
        size=1,
        signal_id="1",
        stop_pct=0.05,
        type=OrderType.TRAILING_STOP,
        clock=clock,
    )
    ccxt_broker.last_prices[ASSET2] = 1.11
    ccxt_broker.execute_order(trailing_stop_order)
    assert len(ccxt_broker.open_orders) == 1
    assert ccxt_broker.open_orders.popleft() == trailing_stop_order
    assert trailing_stop_order.order_id is not None
    assert trailing_stop_order.stop == 1.11 + 1.11 * trailing_stop_order.stop_pct

    ccxt_broker.last_prices[ASSET2] = 1.02
    ccxt_broker.execute_order(trailing_stop_order)
    assert len(ccxt_broker.open_orders) == 1
    assert ccxt_broker.open_orders.popleft() == trailing_stop_order
    assert trailing_stop_order.stop == 1.02 + 1.02 * trailing_stop_order.stop_pct

    ccxt_broker.last_prices[ASSET2] = 1.072
    ccxt_broker.execute_order(trailing_stop_order)
    assert len(ccxt_broker.open_orders) == 0

    execute_market_order_mocked_calls = [call(trailing_stop_order)]
    execute_market_order_mocked.assert_has_calls(execute_market_order_mocked_calls)


@patch("trazy_analysis.broker.ccxt_broker.CcxtBroker.update_balances_and_positions")
@patch("trazy_analysis.broker.ccxt_broker.CcxtBroker.update_lot_size_info")
@patch("trazy_analysis.broker.ccxt_broker.CcxtBroker.update_transactions")
@patch("trazy_analysis.broker.ccxt_broker.CcxtBroker.update_price")
def test_synchronize(
    update_price_mocked,
    update_transactions_mocked,
    update_lot_size_info_mocked,
    update_balances_and_positions_mocked,
):
    clock = LiveClock()
    events = deque()
    exchanges_api_keys = {
        "BINANCE": {
            "key": None,
            "secret": None,
            "password": None,
        }
    }
    parsers = {"binance": CcxtBinanceParser}
    ccxt_connector = CcxtConnector(
        exchanges_api_keys=exchanges_api_keys, parsers=parsers
    )
    ccxt_broker = CcxtBroker(clock=clock, events=events, ccxt_connector=ccxt_connector)
    ccxt_broker.synchronize()

    update_price_mocked_calls = [call(), call()]
    update_price_mocked.assert_has_calls(update_price_mocked_calls)
    update_transactions_mocked_calls = [call(), call()]
    update_transactions_mocked.assert_has_calls(update_transactions_mocked_calls)
    update_lot_size_info_mocked_calls = [call(), call()]
    update_lot_size_info_mocked.assert_has_calls(update_lot_size_info_mocked_calls)
    update_balances_and_positions_mocked_calls = [call(), call()]
    update_balances_and_positions_mocked.assert_has_calls(
        update_balances_and_positions_mocked_calls
    )


@patch("ccxt.binance.fetch_my_trades")
@patch("ccxt.binance.fetch_balance")
@patch("ccxt.binance.fetch_tickers")
@patch("ccxt.binance.fetch_markets")
def test_max_order_entry_size(
    fetch_markets_mocked,
    fetch_tickers_mocked,
    fetch_balance_mocked,
    fetch_my_trades_mocked,
):
    fetch_markets_mocked.return_value = FETCH_MARKETS_RETURN_VALUE
    fetch_tickers_mocked.return_value = FETCH_TICKERS_RETURN_VALUE
    fetch_balance_mocked.return_value = FETCH_BALANCE_RETURN_VALUE
    fetch_my_trades_mocked.side_effect = FETCH_MY_TRADES_SIDE_EFFECT

    clock = LiveClock()
    events = deque()
    exchanges_api_keys = {
        "BINANCE": {
            "key": None,
            "secret": None,
            "password": None,
        }
    }
    parsers = {"binance": CcxtBinanceParser}
    fee_models = {"binance": BinanceFeeModel()}
    ccxt_connector = CcxtConnector(
        exchanges_api_keys=exchanges_api_keys, parsers=parsers, fee_models=fee_models
    )
    ccxt_broker = CcxtBroker(clock=clock, events=events, ccxt_connector=ccxt_connector)

    assert (
        ccxt_broker.max_entry_order_size(asset=ASSET2, direction=Direction.LONG)
        == 130.4863423021991
    )
