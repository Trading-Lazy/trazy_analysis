from collections import deque
from datetime import datetime
from decimal import Decimal
from unittest.mock import call, patch

import pytest
from freezegun import freeze_time
from pytz import timezone

from trazy_analysis.broker.binance_broker import BinanceBroker
from trazy_analysis.common.clock import LiveClock
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.enums import Action, Direction, OrderType
from trazy_analysis.models.order import Order
from trazy_analysis.portfolio.portfolio_event import PortfolioEvent
from trazy_analysis.position.position import Position

INITIAL_CASH = "51.07118"

EXCHANGE = "BINANCE"
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


GET_ACCOUNT_RETURN_VALUE = {
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
        {"asset": "SXP", "free": "0.00000000", "locked": "0.00000000"},
        {"asset": "BTC", "free": "0.00000000", "locked": "0.00000000"},
        {"asset": "LINK", "free": "0.00000000", "locked": "0.00000000"},
        {"asset": "XRP", "free": "-29.47050000", "locked": "0.00000000"},
    ],
    "permissions": ["SPOT"],
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

GET_ALL_TICKERS_RETURN_VALUE = [
    {"symbol": "ETHEUR", "price": "1353.12000000"},
    {"symbol": "SXPEUR", "price": "1.90400000"},
    {"symbol": "BTCEUR", "price": "40825.94000000"},
    {"symbol": "LINKEUR", "price": "22.55000000"},
    {"symbol": "XRPEUR", "price": "0.39100000"},
]

GET_MY_TRADES_SIDE_EFFECT = [
    [
        {
            "symbol": "XRPEUR",
            "id": 6160298,
            "orderId": 131247881,
            "orderListId": -1,
            "price": "0.33822000",
            "qty": "29.50000000",
            "quoteQty": "9.97749000",
            "commission": "0.02950000",
            "commissionAsset": "XRP",
            "time": 1614080757873,
            "isBuyer": True,
            "isMaker": False,
            "isBestMatch": True,
        },
        {
            "symbol": "XRPEUR",
            "id": 6217956,
            "orderId": 132141706,
            "orderListId": -1,
            "price": "0.38805000",
            "qty": "29.40000000",
            "quoteQty": "11.40867000",
            "commission": "0.00000000",
            "commissionAsset": "EUR",
            "time": 1614115784610,
            "isBuyer": False,
            "isMaker": True,
            "isBestMatch": True,
        },
    ],
    [
        {
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
        {
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
    ],
]

GET_EXCHANGE_INFO_RETURN_VALUE = {
    "timezone": "UTC",
    "serverTime": 1614203242528,
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
        {
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
        {
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
        {
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
        {
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
    ],
}

MARKET_BUY_ORDER_RESPONSE = {
    "symbol": "SXPEUR",
    "orderId": 134562464,
    "orderListId": -1,
    "clientOrderId": "bOxEy1MFegaDEqkiQhF3Xg",
    "transactTime": 1614237068817,
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
            "tradeId": 6324526,
        }
    ],
}

MARKET_BUY_ORDER_RESPONSE_ORDER_NOT_FILLED = {
    "symbol": "SXPEUR",
    "orderId": 134562468,
    "orderListId": -1,
    "clientOrderId": "bOxEy1MFegaDEqkiQhF3Xh",
    "transactTime": 1614237068827,
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
            "tradeId": 6324529,
        }
    ],
}

MARKET_SELL_ORDER_RESPONSE = {
    "symbol": "SXPEUR",
    "orderId": 134791954,
    "orderListId": -1,
    "clientOrderId": "KcFWfDoNXNhKnJGiIjUQJn",
    "transactTime": 1614248929811,
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
            "tradeId": 6334009,
        }
    ],
}

MARKET_SELL_ORDER_RESPONSE_NOT_FILLED = {
    "symbol": "SXPEUR",
    "orderId": 134791962,
    "orderListId": -1,
    "clientOrderId": "KcFWfDoNXNhKnJGiIjUQJp",
    "transactTime": 1614248929911,
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
            "tradeId": 6334018,
        }
    ],
}

LIMIT_BUY_ORDER_RESPONSE = {
    "symbol": "SXPEUR",
    "orderId": 134879100,
    "orderListId": -1,
    "clientOrderId": "2rPZy0qJCcuX0r2fVjG5s7",
    "transactTime": 1614254004034,
    "price": "0.35340000",
    "origQty": "29.00000000",
    "executedQty": "0.00000000",
    "cummulativeQuoteQty": "0.00000000",
    "status": "NEW",
    "timeInForce": "GTC",
    "type": "LIMIT",
    "side": "BUY",
    "fills": [],
}

LIMIT_SELL_ORDER_RESPONSE = {
    "symbol": "XRPEUR",
    "orderId": 134912358,
    "orderListId": -1,
    "clientOrderId": "UH2GvNOuXQtBFLDLc6fr1b",
    "transactTime": 1614255967741,
    "price": "0.45340000",
    "origQty": "26.30000000",
    "executedQty": "0.00000000",
    "cummulativeQuoteQty": "0.00000000",
    "status": "NEW",
    "timeInForce": "GTC",
    "type": "LIMIT",
    "side": "SELL",
    "fills": [],
}


@patch("trazy_analysis.broker.binance_broker.BinanceBroker.update_transactions")
@patch(
    "trazy_analysis.broker.binance_broker.BinanceBroker.update_balances_and_positions"
)
@patch("trazy_analysis.broker.binance_broker.BinanceBroker.update_price")
@patch("binance.client.Client.get_exchange_info")
@patch("binance.client.Client.__init__")
def test_lot_size_info(
    init_mocked,
    get_exchange_info_mocked,
    update_price_mocked,
    update_balances_mocked,
    update_transactions_mocked,
):
    init_mocked.return_value = None
    get_exchange_info_mocked.return_value = GET_EXCHANGE_INFO_RETURN_VALUE
    clock = LiveClock()
    events = deque()
    binance_broker = BinanceBroker(clock=clock, events=events)
    assert binance_broker.lot_size == {
        Asset(symbol="BTC/EUR", exchange=EXCHANGE): 0.000001,
        Asset(symbol="ETH/EUR", exchange=EXCHANGE): 0.00001,
        Asset(symbol="LINK/EUR", exchange=EXCHANGE): 0.001,
        Asset(symbol="SXP/EUR", exchange=EXCHANGE): 0.001,
        Asset(symbol="XRP/EUR", exchange=EXCHANGE): 0.1,
    }

    assert isinstance(binance_broker.lot_size_last_update, datetime)
    timestamp = binance_broker.lot_size_last_update

    # Call again to see if the value is cached
    binance_broker.update_lot_size_info()
    assert binance_broker.lot_size_last_update == timestamp


@patch("trazy_analysis.broker.binance_broker.BinanceBroker.update_transactions")
@patch(
    "trazy_analysis.broker.binance_broker.BinanceBroker.update_balances_and_positions"
)
@patch("binance.client.Client.get_all_tickers")
@patch("binance.client.Client.get_exchange_info")
@patch("binance.client.Client.__init__")
def test_update_price(
    init_mocked,
    get_exchange_info_mocked,
    get_all_tickers_mocked,
    update_balances_mocked,
    update_transactions_mocked,
):
    init_mocked.return_value = None
    get_exchange_info_mocked.return_value = GET_EXCHANGE_INFO_RETURN_VALUE
    get_all_tickers_mocked.return_value = GET_ALL_TICKERS_RETURN_VALUE
    clock = LiveClock()
    events = deque()
    binance_broker = BinanceBroker(clock=clock, events=events)
    assert binance_broker.last_prices == {
        Asset(symbol="BTC/EUR", exchange=EXCHANGE): 40825.94,
        Asset(symbol="ETH/EUR", exchange=EXCHANGE): 1353.12,
        Asset(symbol="LINK/EUR", exchange=EXCHANGE): 22.55,
        Asset(symbol="SXP/EUR", exchange=EXCHANGE): 1.904,
        Asset(symbol="XRP/EUR", exchange=EXCHANGE): 0.391,
    }

    assert isinstance(binance_broker.price_last_update, datetime)
    timestamp = binance_broker.price_last_update

    # Call again to see if the value is cached
    binance_broker.update_price()
    assert binance_broker.price_last_update == timestamp


@patch("trazy_analysis.broker.binance_broker.BinanceBroker.update_transactions")
@patch("binance.client.Client.get_all_tickers")
@patch("binance.client.Client.get_account")
@patch("binance.client.Client.get_exchange_info")
@patch("binance.client.Client.__init__")
def test_update_balances_and_positions(
    init_mocked,
    get_exchange_info_mocked,
    get_account_mocked,
    get_all_tickers_mocked,
    update_transactions_mocked,
):
    init_mocked.return_value = None
    get_exchange_info_mocked.side_effect = [GET_EXCHANGE_INFO_RETURN_VALUE] * 2
    get_all_tickers_mocked.return_value = GET_ALL_TICKERS_RETURN_VALUE
    get_account_mocked.return_value = GET_ACCOUNT_RETURN_VALUE
    clock = LiveClock()
    events = deque()
    binance_broker = BinanceBroker(clock=clock, events=events)

    assert binance_broker.cash_balances == {
        "EUR": float(INITIAL_CASH),
        "USDT": 0.0,
    }
    assert isinstance(binance_broker.balances_last_update, datetime)

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

    assert binance_broker.portfolio.pos_handler.positions == expected_positions

    # Call again to see if the value is cached
    timestamp = binance_broker.balances_last_update
    binance_broker.update_balances_and_positions()
    assert binance_broker.balances_last_update == timestamp

    assert binance_broker.get_cash_balance() == {
        "EUR": float(INITIAL_CASH),
        "USDT": 0.0,
    }

    assert binance_broker.get_cash_balance("EUR") == float(INITIAL_CASH)

    with pytest.raises(ValueError):
        assert binance_broker.get_cash_balance("GBP")

    get_account_mocked.assert_has_calls([call()])
    get_all_tickers_mocked.assert_has_calls([call()])


@patch("trazy_analysis.broker.binance_broker.BinanceBroker.update_transactions")
@patch("binance.client.Client.get_all_tickers")
@patch("binance.client.Client.get_exchange_info")
@patch("binance.client.Client.get_account")
@patch("binance.client.Client.__init__")
def test_has_opened_position(
    init_mocked,
    get_account_mocked,
    get_exchange_info_mocked,
    get_all_tickers_mocked,
    update_transactions_mocked,
):
    init_mocked.return_value = None
    get_exchange_info_mocked.return_value = GET_EXCHANGE_INFO_RETURN_VALUE
    get_all_tickers_mocked.return_value = GET_ALL_TICKERS_RETURN_VALUE
    get_account_mocked.return_value = GET_ACCOUNT_RETURN_VALUE_CRYPTO_DUST

    clock = LiveClock()
    events = deque()
    binance_broker = BinanceBroker(clock=clock, events=events)

    assert binance_broker.has_opened_position(ASSET1, Direction.LONG)
    assert not binance_broker.has_opened_position(ASSET1, Direction.SHORT)
    assert binance_broker.has_opened_position(ASSET2, Direction.SHORT)
    assert not binance_broker.has_opened_position(ASSET2, Direction.LONG)
    assert not binance_broker.has_opened_position(ASSET3, Direction.LONG)
    assert not binance_broker.has_opened_position(ASSET3, Direction.SHORT)


@patch("trazy_analysis.common.clock.LiveClock.current_time")
@patch("binance.client.Client.get_my_trades")
@patch("binance.client.Client.get_all_tickers")
@patch("binance.client.Client.get_exchange_info")
@patch("binance.client.Client.get_account")
@patch("binance.client.Client.__init__")
@freeze_time("2021-02-22 11:45:57+00:00")
def test_update_transactions(
    init_mocked,
    get_account_mocked,
    get_exchange_info_mocked,
    get_all_tickers_mocked,
    get_my_trades_mocked,
    current_time_mocked,
):
    init_mocked.return_value = None
    get_exchange_info_mocked.return_value = GET_EXCHANGE_INFO_RETURN_VALUE
    get_all_tickers_mocked.return_value = GET_ALL_TICKERS_RETURN_VALUE
    get_account_mocked.return_value = GET_ACCOUNT_RETURN_VALUE
    get_my_trades_mocked.side_effect = GET_MY_TRADES_SIDE_EFFECT

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
    binance_broker = BinanceBroker(clock=clock, events=events)
    assert isinstance(binance_broker.transactions_last_update, datetime)

    # Call again to see if the value is cached
    timestamp = binance_broker.transactions_last_update
    binance_broker.update_transactions()
    assert binance_broker.transactions_last_update == timestamp

    assert len(binance_broker.portfolio.history) == 4
    assert binance_broker.portfolio.history[0] == PortfolioEvent(
        timestamp=datetime.strptime(
            "2021-02-23 11:45:57.873000+0000", "%Y-%m-%d %H:%M:%S.%f%z"
        ),
        type="symbol_transaction",
        description="BUY LONG 29.5 BINANCE-XRPEUR-0:01:00 0.33822 23/02/2021",
        debit=10.006990000000002,
        credit=0.0,
        balance=51.07118,
    )
    assert binance_broker.portfolio.history[1] == PortfolioEvent(
        timestamp=datetime.strptime(
            "2021-02-23 21:29:44.610000+0000", "%Y-%m-%d %H:%M:%S.%f%z"
        ),
        type="symbol_transaction",
        description="SELL LONG 29.4 BINANCE-XRPEUR-0:01:00 0.38805 23/02/2021",
        debit=0.0,
        credit=11.41,
        balance=51.07,
    )
    assert binance_broker.portfolio.history[2] == PortfolioEvent(
        timestamp=datetime.strptime(
            "2021-02-23 11:45:59.873000+0000", "%Y-%m-%d %H:%M:%S.%f%z"
        ),
        type="symbol_transaction",
        description="BUY LONG 0.000367 BINANCE-ETHEUR-0:01:00 40825.94 23/02/2021",
        debit=15.01261998,
        credit=0.0,
        balance=51.07118,
    )
    assert binance_broker.portfolio.history[3] == PortfolioEvent(
        timestamp=datetime.strptime(
            "2021-02-23 21:29:49.610000+0000", "%Y-%m-%d %H:%M:%S.%f%z"
        ),
        type="symbol_transaction",
        description="SELL LONG 0.000367 BINANCE-ETHEUR-0:01:00 43825.94 23/02/2021",
        debit=0.0,
        credit=16.08,
        balance=51.07,
    )

    get_account_mocked.assert_has_calls([call()])
    get_all_tickers_mocked.assert_has_calls([call()])

    get_my_trades_mocked_calls = [
        call(startTime=1612796880000, symbol="ETHEUR"),
        call(startTime=1612796880000, symbol="XRPEUR"),
    ]
    get_my_trades_mocked.assaert_has_calls(get_my_trades_mocked_calls, any_order=True)


@patch("binance.client.Client.order_market_sell")
@patch("binance.client.Client.order_market_buy")
@patch("binance.client.Client.get_my_trades")
@patch("binance.client.Client.get_all_tickers")
@patch("binance.client.Client.get_exchange_info")
@patch("binance.client.Client.get_account")
@patch("binance.client.Client.__init__")
def test_execute_market_order(
    init_mocked,
    get_account_mocked,
    get_exchange_info_mocked,
    get_all_tickers_mocked,
    get_my_trades_mocked,
    order_market_buy_mocked,
    order_market_sell_mocked,
):
    init_mocked.return_value = None
    get_exchange_info_mocked.return_value = GET_EXCHANGE_INFO_RETURN_VALUE
    get_all_tickers_mocked.return_value = GET_ALL_TICKERS_RETURN_VALUE
    get_account_mocked.return_value = GET_ACCOUNT_RETURN_VALUE
    get_my_trades_mocked.side_effect = GET_MY_TRADES_SIDE_EFFECT
    order_market_buy_mocked.side_effect = [
        MARKET_BUY_ORDER_RESPONSE,
        MARKET_BUY_ORDER_RESPONSE_ORDER_NOT_FILLED,
        Exception(),
    ]
    order_market_sell_mocked.side_effect = [
        MARKET_SELL_ORDER_RESPONSE,
        MARKET_SELL_ORDER_RESPONSE_NOT_FILLED,
    ]

    clock = LiveClock()
    events = deque()
    binance_broker = BinanceBroker(clock=clock, events=events)

    # test buy orders
    buy_order = Order(
        asset=ASSET3,
        action=Action.BUY,
        direction=Direction.LONG,
        size=26.97654,
        signal_id="1",
        type=OrderType.MARKET,
        clock=clock,
    )

    assert binance_broker.currency_pairs_traded == {
        Asset(symbol="XRP/EUR", exchange=EXCHANGE),
        Asset(symbol="ETH/EUR", exchange=EXCHANGE),
    }
    binance_broker.execute_order(buy_order)
    assert buy_order.order_id == "134562464"
    assert binance_broker.currency_pairs_traded == {
        Asset(symbol="XRP/EUR", exchange=EXCHANGE),
        Asset(symbol="ETH/EUR", exchange=EXCHANGE),
        Asset(symbol="SXP/EUR", exchange=EXCHANGE),
    }
    # Order submitted to the broker but not filled yet
    binance_broker.execute_order(buy_order)
    assert binance_broker.open_orders_ids == {"134562468"}

    # test binance buyorder Exception
    binance_broker.execute_order(buy_order)

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

    assert binance_broker.currency_pairs_traded == {
        Asset(symbol="XRP/EUR", exchange=EXCHANGE),
        Asset(symbol="ETH/EUR", exchange=EXCHANGE),
        Asset(symbol="SXP/EUR", exchange=EXCHANGE),
    }
    binance_broker.execute_order(sell_order)
    assert sell_order.order_id == "134791954"
    assert binance_broker.currency_pairs_traded == {
        Asset(symbol="XRP/EUR", exchange=EXCHANGE),
        Asset(symbol="ETH/EUR", exchange=EXCHANGE),
        Asset(symbol="SXP/EUR", exchange=EXCHANGE),
    }
    # Order submitted to the broker but not filled yet
    binance_broker.execute_order(sell_order)
    assert binance_broker.open_orders_ids == {"134562468", "134791962"}

    # check mock calls
    order_market_buy_mocked_calls = [
        call(symbol=ASSET3.symbol, quantity=Decimal("26.976")),
        call(symbol=ASSET3.symbol, quantity=Decimal("26.976")),
        call(symbol=ASSET3.symbol, quantity=Decimal("26.976")),
    ]
    order_market_buy_mocked.assert_has_calls(order_market_buy_mocked_calls)

    order_market_sell_mocked_calls = [
        call(symbol=ASSET3.symbol, quantity=Decimal("26.776")),
        call(symbol=ASSET3.symbol, quantity=Decimal("26.776")),
    ]
    order_market_sell_mocked.assert_has_calls(order_market_sell_mocked_calls)


@patch("binance.client.Client.order_limit_sell")
@patch("binance.client.Client.order_limit_buy")
@patch("binance.client.Client.get_my_trades")
@patch("binance.client.Client.get_all_tickers")
@patch("binance.client.Client.get_exchange_info")
@patch("binance.client.Client.get_account")
@patch("binance.client.Client.__init__")
def test_execute_limit_order(
    init_mocked,
    get_account_mocked,
    get_exchange_info_mocked,
    get_all_tickers_mocked,
    get_my_trades_mocked,
    order_limit_buy_mocked,
    order_limit_sell_mocked,
):
    init_mocked.return_value = None
    get_exchange_info_mocked.return_value = GET_EXCHANGE_INFO_RETURN_VALUE
    get_all_tickers_mocked.return_value = GET_ALL_TICKERS_RETURN_VALUE
    get_account_mocked.return_value = GET_ACCOUNT_RETURN_VALUE
    get_my_trades_mocked.side_effect = GET_MY_TRADES_SIDE_EFFECT
    order_limit_buy_mocked.side_effect = [LIMIT_BUY_ORDER_RESPONSE, Exception()]
    order_limit_sell_mocked.return_value = LIMIT_SELL_ORDER_RESPONSE

    clock = LiveClock()
    events = deque()
    binance_broker = BinanceBroker(clock=clock, events=events)

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

    assert binance_broker.currency_pairs_traded == {
        Asset(symbol="XRP/EUR", exchange=EXCHANGE),
        Asset(symbol="ETH/EUR", exchange=EXCHANGE),
    }
    binance_broker.execute_order(buy_order)
    assert buy_order.order_id == "134879100"
    assert binance_broker.currency_pairs_traded == {
        Asset(symbol="XRP/EUR", exchange=EXCHANGE),
        Asset(symbol="ETH/EUR", exchange=EXCHANGE),
    }
    assert binance_broker.open_orders_ids == {"134879100"}

    # test binance buyorder Exception
    binance_broker.execute_order(buy_order)

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

    assert binance_broker.currency_pairs_traded == {
        Asset(symbol="XRP/EUR", exchange=EXCHANGE),
        Asset(symbol="ETH/EUR", exchange=EXCHANGE),
    }
    binance_broker.execute_order(sell_order)
    assert sell_order.order_id == "134912358"
    assert binance_broker.currency_pairs_traded == {
        Asset(symbol="XRP/EUR", exchange=EXCHANGE),
        Asset(symbol="ETH/EUR", exchange=EXCHANGE),
    }
    assert binance_broker.open_orders_ids == {"134912358", "134879100"}

    # check mock calls
    order_limit_buy_mocked_calls = [
        call(price=0.3534, quantity=Decimal("26.97654"), symbol="ETH/EUR"),
        call(price=0.3534, quantity=Decimal("26.97654"), symbol="ETH/EUR"),
    ]
    order_limit_buy_mocked.assert_has_calls(order_limit_buy_mocked_calls)

    order_limit_sell_mocked_calls = [
        call(price=0.4534, quantity=Decimal("0.15067"), symbol="ETH/EUR")
    ]
    order_limit_sell_mocked.assert_has_calls(order_limit_sell_mocked_calls)


@patch("trazy_analysis.broker.binance_broker.BinanceBroker.execute_market_order")
@patch("binance.client.Client.get_my_trades")
@patch("binance.client.Client.get_all_tickers")
@patch("binance.client.Client.get_exchange_info")
@patch("binance.client.Client.get_account")
@patch("binance.client.Client.__init__")
def test_execute_stop_order(
    init_mocked,
    get_account_mocked,
    get_exchange_info_mocked,
    get_all_tickers_mocked,
    get_my_trades_mocked,
    execute_market_order_mocked,
):
    init_mocked.return_value = None
    get_exchange_info_mocked.return_value = GET_EXCHANGE_INFO_RETURN_VALUE
    get_all_tickers_mocked.return_value = GET_ALL_TICKERS_RETURN_VALUE
    get_account_mocked.return_value = GET_ACCOUNT_RETURN_VALUE
    get_my_trades_mocked.side_effect = GET_MY_TRADES_SIDE_EFFECT

    clock = LiveClock()
    events = deque()
    binance_broker = BinanceBroker(clock=clock, events=events)

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
    binance_broker.execute_order(stop_order_sell)
    assert len(binance_broker.open_orders) == 1
    assert binance_broker.open_orders.popleft() == stop_order_sell

    binance_broker.last_prices[ASSET2] = 0.18
    binance_broker.execute_order(stop_order_sell)
    assert len(binance_broker.open_orders) == 0

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
    binance_broker.execute_order(stop_order_buy)
    assert len(binance_broker.open_orders) == 1
    assert binance_broker.open_orders.popleft() == stop_order_buy

    binance_broker.last_prices[ASSET2] = 0.46
    binance_broker.execute_order(stop_order_buy)
    assert len(binance_broker.open_orders) == 0

    execute_market_order_mocked_calls = [call(stop_order_sell), call(stop_order_buy)]
    execute_market_order_mocked.assert_has_calls(execute_market_order_mocked_calls)


@patch("trazy_analysis.broker.binance_broker.BinanceBroker.execute_market_order")
@patch("binance.client.Client.get_my_trades")
@patch("binance.client.Client.get_all_tickers")
@patch("binance.client.Client.get_exchange_info")
@patch("binance.client.Client.get_account")
@patch("binance.client.Client.__init__")
def test_execute_target_order(
    init_mocked,
    get_account_mocked,
    get_exchange_info_mocked,
    get_all_tickers_mocked,
    get_my_trades_mocked,
    execute_market_order_mocked,
):
    init_mocked.return_value = None
    get_exchange_info_mocked.return_value = GET_EXCHANGE_INFO_RETURN_VALUE
    get_all_tickers_mocked.return_value = GET_ALL_TICKERS_RETURN_VALUE
    get_account_mocked.return_value = GET_ACCOUNT_RETURN_VALUE
    get_my_trades_mocked.side_effect = GET_MY_TRADES_SIDE_EFFECT

    clock = LiveClock()
    events = deque()
    binance_broker = BinanceBroker(clock=clock, events=events)

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
    binance_broker.execute_order(target_order_sell)
    assert len(binance_broker.open_orders) == 1
    assert binance_broker.open_orders.popleft() == target_order_sell

    binance_broker.last_prices[ASSET2] = 1.02
    binance_broker.execute_order(target_order_sell)
    assert len(binance_broker.open_orders) == 0

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
    binance_broker.last_prices[ASSET2] = 1.13
    binance_broker.execute_order(target_order_buy)
    assert len(binance_broker.open_orders) == 1
    assert binance_broker.open_orders.popleft() == target_order_buy

    binance_broker.last_prices[ASSET2] = 0.87
    binance_broker.execute_order(target_order_buy)
    assert len(binance_broker.open_orders) == 0

    execute_market_order_mocked_calls = [
        call(target_order_sell),
        call(target_order_buy),
    ]
    execute_market_order_mocked.assert_has_calls(execute_market_order_mocked_calls)


@patch("trazy_analysis.broker.binance_broker.BinanceBroker.execute_market_order")
@patch("binance.client.Client.get_my_trades")
@patch("binance.client.Client.get_all_tickers")
@patch("binance.client.Client.get_exchange_info")
@patch("binance.client.Client.get_account")
@patch("binance.client.Client.__init__")
def test_execute_trailing_stop_order_sell(
    init_mocked,
    get_account_mocked,
    get_exchange_info_mocked,
    get_all_tickers_mocked,
    get_my_trades_mocked,
    execute_market_order_mocked,
):
    init_mocked.return_value = None
    get_exchange_info_mocked.return_value = GET_EXCHANGE_INFO_RETURN_VALUE
    get_all_tickers_mocked.return_value = GET_ALL_TICKERS_RETURN_VALUE
    get_account_mocked.return_value = GET_ACCOUNT_RETURN_VALUE
    get_my_trades_mocked.side_effect = GET_MY_TRADES_SIDE_EFFECT
    clock = LiveClock()
    events = deque()
    binance_broker = BinanceBroker(clock=clock, events=events)

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
    binance_broker.last_prices[ASSET2] = 1.11
    binance_broker.execute_order(trailing_stop_order)
    assert len(binance_broker.open_orders) == 1
    assert binance_broker.open_orders.popleft() == trailing_stop_order
    assert trailing_stop_order.order_id is not None
    assert trailing_stop_order.stop == 1.11 - 1.11 * trailing_stop_order.stop_pct

    binance_broker.last_prices[ASSET2] = 1.15
    binance_broker.execute_order(trailing_stop_order)
    assert len(binance_broker.open_orders) == 1
    assert binance_broker.open_orders.popleft() == trailing_stop_order
    assert trailing_stop_order.stop == 1.15 - 1.15 * trailing_stop_order.stop_pct

    binance_broker.last_prices[ASSET2] = 1.09
    binance_broker.execute_order(trailing_stop_order)
    assert len(binance_broker.open_orders) == 0

    execute_market_order_mocked_calls = [call(trailing_stop_order)]
    execute_market_order_mocked.assert_has_calls(execute_market_order_mocked_calls)


@patch("trazy_analysis.broker.binance_broker.BinanceBroker.execute_market_order")
@patch("binance.client.Client.get_my_trades")
@patch("binance.client.Client.get_all_tickers")
@patch("binance.client.Client.get_exchange_info")
@patch("binance.client.Client.get_account")
@patch("binance.client.Client.__init__")
def test_execute_trailing_stop_order_buy(
    init_mocked,
    get_account_mocked,
    get_exchange_info_mocked,
    get_all_tickers_mocked,
    get_my_trades_mocked,
    execute_market_order_mocked,
):
    init_mocked.return_value = None
    get_exchange_info_mocked.return_value = GET_EXCHANGE_INFO_RETURN_VALUE
    get_all_tickers_mocked.return_value = GET_ALL_TICKERS_RETURN_VALUE
    get_account_mocked.return_value = GET_ACCOUNT_RETURN_VALUE
    get_my_trades_mocked.side_effect = GET_MY_TRADES_SIDE_EFFECT
    clock = LiveClock()
    events = deque()
    binance_broker = BinanceBroker(clock=clock, events=events)

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
    binance_broker.last_prices[ASSET2] = 1.11
    binance_broker.execute_order(trailing_stop_order)
    assert len(binance_broker.open_orders) == 1
    assert binance_broker.open_orders.popleft() == trailing_stop_order
    assert trailing_stop_order.order_id is not None
    assert trailing_stop_order.stop == 1.11 + 1.11 * trailing_stop_order.stop_pct

    binance_broker.last_prices[ASSET2] = 1.02
    binance_broker.execute_order(trailing_stop_order)
    assert len(binance_broker.open_orders) == 1
    assert binance_broker.open_orders.popleft() == trailing_stop_order
    assert trailing_stop_order.stop == 1.02 + 1.02 * trailing_stop_order.stop_pct

    binance_broker.last_prices[ASSET2] = 1.072
    binance_broker.execute_order(trailing_stop_order)
    assert len(binance_broker.open_orders) == 0

    execute_market_order_mocked_calls = [call(trailing_stop_order)]
    execute_market_order_mocked.assert_has_calls(execute_market_order_mocked_calls)


@patch(
    "trazy_analysis.broker.binance_broker.BinanceBroker.update_balances_and_positions"
)
@patch("trazy_analysis.broker.binance_broker.BinanceBroker.update_lot_size_info")
@patch("trazy_analysis.broker.binance_broker.BinanceBroker.update_transactions")
@patch("trazy_analysis.broker.binance_broker.BinanceBroker.update_price")
@patch("binance.client.Client.__init__")
def test_synchronize(
    init_mocked,
    update_price_mocked,
    update_transactions_mocked,
    update_lot_size_info_mocked,
    update_balances_and_positions_mocked,
):
    init_mocked.return_value = None
    clock = LiveClock()
    events = deque()
    binance_broker = BinanceBroker(clock=clock, events=events)

    binance_broker.synchronize()

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


@patch("binance.client.Client.get_my_trades")
@patch("binance.client.Client.get_all_tickers")
@patch("binance.client.Client.get_exchange_info")
@patch("binance.client.Client.get_account")
@patch("binance.client.Client.__init__")
def test_max_order_entry_size(
    init_mocked,
    get_account_mocked,
    get_exchange_info_mocked,
    get_all_tickers_mocked,
    get_my_trades_mocked,
):
    init_mocked.return_value = None
    get_exchange_info_mocked.return_value = GET_EXCHANGE_INFO_RETURN_VALUE
    get_all_tickers_mocked.return_value = GET_ALL_TICKERS_RETURN_VALUE
    get_account_mocked.return_value = GET_ACCOUNT_RETURN_VALUE
    get_my_trades_mocked.side_effect = GET_MY_TRADES_SIDE_EFFECT
    clock = LiveClock()
    events = deque()

    binance_broker = BinanceBroker(clock=clock, events=events)

    assert (
        binance_broker.max_entry_order_size(asset=ASSET2, direction=Direction.LONG)
        == 130.4863423021991
    )
