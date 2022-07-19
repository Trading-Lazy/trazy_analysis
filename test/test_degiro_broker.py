from collections import deque
from datetime import datetime, timedelta
from unittest.mock import call, patch

import pytest
from pytz import timezone

from trazy_analysis.broker import degiroapi
from trazy_analysis.broker.degiro_broker import DegiroBroker
from trazy_analysis.common.clock import LiveClock
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.enums import Action, Direction, OrderType
from trazy_analysis.models.order import Order
from trazy_analysis.portfolio.portfolio_event import PortfolioEvent
from trazy_analysis.position.position import Position

INITIAL_CASH = "125.08"

GET_DATA_CASH_MOCKED_RETURN_VALUE = ["EUR " + INITIAL_CASH]

PRODUCT_ID1 = 13479113
PRODUCT_ID2 = 867464
PRODUCT_ID3 = 331868

GET_DATA_PORTFOLIO_MOCKED_RETURN_VALUE = [
    {
        "id": str(PRODUCT_ID1),
        "positionType": "PRODUCT",
        "size": 1,
        "price": 3.7,
        "value": 3.7,
        "breakEvenPrice": 3.739,
    },
    {
        "id": str(PRODUCT_ID2),
        "positionType": "PRODUCT",
        "size": 2,
        "price": 636.9,
        "value": 1273.8,
        "breakEvenPrice": 487.7,
    },
    {
        "id": str(PRODUCT_ID3),
        "positionType": "PRODUCT",
        "size": -3,
        "price": 135.03,
        "value": -405.09,
        "breakEvenPrice": 135.03,
    },
    {
        "id": "FLATEX_EUR",
        "positionType": "CASH",
        "size": 121.08,
        "price": 1,
        "value": 121.08,
        "breakEvenPrice": 0,
    },
]

EXCHANGE = "DEGIRO"
SYMBOL1 = "IFMK"
ASSET1 = Asset(symbol=SYMBOL1, exchange=EXCHANGE)
SYMBOL2 = "LQQ"
ASSET2 = Asset(symbol=SYMBOL2, exchange=EXCHANGE)
SYMBOL3 = "AAPL"
ASSET3 = Asset(symbol=SYMBOL3, exchange=EXCHANGE)

PRODUCT_INFO_IFMK = {
    "id": str(PRODUCT_ID1),
    "name": "iFresh",
    "isin": "US4495381075",
    "symbol": SYMBOL1,
    "contractSize": 1.0,
    "productType": "STOCK",
    "productTypeId": 1,
    "tradable": True,
    "category": "D",
    "currency": "EUR",
    "strikePrice": -0.0001,
    "exchangeId": "710",
    "onlyEodPrices": False,
    "orderTimeTypes": ["DAY", "GTC"],
    "buyOrderTypes": ["LIMIT", "MARKET", "STOPLOSS", "STOPLIMIT"],
    "sellOrderTypes": ["LIMIT", "MARKET", "STOPLOSS", "STOPLIMIT"],
    "productBitTypes": [],
    "closePrice": 3.7,
    "closePriceDate": "2021-02-09",
    "feedQuality": "R",
    "orderBookDepth": 0,
    "vwdIdentifierType": "issueid",
    "vwdId": "360115931",
    "qualitySwitchable": False,
    "qualitySwitchFree": False,
    "vwdModuleId": 1,
}

PRODUCT_INFO_AAPL = {
    "id": str(PRODUCT_ID3),
    "name": "Apple",
    "isin": "US0378331005",
    "symbol": SYMBOL3,
    "contractSize": 1.0,
    "productType": "ETF",
    "productTypeId": 131,
    "tradable": True,
    "category": "D",
    "currency": "EUR",
    "strikePrice": -0.0001,
    "exchangeId": "710",
    "onlyEodPrices": False,
    "orderTimeTypes": ["DAY", "GTC"],
    "buyOrderTypes": ["LIMIT", "MARKET", "STOPLOSS", "STOPLIMIT"],
    "sellOrderTypes": ["LIMIT", "MARKET", "STOPLOSS", "STOPLIMIT"],
    "productBitTypes": [],
    "closePrice": 135.03,
    "closePriceDate": "2021-02-09",
    "feedQuality": "R",
    "orderBookDepth": 0,
    "vwdIdentifierType": "issueid",
    "vwdId": "360199557",
    "qualitySwitchable": False,
    "qualitySwitchFree": False,
    "vwdModuleId": 1,
}

PRODUCT_INFO_MOCKED_SIDE_EFFECT = [
    PRODUCT_INFO_IFMK,
    {
        "id": str(PRODUCT_ID2),
        "name": "LYXOR PEA NASDAQ-100 D.2X LEV.",
        "isin": "FR0010342592",
        "symbol": SYMBOL2,
        "contractSize": 1.0,
        "productType": "ETF",
        "productTypeId": 131,
        "tradable": True,
        "category": "D",
        "currency": "EUR",
        "strikePrice": -0.0001,
        "exchangeId": "710",
        "onlyEodPrices": False,
        "orderTimeTypes": ["DAY", "GTC"],
        "buyOrderTypes": ["LIMIT", "MARKET", "STOPLOSS", "STOPLIMIT"],
        "sellOrderTypes": ["LIMIT", "MARKET", "STOPLOSS", "STOPLIMIT"],
        "productBitTypes": [],
        "closePrice": 636.9,
        "closePriceDate": "2021-02-09",
        "feedQuality": "R",
        "orderBookDepth": 0,
        "vwdIdentifierType": "issueid",
        "vwdId": "360199557",
        "qualitySwitchable": False,
        "qualitySwitchFree": False,
        "vwdModuleId": 1,
    },
    PRODUCT_INFO_AAPL,
]

ORDER_ID1 = "c8741cfa-6170-42c9-b952-e915bc614b36"
ORDER_ID2 = "d52d385d-f346-4f0f-88cb-6e845b5dfa75"
ORDER_ID3 = "4f3ff98b-89b5-41d5-ad65-ecad0624053c"


ORDERS_RETURN_VALUE = [
    {
        "buysell": "B",
        "created": "2021-02-06T13:54:03+01:00",
        "currentTradedSize": 0,
        "isActive": False,
        "last": "2021-02-06T13:54:03+01:00",
        "orderId": ORDER_ID3,
        "orderTimeTypeId": 1,
        "orderTypeId": 0,
        "price": 135.03,
        "productId": PRODUCT_ID3,
        "size": 1.0,
        "status": "CONFIRMED",
        "stopPrice": 0.0,
        "totalTradedSize": 0,
        "type": "CREATE",
    },
    {
        "buysell": "S",
        "created": "2021-02-08T16:56:24+01:00",
        "currentTradedSize": 0,
        "isActive": False,
        "last": "2021-02-08T16:56:24+01:00",
        "orderId": ORDER_ID2,
        "orderTimeTypeId": 1,
        "orderTypeId": 3,
        "price": 0.0,
        "productId": PRODUCT_ID2,
        "size": 2.0,
        "status": "CONFIRMED",
        "stopPrice": 600.0,
        "totalTradedSize": 0,
        "type": "CREATE",
    },
    {
        "buysell": "S",
        "created": "2021-02-08T16:57:00+01:00",
        "currentTradedSize": 0,
        "isActive": True,
        "last": "2021-02-08T16:57:00+01:00",
        "orderId": ORDER_ID2,
        "orderTimeTypeId": 1,
        "orderTypeId": 3,
        "price": 0.0,
        "productId": PRODUCT_ID2,
        "size": 2.0,
        "status": "CONFIRMED",
        "stopPrice": 600.0,
        "totalTradedSize": 0,
        "type": "DELETE",
    },
    {
        "buysell": "B",
        "created": "2021-02-08T17:01:13+01:00",
        "currentTradedSize": 0,
        "isActive": False,
        "last": "2021-02-08T17:01:14+01:00",
        "orderId": ORDER_ID1,
        "orderTimeTypeId": 1,
        "orderTypeId": 2,
        "price": 0.0,
        "productId": PRODUCT_ID1,
        "size": 1.0,
        "status": "CONFIRMED",
        "stopPrice": 0.0,
        "totalTradedSize": 0,
        "type": "CREATE",
    },
]


TRANSACTIONS_RETURN_VALUE = [
    {
        "id": 263709890,
        "productId": PRODUCT_ID1,
        "date": "2021-02-08T17:01:14+01:00",
        "buysell": "B",
        "price": 3.739,
        "quantity": 1,
        "total": -3.739,
        "orderTypeId": 2,
        "counterParty": "MK",
        "transfered": False,
        "totalInBaseCurrency": -3.739,
        "totalPlusFeeInBaseCurrency": -3.739,
        "transactionTypeId": 0,
        "tradingVenue": "XPAR",
    },
    {
        "id": 263709890,
        "productId": PRODUCT_ID3,
        "date": "2021-02-08T17:01:14+01:00",
        "buysell": "S",
        "price": 135.03,
        "quantity": 3,
        "total": 405.09,
        "orderTypeId": 2,
        "counterParty": "MK",
        "transfered": False,
        "totalInBaseCurrency": 405.09,
        "totalPlusFeeInBaseCurrency": 405.12,
        "transactionTypeId": 0,
        "tradingVenue": "XPAR",
    },
    {
        "id": 263709890,
        "productId": PRODUCT_ID1,
        "date": "2021-02-08T17:01:14+01:00",
        "buysell": "B",
        "price": 3.739,
        "quantity": 1,
        "total": -3.739,
        "orderTypeId": 2,
        "counterParty": "MK",
        "transfered": False,
        "totalInBaseCurrency": -3.739,
        "totalPlusFeeInBaseCurrency": -3.739,
        "transactionTypeId": 0,
        "tradingVenue": "XPAR",
    },
]

SEARCH_PRODUCTS_MOCKED_RETURN_VALUE = [
    {
        "id": str(PRODUCT_ID1),
        "name": "iFresh",
        "isin": "US4495381075",
        "symbol": SYMBOL1,
        "contractSize": 1.0,
        "productType": "STOCK",
        "productTypeId": 1,
        "tradable": True,
        "category": "D",
        "currency": "USD",
        "exchangeId": "663",
        "onlyEodPrices": False,
        "orderTimeTypes": ["DAY", "GTC"],
        "buyOrderTypes": ["LIMIT", "MARKET", "STOPLOSS", "STOPLIMIT"],
        "sellOrderTypes": ["LIMIT", "MARKET", "STOPLOSS", "STOPLIMIT"],
        "productBitTypes": [],
        "closePrice": 1.5,
        "closePriceDate": "2021-02-10",
        "feedQuality": "D15",
        "orderBookDepth": 0,
        "vwdIdentifierType": "issueid",
        "vwdId": "350013740",
        "qualitySwitchable": True,
        "qualitySwitchFree": False,
        "vwdModuleId": 21,
    }
]

TIMESTAMP = datetime.strptime("2021-02-08 16:01:14+0000", "%Y-%m-%d %H:%M:%S%z")


@patch("trazy_analysis.broker.degiro_broker.DegiroBroker.update_cash_balances")
@patch("trazy_analysis.broker.degiro_broker.DegiroBroker.update_open_positions")
@patch("trazy_analysis.broker.degiro_broker.DegiroBroker.update_transactions")
@patch("trazy_analysis.broker.degiroapi.DeGiro.product_info")
@patch("trazy_analysis.broker.degiroapi.DeGiro.login")
def test_update_product_info_cached_info(
    login_mocked,
    product_info_mocked,
    update_transactions_mocked,
    update_open_positions_mocked,
    update_cash_balances_mocked,
):
    clock = LiveClock()
    events = deque()
    degiro_broker = DegiroBroker(clock=clock, events=events)
    degiro_broker.product_info_last_update[str(PRODUCT_ID1)] = clock.current_time()
    degiro_broker.update_product_info(str(PRODUCT_ID1))

    product_info_mocked.assert_not_called()


@patch("trazy_analysis.broker.degiro_broker.DegiroBroker.update_cash_balances")
@patch("trazy_analysis.broker.degiro_broker.DegiroBroker.update_open_positions")
@patch("trazy_analysis.broker.degiro_broker.DegiroBroker.update_transactions")
@patch("trazy_analysis.broker.degiroapi.DeGiro.search_products")
@patch("trazy_analysis.broker.degiroapi.DeGiro.login")
def test_update_symbol_cached_info(
    login_mocked,
    search_products_mocked,
    update_transactions_mocked,
    update_open_positions_mocked,
    update_cash_balances_mocked,
):
    clock = LiveClock()
    events = deque()
    degiro_broker = DegiroBroker(clock=clock, events=events)
    degiro_broker.asset_info_last_update[ASSET1] = clock.current_time()
    degiro_broker.update_asset_info(ASSET1)

    search_products_mocked.assert_not_called()


@patch("trazy_analysis.broker.degiro_broker.DegiroBroker.update_open_positions")
@patch("trazy_analysis.broker.degiro_broker.DegiroBroker.update_transactions")
@patch("trazy_analysis.broker.degiroapi.DeGiro.getdata")
@patch("trazy_analysis.broker.degiroapi.DeGiro.login")
def test_update_cash_balances(
    login_mocked,
    getdata_mocked,
    update_transactions_mocked,
    update_open_positions_mocked,
):
    getdata_mocked.return_value = GET_DATA_CASH_MOCKED_RETURN_VALUE
    clock = LiveClock()
    events = deque()
    degiro_broker = DegiroBroker(clock=clock, events=events)
    assert degiro_broker.cash_balances == {
        "EUR": float(INITIAL_CASH),
        "USD": float("0.0"),
    }
    assert isinstance(degiro_broker.cash_balances_last_update, datetime)
    timestamp = degiro_broker.cash_balances_last_update

    # Call again to see if the value is cached
    degiro_broker.update_cash_balances()
    assert degiro_broker.cash_balances_last_update == timestamp

    assert degiro_broker.get_cash_balance() == {
        "EUR": float(INITIAL_CASH),
        "USD": 0.0,
    }

    assert degiro_broker.get_cash_balance("EUR") == float(INITIAL_CASH)

    with pytest.raises(ValueError):
        assert degiro_broker.get_cash_balance("GBP")

    getdata_mocked_calls = [call(degiroapi.Data.Type.CASHFUNDS)]
    getdata_mocked.assert_has_calls(getdata_mocked_calls)


@patch("trazy_analysis.broker.degiro_broker.DegiroBroker.update_cash_balances")
@patch("trazy_analysis.broker.degiro_broker.DegiroBroker.update_transactions")
@patch("trazy_analysis.broker.degiroapi.DeGiro.product_info")
@patch("trazy_analysis.broker.degiroapi.DeGiro.getdata")
@patch("trazy_analysis.broker.degiroapi.DeGiro.login")
def test_update_open_positions(
    login_mocked,
    getdata_mocked,
    product_info_mocked,
    update_transactions_mocked,
    update_cash_balances_mocked,
):
    getdata_mocked.return_value = GET_DATA_PORTFOLIO_MOCKED_RETURN_VALUE

    product_info_mocked.side_effect = PRODUCT_INFO_MOCKED_SIDE_EFFECT

    clock = LiveClock()
    events = deque()
    degiro_broker = DegiroBroker(clock=clock, events=events)

    price1 = 3.7
    buy_size1 = 1
    sell_size1 = 0
    direction = Direction.LONG
    expected_position1 = Position(
        asset=ASSET1,
        price=price1,
        buy_size=buy_size1,
        sell_size=sell_size1,
        direction=direction,
    )

    price2 = 636.9
    buy_size2 = 2
    sell_size2 = 0
    expected_position2 = Position(
        asset=ASSET2,
        price=price2,
        buy_size=buy_size2,
        sell_size=sell_size2,
        direction=direction,
    )

    price3 = 135.03
    buy_size3 = 0
    sell_size3 = -3
    direction = Direction.SHORT
    expected_position3 = Position(
        asset=ASSET3,
        price=price3,
        buy_size=buy_size3,
        sell_size=sell_size3,
        direction=direction,
    )

    expected_positions = {
        ASSET1: {Direction.LONG: expected_position1},
        ASSET2: {Direction.LONG: expected_position2},
        ASSET3: {Direction.SHORT: expected_position3},
    }

    assert degiro_broker.portfolio.pos_handler.positions == expected_positions

    # Call again to see if the value is cached
    timestamp = degiro_broker.open_positions_last_update
    degiro_broker.update_open_positions()
    assert degiro_broker.open_positions_last_update == timestamp

    getdata_mocked_calls = [call(degiroapi.Data.Type.PORTFOLIO, filter_zero=True)]
    getdata_mocked.assert_has_calls(getdata_mocked_calls)

    product_info_mocked_calls = [
        call(str(PRODUCT_ID1)),
        call(str(PRODUCT_ID2)),
        call(str(PRODUCT_ID3)),
    ]
    product_info_mocked.assert_has_calls(product_info_mocked_calls)


@patch("trazy_analysis.broker.degiro_broker.DegiroBroker.update_cash_balances")
@patch("trazy_analysis.broker.degiro_broker.DegiroBroker.update_transactions")
@patch("trazy_analysis.broker.degiroapi.DeGiro.product_info")
@patch("trazy_analysis.broker.degiroapi.DeGiro.getdata")
@patch("trazy_analysis.broker.degiroapi.DeGiro.login")
def test_has_opened_position(
    login_mocked,
    getdata_mocked,
    product_info_mocked,
    update_transactions_mocked,
    update_cash_balances_mocked,
):
    getdata_mocked.return_value = GET_DATA_PORTFOLIO_MOCKED_RETURN_VALUE

    product_info_mocked.side_effect = PRODUCT_INFO_MOCKED_SIDE_EFFECT

    clock = LiveClock()
    events = deque()
    degiro_broker = DegiroBroker(clock=clock, events=events)

    assert degiro_broker.has_opened_position(ASSET1, Direction.LONG)
    assert not degiro_broker.has_opened_position(ASSET1, Direction.SHORT)
    assert degiro_broker.has_opened_position(ASSET2, Direction.LONG)
    assert not degiro_broker.has_opened_position(ASSET2, Direction.SHORT)

    getdata_mocked_calls = [call(degiroapi.Data.Type.PORTFOLIO, filter_zero=True)]
    getdata_mocked.assert_has_calls(getdata_mocked_calls)

    product_info_mocked_calls = [call(str(PRODUCT_ID1)), call(str(PRODUCT_ID2))]
    product_info_mocked.assert_has_calls(product_info_mocked_calls)


@patch("trazy_analysis.common.clock.LiveClock.current_time")
@patch("trazy_analysis.broker.degiro_broker.DegiroBroker.update_open_positions")
@patch("trazy_analysis.broker.degiroapi.DeGiro.product_info")
@patch("trazy_analysis.broker.degiroapi.DeGiro.transactions")
@patch("trazy_analysis.broker.degiroapi.DeGiro.orders")
@patch("trazy_analysis.broker.degiroapi.DeGiro.getdata")
@patch("trazy_analysis.broker.degiroapi.DeGiro.login")
def test_update_transactions(
    login_mocked,
    getdata_mocked,
    orders_mocked,
    transactions_mocked,
    product_info_mocked,
    update_open_positions_mocked,
    current_time_mocked,
):
    getdata_mocked.return_value = GET_DATA_CASH_MOCKED_RETURN_VALUE
    orders_mocked.return_value = ORDERS_RETURN_VALUE
    transactions_mocked.return_value = TRANSACTIONS_RETURN_VALUE

    product_info_mocked.side_effect = PRODUCT_INFO_MOCKED_SIDE_EFFECT

    nb_current_time_calls = 12
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
            minute=6,
            second=5,
            tzinfo=timezone("UTC"),
        )
    ]

    clock = LiveClock()
    events = deque()
    degiro_broker = DegiroBroker(clock=clock, events=events)
    assert isinstance(degiro_broker.transactions_last_update, datetime)
    timestamp = degiro_broker.transactions_last_update

    # Call again to see if the value is cached
    degiro_broker.update_transactions()
    assert degiro_broker.transactions_last_update == timestamp

    TIMESTAMP = datetime.strptime("2021-02-08 16:01:14+0000", "%Y-%m-%d %H:%M:%S%z")
    event_type = "asset_transaction"
    description = "BUY LONG 1 DEGIRO-IFMK 3.739 08/02/2021"
    expected_pe = PortfolioEvent(
        timestamp=TIMESTAMP,
        type=event_type,
        description=description,
        debit=3.739,
        credit=-0.0,
        balance=125.08,
    )
    assert len(degiro_broker.portfolio.history) == 1
    assert degiro_broker.portfolio.history[-1] == expected_pe

    assert degiro_broker.executed_orders_ids == {ORDER_ID1}

    getdata_mocked_calls = [call(degiroapi.Data.Type.CASHFUNDS)]
    getdata_mocked.assert_has_calls(getdata_mocked_calls)

    orders_mocked.assert_called_once()

    transactions_mocked.assert_called_once()

    product_info_mocked_calls = [call(PRODUCT_ID1)]
    product_info_mocked.assert_has_calls(product_info_mocked_calls)


@patch("trazy_analysis.common.clock.LiveClock.current_time")
@patch("trazy_analysis.broker.degiroapi.DeGiro.sellorder")
@patch("trazy_analysis.broker.degiroapi.DeGiro.buyorder")
@patch("trazy_analysis.broker.degiroapi.DeGiro.search_products")
@patch("trazy_analysis.broker.degiroapi.DeGiro.product_info")
@patch("trazy_analysis.broker.degiroapi.DeGiro.transactions")
@patch("trazy_analysis.broker.degiroapi.DeGiro.orders")
@patch("trazy_analysis.broker.degiroapi.DeGiro.getdata")
@patch("trazy_analysis.broker.degiroapi.DeGiro.login")
def test_execute_market_order(
    login_mocked,
    getdata_mocked,
    orders_mocked,
    transactions_mocked,
    product_info_mocked,
    search_products_mocked,
    buyorder_mocked,
    sellorder_mocked,
    current_time_mocked,
):
    getdata_mocked.side_effect = [
        GET_DATA_CASH_MOCKED_RETURN_VALUE,
        GET_DATA_PORTFOLIO_MOCKED_RETURN_VALUE,
    ]

    product_info_mocked.side_effect = PRODUCT_INFO_MOCKED_SIDE_EFFECT + [
        PRODUCT_INFO_IFMK,
        PRODUCT_INFO_AAPL,
    ]

    nb_current_time_calls = 26
    current_time_mocked.side_effect = [
        datetime(
            year=2021,
            month=2,
            day=1,
            hour=15,
            minute=i,
            second=0,
            tzinfo=timezone("UTC"),
        )
        for i in range(1, nb_current_time_calls)
    ]

    orders_mocked.return_value = ORDERS_RETURN_VALUE
    transactions_mocked.return_value = TRANSACTIONS_RETURN_VALUE

    search_products_mocked.return_value = SEARCH_PRODUCTS_MOCKED_RETURN_VALUE

    clock = LiveClock()
    events = deque()
    degiro_broker = DegiroBroker(clock=clock, events=events)

    buy_order = Order(asset=ASSET1, time_unit=timedelta(minutes=1), action=Action.BUY, direction=Direction.LONG, size=1,
                      signal_id="1", order_type=OrderType.MARKET, clock=clock)
    buyorder_id = ORDER_ID2
    buyorder_mocked.side_effect = [buyorder_id, Exception()]
    degiro_broker.execute_order(buy_order)
    assert degiro_broker.open_orders_ids == {buyorder_id}
    assert degiro_broker.executed_orders_ids == {ORDER_ID1}

    buyorder_mocked_calls = [
        call(degiroapi.Order.Type.MARKET, str(PRODUCT_ID1), 3, buy_order.size)
    ]
    buyorder_mocked.assert_has_calls(buyorder_mocked_calls)

    # test degiro buyorder Exception
    degiro_broker.execute_order(buy_order)

    sell_order = Order(asset=ASSET1, time_unit=timedelta(minutes=1), action=Action.SELL, direction=Direction.LONG,
                       size=1, signal_id="1", order_type=OrderType.MARKET, clock=clock)
    sellorder_id = ORDER_ID3
    sellorder_mocked.return_value = sellorder_id
    degiro_broker.execute_order(sell_order)
    assert degiro_broker.open_orders_ids == {buyorder_id, sellorder_id}
    assert degiro_broker.executed_orders_ids == {ORDER_ID1}
    sellorder_mocked_calls = [
        call(degiroapi.Order.Type.MARKET, str(PRODUCT_ID1), 3, sell_order.size)
    ]
    sellorder_mocked.assert_has_calls(sellorder_mocked_calls)

    getdata_mocked_calls = [call(degiroapi.Data.Type.CASHFUNDS)]
    getdata_mocked.assert_has_calls(getdata_mocked_calls)

    orders_mocked.assert_called_once()

    transactions_mocked.assert_called_once()

    product_info_mocked_calls = [call(PRODUCT_ID1)]
    product_info_mocked.assert_has_calls(product_info_mocked_calls)

    search_products_mocked_calls = [call(ASSET1.symbol, limit=5)]
    search_products_mocked.assert_has_calls(search_products_mocked_calls)


@patch("trazy_analysis.common.clock.LiveClock.current_time")
@patch("trazy_analysis.broker.degiroapi.DeGiro.sellorder")
@patch("trazy_analysis.broker.degiroapi.DeGiro.buyorder")
@patch("trazy_analysis.broker.degiroapi.DeGiro.search_products")
@patch("trazy_analysis.broker.degiroapi.DeGiro.product_info")
@patch("trazy_analysis.broker.degiroapi.DeGiro.transactions")
@patch("trazy_analysis.broker.degiroapi.DeGiro.orders")
@patch("trazy_analysis.broker.degiroapi.DeGiro.getdata")
@patch("trazy_analysis.broker.degiroapi.DeGiro.login")
def test_execute_limit_order(
    login_mocked,
    getdata_mocked,
    orders_mocked,
    transactions_mocked,
    product_info_mocked,
    search_products_mocked,
    buyorder_mocked,
    sellorder_mocked,
    current_time_mocked,
):
    getdata_mocked.side_effect = [
        GET_DATA_CASH_MOCKED_RETURN_VALUE,
        GET_DATA_PORTFOLIO_MOCKED_RETURN_VALUE,
    ]

    product_info_mocked.side_effect = PRODUCT_INFO_MOCKED_SIDE_EFFECT + [
        PRODUCT_INFO_IFMK,
        PRODUCT_INFO_AAPL,
    ]

    nb_current_time_calls = 26
    current_time_mocked.side_effect = [
        datetime(
            year=2021,
            month=2,
            day=1,
            hour=15,
            minute=i,
            second=0,
            tzinfo=timezone("UTC"),
        )
        for i in range(1, nb_current_time_calls)
    ]

    orders_mocked.return_value = ORDERS_RETURN_VALUE
    transactions_mocked.return_value = TRANSACTIONS_RETURN_VALUE

    search_products_mocked.return_value = SEARCH_PRODUCTS_MOCKED_RETURN_VALUE

    clock = LiveClock()
    events = deque()
    degiro_broker = DegiroBroker(clock=clock, events=events)

    buy_order = Order(asset=ASSET1, time_unit=timedelta(minutes=1), action=Action.BUY, direction=Direction.LONG, size=1,
                      signal_id="1", limit=1.5, order_type=OrderType.LIMIT, clock=clock)
    buyorder_id = ORDER_ID2
    buyorder_mocked.side_effect = [buyorder_id, Exception()]
    degiro_broker.execute_order(buy_order)
    assert degiro_broker.open_orders_ids == {buyorder_id}
    assert degiro_broker.executed_orders_ids == {ORDER_ID1}
    buyorder_mocked_calls = [
        call(
            degiroapi.Order.Type.LIMIT,
            str(PRODUCT_ID1),
            3,
            buy_order.size,
            limit=1.5,
        )
    ]
    buyorder_mocked.assert_has_calls(buyorder_mocked_calls)

    # test degiro buyorder Exception
    degiro_broker.execute_order(buy_order)

    sell_order = Order(asset=ASSET1, time_unit=timedelta(minutes=1), action=Action.SELL, direction=Direction.LONG,
                       size=1, signal_id="1", limit=1.5, order_type=OrderType.LIMIT, clock=clock)
    sellorder_id = ORDER_ID3
    sellorder_mocked.return_value = sellorder_id
    degiro_broker.execute_order(sell_order)
    assert degiro_broker.open_orders_ids == {buyorder_id, sellorder_id}
    assert degiro_broker.executed_orders_ids == {ORDER_ID1}
    sellorder_mocked_calls = [
        call(
            degiroapi.Order.Type.LIMIT,
            str(PRODUCT_ID1),
            3,
            sell_order.size,
            limit=1.5,
        )
    ]
    sellorder_mocked.assert_has_calls(sellorder_mocked_calls)

    getdata_mocked_calls = [call(degiroapi.Data.Type.CASHFUNDS)]
    getdata_mocked.assert_has_calls(getdata_mocked_calls)

    orders_mocked.assert_called_once()

    transactions_mocked.assert_called_once()

    product_info_mocked_calls = [call(PRODUCT_ID1)]
    product_info_mocked.assert_has_calls(product_info_mocked_calls)

    search_products_mocked_calls = [call(ASSET1.symbol, limit=5)]
    search_products_mocked.assert_has_calls(search_products_mocked_calls)


@patch("trazy_analysis.common.clock.LiveClock.current_time")
@patch("trazy_analysis.broker.degiroapi.DeGiro.sellorder")
@patch("trazy_analysis.broker.degiroapi.DeGiro.buyorder")
@patch("trazy_analysis.broker.degiroapi.DeGiro.search_products")
@patch("trazy_analysis.broker.degiroapi.DeGiro.product_info")
@patch("trazy_analysis.broker.degiroapi.DeGiro.transactions")
@patch("trazy_analysis.broker.degiroapi.DeGiro.orders")
@patch("trazy_analysis.broker.degiroapi.DeGiro.getdata")
@patch("trazy_analysis.broker.degiroapi.DeGiro.login")
def test_execute_stop_order(
    login_mocked,
    getdata_mocked,
    orders_mocked,
    transactions_mocked,
    product_info_mocked,
    search_products_mocked,
    buyorder_mocked,
    sellorder_mocked,
    current_time_mocked,
):
    getdata_mocked.side_effect = [
        GET_DATA_CASH_MOCKED_RETURN_VALUE,
        GET_DATA_PORTFOLIO_MOCKED_RETURN_VALUE,
    ]

    product_info_mocked.side_effect = PRODUCT_INFO_MOCKED_SIDE_EFFECT + [
        PRODUCT_INFO_IFMK,
        PRODUCT_INFO_AAPL,
    ]

    nb_current_time_calls = 26
    current_time_mocked.side_effect = [
        datetime(
            year=2021,
            month=2,
            day=1,
            hour=15,
            minute=i,
            second=0,
            tzinfo=timezone("UTC"),
        )
        for i in range(1, nb_current_time_calls)
    ]

    orders_mocked.return_value = ORDERS_RETURN_VALUE
    transactions_mocked.return_value = TRANSACTIONS_RETURN_VALUE

    search_products_mocked.return_value = SEARCH_PRODUCTS_MOCKED_RETURN_VALUE

    clock = LiveClock()
    events = deque()
    degiro_broker = DegiroBroker(clock=clock, events=events)

    buy_order = Order(asset=ASSET1, time_unit=timedelta(minutes=1), action=Action.BUY, direction=Direction.LONG, size=1,
                      signal_id="1", stop=1.5, order_type=OrderType.STOP, clock=clock)
    buyorder_id = ORDER_ID2
    buyorder_mocked.side_effect = [buyorder_id, Exception()]
    degiro_broker.execute_order(buy_order)
    assert degiro_broker.open_orders_ids == {buyorder_id}
    assert degiro_broker.executed_orders_ids == {ORDER_ID1}
    buyorder_mocked_calls = [
        call(
            degiroapi.Order.Type.STOPLOSS,
            str(PRODUCT_ID1),
            3,
            buy_order.size,
            stop_loss=1.5,
        )
    ]
    buyorder_mocked.assert_has_calls(buyorder_mocked_calls)

    # test degiro buyorder Exception
    degiro_broker.execute_order(buy_order)

    sell_order = Order(asset=ASSET1, time_unit=timedelta(minutes=1), action=Action.SELL, direction=Direction.LONG,
                       size=1, signal_id="1", stop=1.5, order_type=OrderType.STOP, clock=clock)
    sellorder_id = ORDER_ID3
    sellorder_mocked.return_value = sellorder_id
    degiro_broker.execute_order(sell_order)
    assert degiro_broker.open_orders_ids == {buyorder_id, sellorder_id}
    assert degiro_broker.executed_orders_ids == {ORDER_ID1}
    sellorder_mocked_calls = [
        call(
            degiroapi.Order.Type.STOPLOSS,
            str(PRODUCT_ID1),
            3,
            sell_order.size,
            stop_loss=1.5,
        )
    ]
    sellorder_mocked.assert_has_calls(sellorder_mocked_calls)

    getdata_mocked_calls = [call(degiroapi.Data.Type.CASHFUNDS)]
    getdata_mocked.assert_has_calls(getdata_mocked_calls)

    orders_mocked.assert_called_once()

    transactions_mocked.assert_called_once()

    product_info_mocked_calls = [call(PRODUCT_ID1)]
    product_info_mocked.assert_has_calls(product_info_mocked_calls)

    search_products_mocked_calls = [call(ASSET1.symbol, limit=5)]
    search_products_mocked.assert_has_calls(search_products_mocked_calls)


@patch("trazy_analysis.common.clock.LiveClock.current_time")
@patch("trazy_analysis.broker.degiro_broker.DegiroBroker.execute_market_order")
@patch("trazy_analysis.broker.degiroapi.DeGiro.product_info")
@patch("trazy_analysis.broker.degiroapi.DeGiro.transactions")
@patch("trazy_analysis.broker.degiroapi.DeGiro.orders")
@patch("trazy_analysis.broker.degiroapi.DeGiro.getdata")
@patch("trazy_analysis.broker.degiroapi.DeGiro.login")
def test_execute_target_order(
    login_mocked,
    getdata_mocked,
    orders_mocked,
    transactions_mocked,
    product_info_mocked,
    execute_market_order_mocked,
    current_time_mocked,
):
    getdata_mocked.side_effect = [
        GET_DATA_CASH_MOCKED_RETURN_VALUE,
        GET_DATA_PORTFOLIO_MOCKED_RETURN_VALUE,
    ]

    product_info_mocked.side_effect = PRODUCT_INFO_MOCKED_SIDE_EFFECT + [
        PRODUCT_INFO_IFMK,
        PRODUCT_INFO_AAPL,
    ]

    nb_current_time_calls = 26
    current_time_mocked.side_effect = [
        datetime(
            year=2021,
            month=2,
            day=1,
            hour=15,
            minute=i,
            second=0,
            tzinfo=timezone("UTC"),
        )
        for i in range(1, nb_current_time_calls)
    ]

    orders_mocked.return_value = ORDERS_RETURN_VALUE
    transactions_mocked.return_value = TRANSACTIONS_RETURN_VALUE

    clock = LiveClock()
    events = deque()
    degiro_broker = DegiroBroker(clock=clock, events=events)

    # target order sell
    target_order = Order(asset=ASSET1, time_unit=timedelta(minutes=1), action=Action.SELL, direction=Direction.LONG,
                         size=1, signal_id="1", target=3.01, order_type=OrderType.TARGET, clock=clock)
    candle = Candle(
        asset=ASSET1,
        open=1.105,
        high=1.12,
        low=1.10,
        close=1.11,
        volume=100,
        timestamp=TIMESTAMP,
    )
    degiro_broker.update_price(candle)
    degiro_broker.execute_order(target_order)
    assert len(degiro_broker.open_orders) == 1
    assert degiro_broker.open_orders.popleft() == target_order

    candle = Candle(
        asset=ASSET1,
        open=3.105,
        high=3.12,
        low=3.10,
        close=3.11,
        volume=100,
        timestamp=TIMESTAMP,
    )
    degiro_broker.update_price(candle)
    degiro_broker.execute_order(target_order)
    assert len(degiro_broker.open_orders) == 0

    # target order buy
    target_order = Order(asset=ASSET1, time_unit=timedelta(minutes=1), action=Action.BUY, direction=Direction.LONG,
                         size=1, signal_id="1", target=0.51, order_type=OrderType.TARGET, clock=clock)
    candle = Candle(
        asset=ASSET1,
        open=1.105,
        high=1.12,
        low=1.10,
        close=1.11,
        volume=100,
        timestamp=TIMESTAMP,
    )
    degiro_broker.update_price(candle)
    degiro_broker.execute_order(target_order)
    assert len(degiro_broker.open_orders) == 1
    assert degiro_broker.open_orders.popleft() == target_order

    candle = Candle(
        asset=ASSET1,
        open=0.495,
        high=0.502,
        low=0.493,
        close=0.494,
        volume=100,
        timestamp=TIMESTAMP,
    )
    degiro_broker.update_price(candle)
    degiro_broker.execute_order(target_order)
    assert len(degiro_broker.open_orders) == 0

    execute_market_order_mocked_calls = [call(target_order)]
    execute_market_order_mocked.assert_has_calls(execute_market_order_mocked_calls)

    getdata_mocked_calls = [call(degiroapi.Data.Type.CASHFUNDS)]
    getdata_mocked.assert_has_calls(getdata_mocked_calls)

    orders_mocked.assert_called_once()

    transactions_mocked.assert_called_once()

    product_info_mocked_calls = [call(PRODUCT_ID1)]
    product_info_mocked.assert_has_calls(product_info_mocked_calls)


@patch("trazy_analysis.common.clock.LiveClock.current_time")
@patch("trazy_analysis.broker.degiroapi.DeGiro.sellorder")
@patch("trazy_analysis.broker.degiroapi.DeGiro.search_products")
@patch("trazy_analysis.broker.degiroapi.DeGiro.product_info")
@patch("trazy_analysis.broker.degiroapi.DeGiro.transactions")
@patch("trazy_analysis.broker.degiroapi.DeGiro.orders")
@patch("trazy_analysis.broker.degiroapi.DeGiro.getdata")
@patch("trazy_analysis.broker.degiroapi.DeGiro.login")
def test_execute_trailing_stop_order_sell(
    login_mocked,
    getdata_mocked,
    orders_mocked,
    transactions_mocked,
    product_info_mocked,
    search_products_mocked,
    sellorder_mocked,
    current_time_mocked,
):
    getdata_mocked.side_effect = [
        GET_DATA_CASH_MOCKED_RETURN_VALUE,
        GET_DATA_PORTFOLIO_MOCKED_RETURN_VALUE,
    ]

    product_info_mocked.side_effect = PRODUCT_INFO_MOCKED_SIDE_EFFECT + [
        PRODUCT_INFO_IFMK,
        PRODUCT_INFO_AAPL,
    ]

    nb_current_time_calls = 59
    current_time_mocked.side_effect = [
        datetime(
            year=2021,
            month=2,
            day=1,
            hour=15,
            minute=i,
            second=0,
            tzinfo=timezone("UTC"),
        )
        for i in range(1, nb_current_time_calls)
    ]

    orders_mocked.return_value = ORDERS_RETURN_VALUE
    transactions_mocked.return_value = TRANSACTIONS_RETURN_VALUE

    search_products_mocked.return_value = SEARCH_PRODUCTS_MOCKED_RETURN_VALUE

    clock = LiveClock()
    events = deque()
    degiro_broker = DegiroBroker(clock=clock, events=events)

    trailing_stop_order = Order(asset=ASSET1, time_unit=timedelta(minutes=1), action=Action.SELL,
                                direction=Direction.LONG, size=1, signal_id="1", stop_pct=0.05,
                                order_type=OrderType.TRAILING_STOP, clock=clock)
    candle = Candle(
        asset=ASSET1,
        open=1.105,
        high=1.12,
        low=1.10,
        close=1.11,
        volume=100,
        timestamp=TIMESTAMP,
    )
    degiro_broker.update_price(candle)
    sellorder_mocked.side_effect = [ORDER_ID2, ORDER_ID3]
    degiro_broker.execute_order(trailing_stop_order)

    assert len(degiro_broker.open_orders) == 1
    assert degiro_broker.open_orders.popleft() == trailing_stop_order
    assert trailing_stop_order.order_id is not None
    assert (
        trailing_stop_order.stop
        == candle.close - candle.close * trailing_stop_order.stop_pct
    )

    candle = Candle(
        asset=ASSET1,
        open=1.145,
        high=1.16,
        low=1.14,
        close=1.15,
        volume=100,
        timestamp=TIMESTAMP,
    )
    degiro_broker.update_price(candle)

    degiro_broker.execute_order(trailing_stop_order)

    assert len(degiro_broker.open_orders) == 1
    assert degiro_broker.open_orders.popleft() == trailing_stop_order
    assert (
        trailing_stop_order.stop
        == candle.close - candle.close * trailing_stop_order.stop_pct
    )

    candle = Candle(
        asset=ASSET1,
        open=1.09,
        high=1.10,
        low=1.08,
        close=1.09,
        volume=100,
        timestamp=TIMESTAMP,
    )
    degiro_broker.update_price(candle)

    degiro_broker.execute_order(trailing_stop_order)
    assert len(degiro_broker.open_orders) == 0

    getdata_mocked_calls = [call(degiroapi.Data.Type.CASHFUNDS)]
    getdata_mocked.assert_has_calls(getdata_mocked_calls)

    orders_mocked.assert_called_once()

    transactions_mocked.assert_called_once()

    product_info_mocked_calls = [call(PRODUCT_ID1)]
    product_info_mocked.assert_has_calls(product_info_mocked_calls)

    search_products_mocked_calls = [call(ASSET1.symbol, limit=5)]
    search_products_mocked.assert_has_calls(search_products_mocked_calls)


@patch("trazy_analysis.common.clock.LiveClock.current_time")
@patch("trazy_analysis.broker.degiroapi.DeGiro.buyorder")
@patch("trazy_analysis.broker.degiroapi.DeGiro.search_products")
@patch("trazy_analysis.broker.degiroapi.DeGiro.product_info")
@patch("trazy_analysis.broker.degiroapi.DeGiro.transactions")
@patch("trazy_analysis.broker.degiroapi.DeGiro.orders")
@patch("trazy_analysis.broker.degiroapi.DeGiro.getdata")
@patch("trazy_analysis.broker.degiroapi.DeGiro.login")
def test_execute_trailing_stop_order_buy(
    login_mocked,
    getdata_mocked,
    orders_mocked,
    transactions_mocked,
    product_info_mocked,
    search_products_mocked,
    buyorder_mocked,
    current_time_mocked,
):
    getdata_mocked.side_effect = [
        GET_DATA_CASH_MOCKED_RETURN_VALUE,
        GET_DATA_PORTFOLIO_MOCKED_RETURN_VALUE,
    ]

    product_info_mocked.side_effect = PRODUCT_INFO_MOCKED_SIDE_EFFECT + [
        PRODUCT_INFO_IFMK,
        PRODUCT_INFO_AAPL,
    ]

    nb_current_time_calls = 26
    current_time_mocked.side_effect = [
        datetime(
            year=2021,
            month=2,
            day=1,
            hour=15,
            minute=i,
            second=0,
            tzinfo=timezone("UTC"),
        )
        for i in range(1, nb_current_time_calls)
    ]

    orders_mocked.return_value = ORDERS_RETURN_VALUE
    transactions_mocked.return_value = TRANSACTIONS_RETURN_VALUE

    search_products_mocked.return_value = SEARCH_PRODUCTS_MOCKED_RETURN_VALUE

    clock = LiveClock()
    events = deque()
    degiro_broker = DegiroBroker(clock=clock, events=events)

    trailing_stop_order = Order(asset=ASSET1, time_unit=timedelta(minutes=1), action=Action.BUY,
                                direction=Direction.SHORT, size=1, signal_id="1", stop_pct=0.05,
                                order_type=OrderType.TRAILING_STOP, clock=clock)
    candle = Candle(
        asset=ASSET1,
        open=1.145,
        high=1.16,
        low=1.14,
        close=1.15,
        volume=100,
        timestamp=TIMESTAMP,
    )

    degiro_broker.update_price(candle)
    buyorder_mocked.side_effect = [ORDER_ID2, ORDER_ID3]
    degiro_broker.execute_order(trailing_stop_order)
    assert len(degiro_broker.open_orders) == 1
    assert degiro_broker.open_orders.popleft() == trailing_stop_order
    assert trailing_stop_order.order_id is not None
    assert (
        trailing_stop_order.stop
        == candle.close + candle.close * trailing_stop_order.stop_pct
    )
    candle = Candle(
        asset=ASSET1,
        open=1.105,
        high=1.12,
        low=1.10,
        close=1.11,
        volume=100,
        timestamp=TIMESTAMP,
    )
    degiro_broker.update_price(candle)

    degiro_broker.execute_order(trailing_stop_order)
    assert len(degiro_broker.open_orders) == 1
    assert degiro_broker.open_orders.popleft() == trailing_stop_order
    assert (
        trailing_stop_order.stop
        == candle.close + candle.close * trailing_stop_order.stop_pct
    )

    candle = Candle(
        asset=ASSET1,
        open=1.185,
        high=1.19,
        low=1.16,
        close=1.17,
        volume=100,
        timestamp=TIMESTAMP,
    )
    degiro_broker.update_price(candle)

    degiro_broker.execute_order(trailing_stop_order)
    assert len(degiro_broker.open_orders) == 0

    getdata_mocked_calls = [call(degiroapi.Data.Type.CASHFUNDS)]
    getdata_mocked.assert_has_calls(getdata_mocked_calls)

    orders_mocked.assert_called_once()

    transactions_mocked.assert_called_once()

    product_info_mocked_calls = [call(PRODUCT_ID1)]
    product_info_mocked.assert_has_calls(product_info_mocked_calls)

    search_products_mocked_calls = [call(ASSET1.symbol, limit=5)]
    search_products_mocked.assert_has_calls(search_products_mocked_calls)


@patch("trazy_analysis.broker.degiro_broker.DegiroBroker.update_cash_balances")
@patch("trazy_analysis.broker.degiro_broker.DegiroBroker.update_open_positions")
@patch("trazy_analysis.broker.degiro_broker.DegiroBroker.update_transactions")
@patch("trazy_analysis.broker.degiroapi.DeGiro.login")
def test_synchronize(
    login_mocked,
    update_transactions_mocked,
    update_open_positions_mocked,
    update_cash_balances_mocked,
):
    clock = LiveClock()
    events = deque()
    degiro_broker = DegiroBroker(clock=clock, events=events)

    degiro_broker.synchronize()

    update_transactions_mocked_calls = [call(), call()]
    update_transactions_mocked.assert_has_calls(update_transactions_mocked_calls)

    update_open_positions_mocked_calls = [call(), call()]
    update_open_positions_mocked.assert_has_calls(update_open_positions_mocked_calls)

    update_cash_balances_mocked_calls = [call(), call()]
    update_cash_balances_mocked.assert_has_calls(update_cash_balances_mocked_calls)
