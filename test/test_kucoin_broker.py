from collections import deque
from datetime import datetime
from unittest.mock import call, patch

import pytest
from freezegun import freeze_time
from pytz import timezone

from broker.kucoin_broker import KucoinBroker
from common.clock import LiveClock
from models.asset import Asset
from models.enums import Action, Direction, OrderType
from models.order import Order
from portfolio.portfolio_event import PortfolioEvent
from position.position import Position

INITIAL_CASH = "32.429"

EXCHANGE = "KUCOIN"

SYMBOL1 = "ETHUSDT"
SYMBOL2 = "XRPUSDT"
SYMBOL3 = "SXPUSDT"

ASSET1 = Asset(symbol=SYMBOL1, exchange=EXCHANGE)
ASSET2 = Asset(symbol=SYMBOL2, exchange=EXCHANGE)
ASSET3 = Asset(symbol=SYMBOL3, exchange=EXCHANGE)

TIMESTAMP = datetime.strptime("2021-02-08 16:01:14+0000", "%Y-%m-%d %H:%M:%S%z")

GET_ALL_TICKERS_RETURN_VALUE = {
    "time": 1617225146041,
    "ticker": [
        {
            "symbol": "BTC-USDT",
            "symbolName": "BTC-USDT",
            "buy": "58947.5",
            "sell": "58947.6",
            "changeRate": "0.005",
            "changePrice": "295.6",
            "high": "59800",
            "low": "56357.5",
            "vol": "4809.50411176",
            "volValue": "281859294.67644065",
            "last": "58947.6",
            "averagePrice": "58503.89969601",
            "takerFeeRate": "0.001",
            "makerFeeRate": "0.001",
            "takerCoefficient": "1",
            "makerCoefficient": "1",
        },
        {
            "symbol": "ETH-USDT",
            "symbolName": "ETH-USDT",
            "buy": "1931.87",
            "sell": "1931.88",
            "changeRate": "0.0484",
            "changePrice": "89.23",
            "high": "1941.95",
            "low": "1765",
            "vol": "71883.91666154",
            "volValue": "132701904.2622455999",
            "last": "1931.87",
            "averagePrice": "1824.76982356",
            "takerFeeRate": "0.001",
            "makerFeeRate": "0.001",
            "takerCoefficient": "1",
            "makerCoefficient": "1",
        },
        {
            "symbol": "LINK-USDT",
            "symbolName": "LINK-USDT",
            "buy": "28.732",
            "sell": "28.7557",
            "changeRate": "0.033",
            "changePrice": "0.9204",
            "high": "28.7721",
            "low": "26.17",
            "vol": "191934.84971182",
            "volValue": "5274309.1126296202",
            "last": "28.7599",
            "averagePrice": "27.4421178",
            "takerFeeRate": "0.001",
            "makerFeeRate": "0.001",
            "takerCoefficient": "1",
            "makerCoefficient": "1",
        },
        {
            "symbol": "SXP-USDT",
            "symbolName": "SXP-USDT",
            "buy": "3.4033",
            "sell": "3.4041",
            "changeRate": "-0.0678",
            "changePrice": "-0.2479",
            "high": "3.7907",
            "low": "3.2402",
            "vol": "808678.59977553",
            "volValue": "2802364.851595283974",
            "last": "3.4033",
            "averagePrice": "3.53676164",
            "takerFeeRate": "0.001",
            "makerFeeRate": "0.001",
            "takerCoefficient": "1",
            "makerCoefficient": "1",
        },
        {
            "symbol": "XRP-USDT",
            "symbolName": "XRP-USDT",
            "buy": "0.55981",
            "sell": "0.56014",
            "changeRate": "-0.0037",
            "changePrice": "-0.00211",
            "high": "0.57152",
            "low": "0.52641",
            "vol": "47014297.89093557",
            "volValue": "25939304.9950362465371",
            "last": "0.56",
            "averagePrice": "0.55267632",
            "takerFeeRate": "0.001",
            "makerFeeRate": "0.001",
            "takerCoefficient": "1",
            "makerCoefficient": "1",
        },
    ],
}

GET_SYMBOL_LIST_RETURN_VALUE = [
    {
        "symbol": "ETH-USDT",
        "name": "ETH-USDT",
        "baseCurrency": "ETH",
        "quoteCurrency": "USDT",
        "feeCurrency": "USDT",
        "market": "USDS",
        "baseMinSize": "0.0001",
        "quoteMinSize": "0.01",
        "baseMaxSize": "10000000000",
        "quoteMaxSize": "999999999",
        "baseIncrement": "0.0000001",
        "quoteIncrement": "0.000001",
        "priceIncrement": "0.01",
        "priceLimitRate": "0.1",
        "isMarginEnabled": True,
        "enableTrading": True,
    },
    {
        "symbol": "SXP-USDT",
        "name": "SXP-USDT",
        "baseCurrency": "SXP",
        "quoteCurrency": "USDT",
        "feeCurrency": "USDT",
        "market": "USDS",
        "baseMinSize": "0.01",
        "quoteMinSize": "0.01",
        "baseMaxSize": "10000000000",
        "quoteMaxSize": "99999999",
        "baseIncrement": "0.0001",
        "quoteIncrement": "0.0001",
        "priceIncrement": "0.0001",
        "priceLimitRate": "0.1",
        "isMarginEnabled": True,
        "enableTrading": True,
    },
    {
        "symbol": "BTC-USDT",
        "name": "BTC-USDT",
        "baseCurrency": "BTC",
        "quoteCurrency": "USDT",
        "feeCurrency": "USDT",
        "market": "USDS",
        "baseMinSize": "0.00001",
        "quoteMinSize": "0.01",
        "baseMaxSize": "10000000000",
        "quoteMaxSize": "99999999",
        "baseIncrement": "0.00000001",
        "quoteIncrement": "0.000001",
        "priceIncrement": "0.1",
        "priceLimitRate": "0.1",
        "isMarginEnabled": True,
        "enableTrading": True,
    },
    {
        "symbol": "LINK-USDT",
        "name": "LINK-USDT",
        "baseCurrency": "LINK",
        "quoteCurrency": "USDT",
        "feeCurrency": "USDT",
        "market": "DeFi",
        "baseMinSize": "0.001",
        "quoteMinSize": "0.01",
        "baseMaxSize": "10000000000",
        "quoteMaxSize": "99999999",
        "baseIncrement": "0.0001",
        "quoteIncrement": "0.0001",
        "priceIncrement": "0.0001",
        "priceLimitRate": "0.1",
        "isMarginEnabled": True,
        "enableTrading": True,
    },
    {
        "symbol": "XRP-USDT",
        "name": "XRP-USDT",
        "baseCurrency": "XRP",
        "quoteCurrency": "USDT",
        "feeCurrency": "USDT",
        "market": "USDS",
        "baseMinSize": "0.1",
        "quoteMinSize": "0.01",
        "baseMaxSize": "10000000000",
        "quoteMaxSize": "99999999",
        "baseIncrement": "0.0001",
        "quoteIncrement": "0.000001",
        "priceIncrement": "0.00001",
        "priceLimitRate": "0.1",
        "isMarginEnabled": True,
        "enableTrading": True,
    },
]

GET_ACCOUNT_LIST_RETURN_VALUE = [
    {
        "id": "60663a5e9fa49e000620aac5",
        "currency": "USDT",
        "type": "main",
        "balance": "33.71",
        "available": "33.71",
        "holds": "0",
    },
    {
        "id": "6066473f5f7775000627f856",
        "currency": "USDT",
        "type": "trade",
        "balance": "32.429",
        "available": "32.429",
        "holds": "0",
    },
    {
        "id": "6066486a51e965000609f747",
        "currency": "XRP",
        "type": "trade",
        "balance": "0.15067000",
        "available": "0.15067000",
        "holds": "0",
    },
    {
        "id": "6066486a51e965000609f749",
        "currency": "ETH",
        "type": "trade",
        "balance": "-29.47050000",
        "available": "-29.47050000",
        "holds": "0",
    },
]

GET_FILL_LIST = {
    "currentPage": 1,
    "pageSize": 50,
    "totalNum": 2,
    "totalPage": 1,
    "items": [
        {
            "symbol": "NANO-USDT",
            "tradeId": "60ecbb16b2e595517a591a2a",
            "orderId": "60ecbb163be06500064327f0",
            "counterOrderId": "60ecbb152575430006cdc381",
            "side": "buy",
            "liquidity": "taker",
            "forceTaker": True,
            "price": "4.401912",
            "size": "0.45434801",
            "funds": "1.99999995739512",
            "fee": "0.00199999995739512",
            "feeRate": "0.001",
            "feeCurrency": "USDT",
            "stop": "",
            "tradeType": "TRADE",
            "type": "market",
            "createdAt": 1626127126000,
        },
        {
            "symbol": "XRP-USDT",
            "tradeId": "60ecb7242e113d325d7c8f85",
            "orderId": "60ecb7243be065000638375d",
            "counterOrderId": "60ecb6d66514160006c0cbc0",
            "side": "buy",
            "liquidity": "taker",
            "forceTaker": True,
            "price": "0.62504",
            "size": "7.99948803",
            "funds": "4.9999999982712",
            "fee": "0.0049999999982712",
            "feeRate": "0.001",
            "feeCurrency": "USDT",
            "stop": "",
            "tradeType": "TRADE",
            "type": "market",
            "createdAt": 1626126116000,
        },
        {
            "symbol": "NANO-USDT",
            "tradeId": "60ecbf5bb2e595517a5931a9",
            "orderId": "60ecbf5b81118200068da563",
            "counterOrderId": "60ecbf4d81118200068d8176",
            "side": "sell",
            "liquidity": "taker",
            "forceTaker": True,
            "price": "4.417691",
            "size": "0.01",
            "funds": "0.04417691",
            "fee": "0.00004417691",
            "feeRate": "0.001",
            "feeCurrency": "USDT",
            "stop": "",
            "tradeType": "TRADE",
            "type": "market",
            "createdAt": 1626128219000,
        },
    ],
}

CREATE_MARKET_ORDER_BUY_RESPONSE = {"orderId": "60ee040c651416000684b835"}
CREATE_MARKET_ORDER_SELL_RESPONSE = {"orderId": "60ee040c651416000684b843"}


@patch("broker.kucoin_broker.KucoinBroker.update_transactions")
@patch("broker.kucoin_broker.KucoinBroker.update_balances_and_positions")
@patch("broker.kucoin_broker.KucoinBroker.update_price")
@patch("kucoin.client.Market.get_symbol_list")
@patch("kucoin.client.User.__init__")
@patch("kucoin.client.Trade.__init__")
@patch("kucoin.client.Market.__init__")
def test_lot_size_info(
    market_client_init_mocked,
    trade_client_init_mocked,
    user_client_init_mocked,
    get_symbols_list_mocked,
    update_price_mocked,
    update_balances_mocked,
    update_transactions_mocked,
):
    market_client_init_mocked.return_value = None
    trade_client_init_mocked.return_value = None
    user_client_init_mocked.return_value = None
    get_symbols_list_mocked.return_value = GET_SYMBOL_LIST_RETURN_VALUE
    clock = LiveClock()
    events = deque()
    kucoin_broker = KucoinBroker(clock=clock, events=events)
    assert kucoin_broker.lot_size == {
        Asset(symbol="BTCUSDT", exchange=EXCHANGE): 0.00001,
        Asset(symbol="ETHUSDT", exchange=EXCHANGE): 0.0001,
        Asset(symbol="LINKUSDT", exchange=EXCHANGE): 0.001,
        Asset(symbol="SXPUSDT", exchange=EXCHANGE): 0.01,
        Asset(symbol="XRPUSDT", exchange=EXCHANGE): 0.1,
    }
    assert kucoin_broker.symbol_to_kucoin_symbol == {
        "BTCUSDT": "BTC-USDT",
        "ETHUSDT": "ETH-USDT",
        "LINKUSDT": "LINK-USDT",
        "SXPUSDT": "SXP-USDT",
        "XRPUSDT": "XRP-USDT",
    }

    assert isinstance(kucoin_broker.lot_size_last_update, datetime)
    timestamp = kucoin_broker.lot_size_last_update

    # Call again to see if the value is cached
    kucoin_broker.update_lot_size_info()
    assert kucoin_broker.lot_size_last_update == timestamp


@patch("broker.kucoin_broker.KucoinBroker.update_transactions")
@patch("broker.kucoin_broker.KucoinBroker.update_balances_and_positions")
@patch("kucoin.client.Market.get_all_tickers")
@patch("kucoin.client.Market.get_symbol_list")
@patch("kucoin.client.User.__init__")
@patch("kucoin.client.Trade.__init__")
@patch("kucoin.client.Market.__init__")
def test_update_price(
    market_client_init_mocked,
    trade_client_init_mocked,
    user_client_init_mocked,
    get_symbols_list_mocked,
    get_all_tickers_mocked,
    update_balances_mocked,
    update_transactions_mocked,
):
    market_client_init_mocked.return_value = None
    trade_client_init_mocked.return_value = None
    user_client_init_mocked.return_value = None
    get_symbols_list_mocked.return_value = GET_SYMBOL_LIST_RETURN_VALUE
    get_all_tickers_mocked.return_value = GET_ALL_TICKERS_RETURN_VALUE
    clock = LiveClock()
    events = deque()
    kucoin_broker = KucoinBroker(clock=clock, events=events)
    assert kucoin_broker.last_prices == {
        Asset(symbol="BTCUSDT", exchange=EXCHANGE): 58947.6,
        Asset(symbol="ETHUSDT", exchange=EXCHANGE): 1931.87,
        Asset(symbol="LINKUSDT", exchange=EXCHANGE): 28.7599,
        Asset(symbol="SXPUSDT", exchange=EXCHANGE): 3.4033,
        Asset(symbol="XRPUSDT", exchange=EXCHANGE): 0.56,
    }

    assert isinstance(kucoin_broker.price_last_update, datetime)
    timestamp = kucoin_broker.price_last_update

    # Call again to see if the value is cached
    kucoin_broker.update_price()
    assert kucoin_broker.price_last_update == timestamp


@patch("broker.kucoin_broker.KucoinBroker.update_transactions")
@patch("kucoin.client.User.get_account_list")
@patch("kucoin.client.Market.get_all_tickers")
@patch("kucoin.client.Market.get_symbol_list")
@patch("kucoin.client.User.__init__")
@patch("kucoin.client.Trade.__init__")
@patch("kucoin.client.Market.__init__")
def test_update_balances_and_positions(
    market_client_init_mocked,
    trade_client_init_mocked,
    user_client_init_mocked,
    get_symbols_list_mocked,
    get_all_tickers_mocked,
    get_account_list_mocked,
    update_transactions_mocked,
):
    market_client_init_mocked.return_value = None
    trade_client_init_mocked.return_value = None
    user_client_init_mocked.return_value = None
    get_symbols_list_mocked.return_value = GET_SYMBOL_LIST_RETURN_VALUE
    get_all_tickers_mocked.return_value = GET_ALL_TICKERS_RETURN_VALUE
    get_account_list_mocked.return_value = GET_ACCOUNT_LIST_RETURN_VALUE
    clock = LiveClock()
    events = deque()
    kucoin_broker = KucoinBroker(clock=clock, events=events)

    assert kucoin_broker.cash_balances == {
        "EUR": 0,
        "USDT": float(INITIAL_CASH),
    }
    assert isinstance(kucoin_broker.balances_last_update, datetime)

    price1 = 1931.87
    buy_size1 = 0
    sell_size1 = -29.47050000
    expected_position1 = Position(
        asset=ASSET1,
        price=price1,
        buy_size=buy_size1,
        sell_size=sell_size1,
        direction=Direction.SHORT,
    )

    price2 = 0.56
    buy_size2 = 0.15067000
    sell_size2 = 0
    expected_position2 = Position(
        asset=ASSET2,
        price=price2,
        buy_size=buy_size2,
        sell_size=sell_size2,
        direction=Direction.LONG,
    )

    expected_positions = {
        ASSET1: {Direction.SHORT: expected_position1},
        ASSET2: {Direction.LONG: expected_position2},
    }

    assert kucoin_broker.portfolio.pos_handler.positions == expected_positions

    # Call again to see if the value is cached
    timestamp = kucoin_broker.balances_last_update
    kucoin_broker.update_balances_and_positions()
    assert kucoin_broker.balances_last_update == timestamp

    assert kucoin_broker.get_cash_balance() == {
        "EUR": 0,
        "USDT": float(INITIAL_CASH),
    }

    assert kucoin_broker.get_cash_balance("USDT") == float(INITIAL_CASH)

    with pytest.raises(ValueError):
        assert kucoin_broker.get_cash_balance("GBP")

    get_account_list_mocked.assert_has_calls([call()])
    get_all_tickers_mocked.assert_has_calls([call()])


@patch("broker.kucoin_broker.KucoinBroker.update_transactions")
@patch("kucoin.client.User.get_account_list")
@patch("kucoin.client.Market.get_all_tickers")
@patch("kucoin.client.Market.get_symbol_list")
@patch("kucoin.client.User.__init__")
@patch("kucoin.client.Trade.__init__")
@patch("kucoin.client.Market.__init__")
def test_has_opened_position(
    market_client_init_mocked,
    trade_client_init_mocked,
    user_client_init_mocked,
    get_symbols_list_mocked,
    get_all_tickers_mocked,
    get_account_list_mocked,
    update_transactions_mocked,
):
    market_client_init_mocked.return_value = None
    trade_client_init_mocked.return_value = None
    user_client_init_mocked.return_value = None
    get_symbols_list_mocked.return_value = GET_SYMBOL_LIST_RETURN_VALUE
    get_all_tickers_mocked.return_value = GET_ALL_TICKERS_RETURN_VALUE
    get_account_list_mocked.return_value = GET_ACCOUNT_LIST_RETURN_VALUE
    clock = LiveClock()
    events = deque()
    kucoin_broker = KucoinBroker(clock=clock, events=events)

    assert kucoin_broker.has_opened_position(ASSET2, Direction.LONG)
    assert not kucoin_broker.has_opened_position(ASSET2, Direction.SHORT)
    assert kucoin_broker.has_opened_position(ASSET1, Direction.SHORT)
    assert not kucoin_broker.has_opened_position(ASSET1, Direction.LONG)
    assert not kucoin_broker.has_opened_position(ASSET3, Direction.LONG)
    assert not kucoin_broker.has_opened_position(ASSET3, Direction.SHORT)


@patch("common.clock.LiveClock.current_time")
@patch("kucoin.client.Trade.get_fill_list")
@patch("kucoin.client.User.get_account_list")
@patch("kucoin.client.Market.get_all_tickers")
@patch("kucoin.client.Market.get_symbol_list")
@patch("kucoin.client.User.__init__")
@patch("kucoin.client.Trade.__init__")
@patch("kucoin.client.Market.__init__")
@freeze_time("2021-02-22 11:45:57+00:00")
def test_update_transactions(
    market_client_init_mocked,
    trade_client_init_mocked,
    user_client_init_mocked,
    get_symbols_list_mocked,
    get_all_tickers_mocked,
    get_account_list_mocked,
    get_fill_list_mocked,
    current_time_mocked,
):
    market_client_init_mocked.return_value = None
    trade_client_init_mocked.return_value = None
    user_client_init_mocked.return_value = None
    get_symbols_list_mocked.return_value = GET_SYMBOL_LIST_RETURN_VALUE
    get_all_tickers_mocked.return_value = GET_ALL_TICKERS_RETURN_VALUE
    get_account_list_mocked.return_value = GET_ACCOUNT_LIST_RETURN_VALUE
    get_fill_list_mocked.return_value = GET_FILL_LIST

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
    kucoin_broker = KucoinBroker(clock=clock, events=events)
    assert isinstance(kucoin_broker.transactions_last_update, datetime)

    # Call again to see if the value is cached
    timestamp = kucoin_broker.transactions_last_update
    kucoin_broker.update_transactions()
    assert kucoin_broker.transactions_last_update == timestamp

    assert len(kucoin_broker.portfolio.history) == 3
    assert kucoin_broker.portfolio.history[0] == PortfolioEvent(
        timestamp=datetime.strptime("2021-07-12 21:58:46+0000", "%Y-%m-%d %H:%M:%S%z"),
        type="symbol_transaction",
        description="BUY LONG 0.45434801 KUCOIN-NANOUSDT 4.401912 12/07/2021",
        debit=2.001999957352515,
        credit=0.0,
        balance=32.429,
    )
    assert kucoin_broker.portfolio.history[1] == PortfolioEvent(
        timestamp=datetime.strptime("2021-07-12 21:41:56+0000", "%Y-%m-%d %H:%M:%S%z"),
        type="symbol_transaction",
        description="BUY LONG 7.99948803 KUCOIN-XRPUSDT 0.62504 12/07/2021",
        debit=5.004999998269472,
        credit=0.0,
        balance=32.429,
    )
    assert kucoin_broker.portfolio.history[2] == PortfolioEvent(
        timestamp=datetime.strptime("2021-07-12 22:16:59+0000", "%Y-%m-%d %H:%M:%S%z"),
        type="symbol_transaction",
        description="SELL LONG 0.01 KUCOIN-NANOUSDT 4.417691 12/07/2021",
        debit=0.0,
        credit=0.04,
        balance=32.43,
    )

    get_account_list_mocked.assert_has_calls([call()])
    get_all_tickers_mocked.assert_has_calls([call()])

    get_fill_list_mocked_calls = [call(startAt=1612796880000, tradeType="TRADE")]
    get_fill_list_mocked.assert_has_calls(get_fill_list_mocked_calls)


@patch("kucoin.client.Trade.create_market_order")
@patch("kucoin.client.Trade.get_fill_list")
@patch("kucoin.client.User.get_account_list")
@patch("kucoin.client.Market.get_all_tickers")
@patch("kucoin.client.Market.get_symbol_list")
@patch("kucoin.client.User.__init__")
@patch("kucoin.client.Trade.__init__")
@patch("kucoin.client.Market.__init__")
def test_execute_market_order(
    market_client_init_mocked,
    trade_client_init_mocked,
    user_client_init_mocked,
    get_symbols_list_mocked,
    get_all_tickers_mocked,
    get_account_list_mocked,
    get_fill_list_mocked,
    create_market_order_mocked,
):
    market_client_init_mocked.return_value = None
    trade_client_init_mocked.return_value = None
    user_client_init_mocked.return_value = None
    get_symbols_list_mocked.return_value = GET_SYMBOL_LIST_RETURN_VALUE
    get_all_tickers_mocked.return_value = GET_ALL_TICKERS_RETURN_VALUE
    get_account_list_mocked.return_value = GET_ACCOUNT_LIST_RETURN_VALUE
    get_fill_list_mocked.return_value = GET_FILL_LIST

    create_market_order_mocked.side_effect = [
        CREATE_MARKET_ORDER_BUY_RESPONSE,
        Exception(),
        CREATE_MARKET_ORDER_SELL_RESPONSE,
    ]

    clock = LiveClock()
    events = deque()
    kucoin_broker = KucoinBroker(clock=clock, events=events)

    # test buy orders
    buy_order = Order(
        asset=ASSET3,
        action=Action.BUY,
        direction=Direction.LONG,
        size=2.5,
        signal_id="1",
        type=OrderType.MARKET,
        clock=clock,
    )

    assert kucoin_broker.currency_pairs_traded == {
        Asset(symbol="XRPUSDT", exchange=EXCHANGE),
        Asset(symbol="ETHUSDT", exchange=EXCHANGE),
    }

    kucoin_broker.execute_order(buy_order)
    assert buy_order.order_id == "60ee040c651416000684b835"
    assert kucoin_broker.currency_pairs_traded == {
        Asset(symbol="XRPUSDT", exchange=EXCHANGE),
        Asset(symbol="ETHUSDT", exchange=EXCHANGE),
        Asset(symbol="SXPUSDT", exchange=EXCHANGE),
    }

    # test kucoin buyorder Exception
    kucoin_broker.execute_order(buy_order)

    # test sell orders
    sell_order = Order(
        asset=ASSET3,
        action=Action.SELL,
        direction=Direction.LONG,
        size=2.4,
        signal_id="1",
        type=OrderType.MARKET,
        clock=clock,
    )

    assert kucoin_broker.currency_pairs_traded == {
        Asset(symbol="XRPUSDT", exchange=EXCHANGE),
        Asset(symbol="ETHUSDT", exchange=EXCHANGE),
        Asset(symbol="SXPUSDT", exchange=EXCHANGE),
    }

    kucoin_broker.execute_order(sell_order)
    assert sell_order.order_id == "60ee040c651416000684b843"
    assert kucoin_broker.currency_pairs_traded == {
        Asset(symbol="XRPUSDT", exchange=EXCHANGE),
        Asset(symbol="ETHUSDT", exchange=EXCHANGE),
        Asset(symbol="SXPUSDT", exchange=EXCHANGE),
    }
    assert kucoin_broker.open_orders_ids == {
        "60ee040c651416000684b843",
        "60ee040c651416000684b835",
    }

    # check mock calls
    create_market_order_mocked_calls = [
        call(side="buy", size=2.5, symbol="SXP-USDT"),
        call(side="buy", size=2.5, symbol="SXP-USDT"),
        call(side="buy", size=2.4, symbol="SXP-USDT"),
    ]
    create_market_order_mocked.assert_has_calls(create_market_order_mocked_calls)


@patch("kucoin.client.Trade.create_limit_order")
@patch("kucoin.client.Trade.get_fill_list")
@patch("kucoin.client.User.get_account_list")
@patch("kucoin.client.Market.get_all_tickers")
@patch("kucoin.client.Market.get_symbol_list")
@patch("kucoin.client.User.__init__")
@patch("kucoin.client.Trade.__init__")
@patch("kucoin.client.Market.__init__")
def test_execute_limit_order(
    market_client_init_mocked,
    trade_client_init_mocked,
    user_client_init_mocked,
    get_symbols_list_mocked,
    get_all_tickers_mocked,
    get_account_list_mocked,
    get_fill_list_mocked,
    create_limit_order_mocked,
):
    market_client_init_mocked.return_value = None
    trade_client_init_mocked.return_value = None
    user_client_init_mocked.return_value = None
    get_symbols_list_mocked.return_value = GET_SYMBOL_LIST_RETURN_VALUE
    get_all_tickers_mocked.return_value = GET_ALL_TICKERS_RETURN_VALUE
    get_account_list_mocked.return_value = GET_ACCOUNT_LIST_RETURN_VALUE
    get_fill_list_mocked.return_value = GET_FILL_LIST

    create_limit_order_mocked.side_effect = [
        CREATE_MARKET_ORDER_BUY_RESPONSE,
        Exception(),
        CREATE_MARKET_ORDER_SELL_RESPONSE,
    ]

    clock = LiveClock()
    events = deque()
    kucoin_broker = KucoinBroker(clock=clock, events=events)

    # test buy orders
    buy_order = Order(
        asset=ASSET2,
        action=Action.BUY,
        direction=Direction.LONG,
        size=6.97654,
        signal_id="1",
        type=OrderType.LIMIT,
        limit=0.3534,
        clock=clock,
    )

    assert kucoin_broker.currency_pairs_traded == {
        Asset(symbol="ETHUSDT", exchange=EXCHANGE),
        Asset(symbol="XRPUSDT", exchange=EXCHANGE),
    }

    kucoin_broker.execute_order(buy_order)
    assert buy_order.order_id == "60ee040c651416000684b835"
    assert kucoin_broker.currency_pairs_traded == {
        Asset(symbol="ETHUSDT", exchange=EXCHANGE),
        Asset(symbol="XRPUSDT", exchange=EXCHANGE),
    }
    assert kucoin_broker.open_orders_ids == {"60ee040c651416000684b835"}

    # test kucoin buyorder Exception
    kucoin_broker.execute_order(buy_order)

    # test sell orders
    sell_order = Order(
        asset=ASSET2,
        action=Action.SELL,
        direction=Direction.LONG,
        size=6.77654,
        signal_id="1",
        type=OrderType.LIMIT,
        limit=0.4534,
        clock=clock,
    )

    assert kucoin_broker.currency_pairs_traded == {
        Asset(symbol="ETHUSDT", exchange=EXCHANGE),
        Asset(symbol="XRPUSDT", exchange=EXCHANGE),
    }
    kucoin_broker.execute_order(sell_order)
    assert sell_order.order_id == "60ee040c651416000684b843"
    assert kucoin_broker.currency_pairs_traded == {
        Asset(symbol="ETHUSDT", exchange=EXCHANGE),
        Asset(symbol="XRPUSDT", exchange=EXCHANGE),
    }
    assert kucoin_broker.open_orders_ids == {
        "60ee040c651416000684b835",
        "60ee040c651416000684b843",
    }

    # check mock calls
    create_limit_order_mocked_calls = [
        call(price=0.3534, side="buy", size=6.9, symbol="XRP-USDT"),
        call(price=0.3534, side="buy", size=6.9, symbol="XRP-USDT"),
        call(price=0.4534, side="buy", size=0.1, symbol="XRP-USDT"),
    ]
    create_limit_order_mocked.assert_has_calls(create_limit_order_mocked_calls)


@patch("broker.kucoin_broker.KucoinBroker.execute_market_order")
@patch("kucoin.client.Trade.get_fill_list")
@patch("kucoin.client.User.get_account_list")
@patch("kucoin.client.Market.get_all_tickers")
@patch("kucoin.client.Market.get_symbol_list")
@patch("kucoin.client.User.__init__")
@patch("kucoin.client.Trade.__init__")
@patch("kucoin.client.Market.__init__")
def test_execute_stop_order(
    market_client_init_mocked,
    trade_client_init_mocked,
    user_client_init_mocked,
    get_symbols_list_mocked,
    get_all_tickers_mocked,
    get_account_list_mocked,
    get_fill_list_mocked,
    execute_market_order_mocked,
):
    market_client_init_mocked.return_value = None
    trade_client_init_mocked.return_value = None
    user_client_init_mocked.return_value = None
    get_symbols_list_mocked.return_value = GET_SYMBOL_LIST_RETURN_VALUE
    get_all_tickers_mocked.return_value = GET_ALL_TICKERS_RETURN_VALUE
    get_account_list_mocked.return_value = GET_ACCOUNT_LIST_RETURN_VALUE
    get_fill_list_mocked.return_value = GET_FILL_LIST

    clock = LiveClock()
    events = deque()
    kucoin_broker = KucoinBroker(clock=clock, events=events)

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
    kucoin_broker.execute_order(stop_order_sell)
    assert len(kucoin_broker.open_orders) == 1
    assert kucoin_broker.open_orders.popleft() == stop_order_sell

    kucoin_broker.last_prices[ASSET2] = 0.18
    kucoin_broker.execute_order(stop_order_sell)
    assert len(kucoin_broker.open_orders) == 0

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
    kucoin_broker.execute_order(stop_order_buy)
    assert len(kucoin_broker.open_orders) == 1
    assert kucoin_broker.open_orders.popleft() == stop_order_buy

    kucoin_broker.last_prices[ASSET2] = 0.46
    kucoin_broker.execute_order(stop_order_buy)
    assert len(kucoin_broker.open_orders) == 0

    execute_market_order_mocked_calls = [call(stop_order_sell), call(stop_order_buy)]
    execute_market_order_mocked.assert_has_calls(execute_market_order_mocked_calls)


@patch("broker.kucoin_broker.KucoinBroker.execute_market_order")
@patch("kucoin.client.Trade.get_fill_list")
@patch("kucoin.client.User.get_account_list")
@patch("kucoin.client.Market.get_all_tickers")
@patch("kucoin.client.Market.get_symbol_list")
@patch("kucoin.client.User.__init__")
@patch("kucoin.client.Trade.__init__")
@patch("kucoin.client.Market.__init__")
def test_execute_target_order(
    market_client_init_mocked,
    trade_client_init_mocked,
    user_client_init_mocked,
    get_symbols_list_mocked,
    get_all_tickers_mocked,
    get_account_list_mocked,
    get_fill_list_mocked,
    execute_market_order_mocked,
):
    market_client_init_mocked.return_value = None
    trade_client_init_mocked.return_value = None
    user_client_init_mocked.return_value = None
    get_symbols_list_mocked.return_value = GET_SYMBOL_LIST_RETURN_VALUE
    get_all_tickers_mocked.return_value = GET_ALL_TICKERS_RETURN_VALUE
    get_account_list_mocked.return_value = GET_ACCOUNT_LIST_RETURN_VALUE
    get_fill_list_mocked.return_value = GET_FILL_LIST

    clock = LiveClock()
    events = deque()
    kucoin_broker = KucoinBroker(clock=clock, events=events)

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
    kucoin_broker.execute_order(target_order_sell)
    assert len(kucoin_broker.open_orders) == 1
    assert kucoin_broker.open_orders.popleft() == target_order_sell

    kucoin_broker.last_prices[ASSET2] = 1.02
    kucoin_broker.execute_order(target_order_sell)
    assert len(kucoin_broker.open_orders) == 0

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
    kucoin_broker.last_prices[ASSET2] = 1.13
    kucoin_broker.execute_order(target_order_buy)
    assert len(kucoin_broker.open_orders) == 1
    assert kucoin_broker.open_orders.popleft() == target_order_buy

    kucoin_broker.last_prices[ASSET2] = 0.87
    kucoin_broker.execute_order(target_order_buy)
    assert len(kucoin_broker.open_orders) == 0

    execute_market_order_mocked_calls = [
        call(target_order_sell),
        call(target_order_buy),
    ]
    execute_market_order_mocked.assert_has_calls(execute_market_order_mocked_calls)


@patch("broker.kucoin_broker.KucoinBroker.execute_market_order")
@patch("kucoin.client.Trade.get_fill_list")
@patch("kucoin.client.User.get_account_list")
@patch("kucoin.client.Market.get_all_tickers")
@patch("kucoin.client.Market.get_symbol_list")
@patch("kucoin.client.User.__init__")
@patch("kucoin.client.Trade.__init__")
@patch("kucoin.client.Market.__init__")
def test_execute_trailing_stop_order_sell(
    market_client_init_mocked,
    trade_client_init_mocked,
    user_client_init_mocked,
    get_symbols_list_mocked,
    get_all_tickers_mocked,
    get_account_list_mocked,
    get_fill_list_mocked,
    execute_market_order_mocked,
):
    market_client_init_mocked.return_value = None
    trade_client_init_mocked.return_value = None
    user_client_init_mocked.return_value = None
    get_symbols_list_mocked.return_value = GET_SYMBOL_LIST_RETURN_VALUE
    get_all_tickers_mocked.return_value = GET_ALL_TICKERS_RETURN_VALUE
    get_account_list_mocked.return_value = GET_ACCOUNT_LIST_RETURN_VALUE
    get_fill_list_mocked.return_value = GET_FILL_LIST

    clock = LiveClock()
    events = deque()
    kucoin_broker = KucoinBroker(clock=clock, events=events)

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
    kucoin_broker.last_prices[ASSET2] = 1.11
    kucoin_broker.execute_order(trailing_stop_order)
    assert len(kucoin_broker.open_orders) == 1
    assert kucoin_broker.open_orders.popleft() == trailing_stop_order
    assert trailing_stop_order.order_id is not None
    assert trailing_stop_order.stop == 1.11 - 1.11 * trailing_stop_order.stop_pct

    kucoin_broker.last_prices[ASSET2] = 1.15
    kucoin_broker.execute_order(trailing_stop_order)
    assert len(kucoin_broker.open_orders) == 1
    assert kucoin_broker.open_orders.popleft() == trailing_stop_order
    assert trailing_stop_order.stop == 1.15 - 1.15 * trailing_stop_order.stop_pct

    kucoin_broker.last_prices[ASSET2] = 1.09
    kucoin_broker.execute_order(trailing_stop_order)
    assert len(kucoin_broker.open_orders) == 0

    execute_market_order_mocked_calls = [call(trailing_stop_order)]
    execute_market_order_mocked.assert_has_calls(execute_market_order_mocked_calls)


@patch("broker.kucoin_broker.KucoinBroker.execute_market_order")
@patch("kucoin.client.Trade.get_fill_list")
@patch("kucoin.client.User.get_account_list")
@patch("kucoin.client.Market.get_all_tickers")
@patch("kucoin.client.Market.get_symbol_list")
@patch("kucoin.client.User.__init__")
@patch("kucoin.client.Trade.__init__")
@patch("kucoin.client.Market.__init__")
def test_execute_trailing_stop_order_buy(
    market_client_init_mocked,
    trade_client_init_mocked,
    user_client_init_mocked,
    get_symbols_list_mocked,
    get_all_tickers_mocked,
    get_account_list_mocked,
    get_fill_list_mocked,
    execute_market_order_mocked,
):
    market_client_init_mocked.return_value = None
    trade_client_init_mocked.return_value = None
    user_client_init_mocked.return_value = None
    get_symbols_list_mocked.return_value = GET_SYMBOL_LIST_RETURN_VALUE
    get_all_tickers_mocked.return_value = GET_ALL_TICKERS_RETURN_VALUE
    get_account_list_mocked.return_value = GET_ACCOUNT_LIST_RETURN_VALUE
    get_fill_list_mocked.return_value = GET_FILL_LIST

    clock = LiveClock()
    events = deque()
    kucoin_broker = KucoinBroker(clock=clock, events=events)

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
    kucoin_broker.last_prices[ASSET2] = 1.11
    kucoin_broker.execute_order(trailing_stop_order)
    assert len(kucoin_broker.open_orders) == 1
    assert kucoin_broker.open_orders.popleft() == trailing_stop_order
    assert trailing_stop_order.order_id is not None
    assert trailing_stop_order.stop == 1.11 + 1.11 * trailing_stop_order.stop_pct

    kucoin_broker.last_prices[ASSET2] = 1.02
    kucoin_broker.execute_order(trailing_stop_order)
    assert len(kucoin_broker.open_orders) == 1
    assert kucoin_broker.open_orders.popleft() == trailing_stop_order
    assert trailing_stop_order.stop == 1.02 + 1.02 * trailing_stop_order.stop_pct

    kucoin_broker.last_prices[ASSET2] = 1.072
    kucoin_broker.execute_order(trailing_stop_order)
    assert len(kucoin_broker.open_orders) == 0

    execute_market_order_mocked_calls = [call(trailing_stop_order)]
    execute_market_order_mocked.assert_has_calls(execute_market_order_mocked_calls)


@patch("broker.kucoin_broker.KucoinBroker.update_balances_and_positions")
@patch("broker.kucoin_broker.KucoinBroker.update_lot_size_info")
@patch("broker.kucoin_broker.KucoinBroker.update_transactions")
@patch("broker.kucoin_broker.KucoinBroker.update_price")
@patch("kucoin.client.User.__init__")
@patch("kucoin.client.Trade.__init__")
@patch("kucoin.client.Market.__init__")
def test_synchronize(
    market_client_init_mocked,
    trade_client_init_mocked,
    user_client_init_mocked,
    update_price_mocked,
    update_transactions_mocked,
    update_lot_size_info_mocked,
    update_balances_and_positions_mocked,
):
    market_client_init_mocked.return_value = None
    trade_client_init_mocked.return_value = None
    user_client_init_mocked.return_value = None
    clock = LiveClock()
    events = deque()
    kucoin_broker = KucoinBroker(clock=clock, events=events)

    kucoin_broker.synchronize()

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


@patch("kucoin.client.Trade.get_fill_list")
@patch("kucoin.client.User.get_account_list")
@patch("kucoin.client.Market.get_all_tickers")
@patch("kucoin.client.Market.get_symbol_list")
@patch("kucoin.client.User.__init__")
@patch("kucoin.client.Trade.__init__")
@patch("kucoin.client.Market.__init__")
def test_max_order_entry_size(
    market_client_init_mocked,
    trade_client_init_mocked,
    user_client_init_mocked,
    get_symbols_list_mocked,
    get_all_tickers_mocked,
    get_account_list_mocked,
    get_fill_list_mocked,
):
    market_client_init_mocked.return_value = None
    trade_client_init_mocked.return_value = None
    user_client_init_mocked.return_value = None
    get_symbols_list_mocked.return_value = GET_SYMBOL_LIST_RETURN_VALUE
    get_all_tickers_mocked.return_value = GET_ALL_TICKERS_RETURN_VALUE
    get_account_list_mocked.return_value = GET_ACCOUNT_LIST_RETURN_VALUE
    get_fill_list_mocked.return_value = GET_FILL_LIST

    clock = LiveClock()
    events = deque()

    kucoin_broker = KucoinBroker(clock=clock, events=events)

    assert (
        kucoin_broker.max_entry_order_size(asset=ASSET2, direction=Direction.LONG)
        == 57.851077493934646
    )
