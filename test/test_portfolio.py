from datetime import datetime

import pandas as pd
import pytest

from trazy_analysis.models.asset import Asset
from trazy_analysis.models.enums import Action, Direction
from trazy_analysis.portfolio.portfolio import Portfolio
from trazy_analysis.portfolio.portfolio_event import PortfolioEvent
from trazy_analysis.position.transaction import Transaction


def test_initial_settings_for_default_portfolio():
    """
    Test that the initial settings are as they should be
    for two specified portfolios.
    """
    start_timestamp = datetime.strptime(
        "2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )

    # Test a default Portfolio
    port1 = Portfolio()
    assert port1.currency == "USD"
    assert port1.starting_cash == 0.0
    assert port1.portfolio_id is None
    assert port1.name is None
    assert port1.total_market_value == 0.0
    assert port1.cash == 0.0
    assert port1.total_equity == 0.0
    assert port1.total_unrealised_pnl == 0
    assert port1.total_realised_pnl == 0
    assert port1.total_pnl == 0

    # Test a Portfolio with keyword arguments
    port2 = Portfolio(
        starting_cash=1234567.56,
        currency="USD",
        portfolio_id=12345,
        name="My Second Test Portfolio",
    )
    assert port2.currency == "USD"
    assert port2.starting_cash == 1234567.56
    assert port2.portfolio_id == 12345
    assert port2.name == "My Second Test Portfolio"
    assert port2.total_equity == 1234567.56
    assert port2.total_market_value == 0.0
    assert port2.total_unrealised_pnl == 0
    assert port2.total_realised_pnl == 0
    assert port2.total_pnl == 0
    assert port2.cash == 1234567.56


def test_portfolio_currency_settings():
    """
    Test that USD and GBP currencies are correctly set with
    some currency keyword arguments and that the currency
    formatter produces the correct strings.
    """
    # Test a US portfolio produces correct values
    cur1 = "USD"
    port1 = Portfolio(currency=cur1)
    assert port1.currency == "USD"

    # Test a UK portfolio produces correct values
    cur2 = "GBP"
    port2 = Portfolio(currency=cur2)
    assert port2.currency == "GBP"


def test_subscribe_funds_behaviour():
    """
    Test subscribe_funds raises for incorrect datetime
    Test subscribe_funds raises for negative amount
    Test subscribe_funds correctly adds positive
    amount, generates correct event and modifies time
    """
    start_timestamp = datetime.strptime(
        "2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    later_timestamp = datetime.strptime(
        "2017-10-06 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    pos_cash = 1000.0
    neg_cash = -1000.0
    port = Portfolio(starting_cash=2000.0, timestamp=start_timestamp)

    # Test subscribe_funds raises for negative amount
    with pytest.raises(ValueError):
        port.subscribe_funds(neg_cash, start_timestamp)

    # Test subscribe_funds correctly adds positive
    # amount, generates correct event and modifies time
    port.subscribe_funds(pos_cash, later_timestamp)

    assert port.cash == 3000.0
    assert port.total_market_value == 0.0
    assert port.total_equity == 3000.0
    assert port.total_unrealised_pnl == 0
    assert port.total_realised_pnl == 0
    assert port.total_pnl == 0

    pe1 = PortfolioEvent(
        timestamp=start_timestamp,
        type="subscription",
        description="SUBSCRIPTION",
        debit=0.0,
        credit=2000.0,
        balance=2000.0,
    )
    pe2 = PortfolioEvent(
        timestamp=later_timestamp,
        type="subscription",
        description="SUBSCRIPTION",
        debit=0.0,
        credit=1000.0,
        balance=3000.0,
    )

    assert port.history == [pe1, pe2]


def test_withdraw_funds_behaviour():
    """
    Test withdraw_funds raises for incorrect datetime
    Test withdraw_funds raises for negative amount
    Test withdraw_funds raises for lack of cash
    Test withdraw_funds correctly subtracts positive
    amount, generates correct event and modifies time
    """
    later_timestamp = datetime.strptime(
        "2017-10-06 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    even_later_timestamp = datetime.strptime(
        "2017-10-07 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    pos_cash = 1000.0
    neg_cash = -1000.0
    port_raise = Portfolio(timestamp=later_timestamp)

    # Test withdraw_funds raises for incorrect datetime
    with pytest.raises(ValueError):
        port_raise.withdraw_funds(pos_cash, later_timestamp)

    # Test withdraw_funds raises for negative amount
    with pytest.raises(ValueError):
        port_raise.withdraw_funds(neg_cash, later_timestamp)

    # Test withdraw_funds raises for not enough cash
    port_broke = Portfolio()
    port_broke.subscribe_funds(1000.0, later_timestamp)

    with pytest.raises(ValueError):
        port_broke.withdraw_funds(2000.0, later_timestamp)

    # Test withdraw_funds correctly subtracts positive
    # amount, generates correct event and modifies time
    # Initial subscribe
    port_cor = Portfolio(timestamp=later_timestamp)
    port_cor.subscribe_funds(pos_cash, timestamp=later_timestamp)
    pe_sub = PortfolioEvent(
        timestamp=later_timestamp,
        type="subscription",
        description="SUBSCRIPTION",
        debit=0.0,
        credit=1000.0,
        balance=1000.0,
    )
    assert port_cor.cash == 1000.0
    assert port_cor.total_market_value == 0.0
    assert port_cor.total_equity == 1000.0
    assert port_cor.total_unrealised_pnl == 0
    assert port_cor.total_realised_pnl == 0
    assert port_cor.total_pnl == 0
    assert port_cor.history == [pe_sub]

    # Now withdraw
    port_cor.withdraw_funds(468.0, even_later_timestamp)
    pe_wdr = PortfolioEvent(
        timestamp=even_later_timestamp,
        type="withdrawal",
        description="WITHDRAWAL",
        debit=468.0,
        credit=0.0,
        balance=532.0,
    )
    assert port_cor.cash == 532.0
    assert port_cor.total_market_value == 0.0
    assert port_cor.total_equity == 532.0
    assert port_cor.total_unrealised_pnl == 0
    assert port_cor.total_realised_pnl == 0
    assert port_cor.total_pnl == 0
    assert port_cor.history == [pe_sub, pe_wdr]


def test_transact_symbol_behaviour():
    """
    Test transact_symbol raises for incorrect time
    Test correct total_cash and total_securities_value
    for correct transaction (commission etc), correct
    portfolio event and correct time update
    """
    start_timestamp = datetime.strptime(
        "2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    earlier_timestamp = datetime.strptime(
        "2017-10-04 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    later_timestamp = datetime.strptime(
        "2017-10-06 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    even_later_timestamp = datetime.strptime(
        "2017-10-07 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    port = Portfolio(timestamp=start_timestamp)
    symbol = "AAA"
    exchange = "IEX"
    asset = Asset(symbol=symbol, exchange=exchange)

    # Test transact_symbol raises for incorrect time
    tn_early = Transaction(
        asset=asset,
        size=100,
        action=Action.BUY,
        direction=Direction.LONG,
        price=567.0,
        order_id=1,
        commission=0.0,
        timestamp=earlier_timestamp,
    )

    port.transact_symbol(tn_early)

    # Test transact_symbol raises for transaction total
    # cost exceeding total cash
    port.subscribe_funds(1000.0, timestamp=later_timestamp)

    assert port.cash == 1000.0
    assert port.total_market_value == 0.0
    assert port.total_equity == 1000.0
    assert port.total_unrealised_pnl == 0
    assert port.total_realised_pnl == 0
    assert port.total_pnl == 0

    pe_sub1 = PortfolioEvent(
        timestamp=later_timestamp,
        type="subscription",
        description="SUBSCRIPTION",
        debit=0.0,
        credit=1000.00,
        balance=1000.00,
    )

    # Test correct total_cash and total_securities_value
    # for correct transaction (commission etc), correct
    # portfolio event and correct time update
    port.subscribe_funds(99000.0, timestamp=even_later_timestamp)

    assert port.cash == 100000.0
    assert port.total_market_value == 0.0
    assert port.total_equity == 100000.0
    assert port.total_unrealised_pnl == 0
    assert port.total_realised_pnl == 0
    assert port.total_pnl == 0

    pe_sub2 = PortfolioEvent(
        timestamp=even_later_timestamp,
        type="subscription",
        description="SUBSCRIPTION",
        debit=0.0,
        credit=99000.00,
        balance=100000.00,
    )
    tn_even_later = Transaction(
        asset=asset,
        size=100,
        action=Action.BUY,
        direction=Direction.LONG,
        price=567.00,
        order_id=1,
        commission=15.78,
        timestamp=even_later_timestamp,
    )
    port.transact_symbol(tn_even_later)

    assert port.cash == 43284.22
    assert port.total_market_value == 56700.00
    assert port.total_equity == 99984.22
    assert port.total_unrealised_pnl == pytest.approx(-15.7800, 0.01)
    assert port.total_realised_pnl == 0
    assert port.total_pnl == pytest.approx(-15.7800, 0.01)

    description = "BUY LONG 100 IEX-AAA-0:01:00 567.0 07/10/2017"
    pe_tn = PortfolioEvent(
        timestamp=even_later_timestamp,
        type="symbol_transaction",
        description=description,
        debit=56715.78,
        credit=0.0,
        balance=43284.22,
    )

    assert port.history == [pe_sub1, pe_sub2, pe_tn]


def test_transact_symbol_behaviour_short():
    """
    Test transact_symbol raises for incorrect time
    Test correct total_cash and total_securities_value
    for correct transaction (commission etc), correct
    portfolio event and correct time update
    """
    start_timestamp = datetime.strptime(
        "2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    earlier_timestamp = datetime.strptime(
        "2017-10-04 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    later_timestamp = datetime.strptime(
        "2017-10-06 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    even_later_timestamp = datetime.strptime(
        "2017-10-07 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    port = Portfolio(timestamp=start_timestamp)
    symbol = "AAA"
    exchange = "IEX"
    asset = Asset(symbol=symbol, exchange=exchange)

    # Test transact_symbol raises for transaction total
    # cost exceeding total cash
    port.subscribe_funds(1000.0, timestamp=later_timestamp)

    assert port.cash == 1000.0
    assert port.total_market_value == 0.0
    assert port.total_equity == 1000.0
    assert port.total_unrealised_pnl == 0
    assert port.total_realised_pnl == 0
    assert port.total_pnl == 0

    pe_sub1 = PortfolioEvent(
        timestamp=later_timestamp,
        type="subscription",
        description="SUBSCRIPTION",
        debit=0.0,
        credit=1000.0,
        balance=1000.0,
    )

    # Test correct total_cash and total_securities_value
    # for correct transaction (commission etc), correct
    # portfolio event and correct time update
    port.subscribe_funds(99000.0, timestamp=even_later_timestamp)

    assert port.cash == 100000.0
    assert port.total_market_value == 0.0
    assert port.total_equity == 100000.0
    assert port.total_unrealised_pnl == 0
    assert port.total_realised_pnl == 0
    assert port.total_pnl == 0

    pe_sub2 = PortfolioEvent(
        timestamp=even_later_timestamp,
        type="subscription",
        description="SUBSCRIPTION",
        debit=0.0,
        credit=99000.00,
        balance=100000.00,
    )
    tn_even_later = Transaction(
        asset=asset,
        size=100,
        action=Action.SELL,
        direction=Direction.SHORT,
        price=567.0,
        order_id=1,
        commission=15.78,
        timestamp=even_later_timestamp,
    )
    port.transact_symbol(tn_even_later)

    assert port.cash == 156684.22
    assert port.total_market_value == -56700.00
    assert port.total_equity == 99984.22
    assert port.total_unrealised_pnl == pytest.approx(-15.7800, 0.01)
    assert port.total_realised_pnl == 0
    assert port.total_pnl == pytest.approx(-15.7800, 0.01)

    description = "SELL SHORT 100 IEX-AAA-0:01:00 567.0 07/10/2017"
    pe_tn = PortfolioEvent(
        timestamp=even_later_timestamp,
        type="symbol_transaction",
        description=description,
        debit=0.0,
        credit=56684.220,
        balance=156684.22,
    )

    assert port.history == [pe_sub1, pe_sub2, pe_tn]


def test_transact_symbol_not_enough_cash():
    """
    Test transact_symbol raises for incorrect time
    Test correct total_cash and total_securities_value
    for correct transaction (commission etc), correct
    portfolio event and correct time update
    """
    start_timestamp = datetime.strptime(
        "2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    earlier_timestamp = datetime.strptime(
        "2017-10-04 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    later_timestamp = datetime.strptime(
        "2017-10-06 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    even_later_timestamp = datetime.strptime(
        "2017-10-07 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    port = Portfolio(timestamp=start_timestamp)
    symbol = "AAA"
    exchange = "IEX"
    asset = Asset(symbol=symbol, exchange=exchange)

    # Test transact_symbol raises for transaction total
    # cost exceeding total cash
    port.subscribe_funds(1000.0, timestamp=later_timestamp)

    assert port.cash == 1000.0
    assert port.total_market_value == 0.0
    assert port.total_equity == 1000.0
    assert port.total_unrealised_pnl == 0
    assert port.total_realised_pnl == 0
    assert port.total_pnl == 0

    tn_even_later = Transaction(
        asset=asset,
        size=100,
        action=Action.BUY,
        direction=Direction.LONG,
        price=567.0,
        order_id=1,
        commission=15.78,
        timestamp=even_later_timestamp,
    )
    port.transact_symbol(tn_even_later)

    assert port.cash == 1000.0
    assert port.total_market_value == 0.0
    assert port.total_equity == 1000.0
    assert port.total_unrealised_pnl == 0
    assert port.total_realised_pnl == 0
    assert port.total_pnl == 0


def test_portfolio_to_dict_empty_portfolio():
    """
    Test 'portfolio_to_dict' method for an empty Portfolio.
    """
    start_timestamp = datetime.strptime(
        "2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    port = Portfolio(timestamp=start_timestamp)
    port.subscribe_funds(100000.0, timestamp=start_timestamp)
    port_dict = port.portfolio_to_dict()
    assert port_dict == {}


def test_portfolio_to_dict_for_two_holdings():
    """
    Test portfolio_to_dict for two holdings.
    """
    start_timestamp = datetime.strptime(
        "2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    symbol1_timestamp = datetime.strptime(
        "2017-10-06 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    symbol2_timestamp = datetime.strptime(
        "2017-10-07 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    update_timestamp = datetime.strptime(
        "2017-10-08 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    exchange = "IEX"
    symbol1 = "AAA"
    asset1 = Asset(symbol=symbol1, exchange=exchange)
    symbol2 = "BBB"
    asset2 = Asset(symbol=symbol2, exchange=exchange)

    port = Portfolio(portfolio_id="1234", timestamp=start_timestamp)
    port.subscribe_funds(100000.0, timestamp=start_timestamp)
    tn_symbol1 = Transaction(
        asset=asset1,
        size=100,
        action=Action.BUY,
        direction=Direction.LONG,
        price=567.0,
        order_id=1,
        commission=15.78,
        timestamp=symbol1_timestamp,
    )
    port.transact_symbol(tn_symbol1)

    tn_symbol2 = Transaction(
        asset=asset2,
        size=100,
        action=Action.BUY,
        direction=Direction.LONG,
        price=123.0,
        order_id=2,
        commission=7.64,
        timestamp=symbol2_timestamp,
    )
    port.transact_symbol(tn_symbol2)
    port.update_market_value_of_symbol(asset2, 134.0, update_timestamp)
    test_holdings = {
        asset1: {
            Direction.LONG: {
                "size": 100,
                "market_value": float("56700.0"),
                "unrealised_pnl": -15.78,
                "realised_pnl": float("0.0"),
                "total_pnl": -15.78,
            }
        },
        asset2: {
            Direction.LONG: {
                "size": 100,
                "market_value": float("13400.0"),
                "unrealised_pnl": float("1092.3600"),
                "realised_pnl": float("0.0"),
                "total_pnl": float("1092.3600"),
            }
        },
    }
    port_holdings = port.portfolio_to_dict()

    # This is needed because we're not using Decimal
    # datatypes and have to compare slightly differing
    # floating point representations
    for asset in (asset1, asset2):
        for key, val in test_holdings[asset].items():
            assert port_holdings[asset.key()][key] == pytest.approx(
                test_holdings[asset][key], 0.01
            )


def test_update_market_value_of_symbol_not_in_list():
    """
    Test update_market_value_of_symbol for symbol not in list.
    """
    start_timestamp = datetime.strptime(
        "2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    later_timestamp = datetime.strptime(
        "2017-10-06 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    port = Portfolio(timestamp=start_timestamp)
    symbol = "AAA"
    exchange = "IEX"
    asset = Asset(symbol=symbol, exchange=exchange)
    update = port.update_market_value_of_symbol(asset, 54.34, later_timestamp)
    assert update is None


def test_update_market_value_of_symbol_negative_price():
    """
    Test update_market_value_of_symbol for
    symbol with negative price.
    """
    start_timestamp = datetime.strptime(
        "2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    later_timestamp = datetime.strptime(
        "2017-10-06 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    port = Portfolio(timestamp=start_timestamp)

    symbol = "AAA"
    exchange = "IEX"
    asset = Asset(symbol=symbol, exchange=exchange)
    port.subscribe_funds(100000.0, timestamp=later_timestamp)
    tn_symbol = Transaction(
        asset=asset,
        size=100,
        action=Action.BUY,
        direction=Direction.LONG,
        price=567.0,
        order_id=1,
        commission=15.78,
        timestamp=later_timestamp,
    )
    port.transact_symbol(tn_symbol)
    with pytest.raises(ValueError):
        port.update_market_value_of_symbol(asset, -54.34, later_timestamp)


def test_update_market_value_of_symbol_earlier_date():
    """
    Test update_market_value_of_symbol for symbol
    with current_trade_date in past
    """
    start_timestamp = datetime.strptime(
        "2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    earlier_timestamp = datetime.strptime(
        "2017-10-04 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    later_timestamp = datetime.strptime(
        "2017-10-06 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    port = Portfolio(portfolio_id="1234", timestamp=start_timestamp)

    symbol = "AAA"
    exchange = "IEX"
    asset = Asset(symbol=symbol, exchange=exchange)
    port.subscribe_funds(100000.0, timestamp=later_timestamp)
    tn_symbol = Transaction(
        asset=asset,
        size=100,
        action=Action.BUY,
        direction=Direction.LONG,
        price=567.0,
        order_id=1,
        commission=15.78,
        timestamp=later_timestamp,
    )
    port.transact_symbol(tn_symbol)


def test_history_to_df_empty():
    """
    Test 'history_to_df' with no events.
    """
    start_timestamp = datetime.strptime(
        "2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    port = Portfolio(timestamp=start_timestamp)
    hist_df = port.history_to_df()
    test_df = pd.DataFrame(
        [], columns=["date", "type", "description", "debit", "credit", "balance"]
    )
    test_df.set_index(keys=["date"], inplace=True)
    assert sorted(test_df.columns) == sorted(hist_df.columns)
    assert len(test_df) == len(hist_df)
    assert len(hist_df) == 0


def test_neq_different_type():
    start_timestamp = datetime.strptime(
        "2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    port1 = Portfolio(
        starting_cash=100000.0,
        portfolio_id="1234",
        timestamp=start_timestamp,
    )
    port2 = Portfolio(
        starting_cash=100000.0,
        portfolio_id="1234",
        timestamp=start_timestamp,
    )
    port3 = Portfolio(
        starting_cash=150000.0,
        portfolio_id="1236",
        timestamp=start_timestamp,
    )
    assert port1 == port2
    assert port1 != port3
    assert port1 != object()
