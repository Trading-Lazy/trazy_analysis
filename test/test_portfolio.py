from decimal import Decimal

import pandas as pd
import pytest
import pytz

from models.enums import Action, Direction
from portfolio.portfolio import Portfolio
from portfolio.portfolio_event import PortfolioEvent
from position.transaction import Transaction


def test_initial_settings_for_default_portfolio():
    """
    Test that the initial settings are as they should be
    for two specified portfolios.
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)

    # Test a default Portfolio
    port1 = Portfolio()
    assert port1.currency == "USD"
    assert port1.starting_cash == Decimal("0.0")
    assert port1.portfolio_id is None
    assert port1.name is None
    assert port1.total_market_value == Decimal("0.0")
    assert port1.cash == Decimal("0.0")
    assert port1.total_equity == Decimal("0.0")
    assert port1.total_unrealised_pnl == Decimal("0")
    assert port1.total_realised_pnl == Decimal("0")
    assert port1.total_pnl == Decimal("0")

    # Test a Portfolio with keyword arguments
    port2 = Portfolio(
        starting_cash=Decimal("1234567.56"),
        currency="USD",
        portfolio_id=12345,
        name="My Second Test Portfolio",
    )
    assert port2.currency == "USD"
    assert port2.starting_cash == Decimal("1234567.56")
    assert port2.portfolio_id == 12345
    assert port2.name == "My Second Test Portfolio"
    assert port2.total_equity == Decimal("1234567.56")
    assert port2.total_market_value == Decimal("0.0")
    assert port2.total_unrealised_pnl == Decimal("0")
    assert port2.total_realised_pnl == Decimal("0")
    assert port2.total_pnl == Decimal("0")
    assert port2.cash == Decimal("1234567.56")


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
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    later_timestamp = pd.Timestamp("2017-10-06 08:00:00", tz=pytz.UTC)
    pos_cash = Decimal("1000.0")
    neg_cash = -Decimal("1000.0")
    port = Portfolio(starting_cash=Decimal("2000.0"), timestamp=start_timestamp)

    # Test subscribe_funds raises for negative amount
    with pytest.raises(ValueError):
        port.subscribe_funds(neg_cash, start_timestamp)

    # Test subscribe_funds correctly adds positive
    # amount, generates correct event and modifies time
    port.subscribe_funds(pos_cash, later_timestamp)

    assert port.cash == Decimal("3000.0")
    assert port.total_market_value == Decimal("0.0")
    assert port.total_equity == Decimal("3000.0")
    assert port.total_unrealised_pnl == Decimal("0")
    assert port.total_realised_pnl == Decimal("0")
    assert port.total_pnl == Decimal("0")

    pe1 = PortfolioEvent(
        timestamp=start_timestamp,
        type="subscription",
        description="SUBSCRIPTION",
        debit=Decimal("0.0"),
        credit=Decimal("2000.0"),
        balance=Decimal("2000.0"),
    )
    pe2 = PortfolioEvent(
        timestamp=later_timestamp,
        type="subscription",
        description="SUBSCRIPTION",
        debit=Decimal("0.0"),
        credit=Decimal("1000.0"),
        balance=Decimal("3000.0"),
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
    later_timestamp = pd.Timestamp("2017-10-06 08:00:00", tz=pytz.UTC)
    even_later_timestamp = pd.Timestamp("2017-10-07 08:00:00", tz=pytz.UTC)
    pos_cash = Decimal("1000.0")
    neg_cash = -Decimal("1000.0")
    port_raise = Portfolio(timestamp=later_timestamp)

    # Test withdraw_funds raises for incorrect datetime
    with pytest.raises(ValueError):
        port_raise.withdraw_funds(pos_cash, later_timestamp)

    # Test withdraw_funds raises for negative amount
    with pytest.raises(ValueError):
        port_raise.withdraw_funds(neg_cash, later_timestamp)

    # Test withdraw_funds raises for not enough cash
    port_broke = Portfolio()
    port_broke.subscribe_funds(Decimal("1000.0"), later_timestamp)

    with pytest.raises(ValueError):
        port_broke.withdraw_funds(Decimal("2000.0"), later_timestamp)

    # Test withdraw_funds correctly subtracts positive
    # amount, generates correct event and modifies time
    # Initial subscribe
    port_cor = Portfolio(timestamp=later_timestamp)
    port_cor.subscribe_funds(pos_cash, timestamp=later_timestamp)
    pe_sub = PortfolioEvent(
        timestamp=later_timestamp,
        type="subscription",
        description="SUBSCRIPTION",
        debit=Decimal("0.0"),
        credit=Decimal("1000.0"),
        balance=Decimal("1000.0"),
    )
    assert port_cor.cash == Decimal("1000.0")
    assert port_cor.total_market_value == Decimal("0.0")
    assert port_cor.total_equity == Decimal("1000.0")
    assert port_cor.total_unrealised_pnl == Decimal("0")
    assert port_cor.total_realised_pnl == Decimal("0")
    assert port_cor.total_pnl == Decimal("0")
    assert port_cor.history == [pe_sub]

    # Now withdraw
    port_cor.withdraw_funds(Decimal("468.0"), even_later_timestamp)
    pe_wdr = PortfolioEvent(
        timestamp=even_later_timestamp,
        type="withdrawal",
        description="WITHDRAWAL",
        debit=Decimal("468.0"),
        credit=Decimal("0.0"),
        balance=Decimal("532.0"),
    )
    assert port_cor.cash == Decimal("532.0")
    assert port_cor.total_market_value == Decimal("0.0")
    assert port_cor.total_equity == Decimal("532.0")
    assert port_cor.total_unrealised_pnl == Decimal("0")
    assert port_cor.total_realised_pnl == Decimal("0")
    assert port_cor.total_pnl == Decimal("0")
    assert port_cor.history == [pe_sub, pe_wdr]


def test_transact_symbol_behaviour():
    """
    Test transact_symbol raises for incorrect time
    Test correct total_cash and total_securities_value
    for correct transaction (commission etc), correct
    portfolio event and correct time update
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    earlier_timestamp = pd.Timestamp("2017-10-04 08:00:00", tz=pytz.UTC)
    later_timestamp = pd.Timestamp("2017-10-06 08:00:00", tz=pytz.UTC)
    even_later_timestamp = pd.Timestamp("2017-10-07 08:00:00", tz=pytz.UTC)
    port = Portfolio(timestamp=start_timestamp)
    symbol = "AAA"

    # Test transact_symbol raises for incorrect time
    tn_early = Transaction(
        symbol=symbol,
        size=100,
        action=Action.BUY,
        direction=Direction.LONG,
        price=Decimal("567.0"),
        order_id=1,
        commission=Decimal("0.0"),
        timestamp=earlier_timestamp,
    )

    port.transact_symbol(tn_early)

    # Test transact_symbol raises for transaction total
    # cost exceeding total cash
    port.subscribe_funds(Decimal("1000.0"), timestamp=later_timestamp)

    assert port.cash == Decimal("1000.0")
    assert port.total_market_value == Decimal("0.0")
    assert port.total_equity == Decimal("1000.0")
    assert port.total_unrealised_pnl == Decimal("0")
    assert port.total_realised_pnl == Decimal("0")
    assert port.total_pnl == Decimal("0")

    pe_sub1 = PortfolioEvent(
        timestamp=later_timestamp,
        type="subscription",
        description="SUBSCRIPTION",
        debit=Decimal("0.0"),
        credit=Decimal("1000.00"),
        balance=Decimal("1000.00"),
    )

    # Test correct total_cash and total_securities_value
    # for correct transaction (commission etc), correct
    # portfolio event and correct time update
    port.subscribe_funds(Decimal("99000.0"), timestamp=even_later_timestamp)

    assert port.cash == Decimal("100000.0")
    assert port.total_market_value == Decimal("0.0")
    assert port.total_equity == Decimal("100000.0")
    assert port.total_unrealised_pnl == Decimal("0")
    assert port.total_realised_pnl == Decimal("0")
    assert port.total_pnl == Decimal("0")

    pe_sub2 = PortfolioEvent(
        timestamp=even_later_timestamp,
        type="subscription",
        description="SUBSCRIPTION",
        debit=Decimal("0.0"),
        credit=Decimal("99000.00"),
        balance=Decimal("100000.00"),
    )
    tn_even_later = Transaction(
        symbol=symbol,
        size=100,
        action=Action.BUY,
        direction=Direction.LONG,
        price=Decimal("567.00"),
        order_id=1,
        commission=Decimal("15.78"),
        timestamp=even_later_timestamp,
    )
    port.transact_symbol(tn_even_later)

    assert port.cash == Decimal("43284.22")
    assert port.total_market_value == Decimal("56700.00")
    assert port.total_equity == Decimal("99984.22")
    assert port.total_unrealised_pnl == Decimal("-15.7800")
    assert port.total_realised_pnl == Decimal("0")
    assert port.total_pnl == Decimal("-15.7800")

    description = "LONG 100 AAA 567.00 07/10/2017"
    pe_tn = PortfolioEvent(
        timestamp=even_later_timestamp,
        type="symbol_transaction",
        description=description,
        debit=Decimal("56715.78"),
        credit=Decimal("0.0"),
        balance=Decimal("43284.22"),
    )

    assert port.history == [pe_sub1, pe_sub2, pe_tn]


def test_transact_symbol_behaviour_short():
    """
    Test transact_symbol raises for incorrect time
    Test correct total_cash and total_securities_value
    for correct transaction (commission etc), correct
    portfolio event and correct time update
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    earlier_timestamp = pd.Timestamp("2017-10-04 08:00:00", tz=pytz.UTC)
    later_timestamp = pd.Timestamp("2017-10-06 08:00:00", tz=pytz.UTC)
    even_later_timestamp = pd.Timestamp("2017-10-07 08:00:00", tz=pytz.UTC)
    port = Portfolio(timestamp=start_timestamp)
    symbol = "AAA"

    # Test transact_symbol raises for incorrect time
    tn_early = Transaction(
        symbol=symbol,
        size=100,
        action=Action.SELL,
        direction=Direction.SHORT,
        price=Decimal("567.0"),
        order_id=1,
        commission=Decimal("0.0"),
        timestamp=earlier_timestamp,
    )

    # Test transact_symbol raises for transaction total
    # cost exceeding total cash
    port.subscribe_funds(Decimal("1000.0"), timestamp=later_timestamp)

    assert port.cash == Decimal("1000.0")
    assert port.total_market_value == Decimal("0.0")
    assert port.total_equity == Decimal("1000.0")
    assert port.total_unrealised_pnl == Decimal("0")
    assert port.total_realised_pnl == Decimal("0")
    assert port.total_pnl == Decimal("0")

    pe_sub1 = PortfolioEvent(
        timestamp=later_timestamp,
        type="subscription",
        description="SUBSCRIPTION",
        debit=Decimal("0.0"),
        credit=Decimal("1000.0"),
        balance=Decimal("1000.0"),
    )

    # Test correct total_cash and total_securities_value
    # for correct transaction (commission etc), correct
    # portfolio event and correct time update
    port.subscribe_funds(Decimal("99000.0"), timestamp=even_later_timestamp)

    assert port.cash == Decimal("100000.0")
    assert port.total_market_value == Decimal("0.0")
    assert port.total_equity == Decimal("100000.0")
    assert port.total_unrealised_pnl == Decimal("0")
    assert port.total_realised_pnl == Decimal("0")
    assert port.total_pnl == Decimal("0")

    pe_sub2 = PortfolioEvent(
        timestamp=even_later_timestamp,
        type="subscription",
        description="SUBSCRIPTION",
        debit=Decimal("0.0"),
        credit=Decimal("99000.00"),
        balance=Decimal("100000.00"),
    )
    tn_even_later = Transaction(
        symbol=symbol,
        size=100,
        action=Action.SELL,
        direction=Direction.SHORT,
        price=Decimal("567.0"),
        order_id=1,
        commission=Decimal("15.78"),
        timestamp=even_later_timestamp,
    )
    port.transact_symbol(tn_even_later)

    assert port.cash == Decimal("156684.22")
    assert port.total_market_value == Decimal("-56700.00")
    assert port.total_equity == Decimal("99984.22")
    assert port.total_unrealised_pnl == Decimal("-15.7800")
    assert port.total_realised_pnl == Decimal("0")
    assert port.total_pnl == Decimal("-15.7800")

    description = "SHORT 100 AAA 567.0 07/10/2017"
    pe_tn = PortfolioEvent(
        timestamp=even_later_timestamp,
        type="symbol_transaction",
        description=description,
        debit=Decimal("0.0"),
        credit=Decimal("56684.220"),
        balance=Decimal("156684.22"),
    )

    assert port.history == [pe_sub1, pe_sub2, pe_tn]


def test_transact_symbol_not_enough_cash():
    """
    Test transact_symbol raises for incorrect time
    Test correct total_cash and total_securities_value
    for correct transaction (commission etc), correct
    portfolio event and correct time update
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    earlier_timestamp = pd.Timestamp("2017-10-04 08:00:00", tz=pytz.UTC)
    later_timestamp = pd.Timestamp("2017-10-06 08:00:00", tz=pytz.UTC)
    even_later_timestamp = pd.Timestamp("2017-10-07 08:00:00", tz=pytz.UTC)
    port = Portfolio(timestamp=start_timestamp)
    symbol = "AAA"

    # Test transact_symbol raises for transaction total
    # cost exceeding total cash
    port.subscribe_funds(Decimal("1000.0"), timestamp=later_timestamp)

    assert port.cash == Decimal("1000.0")
    assert port.total_market_value == Decimal("0.0")
    assert port.total_equity == Decimal("1000.0")
    assert port.total_unrealised_pnl == Decimal("0")
    assert port.total_realised_pnl == Decimal("0")
    assert port.total_pnl == Decimal("0")

    tn_even_later = Transaction(
        symbol=symbol,
        size=100,
        action=Action.BUY,
        direction=Direction.LONG,
        price=Decimal("567.0"),
        order_id=1,
        commission=Decimal("15.78"),
        timestamp=even_later_timestamp,
    )
    port.transact_symbol(tn_even_later)

    assert port.cash == Decimal("1000.0")
    assert port.total_market_value == Decimal("0.0")
    assert port.total_equity == Decimal("1000.0")
    assert port.total_unrealised_pnl == Decimal("0")
    assert port.total_realised_pnl == Decimal("0")
    assert port.total_pnl == Decimal("0")


def test_portfolio_to_dict_empty_portfolio():
    """
    Test 'portfolio_to_dict' method for an empty Portfolio.
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    port = Portfolio(timestamp=start_timestamp)
    port.subscribe_funds(Decimal("100000.0"), timestamp=start_timestamp)
    port_dict = port.portfolio_to_dict()
    assert port_dict == {}


def test_portfolio_to_dict_for_two_holdings():
    """
    Test portfolio_to_dict for two holdings.
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    symbol1_timestamp = pd.Timestamp("2017-10-06 08:00:00", tz=pytz.UTC)
    symbol2_timestamp = pd.Timestamp("2017-10-07 08:00:00", tz=pytz.UTC)
    update_timestamp = pd.Timestamp("2017-10-08 08:00:00", tz=pytz.UTC)
    symbol1 = "AAA"
    symbol2 = "BBB"

    port = Portfolio(portfolio_id="1234", timestamp=start_timestamp)
    port.subscribe_funds(Decimal("100000.0"), timestamp=start_timestamp)
    tn_symbol1 = Transaction(
        symbol=symbol1,
        size=100,
        action=Action.BUY,
        direction=Direction.LONG,
        price=Decimal("567.0"),
        order_id=1,
        commission=Decimal("15.78"),
        timestamp=symbol1_timestamp,
    )
    port.transact_symbol(tn_symbol1)

    tn_symbol2 = Transaction(
        symbol=symbol2,
        size=100,
        action=Action.BUY,
        direction=Direction.LONG,
        price=Decimal("123.0"),
        order_id=2,
        commission=Decimal("7.64"),
        timestamp=symbol2_timestamp,
    )
    port.transact_symbol(tn_symbol2)
    port.update_market_value_of_symbol(symbol2, Decimal("134.0"), update_timestamp)
    test_holdings = {
        symbol1: {
            Direction.LONG: {
                "size": 100,
                "market_value": Decimal("56700.0"),
                "unrealised_pnl": -Decimal("15.78"),
                "realised_pnl": Decimal("0.0"),
                "total_pnl": -Decimal("15.78"),
            }
        },
        symbol2: {
            Direction.LONG: {
                "size": 100,
                "market_value": Decimal("13400.0"),
                "unrealised_pnl": Decimal("1092.3600"),
                "realised_pnl": Decimal("0.0"),
                "total_pnl": Decimal("1092.3600"),
            }
        },
    }
    port_holdings = port.portfolio_to_dict()

    # This is needed because we're not using Decimal
    # datatypes and have to compare slightly differing
    # floating point representations
    for symbol in (symbol1, symbol2):
        for key, val in test_holdings[symbol].items():
            assert port_holdings[symbol][key] == test_holdings[symbol][key]


def test_update_market_value_of_symbol_not_in_list():
    """
    Test update_market_value_of_symbol for symbol not in list.
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    later_timestamp = pd.Timestamp("2017-10-06 08:00:00", tz=pytz.UTC)
    port = Portfolio(timestamp=start_timestamp)
    symbol = "AAA"
    update = port.update_market_value_of_symbol(
        symbol, Decimal("54.34"), later_timestamp
    )
    assert update is None


def test_update_market_value_of_symbol_negative_price():
    """
    Test update_market_value_of_symbol for
    symbol with negative price.
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    later_timestamp = pd.Timestamp("2017-10-06 08:00:00", tz=pytz.UTC)
    port = Portfolio(timestamp=start_timestamp)

    symbol = "AAA"
    port.subscribe_funds(Decimal("100000.0"), timestamp=later_timestamp)
    tn_symbol = Transaction(
        symbol=symbol,
        size=100,
        action=Action.BUY,
        direction=Direction.LONG,
        price=Decimal("567.0"),
        order_id=1,
        commission=Decimal("15.78"),
        timestamp=later_timestamp,
    )
    port.transact_symbol(tn_symbol)
    with pytest.raises(ValueError):
        port.update_market_value_of_symbol(symbol, -Decimal("54.34"), later_timestamp)


def test_update_market_value_of_symbol_earlier_date():
    """
    Test update_market_value_of_symbol for symbol
    with current_trade_date in past
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    earlier_timestamp = pd.Timestamp("2017-10-04 08:00:00", tz=pytz.UTC)
    later_timestamp = pd.Timestamp("2017-10-06 08:00:00", tz=pytz.UTC)
    port = Portfolio(portfolio_id="1234", timestamp=start_timestamp)

    symbol = "AAA"
    port.subscribe_funds(Decimal("100000.0"), timestamp=later_timestamp)
    tn_symbol = Transaction(
        symbol=symbol,
        size=100,
        action=Action.BUY,
        direction=Direction.LONG,
        price=Decimal("567.0"),
        order_id=1,
        commission=Decimal("15.78"),
        timestamp=later_timestamp,
    )
    port.transact_symbol(tn_symbol)


def test_history_to_df_empty():
    """
    Test 'history_to_df' with no events.
    """
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
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
    start_timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    port1 = Portfolio(
        starting_cash=Decimal("100000.0"),
        portfolio_id="1234",
        timestamp=start_timestamp,
    )
    port2 = Portfolio(
        starting_cash=Decimal("100000.0"),
        portfolio_id="1234",
        timestamp=start_timestamp,
    )
    port3 = Portfolio(
        starting_cash=Decimal("150000.0"),
        portfolio_id="1236",
        timestamp=start_timestamp,
    )
    assert port1 == port2
    assert port1 != port3
    assert port1 != object()
