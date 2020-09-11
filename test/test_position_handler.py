from decimal import Decimal

import pandas as pd
import pytz

from position.position_handler import PositionHandler
from position.transaction import Transaction


def test_transact_position_new_position():
    """
    Tests the 'transact_position' method for a transaction
    with a brand new symbol and checks that all objects are
    set correctly.
    """
    # Create the PositionHandler, Transaction and
    # carry out a transaction
    ph = PositionHandler()
    symbol = "AMZN"

    transaction = Transaction(
        symbol,
        size=100,
        timestamp=pd.Timestamp("2015-05-06 15:00:00", tz=pytz.UTC),
        price=Decimal("960.0"),
        order_id="123",
        commission=Decimal("26.83"),
    )
    ph.transact_position(transaction)

    # Check that the position object is set correctly
    pos = ph.positions[symbol]

    assert pos.buy_size == 100
    assert pos.sell_size == 0
    assert pos.net_size == 100
    assert pos.direction == 1
    assert pos.avg_price == Decimal("960.2683")


def test_transact_position_current_position():
    """
    Tests the 'transact_position' method for a transaction
    with a current symbol and checks that all objects are
    set correctly.
    """
    # Create the PositionHandler, Transaction and
    # carry out a transaction
    ph = PositionHandler()
    symbol = "AMZN"
    timestamp = pd.Timestamp("2015-05-06 15:00:00", tz=pytz.UTC)
    new_timestamp = pd.Timestamp("2015-05-06 16:00:00", tz=pytz.UTC)

    transaction_long = Transaction(
        symbol,
        size=100,
        timestamp=timestamp,
        price=Decimal("960.0"),
        order_id="123",
        commission=Decimal("26.83"),
    )
    ph.transact_position(transaction_long)

    transaction_long_again = Transaction(
        symbol,
        size=200,
        timestamp=new_timestamp,
        price=Decimal("990.0"),
        order_id="234",
        commission=Decimal("18.53"),
    )
    ph.transact_position(transaction_long_again)

    # Check that the position object is set correctly
    pos = ph.positions[symbol]

    assert pos.buy_size == 300
    assert pos.sell_size == 0
    assert pos.net_size == 300
    assert pos.direction == 1
    assert pos.avg_price == Decimal("980.1512")


def test_transact_position_size_zero():
    """
    Tests the 'transact_position' method for a transaction
    with net zero size after the transaction to ensure
    deletion of the position.
    """
    # Create the PositionHandler, Transaction and
    # carry out a transaction
    ph = PositionHandler()
    symbol = "AMZN"
    timestamp = pd.Timestamp("2015-05-06 15:00:00", tz=pytz.UTC)
    new_timestamp = pd.Timestamp("2015-05-06 16:00:00", tz=pytz.UTC)

    transaction_long = Transaction(
        symbol,
        size=100,
        timestamp=timestamp,
        price=Decimal("960.0"),
        order_id="123",
        commission=Decimal("26.83"),
    )
    ph.transact_position(transaction_long)

    transaction_close = Transaction(
        symbol,
        size=-100,
        timestamp=new_timestamp,
        price=Decimal("980.0"),
        order_id="234",
        commission=Decimal("18.53"),
    )
    ph.transact_position(transaction_close)

    # Go long and then close, then check that the
    # positions dict is empty
    assert ph.positions == {}


def test_total_values_for_no_transactions():
    """
    Tests 'total_market_value', 'total_unrealised_pnl',
    'total_realised_pnl' and 'total_pnl' for the case
    of no transactions being carried out.
    """
    ph = PositionHandler()
    assert ph.total_market_value() == Decimal("0.0")
    assert ph.total_unrealised_pnl() == Decimal("0.0")
    assert ph.total_realised_pnl() == Decimal("0.0")
    assert ph.total_pnl() == Decimal("0.0")


def test_total_values_for_two_separate_transactions():
    """
    Tests 'total_market_value', 'total_unrealised_pnl',
    'total_realised_pnl' and 'total_pnl' for single
    transactions in two separate symbols.
    """
    ph = PositionHandler()

    # Symbol 1
    symbol1 = "AMZN"
    timestamp1 = pd.Timestamp("2015-05-06 15:00:00", tz=pytz.UTC)
    trans_pos_1 = Transaction(
        symbol1,
        size=75,
        timestamp=timestamp1,
        price=Decimal("483.45"),
        order_id="1",
        commission=Decimal("15.97"),
    )
    ph.transact_position(trans_pos_1)

    # Symbol 2
    symbol2 = "MSFT"
    timestamp2 = pd.Timestamp("2015-05-07 15:00:00", tz=pytz.UTC)
    trans_pos_2 = Transaction(
        symbol2,
        size=250,
        timestamp=timestamp2,
        price=Decimal("142.58"),
        order_id="2",
        commission=Decimal("8.35"),
    )
    ph.transact_position(trans_pos_2)

    # Check all total values
    assert ph.total_market_value() == Decimal("71903.75")
    assert ph.total_unrealised_pnl() == Decimal("-24.3199999999999999999999975")
    assert ph.total_realised_pnl() == Decimal("0.0")
    assert ph.total_pnl() == Decimal("-24.3199999999999999999999975")
