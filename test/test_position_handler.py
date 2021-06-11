from datetime import datetime

import pytest

from models.enums import Action, Direction
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
        action=Action.BUY,
        direction=Direction.LONG,
        price=960.0,
        order_id="123",
        commission=26.83,
        timestamp=datetime.strptime("2015-05-06 15:00:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    )
    ph.transact_position(transaction)

    # Check that the position object is set correctly
    pos = ph.positions[symbol][Direction.LONG]

    assert pos.buy_size == 100
    assert pos.sell_size == 0
    assert pos.net_size == 100
    assert pos.direction == Direction.LONG
    assert pos.avg_price == pytest.approx(960.26, abs=0.01)


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
    timestamp = datetime.strptime("2015-05-06 15:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    new_timestamp = datetime.strptime("2015-05-06 16:00:00+0000", "%Y-%m-%d %H:%M:%S%z")

    transaction_long = Transaction(
        symbol,
        size=100,
        action=Action.BUY,
        direction=Direction.LONG,
        price=960.0,
        order_id="123",
        commission=26.83,
        timestamp=timestamp,
    )
    ph.transact_position(transaction_long)

    transaction_long_again = Transaction(
        symbol,
        size=200,
        action=Action.BUY,
        direction=Direction.LONG,
        price=990.0,
        order_id="234",
        commission=18.53,
        timestamp=new_timestamp,
    )
    ph.transact_position(transaction_long_again)

    # Check that the position object is set correctly
    pos = ph.positions[symbol][Direction.LONG]

    assert pos.buy_size == 300
    assert pos.sell_size == 0
    assert pos.net_size == 300
    assert pos.direction == Direction.LONG
    assert pos.avg_price == pytest.approx(980.15, abs=0.01)


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
    timestamp = datetime.strptime("2015-05-06 15:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    new_timestamp = datetime.strptime("2015-05-06 16:00:00+0000", "%Y-%m-%d %H:%M:%S%z")

    transaction_long = Transaction(
        symbol,
        size=100,
        action=Action.BUY,
        direction=Direction.LONG,
        price=960.0,
        order_id="123",
        commission=26.83,
        timestamp=timestamp,
    )
    ph.transact_position(transaction_long)

    transaction_close = Transaction(
        symbol,
        size=100,
        action=Action.SELL,
        direction=Direction.LONG,
        price=980.0,
        order_id="234",
        commission=18.53,
        timestamp=new_timestamp,
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
    assert ph.total_market_value() == 0.0
    assert ph.total_unrealised_pnl() == 0.0
    assert ph.total_realised_pnl() == 0.0
    assert ph.total_pnl() == 0.0


def test_total_values_for_two_separate_transactions():
    """
    Tests 'total_market_value', 'total_unrealised_pnl',
    'total_realised_pnl' and 'total_pnl' for single
    transactions in two separate symbols.
    """
    ph = PositionHandler()

    # Symbol 1
    symbol1 = "AMZN"
    size1 = 75
    timestamp1 = datetime.strptime("2015-05-06 15:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    trans_pos_1 = Transaction(
        symbol1,
        size=size1,
        action=Action.BUY,
        direction=Direction.LONG,
        price=483.45,
        order_id="1",
        commission=15.97,
        timestamp=timestamp1,
    )
    ph.transact_position(trans_pos_1)
    assert ph.position_size(symbol1, Direction.LONG) == 75

    # Symbol 2
    symbol2 = "MSFT"
    size2 = 250
    timestamp2 = datetime.strptime("2015-05-07 15:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    trans_pos_2 = Transaction(
        symbol2,
        size=size2,
        action=Action.BUY,
        direction=Direction.LONG,
        price=142.58,
        order_id="2",
        commission=8.35,
        timestamp=timestamp2,
    )
    ph.transact_position(trans_pos_2)
    assert ph.position_size(symbol2, Direction.LONG) == size2

    # Check all total values
    assert ph.total_market_value() == 71903.75
    assert ph.total_unrealised_pnl() == pytest.approx(-24.32, 0.01)
    assert ph.total_realised_pnl() == 0.0
    assert ph.total_pnl() == pytest.approx(-24.32, 0.01)


def test_neq_different_type():
    ph1 = PositionHandler()
    symbol1 = "AMZN"
    size1 = 75
    timestamp1 = datetime.strptime("2015-05-06 15:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    trans_pos_1 = Transaction(
        symbol1,
        size=size1,
        action=Action.BUY,
        direction=Direction.LONG,
        price=483.45,
        order_id="1",
        commission=15.97,
        timestamp=timestamp1,
    )
    ph1.transact_position(trans_pos_1)

    ph2 = PositionHandler()
    ph2.transact_position(trans_pos_1)
    ph3 = PositionHandler()
    symbol2 = "MSFT"
    size2 = 250
    timestamp2 = datetime.strptime("2015-05-07 15:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    trans_pos_2 = Transaction(
        symbol2,
        size=size2,
        action=Action.BUY,
        direction=Direction.LONG,
        price=142.58,
        order_id="2",
        commission=8.35,
        timestamp=timestamp2,
    )
    ph3.transact_position(trans_pos_2)
    assert ph1 == ph2
    assert ph1 != ph3
    assert ph1 != object()
