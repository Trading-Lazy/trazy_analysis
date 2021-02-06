from decimal import Decimal

import pandas as pd

from models.enums import Action, Direction
from position.transaction import Transaction

TRANSACTION = Transaction(
    "AAPL",
    size=168,
    action=Action.BUY,
    direction=Direction.LONG,
    price=Decimal("56.18"),
    order_id="153",
    commission=Decimal("5.3"),
    timestamp=pd.Timestamp("2015-05-06"),
)


def test_transaction_representation():
    """
    Tests that the Transaction representation
    correctly recreates the object.
    """
    exp_repr = (
        "Transaction(symbol=AAPL, "
        "size=168, action=BUY, direction=LONG, timestamp=2015-05-06 00:00:00, price=56.18, order_id=153)"
    )
    assert repr(TRANSACTION) == exp_repr


def test_cost_without_commission():
    assert TRANSACTION.cost_without_commission == Decimal("9438.24")


def test_cost_with_commission():
    assert TRANSACTION.cost_with_commission == Decimal("9443.54")
