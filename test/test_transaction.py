from decimal import Decimal

import pandas as pd

from position.transaction import Transaction

TRANSACTION_WITHOUT_COMMISSION = Transaction(
    "AAPL",
    size=168,
    timestamp=pd.Timestamp("2015-05-06"),
    price=Decimal("56.18"),
    order_id="153",
)
TRANSACTION_WITH_COMMISSION = Transaction(
    "AAPL",
    size=168,
    timestamp=pd.Timestamp("2015-05-06"),
    price=Decimal("56.18"),
    order_id="153",
    commission=Decimal("5.3"),
)


def test_transaction_representation():
    """
    Tests that the Transaction representation
    correctly recreates the object.
    """
    exp_repr = (
        "Transaction(symbol=AAPL, "
        "size=168, timestamp=2015-05-06 00:00:00, price=56.18, order_id=153)"
    )
    assert repr(TRANSACTION_WITHOUT_COMMISSION) == exp_repr


def test_cost_without_commission():
    assert TRANSACTION_WITHOUT_COMMISSION.cost_with_commission == Decimal("9438.24")


def test_cost_with_commission():
    assert TRANSACTION_WITH_COMMISSION.cost_with_commission == Decimal("9443.54")
