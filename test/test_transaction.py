from datetime import datetime

import pytest

from models.asset import Asset
from models.enums import Action, Direction
from position.transaction import Transaction

AAPL_SYMBOL = "AAPL"
EXCHANGE = "IEX"
AAPL_ASSET = Asset(symbol=AAPL_SYMBOL, exchange=EXCHANGE)
TRANSACTION = Transaction(
    asset=AAPL_ASSET,
    size=168,
    action=Action.BUY,
    direction=Direction.LONG,
    price=56.18,
    order_id="153",
    commission=5.3,
    timestamp=datetime(2015, 5, 6),
)


def test_transaction_representation():
    """
    Tests that the Transaction representation
    correctly recreates the object.
    """
    exp_repr = (
        'Transaction(asset=Asset(symbol="AAPL",exchange="IEX"), '
        "size=168, action=BUY, direction=LONG, timestamp=2015-05-06 00:00:00, price=56.18, order_id=153)"
    )
    assert repr(TRANSACTION) == exp_repr


def test_cost_without_commission():
    assert TRANSACTION.cost_without_commission == 9438.24


def test_cost_with_commission():
    assert TRANSACTION.cost_with_commission == pytest.approx(9443.54, abs=0.001)
