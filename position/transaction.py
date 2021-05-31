from datetime import datetime, timezone

from common.utils import generate_object_id
from models.asset import Asset
from models.enums import Action, Direction


class Transaction:
    """
    Handles the transaction of a asset, as used in the
    Position class.
    Parameters
    ----------
    asset : `str`
        The asset asset of the transaction
    size : `int`
        Whole number size of shares in the transaction
    timestamp : `datetime`
        The date/time of the transaction
    price : `float`
        The transaction price carried out
    order_id : `str`
        The unique order identifier
    commission : `float`, optional
        The trading commission
    """

    def __init__(
        self,
        asset: Asset,
        size: int,
        action,
        direction: Direction,
        price: float,
        order_id: str,
        commission=0.0,
        timestamp: datetime = datetime.now(timezone.utc),
        transaction_id: str = None,
    ):
        self.asset = asset
        self.size = size
        self.action = action
        self.direction = direction
        self.timestamp = timestamp
        self.price = price
        if transaction_id is None:
            transaction_id = generate_object_id()
        self.transaction_id = transaction_id
        self.order_id = order_id
        self.commission = commission

    def __repr__(self):
        """
        Provides a representation of the Transaction
        to allow full recreation of the object.
        Returns
        -------
        `str`
            The string representation of the Transaction.
        """
        return (
            "%s(asset=%s, size=%s, action=%s, direction=%s, timestamp=%s, "
            "price=%s, order_id=%s)"
            % (
                type(self).__name__,
                self.asset,
                self.size,
                self.action.name,
                self.direction.name,
                self.timestamp,
                self.price,
                self.order_id,
            )
        )

    @property
    def cost_without_commission(self):
        """
        Calculate the cost of the transaction without including
        any commission costs.
        Returns
        -------
        `float`
            The transaction cost without commission.
        """
        if self.action == Action.SELL:
            return -(self.size * self.price)
        else:
            return self.size * self.price

    @property
    def cost_with_commission(self):
        """
        Calculate the cost of the transaction including
        any commission costs.
        Returns
        -------
        `float`
            The transaction cost with commission.
        """
        if self.commission == 0.0:
            return self.cost_without_commission
        else:
            return self.cost_without_commission + self.commission
