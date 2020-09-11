from decimal import Decimal

import numpy as np
import pandas as pd


class Transaction:
    """
    Handles the transaction of a symbol, as used in the
    Position class.
    Parameters
    ----------
    symbol : `str`
        The symbol symbol of the transaction
    size : `int`
        Whole number size of shares in the transaction
    timestamp : `pd.Timestamp`
        The date/time of the transaction
    price : `Decimal`
        The transaction price carried out
    order_id : `str`
        The unique order identifier
    commission : `Decimal`, optional
        The trading commission
    """

    def __init__(
        self,
        symbol: str,
        size: int,
        price: Decimal,
        order_id: str,
        commission=Decimal("0.0"),
        timestamp: pd.Timestamp = pd.Timestamp.now(tz="UTC"),
    ):
        self.symbol = symbol
        self.size = size
        self.direction = np.copysign(1, self.size)
        self.timestamp = timestamp
        self.price = price
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
        return "%s(symbol=%s, size=%s, timestamp=%s, " "price=%s, order_id=%s)" % (
            type(self).__name__,
            self.symbol,
            self.size,
            self.timestamp,
            self.price,
            self.order_id,
        )

    @property
    def cost_without_commission(self):
        """
        Calculate the cost of the transaction without including
        any commission costs.
        Returns
        -------
        `Decimal`
            The transaction cost without commission.
        """
        return self.size * self.price

    @property
    def cost_with_commission(self):
        """
        Calculate the cost of the transaction including
        any commission costs.
        Returns
        -------
        `Decimal`
            The transaction cost with commission.
        """
        if self.commission == Decimal("0.0"):
            return self.cost_without_commission
        else:
            return self.cost_without_commission + self.commission
