import os
from decimal import Decimal
from math import floor
from typing import Any

import numpy as np
import pandas as pd

import settings
from logger import logger
from models.enums import Direction, Action
from position.transaction import Transaction


LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)


class Position:
    """
    Handles the accounting of entering a new position in an
    symbol along with subsequent modifications via additional
    trades.
    The approach taken here separates the long and short side
    for accounting purposes. It also includes an unrealised and
    realised running profit & loss of the position.
    Parameters
    ----------
    symbol : `str`
        The symbol symbol string.
    price : `Decimal`
        The initial price of the Position.
    timestamp : `pd.Timestamp`
        The time at which the Position was created.
    size : `int`
        The amount of the symbol bought.
    size : `int`
        The amount of the symbol sold.
    avg : `Decimal`
        The initial price paid for buying assets.
    avg : `Decimal`
        The initial price paid for selling assets.
    commission : `Decimal`
        The commission spent on buying assets for this position.
    commission : `Decimal`
        The commission spent on selling assets for this position.
    """

    def __init__(
        self,
        symbol: str,
        price: Decimal,
        buy_size: int,
        sell_size: int,
        direction: Direction,
        avg_bought: Decimal,
        avg_sold: Decimal,
        buy_commission: Decimal,
        sell_commission: Decimal,
        timestamp: pd.Timestamp = pd.Timestamp.now(tz="UTC"),
    ) -> None:
        self.symbol = symbol
        self.price = price
        self.buy_size = buy_size
        self.sell_size = sell_size
        self.direction = direction
        self.avg_bought = avg_bought
        self.avg_sold = avg_sold
        self.buy_commission = buy_commission
        self.sell_commission = sell_commission
        self.timestamp = timestamp

    @classmethod
    def open_from_transaction(cls, transaction: Transaction) -> "Position":
        """
        Constructs a new Position instance from the provided
        Transaction.
        Parameters
        ----------
        transaction : `Transaction`
            The transaction with which to open the Position.
        Returns
        -------
        `Position`
            The instantiated position.
        """
        symbol = transaction.symbol
        current_price = transaction.price
        current_dt = transaction.timestamp
        direction = transaction.direction

        if transaction.action == Action.BUY:
            buy_size = transaction.size
            sell_size = 0
            avg_bought = current_price
            avg_sold = Decimal("0.0")
            buy_commission = transaction.commission
            sell_commission = Decimal("0.0")
        elif transaction.action == Action.SELL:
            buy_size = 0
            sell_size = transaction.size
            avg_bought = Decimal("0.0")
            avg_sold = current_price
            buy_commission = Decimal("0.0")
            sell_commission = transaction.commission

        return cls(
            symbol,
            current_price,
            buy_size,
            sell_size,
            direction,
            avg_bought,
            avg_sold,
            buy_commission,
            sell_commission,
            current_dt,
        )

    def _check_set_dt(self, timestamp: pd.Timestamp) -> None:
        """
        Checks that the provided timestamp is valid and if so sets
        the new current time of the Position.
        Parameters
        ----------
        timestamp : `pd.Timestamp`
            The timestamp to be checked and potentially used as
            the new current time.
        """
        if timestamp is not None:
            if timestamp < self.timestamp:
                raise ValueError(
                    'Supplied update time of "%s" is earlier than '
                    'the current time of "%s".' % (timestamp, self.timestamp)
                )
            else:
                self.timestamp = timestamp

    @property
    def market_value(self) -> Decimal:
        """
        Return the market value (respecting the direction) of the
        Position based on the current price available to the Position.
        Returns
        -------
        `Decimal`
            The current market value of the Position.
        """
        return self.price * self.net_size

    @property
    def avg_price(self) -> Decimal:
        """
        The average price paid for all assets on the long or short side.
        Returns
        -------
        `Decimal`
            The average price on either the long or short side.
        """
        if self.net_size == 0:
            return Decimal("0.0")
        elif self.direction == Direction.LONG:
            return (
                self.avg_bought * self.buy_size + self.buy_commission
            ) / self.buy_size
        else:
            return (
                self.avg_sold * self.sell_size - self.sell_commission
            ) / self.sell_size

    @property
    def net_size(self) -> int:
        """
        The difference in the size of assets bought and sold to date.
        Returns
        -------
        `int`
            The net size of assets.
        """
        return self.buy_size - self.sell_size

    @property
    def total_bought(self) -> Decimal:
        """
        Calculates the total average cost of assets bought.
        Returns
        -------
        `Decimal`
            The total average cost of assets bought.
        """
        return self.avg_bought * self.buy_size

    @property
    def total_sold(self) -> Decimal:
        """
        Calculates the total average cost of assets sold.
        Returns
        -------
        `Decimal`
            The total average cost of assets solds.
        """
        return self.avg_sold * self.sell_size

    @property
    def net_total(self) -> Decimal:
        """
        Calculates the net total average cost of assets
        bought and sold.
        Returns
        -------
        `Decimal`
            The net total average cost of assets bought
            and sold.
        """
        return self.total_sold - self.total_bought

    @property
    def commission(self) -> Decimal:
        """
        Calculates the total commission from assets bought and sold.
        Returns
        -------
        `Decimal`
            The total commission from assets bought and sold.
        """
        return self.buy_commission + self.sell_commission

    @property
    def net_incl_commission(self) -> Decimal:
        """
        Calculates the net total average cost of assets bought
        and sold including the commission.
        Returns
        -------
        `Decimal`
            The net total average cost of assets bought and
            sold including the commission.
        """
        return self.net_total - self.commission

    @property
    def realised_pnl(self) -> Decimal:
        """
        Calculates the profit & loss (P&L) that has been 'realised' via
        two opposing symbol transactions in the Position to date.
        Returns
        -------
        `Decimal`
            The calculated realised P&L.
        """
        if self.direction == Direction.LONG:
            if self.sell_size == 0:
                return Decimal("0.0")
            else:
                return (
                    ((self.avg_sold - self.avg_bought) * self.sell_size)
                    - ((Decimal(self.sell_size) / self.buy_size) * self.buy_commission)
                    - self.sell_commission
                )
        elif self.direction == Direction.SHORT:
            if self.buy_size == 0:
                return Decimal("0.0")
            else:
                return (
                    ((self.avg_sold - self.avg_bought) * self.buy_size)
                    - ((Decimal(self.buy_size) / self.sell_size) * self.sell_commission)
                    - self.buy_commission
                )
        else:
            return self.net_incl_commission

    @property
    def unrealised_pnl(self) -> Decimal:
        """
        Calculates the profit & loss (P&L) that has yet to be 'realised'
        in the remaining non-zero size of assets, due to the current
        market price.
        Returns
        -------
        `Decimal`
            The calculated unrealised P&L.
        """
        return (self.price - self.avg_price) * self.net_size

    @property
    def total_pnl(self) -> Decimal:
        """
        Calculates the sum of the unrealised and realised profit & loss (P&L).
        Returns
        -------
        `Decimal`
            The sum of the unrealised and realised P&L.
        """
        return self.realised_pnl + self.unrealised_pnl

    def update_price(self, price, timestamp=None) -> Decimal:
        """
        Updates the Position's awareness of the current market price
        of the symbol, with an optional timestamp.
        Parameters
        ----------
        price : `Decimal`
            The current market price.
        timestamp : `pd.Timestamp`, optional
            The optional timestamp of the current market price.
        """
        self._check_set_dt(timestamp)

        if price <= Decimal("0.0"):
            raise ValueError(
                'Market price "%s" of symbol "%s" must be positive to '
                "update the position." % (price, self.symbol)
            )
        else:
            self.price = price

    def _transact_buy(self, size: int, price: Decimal, commission: Decimal) -> None:
        """
        Handle the accounting for creating a new long leg for the
        Position.
        Parameters
        ----------
        size : `int`
            The additional size of assets to purchase.
        price : `Decimal`
            The price at which this leg was purchased.
        commission : `Decimal`
            The commission paid to the broker for the purchase.
        """
        if self.direction == Direction.SHORT and self.net_size + size > 0:
            LOG.error(
                "ERROR: Position limit reached. Position size %s should be greater or equal to transaction size %s"
                % (-self.net_size, size)
            )
            size = self.net_size
        self.avg_bought = ((self.avg_bought * self.buy_size) + (size * price)) / (
            self.buy_size + size
        )
        self.buy_size += size
        self.buy_commission += commission

    def _transact_sell(self, size: int, price: Decimal, commission: Decimal) -> None:
        """
        Handle the accounting for creating a new short leg for the
        Position.
        Parameters
        ----------
        size : `int`
            The additional size of assets to sell.
        price : `Decimal`
            The price at which this leg was sold.
        commission : `Decimal`
            The commission paid to the broker for the sale.
        """
        if self.direction == Direction.LONG and self.net_size - size < 0:
            LOG.error(
                "ERROR: Position limit reached. Position size %s should be greater or equal to transaction size %s"
                % (self.net_size, size)
            )
            size = self.net_size
        self.avg_sold = ((self.avg_sold * self.sell_size) + (size * price)) / (
            self.sell_size + size
        )
        self.sell_size += size
        self.sell_commission += commission

    def transact(self, transaction: Transaction) -> None:
        """
        Calculates the adjustments to the Position that occur
        once new units in an symbol are bought and sold.
        Parameters
        ----------
        transaction : `Transaction`
            The Transaction to update the Position with.
        """
        if self.symbol != transaction.symbol:
            raise ValueError(
                "Failed to update Position with symbol %s when "
                "carrying out transaction in symbol %s. "
                % (self.symbol, transaction.symbol)
            )

        if self.direction != transaction.direction:
            raise ValueError(
                "Failed to update Position with direction %s when "
                "carrying out transaction in direction %s. "
                % (self.direction.name, transaction.direction.name)
            )

        # Nothing to do if the transaction has no size
        if int(floor(transaction.size)) == 0:
            return

        # Depending upon the direction of the transaction
        # ensure the correct calculation is called
        if transaction.action == Action.BUY:
            self._transact_buy(
                transaction.size, transaction.price, transaction.commission
            )
        elif transaction.action == Action.SELL:
            self._transact_sell(
                transaction.size,
                transaction.price,
                transaction.commission,
            )

        # Update the current trade information
        self.update_price(transaction.price, transaction.timestamp)
        self.timestamp = transaction.timestamp

    def __eq__(self, other: Any):
        if not isinstance(other, Position):
            return False
        return self.__dict__ == other.__dict__
