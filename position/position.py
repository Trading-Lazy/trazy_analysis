import os
from datetime import datetime, timezone
from typing import Any

from math import floor

import settings
from logger import logger
from models.enums import Action, Direction
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
    price : `float`
        The initial price of the Position.
    buy_size : `int`
        The amount of the symbol bought.
    sell_size : `int`
        The amount of the symbol sold.
    direction: `Direction`
        The position type LONG or SHORT
    avg_bought : `float`
        The initial price paid for buying assets.
    avg_sell : `float`
        The initial price paid for selling assets.
    buy_commission : `float`
        The commission spent on buying assets for this position.
    sell_commission : `float`
        The commission spent on selling assets for this position.
    """

    def __init__(
        self,
        symbol: str,
        price: float,
        buy_size: int,
        sell_size: int,
        direction: Direction,
        avg_bought: float = 0.0,
        avg_sold: float = 0.0,
        buy_commission: float = 0.0,
        sell_commission: float = 0.0,
        timestamp: datetime = datetime.now(timezone.utc),
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
        self.last_price_update = timestamp

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
        direction = transaction.direction
        timestamp = transaction.timestamp

        if transaction.action == Action.BUY:
            buy_size = transaction.size
            sell_size = 0
            avg_bought = current_price
            avg_sold = 0.0
            buy_commission = transaction.commission
            sell_commission = 0.0
        elif transaction.action == Action.SELL:
            buy_size = 0
            sell_size = transaction.size
            avg_bought = 0.0
            avg_sold = current_price
            buy_commission = 0.0
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
            timestamp,
        )

    @property
    def market_value(self) -> float:
        """
        Return the market value (respecting the direction) of the
        Position based on the current price available to the Position.
        Returns
        -------
        `float`
            The current market value of the Position.
        """
        return self.price * self.net_size

    @property
    def avg_price(self) -> float:
        """
        The average price paid for all assets on the long or short side.
        Returns
        -------
        `float`
            The average price on either the long or short side.
        """
        if self.net_size == 0:
            return 0.0
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
    def total_bought(self) -> float:
        """
        Calculates the total average cost of assets bought.
        Returns
        -------
        `float`
            The total average cost of assets bought.
        """
        return self.avg_bought * self.buy_size

    @property
    def total_sold(self) -> float:
        """
        Calculates the total average cost of assets sold.
        Returns
        -------
        `float`
            The total average cost of assets solds.
        """
        return self.avg_sold * self.sell_size

    @property
    def net_total(self) -> float:
        """
        Calculates the net total average cost of assets
        bought and sold.
        Returns
        -------
        `float`
            The net total average cost of assets bought
            and sold.
        """
        return self.total_sold - self.total_bought

    @property
    def commission(self) -> float:
        """
        Calculates the total commission from assets bought and sold.
        Returns
        -------
        `float`
            The total commission from assets bought and sold.
        """
        return self.buy_commission + self.sell_commission

    @property
    def net_incl_commission(self) -> float:
        """
        Calculates the net total average cost of assets bought
        and sold including the commission.
        Returns
        -------
        `float`
            The net total average cost of assets bought and
            sold including the commission.
        """
        return self.net_total - self.commission

    @property
    def realised_pnl(self) -> float:
        """
        Calculates the profit & loss (P&L) that has been 'realised' via
        two opposing symbol transactions in the Position to date.
        Returns
        -------
        `float`
            The calculated realised P&L.
        """
        if self.direction == Direction.LONG:
            if self.sell_size == 0:
                return 0.0
            else:
                return (
                    ((self.avg_sold - self.avg_bought) * self.sell_size)
                    - ((float(self.sell_size) / self.buy_size) * self.buy_commission)
                    - self.sell_commission
                )
        else:  # self.direction == Direction.SHORT
            if self.buy_size == 0:
                return 0.0
            else:
                return (
                    ((self.avg_sold - self.avg_bought) * self.buy_size)
                    - ((float(self.buy_size) / self.sell_size) * self.sell_commission)
                    - self.buy_commission
                )

    @property
    def unrealised_pnl(self) -> float:
        """
        Calculates the profit & loss (P&L) that has yet to be 'realised'
        in the remaining non-zero size of assets, due to the current
        market price.
        Returns
        -------
        `float`
            The calculated unrealised P&L.
        """
        return (self.price - self.avg_price) * self.net_size

    @property
    def total_pnl(self) -> float:
        """
        Calculates the sum of the unrealised and realised profit & loss (P&L).
        Returns
        -------
        `float`
            The sum of the unrealised and realised P&L.
        """
        return self.realised_pnl + self.unrealised_pnl

    def update_price(self, price, timestamp=datetime.now(timezone.utc)) -> float:
        """
        Updates the Position's awareness of the current market price
        of the symbol.
        Parameters
        ----------
        price : `float`
            The current market price.
        """
        if price <= 0.0:
            raise ValueError(
                'Market price "%s" of symbol "%s" must be positive to '
                "update the position." % (price, self.symbol)
            )
        else:
            self.price = price
            self.last_price_update = timestamp

    def _transact_buy(self, size: int, price: float, commission: float) -> None:
        """
        Handle the accounting for creating a new long leg for the
        Position.
        Parameters
        ----------
        size : `int`
            The additional size of assets to purchase.
        price : `float`
            The price at which this leg was purchased.
        commission : `float`
            The commission paid to the broker for the purchase.
        """
        if self.direction == Direction.SHORT and self.net_size + size > 0:
            LOG.error(
                "ERROR: Position limit reached. Position size %s should be greater or equal to transaction size %s",
                -self.net_size,
                size,
            )
            size = -self.net_size
        self.avg_bought = ((self.avg_bought * self.buy_size) + (size * price)) / (
            self.buy_size + size
        )
        self.buy_size += size
        self.buy_commission += commission

    def _transact_sell(self, size: int, price: float, commission: float) -> None:
        """
        Handle the accounting for creating a new short leg for the
        Position.
        Parameters
        ----------
        size : `int`
            The additional size of assets to sell.
        price : `float`
            The price at which this leg was sold.
        commission : `float`
            The commission paid to the broker for the sale.
        """
        if self.direction == Direction.LONG and self.net_size - size < 0:
            LOG.error(
                "ERROR: Position limit reached. Position size %s should be greater or equal to transaction size %s",
                self.net_size,
                size,
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

    def __eq__(self, other: Any):
        if not isinstance(other, Position):
            return False
        self_dict = self.__dict__.copy()
        del self_dict["last_price_update"]
        other_dict = other.__dict__.copy()
        del other_dict["last_price_update"]
        t = self_dict == other_dict
        return self_dict == other_dict
