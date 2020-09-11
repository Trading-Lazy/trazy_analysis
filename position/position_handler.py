from decimal import Decimal

from position.position import Position
from position.transaction import Transaction


class PositionHandler:
    """
    A class that keeps track of, and updates, the current
    list of Position instances stored in a Portfolio entity.
    """

    def __init__(self) -> None:
        """
        Initialise the PositionHandler object to generate
        an ordered dictionary containing the current positions.
        """
        self.positions = {}

    def transact_position(self, transaction: Transaction) -> None:
        """
        Execute the transaction and update the appropriate
        position for the transaction's symbol accordingly.
        """
        symbol = transaction.symbol
        if symbol in self.positions:
            self.positions[symbol].transact(transaction)
        else:
            position = Position.open_from_transaction(transaction)
            self.positions[symbol] = position

        # If the position has zero size remove it
        if self.positions[symbol].net_size == 0:
            del self.positions[symbol]

    def total_market_value(self) -> Decimal:
        """
        Calculate the sum of all the positions' market values.
        """
        return sum(pos.market_value for symbol, pos in self.positions.items())

    def total_unrealised_pnl(self) -> Decimal:
        """
        Calculate the sum of all the positions' unrealised P&Ls.
        """
        return sum(pos.unrealised_pnl for symbol, pos in self.positions.items())

    def total_realised_pnl(self) -> Decimal:
        """
        Calculate the sum of all the positions' realised P&Ls.
        """
        return sum(pos.realised_pnl for symbol, pos in self.positions.items())

    def total_pnl(self) -> Decimal:
        """
        Calculate the sum of all the positions' P&Ls.
        """
        return sum(pos.total_pnl for symbol, pos in self.positions.items())
