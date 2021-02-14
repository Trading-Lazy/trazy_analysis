from decimal import Decimal

from common.helper import get_or_create_nested_dict
from models.enums import Direction
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

    def position_size(self, symbol: str, direction: Direction) -> int:
        return self.positions[symbol][direction].net_size

    def transact_position(self, transaction: Transaction) -> None:
        """
        Execute the transaction and update the appropriate
        position for the transaction's symbol accordingly.
        """
        symbol = transaction.symbol
        get_or_create_nested_dict(self.positions, symbol)
        if transaction.direction in self.positions[symbol]:
            self.positions[symbol][transaction.direction].transact(transaction)
        else:
            position = Position.open_from_transaction(transaction)
            self.positions[symbol][transaction.direction] = position

        # If the position has zero size remove it
        if self.positions[symbol][transaction.direction].net_size == 0:
            del self.positions[symbol][transaction.direction]
            if len(self.positions[symbol]) == 0:
                del self.positions[symbol]

    def total_market_value(self) -> Decimal:
        """
        Calculate the sum of all the positions' market values.
        """
        market_value = Decimal("0.0")
        for values in self.positions.values():
            for pos in values.values():
                market_value += pos.market_value
        return market_value

    def total_unrealised_pnl(self) -> Decimal:
        """
        Calculate the sum of all the positions' unrealised P&Ls.
        """
        unrealised_pnl = Decimal("0.0")
        for values in self.positions.values():
            for pos in values.values():
                unrealised_pnl += pos.unrealised_pnl
        return unrealised_pnl

    def total_realised_pnl(self) -> Decimal:
        """
        Calculate the sum of all the positions' realised P&Ls.
        """
        realised_pnl = Decimal("0.0")
        for values in self.positions.values():
            for pos in values.values():
                realised_pnl += pos.realised_pnl
        return realised_pnl

    def total_pnl(self) -> Decimal:
        """
        Calculate the sum of all the positions' P&Ls.
        """
        total_pnl = Decimal("0.0")
        for symbol_values in self.positions.values():
            for pos in symbol_values.values():
                total_pnl += pos.total_pnl
        return total_pnl

    def __eq__(self, other):
        if not isinstance(other, PositionHandler):
            return False
        return self.positions == other.positions
