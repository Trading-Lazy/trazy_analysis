from trazy_analysis.common.helper import get_or_create_nested_dict
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.enums import Direction
from trazy_analysis.position.position import Position
from trazy_analysis.position.transaction import Transaction


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

    def position_size(self, asset: Asset, direction: Direction) -> int:
        return self.positions[asset][direction].net_size

    def transact_position(self, transaction: Transaction) -> None:
        """
        Execute the transaction and update the appropriate
        position for the transaction's asset accordingly.
        """
        asset = transaction.asset
        get_or_create_nested_dict(self.positions, asset)
        if transaction.direction in self.positions[asset]:
            self.positions[asset][transaction.direction].transact(transaction)
        else:
            position = Position.open_from_transaction(transaction)
            self.positions[asset][transaction.direction] = position

        # If the position has zero size remove it
        if self.positions[asset][transaction.direction].net_size == 0:
            del self.positions[asset][transaction.direction]
            if len(self.positions[asset]) == 0:
                del self.positions[asset]

    def total_market_value(self) -> float:
        """
        Calculate the sum of all the positions' market values.
        """
        market_value = 0.0
        for values in self.positions.values():
            for pos in values.values():
                market_value += pos.market_value
        return market_value

    def total_unrealised_pnl(self) -> float:
        """
        Calculate the sum of all the positions' unrealised P&Ls.
        """
        unrealised_pnl = 0.0
        for values in self.positions.values():
            for pos in values.values():
                unrealised_pnl += pos.unrealised_pnl
        return unrealised_pnl

    def total_realised_pnl(self) -> float:
        """
        Calculate the sum of all the positions' realised P&Ls.
        """
        realised_pnl = 0.0
        for values in self.positions.values():
            for pos in values.values():
                realised_pnl += pos.realised_pnl
        return realised_pnl

    def total_pnl(self) -> float:
        """
        Calculate the sum of all the positions' P&Ls.
        """
        total_pnl = 0.0
        for asset_values in self.positions.values():
            for pos in asset_values.values():
                total_pnl += pos.total_pnl
        return total_pnl

    def __eq__(self, other):
        if not isinstance(other, PositionHandler):
            return False
        return self.positions == other.positions
