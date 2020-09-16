import abc
from decimal import Decimal

from models.enums import ActionType, PositionType


class Broker:
    @abc.abstractmethod
    def get_symbol_price(self, symbol: str) -> Decimal:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def submit_order(
        self,
        symbol: str,
        position_type: PositionType,
        action_type: ActionType,
        unit_cost_estimate: Decimal,
        size: int,
    ) -> None:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def get_balance(self) -> Decimal:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def compute_order_size(self, symbol) -> Decimal:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def add_cash(self, cash: Decimal) -> None:  # pragma: no cover
        raise NotImplementedError
