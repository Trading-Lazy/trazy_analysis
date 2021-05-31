from broker.broker import Broker
from models.multiple_order import BracketOrder, CoverOrder
from models.order import Order


class PositionSizer:
    MAXIMUM_RISK_PER_TRADE = 0.10

    def __init__(self, broker: Broker):
        self.broker = broker

    def size_single_order(self, order: Order):
        if order.is_exit_order:
            size = self.broker.position_size(order.asset, order.direction)
        else:
            total_equity = self.broker.portfolio.total_equity
            max_equity_risk = total_equity * PositionSizer.MAXIMUM_RISK_PER_TRADE
            size_relative_to_equity = self.broker.max_entry_order_size(
                order.asset, order.direction, max_equity_risk
            )
            size_relative_to_cash = self.broker.max_entry_order_size(
                order.asset, order.direction
            )
            size = int(min(size_relative_to_equity, size_relative_to_cash))
        order.size = size

    def size_order(self, order: Order):
        if isinstance(order, Order):
            self.size_single_order(order)
        elif isinstance(order, CoverOrder):
            initiation_order = order.initiation_order
            stop_order = order.stop_order
            self.size_order(initiation_order)
            stop_order.size = initiation_order.size
        elif isinstance(order, BracketOrder):
            initiation_order = order.initiation_order
            target_order = order.target_order
            stop_order = order.stop_order
            self.size_order(initiation_order)
            target_order.size = initiation_order.size
            stop_order.size = initiation_order.size
