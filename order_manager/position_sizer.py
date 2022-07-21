from trazy_analysis.broker.broker_manager import BrokerManager
from trazy_analysis.models.multiple_order import (
    ArbitragePairOrder,
    BracketOrder,
    CoverOrder,
)
from trazy_analysis.models.order import Order


class PositionSizer:
    MAXIMUM_RISK_PER_TRADE = 0.10

    def __init__(self, broker_manager: BrokerManager, integer_size: bool = True):
        self.broker_manager = broker_manager
        self.integer_size = integer_size

    def size_single_order(self, order: Order):
        if order.is_exit_order:
            size = self.broker_manager.get_broker(
                exchange=order.asset.exchange
            ).position_size(order.asset, order.direction)
        else:
            total_equity = self.broker_manager.get_broker(
                exchange=order.asset.exchange
            ).portfolio.total_equity
            max_equity_risk = total_equity * PositionSizer.MAXIMUM_RISK_PER_TRADE
            size_relative_to_equity = self.broker_manager.get_broker(
                exchange=order.asset.exchange
            ).max_entry_order_size(order.asset, max_equity_risk)
            size_relative_to_cash = self.broker_manager.get_broker(
                exchange=order.asset.exchange
            ).max_entry_order_size(order.asset)
            size = min(size_relative_to_equity, size_relative_to_cash)
            if self.integer_size:
                size = int(size)
        order.size = size

    def size_arbitrage_pair_order(self, arbitrage_pair_order: ArbitragePairOrder):
        buy_order = arbitrage_pair_order.buy_order
        sell_order = arbitrage_pair_order.sell_order
        self.size_single_order(buy_order)
        self.size_single_order(sell_order)
        min_size = min(buy_order.size, sell_order.size)
        buy_order.size = min_size
        sell_order.size = min_size

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
        elif isinstance(order, ArbitragePairOrder):
            self.size_arbitrage_pair_order(order)
