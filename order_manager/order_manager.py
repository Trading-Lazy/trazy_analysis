import os
from collections import deque

import settings
from broker.broker_manager import BrokerManager
from logger import logger
from models.event import PendingSignalEvent
from models.multiple_order import ArbitragePairOrder, BracketOrder, CoverOrder
from models.order import Order
from models.signal import ArbitragePairSignal, Signal
from order_manager.order_creator import OrderCreator
from order_manager.position_sizer import PositionSizer

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)


class OrderManager:
    def __init__(
        self,
        events: deque,
        broker_manager: BrokerManager,
        position_sizer: PositionSizer,
        order_creator: OrderCreator,
        filter_at_end_of_day=True,
    ) -> None:
        self.events = events
        self.broker_manager = broker_manager
        self.position_sizer: PositionSizer = position_sizer
        self.order_creator = order_creator
        self.pending_signals = deque()
        self.clock = self.broker_manager.clock
        self.filter_at_end_of_day = filter_at_end_of_day

    def process_pending_signal(self, signal: Signal) -> None:
        LOG.info("Processing %s", str(signal))
        order = self.order_creator.create_order(signal, self.clock)
        if order is not None:
            LOG.info("Order has been created and can be dispatched to the broker")
            self.position_sizer.size_order(order)
            if (
                isinstance(order, Order)
                or isinstance(order, CoverOrder)
                or isinstance(order, BracketOrder)
            ):
                self.broker_manager.get_broker(signal.asset.exchange).submit_order(
                    order
                )
            elif isinstance(order, ArbitragePairOrder):
                if order.buy_order.size != 0:
                    self.broker_manager.get_broker(
                        order.buy_order.asset.exchange
                    ).submit_order(order.buy_order)
                    self.broker_manager.get_broker(
                        order.sell_order.asset.exchange
                    ).submit_order(order.sell_order)

    def process_pending_signals(self) -> None:
        # Create an order from the signal
        LOG.info("Start processing pending signals")
        pending_signals = []
        while len(self.pending_signals) != 0:
            signal = self.pending_signals.popleft()
            self.process_pending_signal(signal)

        for pending_signal in pending_signals:
            self.pending_signals.append(pending_signal)
            self.events.append(PendingSignalEvent(bars_delay=1))

    def check_signal(self, signal) -> None:
        # check if signal is still valid
        # filters
        if isinstance(signal, Signal):
            LOG.info("Received new signal %s", str(signal.to_serializable_dict()))
            now = self.clock.current_time()
            LOG.info("now = %s", now)
            if not signal.in_force(now):
                return
            opened_position = self.broker_manager.get_broker(
                signal.asset.exchange
            ).has_opened_position(signal.asset, signal.direction)
            LOG.info("opened position = %s", opened_position)
            if not (
                (signal.is_entry_signal and not opened_position)
                or (signal.is_exit_signal and opened_position)
            ):
                return
        elif isinstance(signal, ArbitragePairSignal):
            LOG.info("Received new arbitrage signal")
            LOG.info("Buy signal %s", str(signal.buy_signal.to_serializable_dict()))
            LOG.info("Sell signal %s", str(signal.sell_signal.to_serializable_dict()))
            buy_signal: Signal = signal.buy_signal
            sell_signal: Signal = signal.sell_signal
            if sell_signal.is_exit_signal:
                opened_position = self.broker_manager.get_broker(
                    sell_signal.asset.exchange
                ).has_opened_position(sell_signal.asset, sell_signal.direction)
                if not opened_position:
                    return
            if buy_signal.is_exit_signal:
                opened_position = self.broker_manager.get_broker(
                    buy_signal.asset.exchange
                ).has_opened_position(buy_signal.asset, buy_signal.direction)
                if not opened_position:
                    return

        LOG.info("Signal passed all checks and will be processed soon.")
        self.pending_signals.append(signal)
        self.events.append(PendingSignalEvent())
