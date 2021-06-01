import os
from collections import deque

import settings
from broker.broker_manager import BrokerManager
from logger import logger
from models.event import PendingSignalEvent
from models.signal import Signal
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
    ):
        self.events = events
        self.broker_manager = broker_manager
        self.position_sizer: PositionSizer = position_sizer
        self.order_creator = order_creator
        self.pending_signals = deque()
        self.clock = self.broker_manager.clock
        self.filter_at_end_of_day = filter_at_end_of_day

    def process_pending_signal(self, signal: Signal):
        LOG.info("Processing %s", str(signal))
        order = self.order_creator.create_order(signal, self.clock)
        if order is not None:
            LOG.info("Order has been created and can be dispatched to the broker")
            self.position_sizer.size_order(order)
            self.broker_manager.get_broker(signal.asset.exchange).submit_order(order)

    def process_pending_signals(self):
        # Create an order from the signal
        LOG.info("Start processing pending signals")
        pending_signals = []
        while len(self.pending_signals) != 0:
            signal = self.pending_signals.popleft()
            self.process_pending_signal(signal)

        for pending_signal in pending_signals:
            self.pending_signals.append(pending_signal)
            self.events.append(
                PendingSignalEvent(asset=pending_signal.asset, bars_delay=1)
            )

    def check_signal(self, signal):
        # check if signal is still valid
        LOG.info("Received new signal %s", str(signal))
        now = self.clock.current_time(asset=signal.asset)
        opened_position = self.broker_manager.get_broker(
            signal.asset.exchange
        ).has_opened_position(signal.asset, signal.direction)
        if (
            signal.in_force(now)
            and (
                (signal.is_entry_signal and not opened_position)
                or (signal.is_exit_signal and opened_position)
            )
            and (
                not self.filter_at_end_of_day or not self.clock.end_of_day(signal.asset)
            )
        ):
            LOG.info("Signal is still in force and will be processed soon.")
            self.pending_signals.append(signal)
            self.events.append(PendingSignalEvent(asset=signal.asset))
