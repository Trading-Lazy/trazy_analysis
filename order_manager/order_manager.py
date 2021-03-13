import os
from collections import deque

import settings
from broker.broker import Broker
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
        self, events: deque, broker: Broker, position_sizer: PositionSizer, order_creator: OrderCreator
    ):
        self.events = events
        self.broker: Broker = broker
        self.position_sizer: PositionSizer = position_sizer
        self.order_creator = order_creator
        self.pending_signals = deque()
        self.clock = self.broker.clock

    def process_pending_signal(self, signal: Signal, pending_signals):
        LOG.info("Processing %s", str(signal))
        order = self.order_creator.create_order(signal, self.clock)
        if order is not None:
            LOG.info("Order has been created and can be dispatched to the broker")
            self.position_sizer.size_order(order)
            self.broker.submit_order(order)

    def process_pending_signals(self):
        # Create an order from the signal
        LOG.info("Start processing pending signals")
        pending_signals = []
        while len(self.pending_signals) != 0:
            signal = self.pending_signals.popleft()
            self.process_pending_signal(signal, pending_signals)

        for pending_signal in pending_signals:
            self.pending_signals.append(pending_signal)
            self.events.append(PendingSignalEvent(symbol=pending_signal.symbol, bars_delay=1))

    def check_signal(self, signal):
        # check if signal is still valid
        LOG.info("Received new signal %s", str(signal))
        now = self.clock.current_time(symbol=signal.symbol)
        opened_position = self.broker.has_opened_position(
            signal.symbol, signal.direction
        )
        if (
            signal.in_force(now)
            and (
                (signal.is_entry_signal and not opened_position)
                or (signal.is_exit_signal and opened_position)
            )
            and not self.clock.end_of_day(signal.symbol)
        ):
            LOG.info("Signal is still in force and will be processed soon.")
            self.pending_signals.append(signal)
            self.events.append(PendingSignalEvent(symbol=signal.symbol))
