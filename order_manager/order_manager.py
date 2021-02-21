import os
from queue import Queue

import settings
from broker.broker import Broker
from logger import logger
from order_manager.order_creator import OrderCreator
from order_manager.position_sizer import PositionSizer

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)


class OrderManager:
    def __init__(
        self, broker: Broker, position_sizer: PositionSizer, order_creator: OrderCreator
    ):
        self.broker: Broker = broker
        self.position_sizer: PositionSizer = position_sizer
        self.order_creator = order_creator
        self.pending_signals = Queue()
        self.clock = self.broker.clock

    def process_signals(self, bars_delay=0):
        # Create an order from the signal
        LOG.info("Start processing pending signals")
        pending_signals = []
        while not self.pending_signals.empty():
            signal_bars_pair = self.pending_signals.get()
            signal = signal_bars_pair[0]
            LOG.info("Processing %s", str(signal))
            bars = signal_bars_pair[1]
            if bars + bars_delay > self.clock.bars(signal.symbol):
                pending_signals.append(signal_bars_pair)
                continue
            LOG.info("Signal can be processed")
            order = self.order_creator.create_order(signal, self.clock)
            if order is not None:
                LOG.info("Order has been created and can be dispatched to the broker")
                self.position_sizer.size_order(order)
                self.broker.submit_order(order)

        for pending_signal in pending_signals:
            self.pending_signals.put(pending_signal)

    def check_signal(self, signal, bars_delay=0):
        # check if signal is still valid
        LOG.info("Received new signal %s", str(signal))
        now = self.clock.current_time(symbol=signal.symbol)
        opened_position = self.broker.has_opened_position(
            signal.symbol, signal.direction
        )
        if signal.in_force(now) and (
            (signal.is_entry_signal and not opened_position)
            or (signal.is_exit_signal and opened_position)
        ):
            LOG.info("Signal is still in force and will be processed soon.")
            self.pending_signals.put((signal, self.clock.bars(signal.symbol)))
        self.process_signals(bars_delay)
