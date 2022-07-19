import os
from collections import deque

import pandas as pd

import trazy_analysis.settings
from trazy_analysis.broker.broker_manager import BrokerManager
from trazy_analysis.common.clock import Clock
from trazy_analysis.common.helper import get_or_create_nested_dict
from trazy_analysis.logger import logger
from trazy_analysis.models.event import PendingSignalEvent
from trazy_analysis.models.multiple_order import (
    ArbitragePairOrder,
    BracketOrder,
    CoverOrder, MultipleOrder,
)
from trazy_analysis.models.order import Order, OrderBase
from trazy_analysis.models.signal import ArbitragePairSignal, Signal
from trazy_analysis.order_manager.order_creator import OrderCreator
from trazy_analysis.order_manager.position_sizer import PositionSizer

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
)


class OrderManager:
    def __init__(
        self,
        events: deque,
        broker_manager: BrokerManager,
        position_sizer: PositionSizer,
        order_creator: OrderCreator,
        clock: Clock,
        filter_at_end_of_day=True,
    ) -> None:
        self.events = events
        self.broker_manager = broker_manager
        self.position_sizer: PositionSizer = position_sizer
        self.order_creator = order_creator
        self.pending_signals = deque()
        self.clock = clock
        self.filter_at_end_of_day = filter_at_end_of_day
        self.orders = {}
        self.orders_df = {}

    def update_orders_df(self):
        for asset in self.orders:
            for time_unit in self.orders[asset]:
                get_or_create_nested_dict(self.orders_df, asset)
                self.orders_df[asset][time_unit] = pd.DataFrame(
                    [
                        [
                            order.generation_time,
                            order.asset.exchange,
                            order.asset.symbol,
                            str(order.time_unit),
                            order.action.name,
                            order.direction.name,
                            order.size,
                            str(order.time_in_force),
                            order.order_type.name,
                            order.submission_time,
                            order.signal_id,
                        ]
                        for order in self.orders[asset][time_unit]
                    ],
                    columns=[
                        "Generation time",
                        "Exchange",
                        "Symbol",
                        "Time unit",
                        "Action",
                        "Direction",
                        "Size",
                        "Time in force",
                        "Order type",
                        "Submission time",
                        "Signal id",
                    ],
                ).set_index("Generation time")

    def add_order(self, order: OrderBase):
        if isinstance(order, Order):
            get_or_create_nested_dict(self.orders, order.asset)
            if order.time_unit not in self.orders[order.asset]:
                self.orders[order.asset][order.time_unit] = []
            self.orders[order.asset][order.time_unit].append(order)
        elif isinstance(order, MultipleOrder):
            for order_base in order.orders:
                self.add_order(order_base)


    def process_pending_signal(self, signal: Signal) -> None:
        LOG.info("Processing %s", str(signal))
        order = self.order_creator.create_order(signal, self.clock)
        self.add_order(order)
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
