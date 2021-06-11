import os

import pandas as pd

import settings
from broker.broker_manager import BrokerManager
from common.clock import Clock
from logger import logger
from models.enums import Action, OrderType
from models.multiple_order import ArbitragePairOrder, BracketOrder, CoverOrder
from models.order import Order
from models.signal import ArbitragePairSignal, Signal

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)


class OrderCreator:
    def __init__(
        self,
        broker_manager: BrokerManager,
        fixed_order_type: OrderType = OrderType.MARKET,
        limit_order_pct=0.005,
        stop_order_pct=0.05,
        target_order_pct=0.01,
        trailing_stop_order_pct=0.05,
        with_cover=False,
        with_bracket=False,
    ):
        self.broker_manager = broker_manager
        self.fixed_order_type = fixed_order_type
        self.limit_order_pct = limit_order_pct
        self.stop_order_pct = stop_order_pct
        self.target_order_pct = target_order_pct
        self.trailing_stop_order_pct = trailing_stop_order_pct
        self.with_cover = with_cover
        self.with_bracket = with_bracket

    def find_best_limit(self, signal: Signal, action: Action) -> float:
        current_price = self.broker_manager.get_broker(
            exchange=signal.asset.exchange
        ).current_price(signal.asset)
        if action == Action.BUY:
            best_limit = current_price - self.limit_order_pct * current_price
        else:
            best_limit = current_price + self.limit_order_pct * current_price
        LOG.info("Current price: %s", current_price)
        LOG.info("Best limit found: %s", best_limit)
        return best_limit

    def find_best_stop(self, signal: Signal, action: Action) -> float:
        current_price = self.broker_manager.get_broker(
            exchange=signal.asset.exchange
        ).current_price(signal.asset)
        if action == Action.BUY:
            best_stop_or_target = current_price + self.stop_order_pct * current_price
        else:
            best_stop_or_target = current_price - self.stop_order_pct * current_price
        LOG.info("Current price: %s", current_price)
        LOG.info("Best stop/target found: %s", best_stop_or_target)
        return best_stop_or_target

    def find_best_target(
        self,
        signal: Signal,
        action: Action,
    ) -> float:
        current_price = self.broker_manager.get_broker(
            exchange=signal.asset.exchange
        ).current_price(signal.asset)
        if action == Action.BUY:
            best_stop_or_target = current_price - self.target_order_pct * current_price
        else:
            best_stop_or_target = current_price + self.target_order_pct * current_price
        LOG.info("Current price: %s", current_price)
        LOG.info("Best stop/target found: %s", best_stop_or_target)
        return best_stop_or_target

    def find_best_trailing_stop(self, signal: Signal, action: Action) -> float:
        return self.trailing_stop_order_pct

    def find_best_order_type(self, signal: Signal) -> OrderType:
        return self.fixed_order_type

    def create_order(self, signal: Signal, clock: Clock) -> Order:
        if isinstance(signal, Signal):
            if (not self.with_cover and not self.with_bracket) or signal.is_exit_signal:
                limit = None
                stop = None
                target = None
                stop_pct = None

                order_type = self.find_best_order_type(signal)
                if order_type == OrderType.LIMIT:
                    limit = self.find_best_limit(signal, signal.action)
                elif order_type == OrderType.STOP:
                    stop = self.find_best_stop(signal, signal.action)
                elif order_type == OrderType.TARGET:
                    target = self.find_best_target(signal, signal.action)
                elif order_type == OrderType.TRAILING_STOP:
                    stop_pct = self.find_best_trailing_stop(signal, signal.action)

                return Order(
                    asset=signal.asset,
                    action=signal.action,
                    direction=signal.direction,
                    size=0,
                    signal_id=signal.signal_id,
                    limit=limit,
                    stop=stop,
                    target=target,
                    stop_pct=stop_pct,
                    type=order_type,
                    clock=clock,
                    time_in_force=signal.time_in_force,
                )

            action = Action.BUY if signal.action == Action.SELL else Action.SELL
            if self.with_cover:
                stop_pct = self.find_best_trailing_stop(signal, action)
                initiation_order = Order(
                    asset=signal.asset,
                    action=signal.action,
                    direction=signal.direction,
                    size=0,
                    signal_id=signal.signal_id,
                    type=OrderType.MARKET,
                    clock=clock,
                    time_in_force=signal.time_in_force,
                )
                trailing_stop_order = Order(
                    asset=signal.asset,
                    action=action,
                    direction=signal.direction,
                    size=0,
                    signal_id=signal.signal_id,
                    stop_pct=stop_pct,
                    type=OrderType.TRAILING_STOP,
                    clock=clock,
                    time_in_force=pd.offsets.Day(1),
                )
                cover_order = CoverOrder(
                    asset=signal.asset,
                    initiation_order=initiation_order,
                    stop_order=trailing_stop_order,
                    clock=clock,
                )
                return cover_order
            elif self.with_bracket:
                target = self.find_best_target(signal, action)
                stop_pct = self.find_best_trailing_stop(signal, action)
                initiation_order = Order(
                    asset=signal.asset,
                    action=signal.action,
                    direction=signal.direction,
                    size=0,
                    signal_id=signal.signal_id,
                    type=OrderType.MARKET,
                    clock=clock,
                    time_in_force=signal.time_in_force,
                )
                target_order = Order(
                    asset=signal.asset,
                    action=action,
                    direction=signal.direction,
                    size=0,
                    signal_id=signal.signal_id,
                    target=target,
                    type=OrderType.TARGET,
                    clock=clock,
                    time_in_force=pd.offsets.Day(1),
                )
                trailing_stop_order = Order(
                    asset=signal.asset,
                    action=action,
                    direction=signal.direction,
                    size=0,
                    signal_id=signal.signal_id,
                    stop_pct=stop_pct,
                    type=OrderType.TRAILING_STOP,
                    clock=clock,
                    time_in_force=pd.offsets.Day(1),
                )
                bracket_order = BracketOrder(
                    asset=signal.asset,
                    initiation_order=initiation_order,
                    target_order=target_order,
                    stop_order=trailing_stop_order,
                    clock=clock,
                )
                return bracket_order
        elif isinstance(signal, ArbitragePairSignal):
            buy_signal = signal.buy_signal
            buy_order = Order(
                asset=buy_signal.asset,
                action=buy_signal.action,
                direction=buy_signal.direction,
                size=0,
                signal_id=buy_signal.signal_id,
                type=OrderType.MARKET,
                clock=clock,
                time_in_force=buy_signal.time_in_force,
            )
            sell_signal = signal.sell_signal
            sell_order = Order(
                asset=sell_signal.asset,
                action=sell_signal.action,
                direction=sell_signal.direction,
                size=0,
                signal_id=sell_signal.signal_id,
                type=OrderType.MARKET,
                clock=clock,
                time_in_force=sell_signal.time_in_force,
            )
            arbitrage_pair_order = ArbitragePairOrder(
                buy_order=buy_order, sell_order=sell_order
            )
            return arbitrage_pair_order
