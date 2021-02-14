from typing import List

import pandas as pd

from common.clock import Clock
from models.enums import OrderStatus, OrderType
from models.order import Order, OrderBase
from models.utils import is_closed_position, is_open_position


class MultipleOrder(OrderBase):
    def __init__(
        self,
        orders: List[OrderBase],
        status: OrderStatus = OrderStatus.CREATED,
        generation_time: pd.Timestamp = pd.Timestamp.now("UTC"),
        time_in_force: str = pd.offsets.Minute(5),
    ):
        self.orders = orders
        super().__init__(status, generation_time, time_in_force)
        self.add_orders_callbacks()

    def add_orders_callbacks(self):
        for order in self.orders:
            order.add_on_complete_callback(self.check_order_completed)

    def check_order_completed(self):
        all_orders_completed = all(
            order.status == OrderStatus.COMPLETED for order in self.orders
        )
        if all_orders_completed:
            self.complete()

    def pending_orders(self):
        return [
            order
            for order in self.orders
            if order.status != OrderStatus.COMPLETED
            and order.status != OrderStatus.EXPIRED
            and order.status != OrderStatus.CANCELLED
        ]

    def completed_orders(self):
        return [order for order in self.orders if order.status == OrderStatus.COMPLETED]

    def submit(self, submission_time: pd.Timestamp = pd.Timestamp.now("UTC")) -> None:
        super().submit(submission_time)
        for order in self.orders:
            order.submit(submission_time)

    def __eq__(self, other):
        if isinstance(other, MultipleOrder):
            return self.orders == other.orders
        return False


class SequentialOrder(MultipleOrder):
    def __init__(
        self,
        orders: List[OrderBase],
        status: OrderStatus = OrderStatus.CREATED,
        generation_time: pd.Timestamp = pd.Timestamp.now("UTC"),
        time_in_force: str = pd.offsets.Minute(5),
    ):
        super().__init__(orders, status, generation_time, time_in_force)

    def add_orders_callbacks(self):
        orders_len = len(self.orders)
        for i in range(0, orders_len - 1):
            order = self.orders[i]
            next_order = self.orders[i + 1]
            order.add_on_complete_callback(next_order.submit)

    def get_first_order(self):
        if len(self.orders) == 0:
            raise Exception(
                "Cannot get the first order. There is no order in the sequence"
            )
        return self.orders[0]

    def submit(self, submission_time: pd.Timestamp = pd.Timestamp.now("UTC")) -> None:
        first_order = self.get_first_order()
        first_order.submit(submission_time)


class OcoOrder(MultipleOrder):
    def __init__(
        self,
        orders: List[OrderBase],
        status: OrderStatus = OrderStatus.CREATED,
        generation_time: pd.Timestamp = pd.Timestamp.now("UTC"),
        time_in_force: str = pd.offsets.Minute(5),
    ):
        super().__init__(orders, status, generation_time, time_in_force)

    def add_orders_callbacks(self):
        for index, order in enumerate(self.orders):
            order.add_on_complete_callback(self.complete, index)

    def add_order(self, order: OrderBase):
        order.add_on_complete_callback(self.complete, len(self.orders))
        order.submit()
        self.orders.append(order)

    def complete(self, completed_order_index: int) -> None:
        for index, order in enumerate(self.orders):
            if index != completed_order_index:
                order.cancel()
        super().complete()


class HomogeneousSequentialOrder(SequentialOrder):
    def check_orders_symbol(self, orders: List[OrderBase]):
        for order in orders:
            if isinstance(order, MultipleOrder):
                self.check_orders_symbol(order.orders)
                continue
            if order.symbol != self.symbol:
                raise Exception(
                    f"All orders in an homogeneous sequential should have the same symbol. There is an "
                    f"order which has symbol {order.symbol} different from the homogeneous sequential "
                    f"symbol {self.symbol}"
                )

    def __init__(
        self,
        symbol: str,
        orders: List[OrderBase],
        clock: Clock = None,
        status: OrderStatus = OrderStatus.CREATED,
        generation_time: pd.Timestamp = pd.Timestamp.now("UTC"),
        time_in_force: str = pd.offsets.Minute(5),
    ):
        self.symbol = symbol
        self.clock = clock
        self.check_orders_symbol(orders)
        super().__init__(orders, status, generation_time, time_in_force)

    def submit(self, submission_time: pd.Timestamp = pd.Timestamp.now("UTC")) -> None:
        if self.clock is not None:
            submission_time = self.clock.current_time(symbol=self.symbol)
        first_order = self.get_first_order()
        first_order.submit(submission_time)
        self.submission_time = submission_time


class CoverOrder(HomogeneousSequentialOrder):
    def __init__(
        self,
        symbol: str,
        initiation_order: Order,
        stop_order: Order,
        clock: Clock = None,
        status: OrderStatus = OrderStatus.CREATED,
        generation_time: pd.Timestamp = pd.Timestamp.now("UTC"),
        time_in_force: Order = pd.offsets.Minute(5),
    ):
        # check allowed orders types
        initiation_order_allowed_types = [OrderType.MARKET, OrderType.LIMIT]
        if initiation_order.type not in initiation_order_allowed_types:
            raise Exception(
                f"A cover order initiation order type should be either MARKET or LIMIT not "
                f"{initiation_order.type.name}"
            )
        stop_order_allowed_types = [OrderType.STOP, OrderType.TRAILING_STOP]
        if stop_order.type not in stop_order_allowed_types:
            raise Exception(
                f"A cover order stop order type should be either STOP or TRAILING_STOP not "
                f"{stop_order.type.name}"
            )

        # check positions types
        if is_closed_position(initiation_order.action, initiation_order.direction):
            raise Exception("A cover order can be created only for opening positions")

        if is_open_position(stop_order.action, stop_order.direction):
            raise Exception("A cover order can be created only for closed positions")

        orders = [initiation_order, stop_order]
        self.initiation_order = initiation_order
        self.stop_order = stop_order
        super().__init__(
            symbol=symbol,
            orders=orders,
            clock=clock,
            status=status,
            generation_time=generation_time,
            time_in_force=time_in_force,
        )


class BracketOrder(HomogeneousSequentialOrder):
    def __init__(
        self,
        symbol: str,
        initiation_order: Order,
        target_order: Order,
        stop_order: Order,
        clock: Clock = None,
        status: OrderStatus = OrderStatus.CREATED,
        generation_time: pd.Timestamp = pd.Timestamp.now("UTC"),
        time_in_force: pd.offsets.DateOffset = pd.offsets.Minute(5),
    ):
        # check allowed orders
        initiation_order_allowed_types = [OrderType.MARKET, OrderType.LIMIT]
        if initiation_order.type not in initiation_order_allowed_types:
            raise Exception(
                f"A bracket order initiation order type should be either MARKET or LIMIT not "
                f"{initiation_order.type.name}"
            )
        target_order_allowed_types = [OrderType.TARGET]
        if target_order.type not in target_order_allowed_types:
            raise Exception(
                f"A bracket order target order type should be TARGET not "
                f"{target_order.type.name}"
            )
        stop_order_allowed_types = [OrderType.STOP, OrderType.TRAILING_STOP]
        if stop_order.type not in stop_order_allowed_types:
            raise Exception(
                f"A bracket order stop order type should be either STOP or TRAILING_STOP not "
                f"{stop_order.type.name}"
            )

        # check positions types
        if is_closed_position(initiation_order.action, initiation_order.direction):
            raise Exception(
                "A initiation order can be created only for opening positions"
            )

        if is_open_position(target_order.action, target_order.direction):
            raise Exception("A target order can be created only for closed positions")

        if is_open_position(stop_order.action, stop_order.direction):
            raise Exception("A stop order can be created only for closed positions")

        oco_order = OcoOrder([target_order, stop_order], generation_time, time_in_force)
        orders = [initiation_order, oco_order]
        self.initiation_order = initiation_order
        self.target_order = target_order
        self.stop_order = stop_order
        super().__init__(
            symbol=symbol,
            orders=orders,
            clock=clock,
            status=status,
            generation_time=generation_time,
            time_in_force=time_in_force,
        )
