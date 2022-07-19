from datetime import datetime, timedelta

import numpy as np
import pytz

from trazy_analysis.common.clock import Clock
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.enums import Action, OrderStatus, OrderType
from trazy_analysis.models.order import Order, OrderBase
from trazy_analysis.models.utils import is_closed_position, is_open_position


class MultipleOrder(OrderBase):
    def __init__(
        self,
        orders: np.array,  # [OrderBase]
        status: OrderStatus = OrderStatus.CREATED,
        generation_time: datetime = datetime.now(pytz.UTC),
        time_in_force: str = timedelta(minutes=5),
    ):
        self.orders = orders
        super().__init__(status, generation_time, time_in_force)
        self.add_orders_callbacks()

    def add_orders_callbacks(self) -> None:
        for order in self.orders:
            order.add_on_complete_callback(self.check_order_completed)
        for order in self.orders:
            order.add_on_cancel_callback(self.check_order_cancelled)

    def check_order_completed(self) -> None:
        all_orders_completed = all(
            order.status == OrderStatus.COMPLETED for order in self.orders
        )
        if all_orders_completed:
            self.complete()

    def check_order_cancelled(self) -> None:
        any_orders_cancelled = any(
            order.status == OrderStatus.CANCELLED for order in self.orders
        )
        if any_orders_cancelled:
            self.cancel()

    def pending_orders(self) -> None:
        return [
            order
            for order in self.orders
            if order.status != OrderStatus.COMPLETED
            and order.status != OrderStatus.EXPIRED
            and order.status != OrderStatus.CANCELLED
        ]

    def completed_orders(self) -> None:
        return [order for order in self.orders if order.status == OrderStatus.COMPLETED]

    def submit(self, submission_time: datetime = datetime.now(pytz.UTC)) -> None:
        super().submit(submission_time)
        for order in self.orders:
            order.submit(submission_time)

    def __eq__(self, other) -> None:
        if isinstance(other, MultipleOrder):
            return self.orders == other.orders
        return False


class SequentialOrder(MultipleOrder):
    def __init__(
        self,
        orders: np.array,  # [OrderBase]
        status: OrderStatus = OrderStatus.CREATED,
        generation_time: datetime = datetime.now(pytz.UTC),
        time_in_force: str = timedelta(minutes=5),
    ) -> None:
        super().__init__(orders, status, generation_time, time_in_force)

    def add_orders_callbacks(self) -> None:
        orders_len = len(self.orders)
        for i in range(0, orders_len - 1):
            order = self.orders[i]
            next_order = self.orders[i + 1]
            order.add_on_complete_callback(next_order.submit)

    def get_first_order(self) -> None:
        if len(self.orders) == 0:
            raise Exception(
                "Cannot get the first order. There is no order in the sequence"
            )
        return self.orders[0]

    def submit(self, submission_time: datetime = datetime.now(pytz.UTC)) -> None:
        first_order = self.get_first_order()
        first_order.submit(submission_time)


class OcoOrder(MultipleOrder):
    def __init__(
        self,
        orders: np.array,  # [OrderBase]
        status: OrderStatus = OrderStatus.CREATED,
        generation_time: datetime = datetime.now(pytz.UTC),
        time_in_force: str = timedelta(minutes=5),
    ) -> None:
        super().__init__(orders, status, generation_time, time_in_force)

    def add_orders_callbacks(self) -> None:
        for index, order in enumerate(self.orders):
            order.add_on_complete_callback(self.complete, index)

    def add_order(self, order: OrderBase) -> None:
        order.add_on_complete_callback(self.complete, len(self.orders))
        order.submit()
        self.orders.append(order)

    def complete(self, completed_order_index: int) -> None:
        for index, order in enumerate(self.orders):
            if index != completed_order_index:
                order.cancel()
        super().complete()


class HomogeneousSequentialOrder(SequentialOrder):
    def check_orders_asset(self, orders: np.array):
        for order in orders:
            if isinstance(order, MultipleOrder):
                self.check_orders_asset(order.orders)
                continue
            if order.asset != self.asset:
                raise Exception(
                    f"All orders in an homogeneous sequential should have the same asset. There is an "
                    f"order which has asset {order.asset} different from the homogeneous sequential "
                    f"asset {self.asset}"
                )

    def __init__(
        self,
        asset: Asset,
        orders: np.array,  # [OrderBase]
        clock: Clock = None,
        status: OrderStatus = OrderStatus.CREATED,
        generation_time: datetime = datetime.now(pytz.UTC),
        time_in_force: str = timedelta(minutes=5),
    ):
        self.asset = asset
        self.clock = clock
        self.check_orders_asset(orders)
        super().__init__(orders, status, generation_time, time_in_force)

    def submit(self, submission_time: datetime = datetime.now(pytz.UTC)) -> None:
        if self.clock is not None:
            submission_time = self.clock.current_time()
        first_order = self.get_first_order()
        first_order.submit(submission_time)
        self.submission_time = submission_time


class CoverOrder(HomogeneousSequentialOrder):
    def __init__(
        self,
        asset: Asset,
        initiation_order: Order,
        stop_order: Order,
        clock: Clock = None,
        status: OrderStatus = OrderStatus.CREATED,
        generation_time: datetime = datetime.now(pytz.UTC),
        time_in_force: Order = timedelta(minutes=5),
    ):
        # check allowed orders types
        initiation_order_allowed_types = [OrderType.MARKET, OrderType.LIMIT]
        if initiation_order.order_type not in initiation_order_allowed_types:
            raise Exception(
                f"A cover order initiation order type should be either MARKET or LIMIT not "
                f"{initiation_order.order_type.name}"
            )
        stop_order_allowed_types = [OrderType.STOP, OrderType.TRAILING_STOP]
        if stop_order.order_type not in stop_order_allowed_types:
            raise Exception(
                f"A cover order stop order type should be either STOP or TRAILING_STOP not "
                f"{stop_order.order_type.name}"
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
            asset=asset,
            orders=orders,
            clock=clock,
            status=status,
            generation_time=generation_time,
            time_in_force=time_in_force,
        )


class BracketOrder(HomogeneousSequentialOrder):
    def __init__(
        self,
        asset: Asset,
        initiation_order: Order,
        target_order: Order,
        stop_order: Order,
        clock: Clock = None,
        status: OrderStatus = OrderStatus.CREATED,
        generation_time: datetime = datetime.now(pytz.UTC),
        time_in_force: timedelta = timedelta(minutes=5),
    ):
        # check allowed orders
        initiation_order_allowed_types = [OrderType.MARKET, OrderType.LIMIT]
        if initiation_order.order_type not in initiation_order_allowed_types:
            raise Exception(
                f"A bracket order initiation order type should be either MARKET or LIMIT not "
                f"{initiation_order.order_type.name}"
            )
        target_order_allowed_types = [OrderType.TARGET]
        if target_order.order_type not in target_order_allowed_types:
            raise Exception(
                f"A bracket order target order type should be TARGET not "
                f"{target_order.order_type.name}"
            )
        stop_order_allowed_types = [OrderType.STOP, OrderType.TRAILING_STOP]
        if stop_order.order_type not in stop_order_allowed_types:
            raise Exception(
                f"A bracket order stop order type should be either STOP or TRAILING_STOP not "
                f"{stop_order.order_type.name}"
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
            asset=asset,
            orders=orders,
            clock=clock,
            status=status,
            generation_time=generation_time,
            time_in_force=time_in_force,
        )


class ArbitragePairOrder(MultipleOrder):
    def __init__(
        self,
        buy_order: Order,
        sell_order: Order,
        status: OrderStatus = OrderStatus.CREATED,
        generation_time: datetime = datetime.now(pytz.UTC),
        time_in_force: str = timedelta(minutes=5),
    ):
        if buy_order.action != Action.BUY:
            raise Exception(
                f"buy signal action should be BUY not {buy_order.action.name}"
            )
        if sell_order.action != Action.SELL:
            raise Exception(
                f"sell signal action should be SELL not {sell_order.action.name}"
            )
        self.buy_order = buy_order
        self.sell_order = sell_order
        orders = np.array([buy_order, sell_order], dtype=Order)
        super().__init__(orders, status, generation_time, time_in_force)
