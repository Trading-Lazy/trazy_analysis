import os
from abc import ABC
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, List, Tuple

import settings
from common.clock import Clock
from common.helper import parse_timedelta_str
from common.utils import generate_object_id
from logger import logger
from models.enums import Action, Direction, OrderCondition, OrderStatus, OrderType
from models.utils import is_closed_position

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)


class OrderBase(ABC):
    def __init__(
        self,
        status: OrderStatus = OrderStatus.CREATED,
        generation_time: datetime = datetime.now(timezone.utc),
        time_in_force: timedelta = timedelta(minutes=5),
    ):
        self.status = status
        self.generation_time = generation_time
        self.time_in_force = time_in_force
        self.submission_time = None
        self.on_complete_callbacks: Tuple[List[Callable], List[Any]] = []

    def add_on_complete_callback(self, callback: Callable, *args):
        self.on_complete_callbacks.append((callback, args))

    def submit(self, submission_time: datetime = datetime.now(timezone.utc)) -> None:
        self.submission_time = submission_time
        self.status = OrderStatus.SUBMITTED

    def complete(self) -> None:
        self.status = OrderStatus.COMPLETED
        for callback, args in self.on_complete_callbacks:
            callback(*args)

    def cancel(self) -> None:
        self.status = OrderStatus.CANCELLED

    def disable(self):
        self.status = OrderStatus.EXPIRED

    def in_force(self, timestamp: datetime) -> bool:
        in_force = self.submission_time + self.time_in_force > timestamp
        if not in_force:
            self.disable()
        return in_force


class Order(OrderBase):
    def __init__(
        self,
        symbol: str,
        action: Action,
        direction: Direction,
        size: int,
        signal_id: str,
        limit: float = None,
        stop: float = None,
        target: float = None,
        stop_pct: float = None,
        type: OrderType = OrderType.MARKET,
        condition: OrderCondition = OrderCondition.GTC,
        clock: Clock = None,
        time_in_force: timedelta = timedelta(minutes=5),
        status: OrderStatus = OrderStatus.CREATED,
        generation_time: datetime = datetime.now(timezone.utc),
        order_id: str = None,
    ):
        self.symbol = symbol
        self.action = action
        self.direction = direction
        self.size = size
        self.signal_id = signal_id
        self.limit = limit
        self.stop = stop
        self.target = target
        self.stop_pct = stop_pct
        self.clock = clock
        if order_id is None:
            order_id = generate_object_id()
        self.order_id = order_id
        if self.clock is not None:
            generation_time = self.clock.current_time(symbol=symbol)
        self.type: OrderType = type
        self.condition: OrderCondition = condition
        super().__init__(
            status=status, generation_time=generation_time, time_in_force=time_in_force
        )

    def submit(self, submission_time: datetime = datetime.now(timezone.utc)) -> None:
        if self.clock is not None:
            submission_time = self.clock.current_time(symbol=self.symbol)
        super().submit(submission_time)
        LOG.info("Submitted order: %s, qty: %s", self.symbol, self.size)

    def in_force(self, timestamp: datetime = None) -> bool:
        if timestamp is None:
            timestamp = self.clock.current_time(symbol=self.symbol)
        return super().in_force(timestamp)

    @property
    def is_entry_order(self):
        return not self.is_exit_order

    @property
    def is_exit_order(self):
        return is_closed_position(self.action, self.direction)

    @staticmethod
    def from_serializable_dict(order_dict: dict) -> "Order":
        order: Order = Order(
            symbol=order_dict["symbol"],
            action=Action[order_dict["action"]],
            direction=Direction[order_dict["direction"]],
            size=int(order_dict["size"]),
            type=OrderType[order_dict["type"]],
            condition=OrderCondition[order_dict["condition"]],
            signal_id=order_dict["signal_id"],
            time_in_force=parse_timedelta_str(order_dict["time_in_force"]),
            status=OrderStatus[order_dict["status"]],
            generation_time=order_dict["generation_time"],
        )
        return order

    def to_serializable_dict(self, with_order_id=False) -> dict:
        dict = self.__dict__.copy()
        dict["action"] = dict["action"].name
        dict["direction"] = dict["direction"].name
        dict["signal_id"] = str(dict["signal_id"])
        dict["generation_time"] = str(dict["generation_time"])
        dict["time_in_force"] = str(dict["time_in_force"])
        dict["status"] = dict["status"].name
        dict["type"] = dict["type"].name
        dict["condition"] = dict["condition"].name
        if not with_order_id:
            del dict["order_id"]
        del dict["on_complete_callbacks"]
        del dict["clock"]
        return dict

    def __eq__(self, other) -> bool:
        if isinstance(other, Order):
            return self.to_serializable_dict() == other.to_serializable_dict()
        return False

    def __ne__(self, other) -> bool:
        return not self.__eq__(other)
