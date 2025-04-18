import os
from abc import ABC
from datetime import datetime, timedelta
from typing import Any, Callable, List, Tuple, TypeVar
import uuid

import pytz

import trazy_analysis.settings
from trazy_analysis.common.clock import Clock
from trazy_analysis.common.helper import parse_timedelta_str
from trazy_analysis.common.utils import generate_object_id
from trazy_analysis.logger import logger
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.enums import (
    Action,
    Direction,
    OrderCondition,
    OrderStatus,
    OrderType,
)
from trazy_analysis.models.utils import is_closed_position

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
)

TOrder = TypeVar("TOrder", bound="Order")

class OrderBase(ABC):
    def __init__(
        self,
        status: OrderStatus = OrderStatus.CREATED,
        generation_time: datetime = datetime.now(pytz.UTC),
        time_in_force: timedelta = timedelta(minutes=5),
    ):
        self.status = status
        self.generation_time = generation_time
        self.time_in_force = time_in_force
        self.submission_time = None
        self.on_complete_callbacks: Tuple[list[Callable], list[Any]] = []
        self.on_cancel_callbacks: Tuple[list[Callable], list[Any]] = []

    def add_on_complete_callback(self, callback: Callable, *args):
        self.on_complete_callbacks.append((callback, args))

    def add_on_cancel_callback(self, callback: Callable, *args):
        self.on_cancel_callbacks.append((callback, args))

    def submit(self, submission_time: datetime = datetime.now(pytz.UTC)) -> None:
        self.submission_time = submission_time
        self.status = OrderStatus.SUBMITTED

    def complete(self) -> None:
        self.status = OrderStatus.COMPLETED
        for callback, args in self.on_complete_callbacks:
            callback(*args)

    def cancel(self) -> None:
        self.status = OrderStatus.CANCELLED
        for callback, args in self.on_cancel_callbacks:
            callback(*args)

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
        asset: Asset,
        time_unit: timedelta,
        action: Action,
        direction: Direction,
        size: float,
        signal_id: str,
        limit: float = None,
        stop: float = None,
        target: float = None,
        stop_pct: float = None,
        order_type: OrderType = OrderType.MARKET,
        condition: OrderCondition = OrderCondition.GTC,
        clock: Clock = None,
        time_in_force: timedelta = timedelta(minutes=5),
        status: OrderStatus = OrderStatus.CREATED,
        generation_time: datetime = datetime.now(pytz.UTC),
        order_id: str = None,
    ):
        self.asset = asset
        self.time_unit = time_unit
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
            order_id = uuid.uuid4()
        self.order_id = order_id
        if self.clock is not None:
            generation_time = self.clock.current_time()
        self.order_type: OrderType = order_type
        self.condition: OrderCondition = condition
        super().__init__(
            status=status, generation_time=generation_time, time_in_force=time_in_force
        )

    def submit(self, submission_time: datetime = datetime.now(pytz.UTC)) -> None:
        if self.clock is not None:
            submission_time = self.clock.current_time()
        super().submit(submission_time)
        LOG.info("Submitted order: %s-%s, qty: %s", self.asset, str(self.time_unit), self.size)

    def in_force(self, timestamp: datetime = None) -> bool:
        if timestamp is None:
            timestamp = self.clock.current_time()
        return super().in_force(timestamp)

    @property
    def is_entry_order(self):
        return not self.is_exit_order

    @property
    def is_exit_order(self):
        return is_closed_position(self.action, self.direction)

    @staticmethod
    def from_serializable_dict(order_dict: dict) -> TOrder:
        order: Order = Order(
            asset=Asset.from_dict(order_dict["asset"]),
            time_unit=parse_timedelta_str(order_dict["time_unit"]),
            action=Action[order_dict["action"]],
            direction=Direction[order_dict["direction"]],
            size=int(order_dict["size"]),
            signal_id=order_dict["signal_id"],
            order_type=OrderType[order_dict["order_type"]],
            condition=OrderCondition[order_dict["condition"]],
            time_in_force=parse_timedelta_str(order_dict["time_in_force"]),
            status=OrderStatus[order_dict["status"]],
            generation_time=order_dict["generation_time"],
        )
        return order

    def to_serializable_dict(self, with_order_id=False) -> dict:
        dict = self.__dict__.copy()
        dict["asset"] = dict["asset"].to_dict()
        dict["time_unit"] = str(dict["time_unit"])
        dict["action"] = dict["action"].name
        dict["direction"] = dict["direction"].name
        dict["signal_id"] = str(dict["signal_id"])
        dict["generation_time"] = str(dict["generation_time"])
        dict["time_in_force"] = str(dict["time_in_force"])
        dict["status"] = dict["status"].name
        dict["order_type"] = dict["order_type"].name
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

    def __str__(self):
        return (
            "Order("
            "asset={},"
            "time_unit={},"
            "action={},"
            "direction={},"
            "size={},"
            "signal_id={},"
            "limit={},"
            "stop={},"
            "target={},"
            "stop_pct={},"
            "type={},"
            "condition={},"
            "time_in_force={},"
            "status={},"
            "generation_time={},"
            "order_id={},"
            "submission_time={})".format(
                self.asset,
                self.time_unit,
                self.action.name,
                self.direction.name,
                self.size,
                self.signal_id,
                self.limit,
                self.stop,
                self.target,
                self.stop_pct,
                self.order_type.name,
                self.condition.name,
                self.time_in_force,
                self.status.name,
                self.generation_time,
                self.order_id,
                self.submission_time,
            )
        )
