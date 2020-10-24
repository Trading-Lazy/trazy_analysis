from enum import Enum


class Action(Enum):
    BUY = "BUY"
    SELL = "SELL"


class Direction(Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class OrderStatus(Enum):
    CANCELLED = "CANCELLED"
    CREATED = "CREATED"
    COMPLETE = "COMPLETE"
    EXPIRED = "EXPIRED"
    SUBMITTED = "SUBMITTED"
