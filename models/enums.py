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
    COMPLETED = "COMPLETED"
    EXPIRED = "EXPIRED"
    SUBMITTED = "SUBMITTED"


class OrderType(Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    TARGET = "TARGET"
    TRAILING_STOP = "TRAILING_STOP"


class OrderCondition(Enum):
    EOD = "EOD"
    GTC = "GTC"
