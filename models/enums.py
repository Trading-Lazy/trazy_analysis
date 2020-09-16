from enum import Enum


class ActionType(Enum):
    BUY = "BUY"
    SELL = "SELL"


class PositionType(Enum):
    LONG = "LONG"
    SHORT = "SHORT"
