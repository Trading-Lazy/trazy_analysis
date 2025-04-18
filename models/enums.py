from enum import Enum


class Action(Enum):
    BUY = "BUY"
    SELL = "SELL"


class Direction(Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class CandleDirection(Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    DOJI = "DOJI"
    NOT_COMPUTED = "NOT_COMPUTED"


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


class EventType(Enum):
    MARKET_DATA = "MARKET_DATA"
    SYNCHRONIZED_MARKET_DATA = "MARKET_DATA"
    SIGNAL = "SIGNAL"
    MARKET_DATA_END = "MARKET_DATA_END"
    OPEN_ORDERS = "OPEN_ORDERS"
    MARKET_EOD_DATA = "MARKET_EOD_DATA"
    PENDING_SIGNAL = "PENDING_SIGNAL"


class BrokerIsolation(Enum):
    EXCHANGE = ("EXCHANGE",)
    ASSET = ("ASSET",)
    STRATEGY = ("STRATEGY",)
    STRATEGY_AND_EXCHANGE = ("STRATEGY_AND_EXCHANGE",)
    STRATEGY_AND_ASSET = ("STRATEGY_AND_ASSET",)


class StrategyParametersIsolation(Enum):
    STRATEGY = ("STRATEGY",)
    STRATEGY_AND_ASSET = ("STRATEGY_AND_ASSET",)
    STRATEGY_AND_TIME_UNIT = ("STRATEGY_AND_TIME_UNIT",)


class IndicatorMode(Enum):
    LIVE = ("LIVE",)
    BATCH = ("BATCH",)

class EventLoopMode(Enum):
    LIVE = ("LIVE",)
    BATCH = ("BATCH",)