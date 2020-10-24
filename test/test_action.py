from decimal import Decimal

import pandas as pd

from models.order import Order
from models.enums import Action, Direction, OrderType

ORDER1 = Order(generation_time=pd.Timestamp("2020-05-08 14:17:00+00:00"))
ORDER1_DICT = {
    "action": "BUY",
    "direction": "LONG",
    "order_type": "MARKET_ORDER",
    "size": 5,
    "confidence_level": "0.05",
    "strategy": "SmaCrossoverStrategy",
    "symbol": "IVV",
    "candle_timestamp": "2020-05-08 14:16:00+00:00",
    "parameters": {},
    "timestamp": "2020-05-08 14:17:00+00:00",
    "limit": None,
    "stop": None,
    "stop_pct": None,
    "expiration_timestamp": None,
}

ORDER2 = Order(generation_time=pd.Timestamp("2020-05-08 14:17:00+00:00"))

ORDER3 = Order(generation_time=pd.Timestamp("2020-05-08 14:17:00+00:00"))


def test_eq():
    assert ORDER1 == ORDER2
    assert not ORDER2 == ORDER3
    assert not ORDER1 == object()


def test_ne():
    assert not ORDER1 != ORDER2
    assert ORDER2 != ORDER3


def test_from_serializable_dict():
    assert Order.from_serializable_dict(ORDER1_DICT) == ORDER1


def test_to_serializable_dict():
    assert ORDER1_DICT == ORDER1.to_serializable_dict()
