from decimal import Decimal

import pandas as pd

from models.action import Action
from models.enums import ActionType, PositionType

ACTION1 = Action(
    action_type=ActionType.BUY,
    position_type=PositionType.LONG,
    size=5,
    confidence_level=Decimal("0.05"),
    strategy="SmaCrossoverStrategy",
    symbol="IVV",
    candle_timestamp=pd.Timestamp("2020-05-08 14:16:00+00:00"),
    parameters={},
    timestamp=pd.Timestamp("2020-05-08 14:17:00+00:00"),
)
ACTION1_DICT = {
    "action_type": "BUY",
    "position_type": "LONG",
    "size": 5,
    "confidence_level": "0.05",
    "strategy": "SmaCrossoverStrategy",
    "symbol": "IVV",
    "candle_timestamp": "2020-05-08 14:16:00+00:00",
    "parameters": {},
    "timestamp": "2020-05-08 14:17:00+00:00",
}

ACTION2 = Action(
    action_type=ActionType.BUY,
    position_type=PositionType.LONG,
    size=5,
    confidence_level=Decimal("0.05"),
    strategy="SmaCrossoverStrategy",
    symbol="IVV",
    candle_timestamp=pd.Timestamp("2020-05-08 14:16:00+00:00"),
    parameters={},
    timestamp=pd.Timestamp("2020-05-08 14:17:00+00:00"),
)

ACTION3 = Action(
    action_type=ActionType.BUY,
    position_type=PositionType.LONG,
    size=10,
    confidence_level=Decimal("0.05"),
    strategy="SmaCrossoverStrategy",
    symbol="IVV",
    candle_timestamp=pd.Timestamp("2020-05-08 14:16:00+00:00"),
    parameters={},
    timestamp=pd.Timestamp("2020-05-08 14:17:00+00:00"),
)


def test_eq():
    assert ACTION1 == ACTION2
    assert not ACTION2 == ACTION3
    assert not ACTION1 == object()


def test_ne():
    assert not ACTION1 != ACTION2
    assert ACTION2 != ACTION3


def test_from_serializable_dict():
    assert Action.from_serializable_dict(ACTION1_DICT) == ACTION1


def test_to_serializable_dict():
    assert ACTION1_DICT == ACTION1.to_serializable_dict()
