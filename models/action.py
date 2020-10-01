from decimal import Decimal

import pandas as pd

from models.enums import ActionType, PositionType


class Action:
    def __init__(
        self,
        action_type: ActionType,
        position_type: PositionType,
        size: int,
        confidence_level: Decimal,
        strategy: str,
        symbol: str,
        candle_timestamp: pd.Timestamp,
        parameters: dict,
        timestamp: pd.Timestamp = pd.Timestamp.now(tz="UTC"),
    ):
        self.action_type = action_type
        self.position_type = position_type
        self.size = size
        self.confidence_level = confidence_level
        self.strategy = strategy
        self.symbol = symbol
        self.candle_timestamp = candle_timestamp
        self.parameters = parameters
        self.timestamp = timestamp

    @staticmethod
    def from_serializable_dict(action_dict: dict) -> "Action":
        action: Action = Action(
            action_type=ActionType[action_dict["action_type"]],
            position_type=PositionType[action_dict["position_type"]],
            size=action_dict["size"],
            confidence_level=Decimal(action_dict["confidence_level"]),
            strategy=action_dict["strategy"],
            symbol=action_dict["symbol"],
            candle_timestamp=action_dict["candle_timestamp"],
            parameters=action_dict["parameters"],
            timestamp=action_dict["timestamp"],
        )
        return action

    def to_serializable_dict(self) -> dict:
        dict = self.__dict__.copy()
        dict["action_type"] = dict["action_type"].name
        dict["position_type"] = dict["position_type"].name
        dict["confidence_level"] = str(dict["confidence_level"])
        dict["candle_timestamp"] = str(dict["candle_timestamp"])
        dict["timestamp"] = str(dict["timestamp"])
        return dict

    def __eq__(self, other):
        if isinstance(other, Action):
            return self.to_serializable_dict() == other.to_serializable_dict()
        return False

    def __ne__(self, other):
        return not self.__eq__(other)
