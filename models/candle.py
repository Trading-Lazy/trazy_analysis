import json
from decimal import Decimal

import pandas as pd


class Candle:
    def __init__(
        self,
        symbol: str,
        open: Decimal,
        high: Decimal,
        low: Decimal,
        close: Decimal,
        volume: int,
        timestamp: pd.Timestamp = pd.Timestamp.now("UTC"),
    ):
        self.symbol: str = symbol
        self.open: Decimal = open
        self.high: Decimal = high
        self.low: Decimal = low
        self.close: Decimal = close
        self.volume: int = volume
        from common.utils import timestamp_to_utc

        self.timestamp = timestamp_to_utc(timestamp)

    @staticmethod
    def from_serializable_dict(candle_dict: dict) -> "Candle":
        from common.utils import timestamp_to_utc

        timestamp = timestamp_to_utc(pd.Timestamp(candle_dict["timestamp"]))
        candle: Candle = Candle(
            symbol=candle_dict["symbol"],
            open=Decimal(candle_dict["open"]),
            high=Decimal(candle_dict["high"]),
            low=Decimal(candle_dict["low"]),
            close=Decimal(candle_dict["close"]),
            volume=candle_dict["volume"],
            timestamp=timestamp,
        )
        return candle

    @staticmethod
    def from_dict(candle_dict: dict) -> "Candle":
        candle: Candle = Candle(
            symbol=candle_dict["symbol"],
            open=candle_dict["open"],
            high=candle_dict["high"],
            low=candle_dict["low"],
            close=candle_dict["close"],
            volume=candle_dict["volume"],
            timestamp=candle_dict["timestamp"],
        )
        return candle

    @staticmethod
    def from_json(candle_json: str) -> "Candle":
        candle_dict = json.loads(candle_json)
        candle_dict["timestamp"] = pd.Timestamp(candle_dict["timestamp"], tz="UTC")
        return Candle.from_serializable_dict(candle_dict)

    def to_serializable_dict(self) -> dict:
        dict = self.__dict__.copy()
        dict["open"] = str(dict["open"])
        dict["high"] = str(dict["high"])
        dict["low"] = str(dict["low"])
        dict["close"] = str(dict["close"])
        dict["timestamp"] = str(dict["timestamp"])
        return dict

    def to_json(self) -> str:
        return json.dumps(self.to_serializable_dict())

    def copy(self) -> "Candle":
        return Candle.from_dict(self.__dict__)

    def __eq__(self, other):
        if isinstance(other, Candle):
            return self.to_serializable_dict() == other.to_serializable_dict()
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return (
            "Candle("
            'symbol="{}",'
            'open=Decimal("{}"),'
            'high=Decimal("{}"),'
            'low=Decimal("{}"),'
            'close=Decimal("{}"),'
            "volume={},"
            'timestamp=pd.Timestamp("{}"))'.format(
                self.symbol,
                self.open,
                self.high,
                self.low,
                self.close,
                self.volume,
                self.timestamp,
            )
        )
