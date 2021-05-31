import json

from datetime import datetime, timezone

from models.asset import Asset


class Candle:
    def __init__(
        self,
        asset: Asset,
        open: float,
        high: float,
        low: float,
        close: float,
        volume: int,
        timestamp: datetime = datetime.now(timezone.utc),
    ):
        self.asset: Asset = asset
        self.open: float = open
        self.high: float = high
        self.low: float = low
        self.close: float = close
        self.volume: int = volume
        from common.utils import timestamp_to_utc

        self.timestamp = timestamp_to_utc(timestamp)

    @staticmethod
    def from_serializable_dict(candle_dict: dict) -> "Candle":
        from common.utils import timestamp_to_utc

        timestamp = timestamp_to_utc(candle_dict["timestamp"])
        candle: Candle = Candle(
            asset=Asset.from_dict(candle_dict["asset"]),
            open=float(candle_dict["open"]),
            high=float(candle_dict["high"]),
            low=float(candle_dict["low"]),
            close=float(candle_dict["close"]),
            volume=candle_dict["volume"],
            timestamp=timestamp,
        )
        return candle

    @staticmethod
    def from_dict(candle_dict: dict) -> "Candle":
        candle: Candle = Candle(
            asset=candle_dict["asset"],
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
        candle_dict["timestamp"] = datetime.strptime(
            candle_dict["timestamp"], "%Y-%m-%d %H:%M:%S%z"
        )
        return Candle.from_serializable_dict(candle_dict)

    def to_serializable_dict(self) -> dict:
        dict = self.__dict__.copy()
        dict["asset"] = dict["asset"].to_dict()
        dict["open"] = str(dict["open"])
        dict["high"] = str(dict["high"])
        dict["low"] = str(dict["low"])
        dict["close"] = str(dict["close"])
        return dict

    def to_json(self) -> str:
        candle_dict = self.to_serializable_dict()
        # candle_dict["asset"] = candle_dict["asset"].to_dict()
        candle_dict["timestamp"] = candle_dict["timestamp"].strftime(
            "%Y-%m-%d %H:%M:%S%z"
        )
        return json.dumps(candle_dict)

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
            "asset={},"
            "open={},"
            "high={},"
            "low={},"
            "close={},"
            "volume={},"
            "timestamp={})".format(
                self.asset,
                self.open,
                self.high,
                self.low,
                self.close,
                self.volume,
                self.timestamp,
            )
        )
