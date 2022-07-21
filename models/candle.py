import json
from datetime import datetime, timedelta
from typing import TypeVar

import pytz

from trazy_analysis.models.asset import Asset
from trazy_analysis.models.enums import CandleDirection

TCandle = TypeVar("TCandle", bound="Candle")

class Candle:
    def __init__(
        self,
        asset: Asset,
        open: float,
        high: float,
        low: float,
        close: float,
        volume: int,
        timestamp: datetime = datetime.now(pytz.UTC),
        time_unit=timedelta(minutes=1),
    ):
        """
        :param asset: The asset that this candle represents
        :type asset: Asset
        :param open: The price of the asset at the beginning of the time unit
        :type open: float
        :param high: The highest price of the asset during the time unit
        :type high: float
        :param low: The lowest price of the asset during the time unit
        :type low: float
        :param close: The close price of the asset at the end of the time unit
        :type close: float
        :param volume: The volume of the asset traded during the time unit
        :type volume: int
        :param timestamp: The timestamp of the candle
        :type timestamp: datetime
        :param time_unit: The time unit of the candle
        """
        self.asset: Asset = asset
        self.open: float = open
        self.high: float = high
        self.low: float = low
        self.close: float = close
        self.volume: int = volume
        self.time_unit = time_unit

        from trazy_analysis.common.utils import timestamp_to_utc

        self.timestamp = timestamp_to_utc(timestamp)

    @property
    def direction(self) -> CandleDirection:
        if self.open < self.close:
            return CandleDirection.BULLISH
        elif self.open > self.close:
            return CandleDirection.BEARISH
        else:
            return CandleDirection.DOJI

    @staticmethod
    def from_serializable_dict(candle_dict: dict) -> TCandle:
        from trazy_analysis.common.utils import timestamp_to_utc
        from trazy_analysis.common.helper import parse_timedelta_str

        timestamp = timestamp_to_utc(candle_dict["timestamp"])
        candle: Candle = Candle(
            asset=Asset.from_dict(candle_dict["asset"]),
            open=float(candle_dict["open"]),
            high=float(candle_dict["high"]),
            low=float(candle_dict["low"]),
            close=float(candle_dict["close"]),
            volume=candle_dict["volume"],
            timestamp=timestamp,
            time_unit=parse_timedelta_str(candle_dict["time_unit"])
        )
        return candle

    @staticmethod
    def from_dict(candle_dict: dict) -> TCandle:
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
    def from_json(candle_json: str) -> TCandle:
        candle_dict = json.loads(candle_json)
        candle_dict["timestamp"] = datetime.strptime(
            candle_dict["timestamp"], "%Y-%m-%d %H:%M:%S%z"
        )
        return Candle.from_serializable_dict(candle_dict)

    def to_serializable_dict(self) -> dict:
        candle_dict = self.__dict__.copy()
        candle_dict["asset"] = candle_dict["asset"].to_dict()
        candle_dict["open"] = str(candle_dict["open"])
        candle_dict["high"] = str(candle_dict["high"])
        candle_dict["low"] = str(candle_dict["low"])
        candle_dict["close"] = str(candle_dict["close"])
        candle_dict["time_unit"] = str(candle_dict["time_unit"])
        return candle_dict

    def to_json(self) -> str:
        candle_dict = self.to_serializable_dict()
        candle_dict["timestamp"] = candle_dict["timestamp"].strftime(
            "%Y-%m-%d %H:%M:%S%z"
        )
        return json.dumps(candle_dict)

    def copy(self) -> TCandle:
        return Candle.from_dict(self.__dict__)

    def __hash__(self):
        return hash(
            (
                self.asset.__hash__(),
                self.open,
                self.high,
                self.low,
                self.close,
                self.volume,
                self.timestamp,
            )
        )

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
            "time_unit={},"
            "timestamp={})".format(
                self.asset,
                self.open,
                self.high,
                self.low,
                self.close,
                self.volume,
                self.time_unit,
                self.timestamp,
            )
        )
