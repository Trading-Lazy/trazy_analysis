from abc import abstractmethod
from datetime import datetime, timedelta, timezone
from typing import List

from common.clock import Clock
from common.helper import parse_timedelta_str
from models.asset import Asset
from models.enums import Action, Direction
from models.utils import is_closed_position


class SignalBase:
    def __init__(
        self,
        action: Action,
        direction: Direction,
        confidence_level: float,
        strategy: str,
        root_candle_timestamp: datetime,
        parameters: dict,
        time_in_force: timedelta = timedelta(minutes=5),
    ):
        self.action = action
        self.direction = direction
        self.confidence_level = confidence_level
        self.strategy = strategy
        self.root_candle_timestamp = root_candle_timestamp
        self.parameters = parameters
        self.time_in_force = time_in_force

    @staticmethod
    def from_serializable_dict(signal_dict: dict) -> "Signal":
        signal: Signal = Signal(
            action=Action[signal_dict["action"]],
            direction=Direction[signal_dict["direction"]],
            confidence_level=float(signal_dict["confidence_level"]),
            strategy=signal_dict["strategy"],
            root_candle_timestamp=signal_dict["root_candle_timestamp"],
            parameters=signal_dict["parameters"],
        )
        return signal

    @staticmethod
    def to_serializable_dict(self) -> dict:
        dict = self.__dict__.copy()
        dict["action"] = dict["action"].name
        dict["direction"] = dict["direction"].name
        dict["confidence_level"] = str(dict["confidence_level"])
        dict["root_candle_timestamp"] = str(dict["root_candle_timestamp"])
        return dict

    @abstractmethod
    def in_force(self, timestamp=None) -> bool:
        raise NotImplementedError("Should implement in_force()")

    @property
    @abstractmethod
    def expiration_time(self) -> datetime:
        raise NotImplementedError("Should implement expiration_time()")

    @property
    def is_entry_signal(self):
        return not self.is_exit_signal

    @property
    def is_exit_signal(self):
        return is_closed_position(self.action, self.direction)

    def __eq__(self, other):
        if isinstance(other, SignalBase):
            return self.to_serializable_dict() == other.to_serializable_dict()
        return False

    def __ne__(self, other):
        return not self.__eq__(other)


class Signal(SignalBase):
    def __init__(
        self,
        asset: Asset,
        action: Action,
        direction: Direction,
        confidence_level: float,
        strategy: str,
        root_candle_timestamp: datetime,
        parameters: dict,
        clock: Clock = None,
        time_in_force: timedelta = timedelta(minutes=5),
        generation_time: datetime = None,
    ):
        super().__init__(
            action=action,
            direction=direction,
            confidence_level=confidence_level,
            strategy=strategy,
            root_candle_timestamp=root_candle_timestamp,
            parameters=parameters,
        )
        self.asset = asset
        self.clock = clock
        if generation_time is not None:
            self.generation_time = generation_time
        elif self.clock is not None:
            self.generation_time = self.clock.current_time(asset=asset)
        else:
            self.generation_time = datetime.now(timezone.utc)
        self.time_in_force = time_in_force
        self.signal_id = asset.key() + "-" + strategy + "-" + str(root_candle_timestamp)

    @staticmethod
    def from_serializable_dict(signal_dict: dict) -> "Signal":
        signal: Signal = Signal(
            asset=Asset.from_dict(signal_dict["asset"]),
            action=Action[signal_dict["action"]],
            direction=Direction[signal_dict["direction"]],
            confidence_level=float(signal_dict["confidence_level"]),
            strategy=signal_dict["strategy"],
            root_candle_timestamp=signal_dict["root_candle_timestamp"],
            parameters=signal_dict["parameters"],
            generation_time=signal_dict["generation_time"],
            time_in_force=parse_timedelta_str(signal_dict["time_in_force"]),
        )
        return signal

    def to_serializable_dict(self) -> dict:
        dict = self.__dict__.copy()
        dict["asset"] = dict["asset"].to_dict()
        dict["action"] = dict["action"].name
        dict["direction"] = dict["direction"].name
        dict["confidence_level"] = str(dict["confidence_level"])
        dict["root_candle_timestamp"] = str(dict["root_candle_timestamp"])
        dict["generation_time"] = str(dict["generation_time"])
        dict["time_in_force"] = str(dict["time_in_force"])
        del dict["signal_id"]
        del dict["clock"]
        return dict

    @property
    def expiration_time(self) -> datetime:
        return self.generation_time + self.time_in_force

    def in_force(self, timestamp=None) -> bool:
        if self.clock is None:
            return True
        if timestamp is None:
            timestamp = self.clock.current_time(asset=self.asset)
        return self.expiration_time > timestamp


class MultipleSignal(SignalBase):
    def __init__(
        self,
        assets: List[str],
        action: Action,
        direction: Direction,
        confidence_level: float,
        strategy: str,
        root_candle_timestamp: datetime,
        parameters: dict,
        clock: Clock = None,
        time_in_force: timedelta = timedelta(minutes=5),
        generation_time: datetime = None,
    ):
        super().__init__(
            action=action,
            direction=direction,
            confidence_level=confidence_level,
            strategy=strategy,
            root_candle_timestamp=root_candle_timestamp,
            parameters=parameters,
        )
        self.assets = assets
        self.clock = clock
        if generation_time is not None:
            self.generation_time = generation_time
        # elif self.clock is not None:
        #     self.generation_time = self.clock.current_time(asset=asset)
        else:
            self.generation_time = datetime.now(timezone.utc)
        self.time_in_force = time_in_force
        self.signal_id = (
            "-".join(self.assets) + "-" + strategy + "-" + str(root_candle_timestamp)
        )

    @staticmethod
    def from_serializable_dict(signal_dict: dict) -> "Signal":
        signal: Signal = Signal(
            asset=signal_dict["asset"],
            action=Action[signal_dict["action"]],
            direction=Direction[signal_dict["direction"]],
            confidence_level=float(signal_dict["confidence_level"]),
            strategy=signal_dict["strategy"],
            root_candle_timestamp=signal_dict["root_candle_timestamp"],
            parameters=signal_dict["parameters"],
            generation_time=signal_dict["generation_time"],
            time_in_force=parse_timedelta_str(signal_dict["time_in_force"]),
        )
        return signal

    def to_serializable_dict(self) -> dict:
        dict = self.__dict__.copy()
        dict["action"] = dict["action"].name
        dict["direction"] = dict["direction"].name
        dict["confidence_level"] = str(dict["confidence_level"])
        dict["root_candle_timestamp"] = str(dict["root_candle_timestamp"])
        dict["generation_time"] = str(dict["generation_time"])
        dict["time_in_force"] = str(dict["time_in_force"])
        del dict["signal_id"]
        del dict["clock"]
        return dict

    @property
    def expiration_time(self) -> datetime:
        return self.generation_time + self.time_in_force

    def in_force(self, timestamp=None) -> bool:
        if self.clock is None:
            return True
        if timestamp is None:
            timestamp = self.clock.current_time(asset=self.asset)
        return self.expiration_time > timestamp
