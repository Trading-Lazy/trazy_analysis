from datetime import datetime, timedelta, timezone

from common.clock import Clock
from common.helper import parse_timedelta_str
from models.enums import Action, Direction
from models.utils import is_closed_position


class Signal:
    def __init__(
        self,
        symbol: str,
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
        self.symbol = symbol
        self.action = action
        self.direction = direction
        self.confidence_level = confidence_level
        self.strategy = strategy
        self.root_candle_timestamp = root_candle_timestamp
        self.parameters = parameters
        self.clock = clock
        if generation_time is not None:
            self.generation_time = generation_time
        elif self.clock is not None:
            self.generation_time = self.clock.current_time(symbol=symbol)
        else:
            self.generation_time = datetime.now(timezone.utc)
        self.time_in_force = time_in_force
        self.signal_id = symbol + "-" + strategy + "-" + str(root_candle_timestamp)

    @staticmethod
    def from_serializable_dict(signal_dict: dict) -> "Signal":
        signal: Signal = Signal(
            symbol=signal_dict["symbol"],
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
            timestamp = self.clock.current_time(symbol=self.symbol)
        return self.expiration_time > timestamp

    @property
    def is_entry_signal(self):
        return not self.is_exit_signal

    @property
    def is_exit_signal(self):
        return is_closed_position(self.action, self.direction)

    def __eq__(self, other):
        if isinstance(other, Signal):
            return self.to_serializable_dict() == other.to_serializable_dict()
        return False

    def __ne__(self, other):
        return not self.__eq__(other)
