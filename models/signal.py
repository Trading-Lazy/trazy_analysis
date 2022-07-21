from abc import abstractmethod
from datetime import datetime, timedelta
from typing import TypeVar

import numpy as np
import pytz

from trazy_analysis.common.clock import Clock
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.enums import Action, Direction
from trazy_analysis.models.utils import is_closed_position

TSignalBase = TypeVar("TSignalBase", bound="SignalBase")

class SignalBase:
    def __init__(
        self,
        strategy: str = None,
        parameters: dict = None,
        time_in_force: timedelta = timedelta(minutes=5),
    ):
        self.strategy = strategy
        self.parameters = parameters
        self.time_in_force = time_in_force

    @abstractmethod
    def from_serializable_dict(signal_dict: dict) -> TSignalBase:
        raise NotImplementedError("Should implement from_serializable_dict()")

    @abstractmethod
    def to_serializable_dict(self) -> dict:
        raise NotImplementedError("Should implement to_serializable_dict()")

    @abstractmethod
    def in_force(self, timestamp=None) -> bool:
        raise NotImplementedError("Should implement in_force()")

    @property
    @abstractmethod
    def expiration_time(self) -> datetime:
        raise NotImplementedError("Should implement expiration_time()")

    def __eq__(self, other):
        if isinstance(other, SignalBase):
            return self.to_serializable_dict() == other.to_serializable_dict()
        return False

    def __ne__(self, other):
        return not self.__eq__(other)


TSignal = TypeVar("TSignal", bound="Signal")


class Signal(SignalBase):
    def __init__(
        self,
        asset: Asset,
        time_unit: timedelta,
        action: Action,
        direction: Direction,
        confidence_level: float = None,
        strategy: str = None,
        root_candle_timestamp: datetime = None,
        parameters: dict = None,
        clock: Clock = None,
        time_in_force: timedelta = timedelta(minutes=5),
        generation_time: datetime = None,
    ):
        self.action = action
        self.direction = direction
        self.confidence_level = confidence_level
        super().__init__(
            strategy=strategy, parameters=parameters, time_in_force=time_in_force
        )
        self.root_candle_timestamp = root_candle_timestamp
        self.asset = asset
        self.time_unit = time_unit
        self.clock = clock
        if generation_time is not None:
            self.generation_time = generation_time
        elif self.clock is not None:
            self.generation_time = self.clock.current_time()
        else:
            self.generation_time = datetime.now(pytz.UTC)
        self.signal_id = None
        if strategy is not None and root_candle_timestamp is not None:
            self.signal_id = (
                asset.key() + "-" + strategy + "-" + str(root_candle_timestamp)
            )

    def enrich_signal(
        self,
        strategy: str = None,
        root_candle_timestamp: datetime = None,
        parameters: dict = None,
        clock: Clock = None,
        generation_time: datetime = None,
        confidence_level: float = None,
    ):
        self.strategy = strategy
        self.confidence_level = confidence_level
        self.root_candle_timestamp = root_candle_timestamp
        self.parameters = parameters
        self.clock = clock
        if generation_time is not None:
            self.generation_time = generation_time
        if self.clock is not None:
            self.generation_time = self.clock.current_time()
        else:
            self.generation_time = datetime.now(pytz.UTC)
        if self.strategy is not None and root_candle_timestamp is not None:
            self.signal_id = (
                self.asset.key() + "-" + strategy + "-" + str(root_candle_timestamp)
            )

    @staticmethod
    def from_serializable_dict(signal_dict: dict) -> TSignal:
        from trazy_analysis.common.helper import parse_timedelta_str

        signal: Signal = Signal(
            asset=Asset.from_dict(signal_dict["asset"]),
            time_unit=parse_timedelta_str(signal_dict["time_unit"]),
            action=Action[signal_dict["action"]],
            direction=Direction[signal_dict["direction"]],
            confidence_level=float(signal_dict["confidence_level"]),
            strategy=signal_dict["strategy"],
            root_candle_timestamp=signal_dict["root_candle_timestamp"],
            parameters=signal_dict["parameters"],
            time_in_force=parse_timedelta_str(signal_dict["time_in_force"]),
            generation_time=signal_dict["generation_time"],
        )
        return signal

    def to_serializable_dict(self) -> dict:
        signal_dict = self.__dict__.copy()
        signal_dict["asset"] = signal_dict["asset"].to_dict()
        signal_dict["time_unit"] = str(signal_dict["time_unit"])
        signal_dict["action"] = signal_dict["action"].name
        signal_dict["direction"] = signal_dict["direction"].name
        signal_dict["confidence_level"] = str(signal_dict["confidence_level"])
        signal_dict["root_candle_timestamp"] = str(signal_dict["root_candle_timestamp"])
        signal_dict["generation_time"] = str(signal_dict["generation_time"])
        signal_dict["time_in_force"] = str(signal_dict["time_in_force"])
        del signal_dict["signal_id"]
        del signal_dict["clock"]
        return signal_dict

    @property
    def expiration_time(self) -> datetime:
        return self.generation_time + self.time_in_force

    def in_force(self, timestamp=None) -> bool:
        if self.clock is None:
            return True
        if timestamp is None:
            timestamp = self.clock.current_time()
        return self.expiration_time > timestamp

    @property
    def is_entry_signal(self):
        return not self.is_exit_signal

    @property
    def is_exit_signal(self):
        return is_closed_position(self.action, self.direction)


class MultipleSignal(SignalBase):
    def __init__(
        self,
        signals: np.array,  # [SignalBase]
        strategy: str = None,
        parameters: dict = None,
        time_in_force: str = timedelta(minutes=5),
    ):
        self.signals = signals
        super().__init__(strategy, parameters, time_in_force)


class ArbitragePairSignal(MultipleSignal):
    def __init__(
        self,
        buy_signal: Signal,
        sell_signal: Signal,
        strategy: str = None,
        parameters: dict = None,
    ):
        if buy_signal.asset.symbol != sell_signal.asset.symbol:
            raise Exception(
                f"buy signal asset symbol {buy_signal.asset.symbol} should be the same as sell signal "
                f"asset symbol {sell_signal.asset.symbol}"
            )
        if buy_signal.action != Action.BUY:
            raise Exception(
                f"buy signal action should be BUY not {buy_signal.action.name}"
            )
        if sell_signal.action != Action.SELL:
            raise Exception(
                f"sell signal action should be SELL not {sell_signal.action.name}"
            )
        self.buy_signal = buy_signal
        self.sell_signal = sell_signal
        signals = np.array([buy_signal, sell_signal], dtype=Signal)
        super().__init__(signals=signals, strategy=strategy, parameters=parameters)
