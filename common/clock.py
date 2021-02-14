import abc
from typing import Dict

import pandas as pd


class Clock:
    @abc.abstractmethod
    def current_time(
        self, tz="UTC", symbol: str = ""
    ) -> pd.Timestamp:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def update_time(
        self, symbol: str, timestamp: pd.Timestamp
    ) -> None:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def update_bars(self, symbol: str) -> None:  # pragma: no cover
        raise NotImplementedError

    def update(self, symbol: str, timestamp: pd.Timestamp):
        self.update_bars(symbol)
        self.update_time(symbol, timestamp)

    @abc.abstractmethod
    def bars(self, symbol) -> int:  # pragma: no cover
        raise NotImplementedError


class LiveClock(Clock):
    def current_time(self, tz="UTC", symbol: str = "") -> pd.Timestamp:
        return pd.Timestamp.now(tz=tz)

    def update_time(
        self, symbol: str, timestamp: pd.Timestamp
    ) -> None:  # pragma: no cover
        pass

    def update_bars(self, symbol: str) -> None:  # pragma: no cover
        pass

    def bars(self, symbol) -> int:  # pragma: no cover
        return 0


class SimulatedClock(Clock):
    def __init__(self) -> None:
        self.time_dict: Dict[str, pd.Timestamp] = {}
        self.bars_dict: Dict[str, int] = {}

    def update_time(self, symbol: str, timestamp: pd.Timestamp) -> None:
        self.time_dict[symbol] = timestamp

    def update_bars(self, symbol: str) -> None:
        if symbol not in self.bars_dict:
            self.bars_dict[symbol] = 1
        else:
            self.bars_dict[symbol] += 1

    def current_time(self, tz="UTC", symbol: str = "") -> pd.Timestamp:
        if symbol not in self.time_dict:
            self.time_dict[symbol] = pd.Timestamp.now(tz=tz)
        return self.time_dict[symbol]

    def bars(self, symbol) -> int:
        if symbol not in self.bars_dict:
            return 0
        return self.bars_dict[symbol]
