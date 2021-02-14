import abc
from datetime import datetime, timezone
from typing import Dict


class Clock:
    @abc.abstractmethod
    def current_time(self, tz="UTC", symbol: str = "") -> datetime:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def update_time(self, symbol: str, timestamp: datetime) -> None:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def update_bars(self, symbol: str) -> None:  # pragma: no cover
        raise NotImplementedError

    def update(self, symbol: str, timestamp: datetime):
        self.update_bars(symbol)
        self.update_time(symbol, timestamp)

    @abc.abstractmethod
    def bars(self, symbol) -> int:  # pragma: no cover
        raise NotImplementedError


class LiveClock(Clock):
    def current_time(self, tz=timezone.utc, symbol: str = "") -> datetime:
        return datetime.now(tz=tz)

    def update_time(self, symbol: str, timestamp: datetime) -> None:  # pragma: no cover
        pass

    def update_bars(self, symbol: str) -> None:  # pragma: no cover
        pass

    def bars(self, symbol) -> int:  # pragma: no cover
        return 0


class SimulatedClock(Clock):
    def __init__(self) -> None:
        self.time_dict: Dict[str, datetime] = {}
        self.bars_dict: Dict[str, int] = {}

    def update_time(self, symbol: str, timestamp: datetime) -> None:
        self.time_dict[symbol] = timestamp

    def update_bars(self, symbol: str) -> None:
        if symbol not in self.bars_dict:
            self.bars_dict[symbol] = 1
        else:
            self.bars_dict[symbol] += 1

    def current_time(self, tz=timezone.utc, symbol: str = "") -> datetime:
        if symbol not in self.time_dict:
            self.time_dict[symbol] = datetime.now(timezone.utc)
        return self.time_dict[symbol]

    def bars(self, symbol) -> int:
        if symbol not in self.bars_dict:
            return 0
        return self.bars_dict[symbol]
