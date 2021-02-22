import abc
from typing import Callable

from models.candle import Candle


class CandlesQueue:
    def __init__(self, queue_name: str):
        self.queue_name = queue_name
        self.on_complete_callbacks = {}

    @abc.abstractmethod
    def add_consumer_no_retry(
        self, callback: Callable[[Candle], None]
    ) -> None:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def add_consumer(
        self, callback: Callable[[Candle], None]
    ) -> None:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def push(self, queue_elt: str) -> None:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def flush(self) -> None:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def size(self) -> int:  # pragma: no cover
        raise NotImplementedError

    def complete(self, symbol: str):
        if symbol not in self.on_complete_callbacks:
            return
        for callback in self.on_complete_callbacks[symbol]:
            callback(symbol)

    def add_on_complete_callback(self, symbol: str, callback: Callable[[str], None]):
        if symbol not in self.on_complete_callbacks:
            self.on_complete_callbacks[symbol] = [callback]
        else:
            self.on_complete_callbacks[symbol].append(callback)
