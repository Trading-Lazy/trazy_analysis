import abc
from typing import Callable


class CandlesQueue:
    def __init__(self, queue_name: str):
        self.queue_name = queue_name

    @abc.abstractmethod
    def add_consumer(self, callback: Callable[[str], None]) -> None:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def add_consumer_with_ack(
        self, callback: Callable[[str], None]
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
