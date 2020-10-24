from collections import deque
from typing import Callable

from rx.subject import Subject

from candles_queue.candles_queue import CandlesQueue


class SimpleQueue(CandlesQueue):
    def __init__(self, queue_name: str):
        super().__init__(queue_name)
        self.subject = Subject()
        self.queue: deque = deque()

    def add_consumer_helper(self, handle_new_elt: Callable[[], None]):
        # process previous elt in queue
        while self.size() > 0:
            handle_new_elt()

        self.subject.subscribe(lambda _: handle_new_elt())

    def add_consumer_no_retry(self, callback: Callable[[str], None]) -> None:
        def handle_new_elt():
            callback(self.queue.pop())

        self.add_consumer_helper(handle_new_elt)

    def add_consumer(self, callback: Callable[[str], None]) -> None:
        def handle_new_elt():
            try:
                queue_elt = self.queue.pop()
                callback(queue_elt)
            except:
                self.queue.append(queue_elt)
                self.subject.on_next(queue_elt)

        self.add_consumer_helper(handle_new_elt)

    def push(self, queue_elt: str):
        self.queue.appendleft(queue_elt)
        self.subject.on_next(queue_elt)

    def flush(self):
        self.queue.clear()

    def size(self):
        return len(self.queue)
