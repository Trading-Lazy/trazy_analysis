import os
import traceback
from typing import Callable

import settings
from candles_queue.candles_queue import CandlesQueue
from logger import logger
from models.candle import Candle

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)


class FakeQueue(CandlesQueue):
    def __init__(self, queue_name: str):
        super().__init__(queue_name)
        self.consumer_callbacks = []

    def add_consumer_helper(self, callback: Callable[[], None], retry=True):
        self.consumer_callbacks.append((callback, retry))

    def add_consumer_no_retry(self, callback: Callable[[Candle], None]) -> None:
        self.add_consumer_helper(callback, retry=False)

    def add_consumer(self, callback: Callable[[str], None]) -> None:
        self.add_consumer_helper(callback)

    def push(self, queue_elt: Candle):
        for callback, retry in self.consumer_callbacks:
            if not retry:
                callback(queue_elt)
            else:
                done = False
                while not done:
                    try:
                        callback(queue_elt)
                        done = True
                    except Exception as e:
                        LOG.error(
                            "Exception %s, Traceback %s, Will reenqueue candle:",
                            e,
                            traceback.format_exc(),
                        )

    def flush(self):
        pass

    def size(self):
        return 0
