import os
import traceback
from typing import Callable

import trazy_analysis.settings
from trazy_analysis.candles_queue.candles_queue import CandlesQueue
from trazy_analysis.logger import logger
from trazy_analysis.models.candle import Candle

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
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

    def reset(self):
        self.consumer_callbacks = []
        self.on_complete_callbacks = {}

    def flush(self):
        pass

    def size(self):
        return 0
