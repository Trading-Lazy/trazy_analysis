import abc
import os

import logger
import settings
from common.clock import Clock
from db_storage.db_storage import DbStorage
from indicators.indicators import IndicatorsManager
from indicators.stream import StreamData
from models.candle import Candle
from models.signal import Signal
from order_manager.order_manager import OrderManager

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)


class Strategy(StreamData):
    def __init__(
        self,
        symbol: str,
        db_storage: DbStorage,
        order_manager: OrderManager,
        indicators_manager: IndicatorsManager = IndicatorsManager(),
    ):
        super().__init__()
        self.symbol = symbol
        self.db_storage = db_storage
        self.order_manager = order_manager
        self.indicators_manager = indicators_manager
        self.is_opened = False
        self.name = self.__class__.__name__

    @abc.abstractmethod
    def generate_signal(self, candle) -> Signal:  # pragma: no cover
        raise NotImplementedError

    def process_candle(self, candle: Candle, clock: Clock):
        signal = self.generate_signal(candle, clock)
        if signal is not None:
            self.on_next(signal)
            return signal
