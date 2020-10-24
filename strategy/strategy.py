import abc
import os
import logger
import settings
from broker.broker import Broker
from common.exchange_calendar_euronext import EuronextExchangeCalendar
from db_storage.db_storage import DbStorage
from models.order import Order
from models.candle import Candle
from models.signal import Signal
from order_manager.order_management import OrderManager

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)

euronext_cal = EuronextExchangeCalendar()


class Strategy:
    def __init__(self, symbol: str, db_storage: DbStorage, order_manager: OrderManager):
        self.symbol = symbol
        self.db_storage = db_storage
        self.order_manager = order_manager
        self.is_opened = False
        self.name = self.__class__.__name__

    @abc.abstractmethod
    def generate_signal(self, candle) -> Signal:  # pragma: no cover
        raise NotImplementedError

    def process_candle(self, candle: Candle):
        signal = self.generate_signal(candle)
        if signal is not None:
            self.order_manager.push(signal)
