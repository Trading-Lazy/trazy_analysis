import abc
import os
import logger
import settings
from broker.broker import Broker
from common.exchange_calendar_euronext import EuronextExchangeCalendar
from db_storage.db_storage import DbStorage
from models.action import Action
from models.candle import Candle

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)

euronext_cal = EuronextExchangeCalendar()


class Strategy:
    def __init__(self, symbol: str, db_storage: DbStorage, broker: Broker):
        self.symbol = symbol
        self.db_storage = db_storage
        self.broker = broker
        self.is_opened = False
        self.name = self.__class__.__name__

    @abc.abstractmethod
    def compute_action(self, candle) -> Action:  # pragma: no cover
        raise NotImplementedError

    def process_candle(self, candle: Candle):
        action = self.compute_action(candle)
        if action is not None:
            self.db_storage.add_action(action)
