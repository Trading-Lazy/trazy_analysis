import abc
import os
from enum import Enum, auto

from pymongo import MongoClient

import logger
import settings
from actionsapi.models import Action, Candle
from common.exchange_calendar_euronext import EuronextExchangeCalendar

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, 'output.log'))

m_client = MongoClient(settings.DB_CONN)
db = m_client['djongo_connection']
euronext_cal = EuronextExchangeCalendar()


class StrategyName(Enum):
    SMA_CROSSOVER = auto()
    DUMB_LONG_STRATEGY = auto()
    DUMB_SHORT_STRATEGY = auto()
    BUY_AND_SELL_LONG_STRATEGY = auto()
    SELL_AND_BUY_SHORT_STRATEGY = auto()


class Strategy:
    def __init__(self, name: str):
        self.is_opened = False
        self.name = name
        self.__parameters = self.init_default_parameters()

    @abc.abstractmethod
    def compute_action(self, candle) -> Action:
        raise NotImplementedError

    @abc.abstractmethod
    def init_default_parameters(self):
        raise NotImplementedError

    def process_candle(self, candle: Candle):
        action = self.compute_action(candle)
        if action is not None:
            action.save()


