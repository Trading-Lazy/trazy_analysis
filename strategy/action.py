from enum import Enum, auto
from datetime import datetime
import os
from pymongo import MongoClient
import settings
import logger

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, 'output.log'))

m_client = MongoClient(settings.DB_CONN)


class ActionType(Enum):
    BUY = auto()
    SELL = auto()


class PositionType(Enum):
    LONG = auto()
    SHORT = auto()


class Action:
    def __init__(self,
                 strategy: str,
                 symbol: str,
                 market_price: float,
                 confidence_level: float,
                 action_type: ActionType = ActionType.WAIT,
                 position_type: PositionType = PositionType.NONE,
                 timestamp: datetime = datetime.now()):
        self.strategy = strategy
        self.symbol = symbol
        self.market_price = market_price
        self.confidence_level = confidence_level
        self.action_type = action_type
        self.position_type = position_type
        self.timestamp = timestamp

    def serialize(self):
        d = self.__dict__
        d['action_type'] = self.action_type.name
        d['position_type'] = self.position_type.name
        return d

    def serialize_to_save(self, id):
        d = self.serialize()
        d['id'] = id
        return d

    def save(self):
        db = m_client['djongo_connection']
        new_id = int(db['actions_gen_action'].find_one(sort=[("id", -1)])['id']) + 1
        key = {'timestamp': self.timestamp,
               'symbol': self.symbol,
               'strategy': self.strategy}
        db['actions_gen_action'].update(key, self.serialize_to_save(new_id), upsert=True)
        LOG.info("New action has been saved!")
