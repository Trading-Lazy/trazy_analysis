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
    WAIT = auto()


class PositionType(Enum):
    LONG = auto()
    SHORT = auto()
    NONE = auto()


class Action:
    def __init__(self,
                 strategy: str,
                 symbol: str,
                 confidence_level: float,
                 action_type: ActionType = ActionType.WAIT,
                 position_type: PositionType = PositionType.NONE,
                 timestamp: datetime = datetime.now()):
        self.strategy = strategy
        self.symbol = symbol
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
        last_action = db['actionsapi_action'].find_one(sort=[("id", -1)])
        new_id = 1
        if not last_action is None:
            new_id = (last_action['id']) + 1
        key = {'timestamp': self.timestamp,
               'symbol': self.symbol,
               'strategy': self.strategy}
        db['actionsapi_action'].update(key, self.serialize_to_save(new_id), upsert=True)
        LOG.info("New action has been saved!")
