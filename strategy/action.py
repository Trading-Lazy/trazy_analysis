from enum import Enum, auto
from datetime import datetime
from pymongo import MongoClient
import settings

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

    def save(self):
        db = m_client['djongo_connection']
        key = {'timestamp': self.timestamp,
               'symbol': self.symbol,
               'strategy': self.strategy}
        db['actions_gen_action'].update(key, self.__dict__, upsert=True)

