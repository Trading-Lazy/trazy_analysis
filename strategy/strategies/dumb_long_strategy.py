import random

from models.action import Action
from models.candle import Candle
from models.enums import ActionType, PositionType
from strategy.strategy import Strategy


class DumbLongStrategy(Strategy):
    def compute_action(self, candle: Candle) -> Action:
        buy = bool(random.getrandbits(1))
        if buy and self.is_opened or not buy and not self.is_opened:
            return None
        self.is_opened = buy

        computed_position = PositionType.LONG
        computed_action = ActionType.BUY if buy else ActionType.SELL

        action = Action(
            action_type=computed_action,
            position_type=computed_position,
            size=1,
            confidence_level=1,
            strategy=self.name,
            symbol=candle.symbol,
            candle_id=candle._id,
            parameters={},
        )
        return action

    def init_default_parameters(self):
        pass
