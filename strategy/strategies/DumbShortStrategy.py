import random

from actionsapi.models import Candle, Action, PositionType, ActionType
from strategy.strategy import Strategy, StrategyName


class DumbShortStrategy(Strategy):
    def __init__(self):
        super().__init__(StrategyName.DUMB_SHORT_STRATEGY.name)

    def compute_action(self, candle: Candle) -> Action:
        sell = bool(random.getrandbits(1))
        if sell and self.is_opened or not sell and not self.is_opened:
            return None
        self.is_opened = sell

        computed_position = PositionType.SHORT
        computed_action = ActionType.SELL if sell else ActionType.BUY

        action = Action(
            action_type=computed_action,
            position_type=computed_position,
            amount=1,
            confidence_level=1,
            strategy=self.name,
            symbol=candle.symbol,
            candle_id=candle._id,
            parameters={},
        )
        return action
