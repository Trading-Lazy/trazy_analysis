import random

from actionsapi.models import Candle, Action, PositionType, ActionType
from strategy.strategy import Strategy, StrategyName


class SellAndBuyShortStrategy(Strategy):
    def __init__(self):
        super().__init__(StrategyName.SELL_AND_BUY_SHORT_STRATEGY.name)

    def init_default_parameters(self):
        pass

    def compute_action(self, candle: Candle) -> Action:
        computed_position = PositionType.SHORT
        computed_action = ActionType.BUY if self.is_opened else ActionType.SELL
        self.is_opened = not self.is_opened

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
