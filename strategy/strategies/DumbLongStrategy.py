import random

from actionsapi.models import Candle, Action, PositionType, ActionType
from strategy.strategy import Strategy, StrategyName


class DumbLongStrategy(Strategy):
    def __init__(self):
        super().__init__(StrategyName.DUMB_LONG_STRATEGY.name)

    def compute_action(self, candle: Candle) -> Action:
        buy = bool(random.getrandbits(1))
        if buy and self.is_opened or not buy and not self.is_opened:
            return None
        self.is_opened = buy

        computed_position = PositionType.LONG
        computed_action = ActionType.BUY if buy else ActionType.SELL

        action = Action(
            action_type = computed_action,
            position_type = computed_position,
            amount = 1,
            confidence_level = 1,
            strategy = self.name,
            symbol = candle.symbol,
            candle_id = candle.id,
            parameters = {}
        )
        return action