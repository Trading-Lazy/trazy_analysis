from models.action import Action
from models.candle import Candle
from models.enums import ActionType, PositionType
from strategy.strategy import Strategy


class SellAndBuyShortStrategy(Strategy):
    def init_default_parameters(self):
        pass

    def compute_action(self, candle: Candle) -> Action:
        computed_position = PositionType.SHORT
        computed_action = ActionType.BUY if self.is_opened else ActionType.SELL
        self.is_opened = not self.is_opened

        action = Action(
            action_type=computed_action,
            position_type=computed_position,
            size=1,
            confidence_level=1,
            strategy=self.name,
            symbol=candle.symbol,
            candle_timestamp=candle.timestamp,
            parameters={},
        )
        return action
