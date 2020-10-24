from decimal import Decimal

from models.candle import Candle
from models.enums import Action, Direction
from models.signal import Signal
from strategy.strategy import Strategy


class SellAndBuyShortStrategy(Strategy):
    def init_default_parameters(self):
        pass

    def generate_signal(self, candle: Candle) -> Signal:
        generated_direction = Direction.SHORT
        generated_action = Action.BUY if self.is_opened else Action.SELL
        self.is_opened = not self.is_opened

        signal = Signal(
            action=generated_action,
            direction=generated_direction,
            confidence_level=Decimal("1.0"),
            strategy=self.name,
            symbol=candle.symbol,
            root_candle_timestamp=candle.timestamp,
            parameters={},
        )
        return signal
