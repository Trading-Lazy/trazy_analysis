import random
from decimal import Decimal

from models.order import Order
from models.candle import Candle
from models.enums import Action, Direction
from models.signal import Signal
from strategy.strategy import Strategy


class DumbShortStrategy(Strategy):
    def generate_signal(self, candle: Candle) -> Order:
        sell = bool(random.getrandbits(1))
        if sell and self.is_opened or not sell and not self.is_opened:
            return None
        self.is_opened = sell

        generated_direction = Direction.SHORT
        generated_action = Action.SELL if sell else Action.BUY

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

    def init_default_parameters(self):
        pass
