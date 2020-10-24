import random
from decimal import Decimal

from models.order import Order
from models.candle import Candle
from models.enums import Action, Direction
from models.signal import Signal
from strategy.strategy import Strategy


class DumbLongStrategy(Strategy):
    def generate_signal(self, candle: Candle) -> Order:
        buy = bool(random.getrandbits(1))
        if buy and self.is_opened or not buy and not self.is_opened:
            return None
        self.is_opened = buy

        generated_direction = Direction.LONG
        generated_action = Action.BUY if buy else Action.SELL

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
