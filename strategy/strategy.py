import abc
from enum import Enum, auto
from .action import Action, PositionType, ActionType
from common.candle import *


class StrategyName(Enum):
    SMA_CROSSOVER = auto()


class Strategy:
    def __init__(self, name: str):
        self.is_opened = False
        self.name = name

    @abc.abstractmethod
    def compute_action(self, candle) -> Action:
        raise NotImplementedError

    def process_candle(self, candle: Candle):
        action = self.compute_action(candle)
        action.save()


class SmaCrossoverStrategy(Strategy):
    def __init__(self):
        super().__init__(StrategyName.SMA_CROSSOVER.name)

    def compute_action(self, candle: Candle) -> Action:
        computed_position = PositionType.NONE
        computed_action = ActionType.WAIT

        action = Action(self.name,
                        candle.symbol,
                        0.0,
                        computed_action,
                        computed_position)
        return action

