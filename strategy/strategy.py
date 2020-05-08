import abc

from .action import Action


class Strategy:
    def __init__(self, name: str):
        self.is_opened = False
        self.name = name

    @abc.abstractmethod
    def compute_action(self, candle) -> Action:
        raise NotImplementedError

    def process_candle(self, candle):
        action = self.compute_action(candle)
        action.save()
