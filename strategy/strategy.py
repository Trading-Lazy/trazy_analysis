from action import Action

class Strategy:
    def __init__(self):
        self.is_opened = False

    def compute_action(self, candle) -> Action:
        pass

    def store_action(self, a: Action):
        pass

    def process_candle(self, candle):
        a = self.compute_action(candle)
        self.store_action(a)