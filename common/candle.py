class Candle:
    def __init__(self, symbol=None, open=None, high=None, low=None, close=None, volume=None, timestamp=None):
        self.symbol: str = symbol
        self.open: float = open
        self.high: float = high
        self.low: float = low
        self.close: float = close
        self.volume: int = volume
        self.timestamp = timestamp

    def set_from_dict(self, dict_candle):
        self.symbol = dict_candle['symbol']
        self.open = dict_candle['open']
        self.high = dict_candle['high']
        self.low = dict_candle['low']
        self.close = dict_candle['close']
        self.volume = dict_candle['volume']
        self.timestamp = dict_candle['timestamp']