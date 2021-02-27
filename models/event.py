from models.candle import Candle
from models.enums import EventType
from models.signal import Signal


class Event:
    def __init__(self, type: EventType):
        self.type = type


class SymbolSpecificEvent(Event):
    def __init__(self, type: EventType, symbol: str, bars_delay: int = 0):
        super().__init__(type)
        self.bars_delay = bars_delay
        self.symbol = symbol


class MarketDataEvent(SymbolSpecificEvent):
    def __init__(self, candle: Candle, bars_delay: int = 0):
        super().__init__(EventType.MARKET_DATA, candle.symbol, bars_delay)
        self.candle: Candle = candle


class MarketEodDataEvent(SymbolSpecificEvent):
    def __init__(self, symbol: str, bars_delay: int = 0):
        super().__init__(EventType.MARKET_EOD_DATA, symbol, bars_delay)


class MarketDataEndEvent(SymbolSpecificEvent):
    def __init__(self, symbol: str, bars_delay: int = 0):
        super().__init__(EventType.MARKET_DATA_END, symbol, bars_delay)


class SignalEvent(SymbolSpecificEvent):
    def __init__(self, signal: Signal, bars_delay: int = 0):
        super().__init__(EventType.SIGNAL, signal.symbol, bars_delay)
        self.signal = signal


class OpenOrdersEvent(SymbolSpecificEvent):
    def __init__(self, symbol: str, bars_delay: int = 0):
        super().__init__(EventType.OPEN_ORDERS, symbol, bars_delay)


class PendingSignalEvent(SymbolSpecificEvent):
    def __init__(self, symbol: str, bars_delay: int = 0):
        super().__init__(EventType.PENDING_SIGNAL, symbol, bars_delay)
