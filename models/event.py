from datetime import datetime
from typing import List, Dict

import numpy as np

from models.asset import Asset
from models.candle import Candle
from models.enums import EventType
from models.signal import Signal


class Event:
    def __init__(self, type: EventType):
        self.type = type


class AssetSpecificEvent(Event):
    def __init__(self, type: EventType, asset: str, bars_delay: int = 0):
        super().__init__(type)
        self.asset = asset
        self.bars_delay = bars_delay


class DataEvent(Event):
    def __init__(
        self,
        type: EventType,
        assets: List[Asset],
        timestamp: datetime,
        bars_delay: int = 0,
    ):
        super().__init__(type)
        self.assets = assets
        self.timestamp = timestamp
        self.bars_delay = bars_delay


class MarketDataEvent(DataEvent):
    def __init__(
        self, candles: Dict[Asset, np.array], timestamp: datetime, bars_delay: int = 0
    ):
        super().__init__(
            EventType.MARKET_DATA, list(candles.keys()), timestamp, bars_delay
        )
        self.candles: Dict[str, np.array] = candles


class MarketEodDataEvent(DataEvent):
    def __init__(self, assets: List[Asset], timestamp: datetime, bars_delay: int = 0):
        super().__init__(EventType.MARKET_EOD_DATA, assets, timestamp, bars_delay)


class MarketDataEndEvent(DataEvent):
    def __init__(self, assets: List[Asset], timestamp: datetime, bars_delay: int = 0):
        super().__init__(EventType.MARKET_DATA_END, assets, timestamp, bars_delay)


class SignalEvent(AssetSpecificEvent):
    def __init__(self, signal: Signal, bars_delay: int = 0):
        super().__init__(EventType.SIGNAL, signal.asset, bars_delay)
        self.signal = signal


class OpenOrdersEvent(AssetSpecificEvent):
    def __init__(self, asset: str, bars_delay: int = 0):
        super().__init__(EventType.OPEN_ORDERS, asset, bars_delay)


class PendingSignalEvent(AssetSpecificEvent):
    def __init__(self, asset: str, bars_delay: int = 0):
        super().__init__(EventType.PENDING_SIGNAL, asset, bars_delay)
