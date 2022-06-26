from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from sortedcontainers import SortedSet

from trazy_analysis.models.asset import Asset
from trazy_analysis.models.enums import EventType
from trazy_analysis.models.signal import SignalBase


class Event:
    def __init__(self, event_type: EventType):
        self.event_type = event_type


class AssetSpecificEvent(Event):
    def __init__(self, event_type: EventType, asset: str, bars_delay: int = 0):
        super().__init__(event_type)
        self.asset = asset
        self.bars_delay = bars_delay


class DataEvent(Event):
    def __init__(
        self,
        event_type: EventType,
        assets: Dict[Asset, List[timedelta]],
        timestamp: datetime,
        bars_delay: int = 0,
    ):
        super().__init__(event_type)
        self.assets = set(assets)
        self.timestamp = timestamp
        self.bars_delay = bars_delay


class MarketDataEvent(DataEvent):
    def __init__(
        self,
        candles: Dict[Asset, Dict[timedelta, SortedSet]],
        timestamp: datetime,
        bars_delay: int = 0,
    ):
        super().__init__(
            EventType.MARKET_DATA,
            {asset: list(candles[asset].keys()) for asset in candles},
            timestamp,
            bars_delay,
        )
        self.candles: Dict[Asset, Dict[timedelta, SortedSet]] = candles


class MarketEodDataEvent(DataEvent):
    def __init__(
        self,
        assets: Dict[Asset, List[timedelta]],
        timestamp: datetime,
        bars_delay: int = 0,
    ):
        super().__init__(EventType.MARKET_EOD_DATA, assets, timestamp, bars_delay)


class MarketDataEndEvent(DataEvent):
    def __init__(
        self,
        assets: Dict[Asset, List[timedelta]],
        timestamp: datetime,
        bars_delay: int = 0,
    ):
        super().__init__(EventType.MARKET_DATA_END, assets, timestamp, bars_delay)


class SignalEvent(Event):
    def __init__(self, signals: List[SignalBase], bars_delay: int = 0):
        super().__init__(EventType.SIGNAL)
        self.signals = signals
        self.bars_delay = bars_delay


class OpenOrdersEvent(AssetSpecificEvent):
    def __init__(self, asset: str, bars_delay: int = 0):
        super().__init__(EventType.OPEN_ORDERS, asset, bars_delay)


class PendingSignalEvent(Event):
    def __init__(self, bars_delay: int = 0):
        super().__init__(EventType.PENDING_SIGNAL)
        self.bars_delay = bars_delay
