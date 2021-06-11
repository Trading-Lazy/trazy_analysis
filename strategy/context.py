import os
from collections import deque
from datetime import timedelta
from typing import Dict, List

import logger
import settings
from common.constants import MAX_TIMESTAMP
from models.asset import Asset
from models.candle import Candle

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)


class Context:
    UPDATE_LAST_PRICES_PERIOD = timedelta(seconds=10)

    def __init__(self, assets: List[Asset]):
        self.candles: Dict[Asset, deque] = {asset: deque() for asset in assets}
        self.last_candles: Dict[Asset] = {}
        self.current_timestamp = MAX_TIMESTAMP

    def add_candle(self, candle: Candle) -> None:
        if candle.asset not in self.candles:
            self.candles[candle.asset] = deque()
        self.candles[candle.asset].append(candle)
        self.current_timestamp = min(self.current_timestamp, candle.timestamp)

    def get_last_candles(self) -> List[Candle]:
        return [self.last_candles[asset] for asset in self.last_candles]

    def get_last_candle(self, asset: Asset) -> Candle:
        return self.last_candles[asset]

    def update(self) -> List[Candle]:
        self.last_candles = {}
        current_timestamp = self.current_timestamp
        next_timestamp = MAX_TIMESTAMP
        assets = list(self.candles.keys())
        for asset in assets:
            if asset not in self.candles or len(self.candles[asset]) == 0:
                continue
            first_candle = self.candles[asset].popleft()
            if first_candle.timestamp == current_timestamp:
                self.last_candles[first_candle.asset] = first_candle
            if len(self.candles[asset]) != 0:
                next_timestamp = min(next_timestamp, self.candles[asset][0])
                del self.candles[asset]
        self.current_timestamp = next_timestamp
