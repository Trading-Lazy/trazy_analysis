import os
from collections import deque
from datetime import timedelta
from typing import Dict, List

import trazy_analysis.logger
import trazy_analysis.settings
from trazy_analysis.common.constants import MAX_TIMESTAMP
from trazy_analysis.common.helper import get_or_create_nested_dict
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
)


class Context:
    UPDATE_LAST_PRICES_PERIOD = timedelta(seconds=10)

    def __init__(self, assets: Dict[Asset, List[timedelta]]):
        self.candles: Dict[Asset, Dict[timedelta, deque]] = {
            asset: {time_unit: deque() for time_unit in assets[asset]}
            for asset in assets
        }
        self.last_candles: Dict[Asset, Dict[timedelta, Candle]] = {}
        self.current_timestamp = MAX_TIMESTAMP

    def add_candle(self, candle: Candle) -> None:
        get_or_create_nested_dict(self.candles, candle.asset)
        if candle.time_unit not in self.candles[candle.asset]:
            self.candles[candle.asset][candle.time_unit] = deque()
        self.candles[candle.asset][candle.time_unit].append(candle)
        self.current_timestamp = min(self.current_timestamp, candle.timestamp)

    def get_last_candles(self) -> List[Candle]:
        return [
            self.last_candles[asset][time_unit]
            for asset in self.last_candles
            for time_unit in self.last_candles[asset]
        ]

    def get_last_candle(self, asset: Asset, time_unit: timedelta) -> Candle:
        if asset in self.last_candles and time_unit in self.last_candles[asset]:
            return self.last_candles[asset][time_unit]

    def update(self) -> None:
        self.last_candles = {}
        current_timestamp = self.current_timestamp
        min_timestamp = next_timestamp = MAX_TIMESTAMP
        candles = {asset: list(self.candles[asset]) for asset in self.candles}
        for asset in candles:
            get_or_create_nested_dict(self.last_candles, asset)
            for time_unit in candles[asset]:
                if len(self.candles[asset][time_unit]) == 0:
                    continue
                first_candle = self.candles[asset][time_unit].popleft()
                if first_candle.timestamp >= current_timestamp:
                    min_timestamp = min(min_timestamp, first_candle.timestamp)
                    self.last_candles[asset][time_unit] = first_candle
                if len(self.candles[asset][time_unit]) != 0:
                    next_timestamp = min(
                        next_timestamp, self.candles[asset][time_unit][0].timestamp
                    )
                else:
                    del self.candles[asset][time_unit]
            if len(self.candles[asset]) == 0:
                del self.candles[asset]
        if min_timestamp != MAX_TIMESTAMP:
            self.current_timestamp = min_timestamp
