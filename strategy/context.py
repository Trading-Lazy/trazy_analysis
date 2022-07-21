import os
from collections import deque
from datetime import timedelta
from typing import Dict, List

import trazy_analysis.logger
import trazy_analysis.settings
from trazy_analysis.broker.broker_manager import BrokerManager
from trazy_analysis.common.constants import MAX_TIMESTAMP
from trazy_analysis.common.helper import get_or_create_nested_dict
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.event import Event
from trazy_analysis.order_manager.order_manager import OrderManager

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
)


# > The Context class is a container for the state of the application
class Context:
    # A constant that is used to update the last prices.
    UPDATE_LAST_PRICES_PERIOD = timedelta(seconds=10)

    def __init__(
        self,
        assets: dict[Asset, list[timedelta]],
        order_manager: OrderManager,
        broker_manager: BrokerManager,
        events: deque,
    ):
        """
        It creates a dictionary of dictionaries of deques

        :param assets: dict[Asset, list[timedelta]]
        :type assets: dict[Asset, list[timedelta]]
        """
        self.candles: dict[Asset, dict[timedelta, deque]] = {
            asset: {time_unit: deque() for time_unit in assets[asset]}
            for asset in assets
        }
        self.last_candles: dict[Asset, dict[timedelta, Candle]] = {}
        self.order_manager = order_manager
        self.broker_manager = broker_manager
        self.events = events
        self.current_timestamp = MAX_TIMESTAMP

    def add_candle(self, candle: Candle) -> None:
        """
        It adds a candle to the candles dictionary, which is a nested dictionary

        :param candle: The candle to add to the data feed
        :type candle: Candle
        """
        get_or_create_nested_dict(self.candles, candle.asset)
        if candle.time_unit not in self.candles[candle.asset]:
            self.candles[candle.asset][candle.time_unit] = deque()
        self.candles[candle.asset][candle.time_unit].append(candle)
        self.current_timestamp = min(self.current_timestamp, candle.timestamp)

    def get_last_candles(self) -> list[Candle]:
        """
        It returns a list of all the last candles for all assets and time units
        :return: A list of candles.
        """
        return [
            self.last_candles[asset][time_unit]
            for asset in self.last_candles
            for time_unit in self.last_candles[asset]
        ]

    def get_last_candle(self, asset: Asset, time_unit: timedelta) -> Candle:
        """
        If the asset is in the last_candles dictionary and the time_unit is in the last_candles[asset] dictionary, return
        the last_candles[asset][time_unit] value

        :param asset: The asset you want to get the last candle for
        :type asset: Asset
        :param time_unit: The time unit of the candle
        :type time_unit: timedelta
        :return: The last candle for the given asset and time_unit.
        """
        if asset in self.last_candles and time_unit in self.last_candles[asset]:
            return self.last_candles[asset][time_unit]

    def update(self) -> None:
        """
        It takes the first candle from each time unit for each asset, and if it's timestamp is greater than the current
        timestamp, it sets the current timestamp to the candle's timestamp, and sets the last candle for that asset and time
        unit to the candle
        """
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

    def add_event(self, event: Event) -> None:
        self.events.append(event)
