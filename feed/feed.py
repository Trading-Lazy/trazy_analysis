from collections import deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union

import numpy as np
import pytz
from pandas_market_calendars import MarketCalendar

from pandas_market_calendars.exchange_calendar_iex import IEXExchangeCalendar
from sortedcontainers import SortedSet

from trazy_analysis.common.constants import MAX_TIMESTAMP
from trazy_analysis.common.helper import get_or_create_nested_dict, normalize_assets
from trazy_analysis.common.types import CandleDataFrame
from trazy_analysis.db_storage.db_storage import DbStorage
from trazy_analysis.feed.loader import (
    CsvLoader,
    ExternalStorageLoader,
    HistoricalDataLoader,
)
from trazy_analysis.file_storage.file_storage import FileStorage
from trazy_analysis.market_data.historical.historical_data_handler import (
    HistoricalDataHandler,
)
from trazy_analysis.market_data.live.live_data_handler import LiveDataHandler
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.event import (
    MarketDataEndEvent,
    MarketDataEvent,
)


class Feed:
    def __init__(
        self,
        events: deque = deque(),
        candles: Dict[Asset, Dict[timedelta, np.array]] = {},
        candle_dataframes: Dict[Asset, Dict[timedelta, CandleDataFrame]] = {},
    ):
        self.assets = list(candles.keys())
        self.events = events
        self.candles = candles
        self.indexes = {}
        self.completed = False
        self.candle_dataframes = candle_dataframes
        current_timestamp = MAX_TIMESTAMP
        for asset in self.candles:
            get_or_create_nested_dict(self.indexes, asset)
            for time_unit in self.candles[asset]:
                self.indexes[asset][time_unit] = 0
                if len(self.candles[asset][time_unit]) == 0:
                    continue
                first_candle = self.candles[asset][time_unit][0]
                min_timestamp = first_candle.timestamp
                current_timestamp = min(current_timestamp, min_timestamp)
        self.min_timestamp = self.current_timestamp = current_timestamp

    def reset(self):
        self.indexes = {
            asset: {time_unit: 0}
            for asset in self.assets
            for time_unit in self.assets[asset]
        }
        self.completed = False
        self.current_timestamp = self.min_timestamp

    def update_latest_data(self):
        candles = []
        min_timestamp = MAX_TIMESTAMP
        completed = 0
        for asset in self.candles:
            for time_unit in self.candles[asset]:
                if self.indexes[asset][time_unit] < len(self.candles[asset][time_unit]):
                    index = self.indexes[asset][time_unit]
                    candle = self.candles[asset][time_unit][index]
                    if candle.timestamp < self.current_timestamp:
                        continue
                    candles.append(candle)
                    min_timestamp = min(min_timestamp, candle.timestamp)
                else:
                    completed += 1
        if candles:
            self.current_timestamp = min_timestamp
            assets = {}
            for candle in candles:
                get_or_create_nested_dict(assets, candle.asset)
                if candle.timestamp == self.current_timestamp:
                    self.indexes[candle.asset][candle.time_unit] += 1
                    assets[candle.asset][candle.time_unit] = SortedSet(
                        [candle],
                        key=lambda candle_param: candle_param.timestamp,
                    )
            self.events.append(MarketDataEvent(assets, self.current_timestamp))
        elif completed == sum(len(self.candles[asset]) for asset in self.candles):
            self.events.append(
                MarketDataEndEvent(
                    {asset: list(self.candles[asset].keys()) for asset in self.candles},
                    self.current_timestamp,
                )
            )
            self.completed = True


class LiveFeed(Feed):
    def __init__(
        self,
        assets: Dict[Asset, Union[timedelta, List[timedelta]]],
        live_data_handlers: Dict[str, LiveDataHandler],
        events: deque = deque(),
        candles: Dict[Asset, Dict[timedelta, np.array]] = {},
        candle_dataframes: Dict[Asset, Dict[timedelta, CandleDataFrame]] = {},
    ):
        super().__init__(
            events=events, candles=candles, candle_dataframes=candle_dataframes
        )
        self.assets = normalize_assets(assets)
        self.live_data_handlers = live_data_handlers

    def update_latest_data(self):
        now = datetime.now(pytz.UTC)
        candles_dict = {}
        one_minute_timedelta = timedelta(minutes=1)
        min_timestamp = MAX_TIMESTAMP
        for asset in self.assets:
            candles = self.live_data_handlers[
                asset.exchange.lower()
            ].request_ticker_lastest_candles(asset, nb_candles=10)
            if len(candles) > 0:
                get_or_create_nested_dict(candles_dict, asset)
                candles_dict[asset][one_minute_timedelta] = SortedSet(
                    [
                        candle
                        for candle in candles
                        if candle.timestamp + one_minute_timedelta < now
                    ],
                    key=lambda candle: candle.timestamp,
                )
                min_timestamp = min(min_timestamp, candles[0].timestamp)
        if candles_dict:
            self.events.append(
                MarketDataEvent(candles=candles_dict, timestamp=min_timestamp)
            )


class ExternalStorageFeed(Feed):
    def __init__(
        self,
        assets: Dict[Asset, Union[timedelta, List[timedelta]]],
        start: datetime,
        end: datetime = datetime.now(pytz.UTC),
        events: deque = deque(),
        db_storage: DbStorage = None,
        file_storage: FileStorage = None,
        market_cal: MarketCalendar = IEXExchangeCalendar(),
    ):
        external_storage_loader = ExternalStorageLoader(
            assets=assets,
            start=start,
            end=end,
            db_storage=db_storage,
            file_storage=file_storage,
            market_cal=market_cal,
        )
        external_storage_loader.load()
        candle_dataframes = external_storage_loader.candle_dataframes
        candles = external_storage_loader.candles
        super().__init__(events, candles, candle_dataframes)


class HistoricalFeed(Feed):
    def __init__(
        self,
        assets: Dict[Asset, Union[timedelta, List[timedelta]]],
        historical_data_handlers: Dict[str, HistoricalDataHandler],
        start: datetime,
        end: datetime,
        events: deque = deque(),
    ):
        historical_data_loader = HistoricalDataLoader(
            assets=assets,
            historical_data_handlers=historical_data_handlers,
            start=start,
            end=end,
        )
        historical_data_loader.load()
        candle_dataframes = historical_data_loader.candle_dataframes
        candles = historical_data_loader.candles

        super().__init__(events, candles, candle_dataframes)


class PandasFeed(Feed):
    def __init__(self, candle_dataframes: np.array, events: deque = deque()):
        candles = {}
        candle_dataframes_dict = {}
        for candle_dataframe in candle_dataframes:
            get_or_create_nested_dict(candles, candle_dataframe.asset)
            get_or_create_nested_dict(candle_dataframes_dict, candle_dataframe.asset)
            candles[candle_dataframe.asset][
                candle_dataframe.time_unit
            ] = candle_dataframe.to_candles()
            candle_dataframes_dict[candle_dataframe.asset][
                candle_dataframe.time_unit
            ] = candle_dataframe
        super().__init__(events, candles, candle_dataframes_dict)


class CsvFeed(Feed):
    def __init__(
        self,
        asset: Optional[Asset] = None,
        time_unit: Optional[timedelta] = timedelta(minutes=1),
        csv_filename: Optional[str] = None,
        csv_filenames: Optional[Dict[Asset, Dict[timedelta, str]]] = None,
        events: deque = deque(),
        sep: str = ",",
    ):
        csv_loader = CsvLoader(
            asset=asset,
            time_unit=time_unit,
            csv_filename=csv_filename,
            csv_filenames=csv_filenames,
            sep=sep,
        )
        csv_loader.load()
        candle_dataframes = csv_loader.candle_dataframes
        candles = csv_loader.candles

        super().__init__(events, candles, candle_dataframes)
