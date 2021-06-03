from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Dict, List

import numpy as np
from pandas_market_calendars import MarketCalendar

from common.american_stock_exchange_calendar import AmericanStockExchangeCalendar
from common.constants import MAX_TIMESTAMP
from db_storage.db_storage import DbStorage
from feed.loader import CsvLoader, ExternalStorageLoader, HistoricalDataLoader
from file_storage.file_storage import FileStorage
from market_data.historical.historical_data_handler import HistoricalDataHandler
from market_data.live.live_data_handler import LiveDataHandler
from models.asset import Asset
from models.candle import Candle
from models.event import (
    MarketDataEndEvent,
    MarketDataEvent,
)


class Feed:
    def set_assets(self, assets: List[Asset]):
        self.assets = assets

    def __init__(self, events: deque, candles: Dict[str, np.array] = {}):
        self.assets = None
        self.set_assets(candles.keys())
        self.events = events
        self.candles = candles
        self.indexes = {}
        self.completed = False
        current_timestamp = MAX_TIMESTAMP
        for asset in self.assets:
            self.indexes[asset] = 0
            first_candle = self.candles[asset][0]
            min_timestamp = first_candle.timestamp
            current_timestamp = min(current_timestamp, min_timestamp)
        self.current_timestamp = current_timestamp

    def update_latest_data(self):
        candles = []
        min_timestamp = MAX_TIMESTAMP
        completed = 0
        for asset in self.candles:
            if self.indexes[asset] < len(self.candles[asset]):
                index = self.indexes[asset]
                candle = self.candles[asset][index]
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
                if candle.timestamp == self.current_timestamp:
                    self.indexes[candle.asset] += 1
                    assets[candle.asset] = np.array([candle], dtype=Candle)
            self.events.append(MarketDataEvent(assets, self.current_timestamp))
        elif completed == len(self.candles):
            self.events.append(
                MarketDataEndEvent(list(self.candles.keys()), self.current_timestamp)
            )


class LiveFeed(Feed):
    def __init__(
        self,
        assets: List[Asset],
        events: deque,
        live_data_handler: LiveDataHandler,
        candles: Dict[str, np.array] = {},
    ):
        super().__init__(events=events, candles=candles)
        self.set_assets(assets)
        self.live_data_handler = live_data_handler

    def update_latest_data(self):
        now = datetime.now(timezone.utc)
        candles_dict = {}
        min_timestamp = MAX_TIMESTAMP
        for asset in self.assets:
            candles = self.live_data_handler.request_ticker_lastest_candles(
                asset, nb_candles=10
            )
            if len(candles) > 0:
                candles_dict[asset] = [
                    candle
                    for candle in candles
                    if candle.timestamp + timedelta(minutes=1) < now
                ]
                min_timestamp = min(min_timestamp, candles[0].timestamp)
        if candles_dict:
            self.events.append(
                MarketDataEvent(candles=candles_dict, timestamp=min_timestamp)
            )


class ExternalStorageFeed(Feed):
    def __init__(
        self,
        assets: List[Asset],
        events: deque,
        time_unit: timedelta,
        start: datetime,
        end: datetime = datetime.now(timezone.utc),
        db_storage: DbStorage = None,
        file_storage: FileStorage = None,
        market_cal: MarketCalendar = AmericanStockExchangeCalendar(),
    ):
        external_storage_loader = ExternalStorageLoader(
            assets=assets,
            time_unit=time_unit,
            start=start,
            end=end,
            db_storage=db_storage,
            file_storage=file_storage,
            market_cal=market_cal,
        )
        external_storage_loader.load()
        self.candle_dataframes = external_storage_loader.candle_dataframes
        candles = external_storage_loader.candles
        super().__init__(events, candles)


class HistoricalFeed(Feed):
    def __init__(
        self,
        assets: List[Asset],
        events: deque,
        historical_data_handler: HistoricalDataHandler,
        start: datetime,
        end: datetime,
    ):
        historical_data_loader = HistoricalDataLoader(
            assets=assets,
            historical_data_handler=historical_data_handler,
            start=start,
            end=end,
        )
        historical_data_loader.load()
        self.candle_dataframes = historical_data_loader.candle_dataframes
        candles = historical_data_loader.candles

        super().__init__(events, candles)


class PandasFeed(Feed):
    def __init__(self, candle_dataframes: np.array, events: deque):
        candles = {}
        for candle_dataframe in candle_dataframes:
            candles[candle_dataframe.asset] = candle_dataframe.to_candles()
        super().__init__(events, candles)


class CsvFeed(Feed):
    def __init__(
        self,
        csv_filenames: Dict[str, str],
        events: deque,
        sep: str = ",",
    ):
        csv_loader = CsvLoader(csv_filenames=csv_filenames, sep=sep)
        csv_loader.load()
        self.candle_dataframes = csv_loader.candle_dataframes
        candles = csv_loader.candles

        super().__init__(events, candles)
