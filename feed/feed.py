from collections import deque
from datetime import datetime, timedelta
from typing import Dict, List

import numpy as np
import pytz
from pandas_market_calendars import MarketCalendar

from pandas_market_calendars.exchange_calendar_iex import IEXExchangeCalendar
from trazy_analysis.common.constants import MAX_TIMESTAMP
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
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.event import (
    MarketDataEndEvent,
    MarketDataEvent,
)


class Feed:
    def set_assets(self, assets: List[Asset]):
        self.assets = assets

    def __init__(self, events: deque, candles: Dict[Asset, np.array] = {}):
        self.assets = None
        self.set_assets(candles.keys())
        self.events = events
        self.candles = candles
        self.indexes = {}
        self.completed = False
        current_timestamp = MAX_TIMESTAMP
        for asset in self.assets:
            self.indexes[asset] = 0
            if len(self.candles[asset]) == 0:
                continue
            first_candle = self.candles[asset][0]
            min_timestamp = first_candle.timestamp
            current_timestamp = min(current_timestamp, min_timestamp)
        self.current_timestamp = current_timestamp

    def update_latest_data(self):
        candles = []
        min_timestamp = MAX_TIMESTAMP
        completed = 0
        for asset in self.candles:
            # print(f"asset = {asset}")
            # indexes = {str(k): v for k, v in self.indexes.items()}
            # print(f"indexes = {indexes}")
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
            # print(f"candles = {[str(candle) for candle in candles]}")
            self.current_timestamp = min_timestamp
            assets = {}
            for candle in candles:
                if candle.timestamp == self.current_timestamp:
                    self.indexes[candle.asset] += 1
                    assets[candle.asset] = np.array([candle], dtype=Candle)
            str_assets = {str(k): v for k, v in assets.items()}
            # print(f"{str_assets}")
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
        now = datetime.now(pytz.UTC)
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
        end: datetime = datetime.now(pytz.UTC),
        db_storage: DbStorage = None,
        file_storage: FileStorage = None,
        market_cal: MarketCalendar = IEXExchangeCalendar(),
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
