from collections import deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union

import ccxt
import numpy as np
import pytz
from pandas import DataFrame
from pandas_market_calendars import MarketCalendar
from pandas_market_calendars.exchange_calendar_iex import IEXExchangeCalendar
from sortedcontainers import SortedSet

from trazy_analysis.common.ccxt_connector import CcxtConnector
from trazy_analysis.common.constants import MAX_TIMESTAMP, NONE_API_KEYS
from trazy_analysis.common.crypto_exchange_calendar import CryptoExchangeCalendar
from trazy_analysis.common.helper import get_or_create_nested_dict, normalize_assets
from trazy_analysis.common.types import CandleDataFrame
from trazy_analysis.db_storage.db_storage import DbStorage
from trazy_analysis.feed.loader import (
    CsvLoader,
    ExternalStorageLoader,
    HistoricalDataLoader,
)
from trazy_analysis.file_storage.file_storage import FileStorage
from trazy_analysis.market_data.historical.ccxt_historical_data_handler import (
    CcxtHistoricalDataHandler,
)
from trazy_analysis.market_data.historical.historical_data_handler import (
    HistoricalDataHandler,
)
from trazy_analysis.market_data.historical.tiingo_historical_data_handler import (
    TiingoHistoricalDataHandler,
)
from trazy_analysis.market_data.live.live_data_handler import LiveDataHandler
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.event import (
    MarketDataEndEvent,
    MarketDataEvent,
)


# It's a class that represents a feed
class Feed:
    def __init__(
        self,
        events: deque = None,
        candles: dict[Asset, dict[timedelta, np.array]] = None,
        candle_dataframes: dict[Asset, dict[timedelta, CandleDataFrame]] = None,
    ):
        """
        :param events: A deque of events that will be processed by the backtest
        :type events: deque
        :param candles: A dictionary of dictionaries of numpy arrays. The first key is the asset, the second key is the time
        unit, and the value is a numpy array of candles
        :type candles: dict[Asset, dict[timedelta, np.array]]
        :param candle_dataframes: A dictionary of dictionaries of CandleDataFrames. The first key is the asset, the second
        key is the time_unit
        :type candle_dataframes: dict[Asset, dict[timedelta, CandleDataFrame]]
        """
        self.assets = {asset: list(time_units.keys()) for asset, time_units in candles.items()}
        self.events = events if events is not None else deque()
        self.candles = candles if candles is not None else {}
        self.candle_dataframes = (
            candle_dataframes if candle_dataframes is not None else {}
        )
        self.indexes = {}
        self.completed = False
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
        """
        It resets the index of the data to the first row of the first asset.
        """
        self.indexes = {
            asset: {time_unit: 0}
            for asset in self.assets
            for time_unit in self.assets[asset]
        }
        self.completed = False
        self.current_timestamp = self.min_timestamp

    def update_latest_data(self):
        """
        If there are candles to be added to the event queue, add them and update the current timestamp.

        If there are no candles to be added to the event queue, but all the candles have been added, add a
        MarketDataEndEvent to the event queue.
        """
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


# It's a subclass of the Feed class that add candles to the event queue in real time
class LiveFeed(Feed):
    def __init__(
        self,
        assets: dict[Asset, timedelta | list[timedelta]],
        live_data_handlers: dict[str, LiveDataHandler],
        events: deque = deque(),
        candles: dict[Asset, dict[timedelta, np.array]] = {},
        candle_dataframes: dict[Asset, dict[timedelta, CandleDataFrame]] = {},
    ):
        super().__init__(
            events=events, candles=candles, candle_dataframes=candle_dataframes
        )
        self.assets = normalize_assets(assets)
        self.live_data_handlers = live_data_handlers

    def update_latest_data(self):
        """
        It takes the latest 10 candles from the exchange, and if the latest candle is not older more than 1 minute,
        it adds it to the list of events
        """
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


# It's a subclass of the Feed class, which is a class that Django provides for us
class ExternalStorageFeed(Feed):
    def __init__(
        self,
        assets: dict[Asset, timedelta | list[timedelta]],
        start: datetime,
        end: datetime = datetime.now(pytz.UTC),
        events: deque = deque(),
        db_storage: DbStorage = None,
        file_storage: FileStorage = None,
        market_cal: MarketCalendar = IEXExchangeCalendar(),
    ):
        """
        `__init__` is a function that takes in a dictionary of assets and their corresponding time intervals, a start date,
        an end date, a deque of events, a database storage object, a file storage object, and a market calendar object, and
        then loads the data from the external storage.

        :param assets: dict[Asset, timedelta | list[timedelta]]
        :type assets: dict[Asset, timedelta | list[timedelta]]
        :param start: The start date of the backtest
        :type start: datetime
        :param end: datetime = datetime.now(pytz.UTC),
        :type end: datetime
        :param events: deque = deque()
        :type events: deque
        :param db_storage: DbStorage = None,
        :type db_storage: DbStorage
        :param file_storage: FileStorage = None,
        :type file_storage: FileStorage
        :param market_cal: The market calendar to use
        :type market_cal: MarketCalendar
        """
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


# > This class is a subclass of the Feed class, and it's used to create a feed of historical data
class HistoricalFeed(Feed):
    def __init__(
        self,
        assets: dict[Asset, timedelta | list[timedelta]],
        historical_data_handlers: dict[str, HistoricalDataHandler],
        start: datetime,
        end: datetime,
        events: deque = deque(),
    ):
        """
        > Takes in a dictionary of assets and their corresponding historical
        data handlers, a start and end date, and a queue of events. It then creates a `HistoricalDataLoader` object, which
        loads the historical data for the assets

        :param assets: A dictionary of assets and their corresponding timeframes
        :type assets: dict[Asset, timedelta | list[timedelta]]
        :param historical_data_handlers: A dictionary of historical data handlers
        :type historical_data_handlers: dict[str, HistoricalDataHandler]
        :param start: The start date of the backtest
        :type start: datetime
        :param end: The end date of the backtest
        :type end: datetime
        :param events: A deque of events
        :type events: deque
        """
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


# It's a subclass of the Feed class that takes a Pandas DataFrames as inputs.
class PandasFeed(Feed):
    def __init__(self, candle_dataframes: np.array, events: deque = deque()):
        """
        :param candle_dataframes: np.array
        :type candle_dataframes: np.array
        :param events: a deque of events that will be processed by the strategy
        :type events: deque
        """
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


# It's a subclass of the Feed class, that takes csv files as inputs
class CsvFeed(Feed):
    def __init__(
        self,
        asset: Optional[Asset] = None,
        time_unit: Optional[timedelta] = timedelta(minutes=1),
        csv_filename: Optional[str] = None,
        csv_filenames: Optional[dict[Asset, dict[timedelta, str]]] = None,
        events: deque = deque(),
        sep: str = ",",
    ):
        """
        > The `__init__` function of the `CsvFeed` class takes in a bunch of optional arguments, and then uses the
        `CsvLoader` class to load the data

        :param asset: The asset to load data for
        :type asset: Optional[Asset]
        :param time_unit: The time unit of the candles
        :type time_unit: Optional[timedelta]
        :param csv_filename: The path to the CSV file
        :type csv_filename: Optional[str]
        :param csv_filenames: A dictionary of dictionaries. The outer dictionary is keyed by asset, and the inner dictionary
        is keyed by time_unit. The value of the inner dictionary is the filename of the CSV file
        :type csv_filenames: Optional[dict[Asset, dict[timedelta, str]]]
        :param events: deque = deque()
        :type events: deque
        :param sep: str = ",",, defaults to ,
        :type sep: str (optional)
        """
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
