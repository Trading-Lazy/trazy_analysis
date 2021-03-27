from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Dict, List

import numpy as np
from pandas_market_calendars import MarketCalendar

from common.american_stock_exchange_calendar import AmericanStockExchangeCalendar
from db_storage.db_storage import DbStorage
from feed.loader import CsvLoader, ExternalStorageLoader, HistoricalDataLoader
from file_storage.file_storage import FileStorage
from market_data.historical.historical_data_handler import HistoricalDataHandler
from market_data.live.live_data_handler import LiveDataHandler
from models.event import MarketDataEndEvent, MarketDataEvent


class Feed:
    def set_symbols(self, symbols: List[str]):
        self.symbols = symbols

    def __init__(self, events: deque, candles: Dict[str, np.array] = {}):
        self.symbols = None
        self.set_symbols(candles.keys())
        self.events = events
        self.candles = candles
        self.indexes = {}
        self.completed = False
        for symbol in self.symbols:
            self.indexes[symbol] = 0

    def update_latest_data(self):
        for symbol in self.candles:
            if self.indexes[symbol] < len(self.candles[symbol]):
                index = self.indexes[symbol]
                candle = self.candles[symbol][index]
                self.indexes[symbol] += 1
                self.events.append(MarketDataEvent(candle))
            else:
                self.events.append(MarketDataEndEvent(symbol))


class LiveFeed(Feed):
    def __init__(
        self,
        symbols: List[str],
        events: deque,
        live_data_handler: LiveDataHandler,
        candles: Dict[str, np.array] = {},
    ):
        super().__init__(events=events, candles=candles)
        self.set_symbols(symbols)
        self.live_data_handler = live_data_handler

    def update_latest_data(self):
        for symbol in self.symbols:
            candles = self.live_data_handler.request_ticker_lastest_candles(
                symbol, nb_candles=10
            )
            for candle in candles:
                if candle.timestamp + timedelta(minutes=1) < datetime.now(timezone.utc):
                    self.events.append(MarketDataEvent(candle))


class ExternalStorageFeed(Feed):
    def __init__(
        self,
        symbols: List[str],
        events: deque,
        time_unit: timedelta,
        start: datetime,
        end: datetime = datetime.now(timezone.utc),
        db_storage: DbStorage = None,
        file_storage: FileStorage = None,
        market_cal: MarketCalendar = AmericanStockExchangeCalendar(),
    ):
        external_storage_loader = ExternalStorageLoader(
            symbols=symbols,
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
        symbols: List[str],
        events: deque,
        historical_data_handler: HistoricalDataHandler,
        start: datetime,
        end: datetime,
    ):
        historical_data_loader = HistoricalDataLoader(
            symbols=symbols,
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
            candles[candle_dataframe.symbol] = candle_dataframe.to_candles()
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
