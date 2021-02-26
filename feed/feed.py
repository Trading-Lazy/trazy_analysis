from datetime import datetime, timedelta, timezone
from typing import Dict, List

import numpy as np
from pandas_market_calendars import MarketCalendar
from rx import interval

from candles_queue.candles_queue import CandlesQueue
from common.american_stock_exchange_calendar import AmericanStockExchangeCalendar
from db_storage.db_storage import DbStorage
from feed.loader import CsvLoader, ExternalStorageLoader, HistoricalDataLoader
from file_storage.file_storage import FileStorage
from market_data.historical.historical_data_handler import HistoricalDataHandler
from market_data.live.live_data_handler import LiveDataHandler


class Feed:
    def set_symbols(self, symbols: List[str]):
        self.symbols = symbols

    def __init__(
        self,
        candles_queue: CandlesQueue,
        candles: Dict[str, np.array] = {},
        **scheduler_kwargs,
    ):
        self.symbols = None
        self.set_symbols(candles.keys())
        self.candles_queue = candles_queue
        self.candles = candles
        self.indexes = {}
        self.completed = False
        for symbol in self.symbols:
            self.indexes[symbol] = 0
        if scheduler_kwargs:
            self.interval = interval(timedelta(**scheduler_kwargs))
        else:
            self.interval = interval(timedelta(minutes=1))
        self.disposables = {}

    def feed_queue(self, symbol):
        if self.indexes[symbol] < len(self.candles[symbol]):
            index = self.indexes[symbol]
            candle = self.candles[symbol][index]
            self.indexes[symbol] += 1
            self.candles_queue.push(candle)
        else:
            if symbol in self.disposables:
                self.disposables[symbol].dispose()
                del self.disposables[symbol]
                if not self.disposables:
                    self.stop()

    def start(self):
        for symbol in self.symbols:
            self.disposables[symbol] = self.interval.subscribe(
                eval(f'lambda _: self.feed_queue("{symbol}")', {"self": self})
            )
        while not self.completed:
            pass

    def stop(self) -> None:
        for symbol in self.disposables:
            self.disposables[symbol].dispose()
        self.disposables = {}
        self.completed = True


class LiveFeed(Feed):
    def __init__(
        self,
        symbols: List[str],
        candles_queue: CandlesQueue,
        live_data_handler: LiveDataHandler,
        candles: Dict[str, np.array] = {},
        **scheduler_kwargs,
    ):
        super().__init__(candles_queue, candles=candles, **scheduler_kwargs)
        self.set_symbols(symbols)
        self.live_data_handler = live_data_handler

    def feed_queue(self, symbol):
        candles = self.live_data_handler.request_ticker_lastest_candles(
            symbol, nb_candles=2
        )
        for candle in candles:
            # wait for the candle minute to pass before getting the complete candle
            while candle.timestamp + timedelta(minutes=1) > datetime.now(timezone.utc):
                pass
            self.candles_queue.push(candle)


class OfflineFeed(Feed):
    def __init__(self, candles_queue: CandlesQueue, candles: Dict[str, np.array] = {}):
        super().__init__(candles_queue, candles)

    def start(self):
        for symbol in self.candles:
            for candle in self.candles[symbol]:
                self.candles_queue.push(candle)
            self.candles_queue.complete(symbol=symbol)

    def stop(self):
        pass


class ExternalStorageFeed(OfflineFeed):
    def __init__(
        self,
        symbols: List[str],
        candles_queue: CandlesQueue,
        start: datetime,
        end: datetime = datetime.now(timezone.utc),
        db_storage: DbStorage = None,
        file_storage: FileStorage = None,
        market_cal: MarketCalendar = AmericanStockExchangeCalendar(),
    ):
        external_storage_loader = ExternalStorageLoader(
            symbols=symbols,
            start=start,
            end=end,
            db_storage=db_storage,
            file_storage=file_storage,
            market_cal=market_cal,
        )
        external_storage_loader.load()
        self.candle_dataframes = external_storage_loader.candle_dataframes
        candles = external_storage_loader.candles
        super().__init__(candles_queue, candles)


class HistoricalFeed(OfflineFeed):
    def __init__(
        self,
        symbols: List[str],
        candles_queue: CandlesQueue,
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

        super().__init__(candles_queue, candles)


class PandasFeed(OfflineFeed):
    def __init__(self, candle_dataframes: np.array, candles_queue: CandlesQueue):
        candles = {}
        for candle_dataframe in candle_dataframes:
            candles[candle_dataframe.symbol] = candle_dataframe.to_candles()
        super().__init__(candles_queue, candles)


class CsvFeed(OfflineFeed):
    def __init__(
        self,
        csv_filenames: Dict[str, str],
        candles_queue: CandlesQueue,
        sep: str = ",",
    ):
        csv_loader = CsvLoader(csv_filenames=csv_filenames, sep=sep)
        csv_loader.load()
        self.candle_dataframes = csv_loader.candle_dataframes
        candles = csv_loader.candles

        super().__init__(candles_queue, candles)
