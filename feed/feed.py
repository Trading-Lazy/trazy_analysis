from datetime import timedelta
from typing import Dict, List

import pandas as pd
from rx import interval

from candles_queue.candles_queue import CandlesQueue
from common.types import CandleDataFrame
from market_data.historical.historical_data_handler import HistoricalDataHandler
from market_data.live.live_data_handler import LiveDataHandler
from models.candle import Candle


class Feed:
    def set_symbols(self, symbols: List[str]):
        self.symbols = symbols
        self.ids = {symbol: (str(id(self)) + symbol) for symbol in self.symbols}

    def __init__(
        self,
        candles_queue: CandlesQueue,
        candles: Dict[str, List[Candle]] = {},
        **scheduler_kwargs,
    ):
        self.symbols = None
        self.ids = None
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
            candle_json = candle.to_json()
            self.candles_queue.push(candle_json)
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
        **scheduler_kwargs,
    ):
        super().__init__(candles_queue, candles={}, **scheduler_kwargs)
        self.set_symbols(symbols)
        self.live_data_handler = live_data_handler

    def feed_queue(self, symbol):
        candles = self.live_data_handler.request_ticker_lastest_candles(
            symbol, nb_candles=2
        )
        for candle in candles:
            self.candles_queue.push(candle.to_json())


class OfflineFeed(Feed):
    def __init__(
        self, candles_queue: CandlesQueue, candles: Dict[str, List[Candle]] = {}
    ):
        super().__init__(candles_queue, candles)

    def start(self):
        for symbol in self.candles:
            while self.indexes[symbol] < len(self.candles[symbol]):
                index = self.indexes[symbol]
                candle = self.candles[symbol][index]
                self.indexes[symbol] += 1
                candle_json = candle.to_json()
                self.candles_queue.push(candle_json)

    def stop(self):
        pass


class HistoricalFeed(OfflineFeed):
    def __init__(
        self,
        symbols: List[str],
        candles_queue: CandlesQueue,
        historical_data_handler: HistoricalDataHandler,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ):
        candles: Dict[str, List[Candle]] = {}
        for symbol in symbols:
            self.historical_data_handler = historical_data_handler
            (
                candle_dataframe,
                _,
                _,
            ) = self.historical_data_handler.request_ticker_data_in_range(
                symbol, start, end
            )
            candles[symbol] = candle_dataframe.to_candles()
            self.candle_dataframe = candle_dataframe
        super().__init__(candles_queue, candles)


class PandasFeed(OfflineFeed):
    def __init__(
        self, candle_dataframes: List[CandleDataFrame], candles_queue: CandlesQueue
    ):
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
        dtype = {
            "timestamp": str,
            "open": str,
            "high": str,
            "low": str,
            "close": str,
            "volume": int,
        }
        candles = {}
        for symbol, csv_filename in csv_filenames.items():
            dataframe = pd.read_csv(csv_filename, dtype=dtype, sep=sep)
            candle_dataframe = CandleDataFrame.from_dataframe(dataframe, symbol)
            self.candle_dataframe = candle_dataframe
            candles[candle_dataframe.symbol] = candle_dataframe.to_candles()
        super().__init__(candles_queue, candles)
