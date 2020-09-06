import copy
from typing import Dict, List

import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler

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
        **scheduler_kwargs
    ):
        self.symbols = None
        self.ids = None
        self.set_symbols(candles.keys())
        self.candles_queue = candles_queue
        self.candles = copy.deepcopy(candles)
        self.sched = BlockingScheduler()
        self.scheduler_kwargs = scheduler_kwargs

    def job(self, symbol):
        if len(self.candles[symbol]) > 0:
            candle = self.candles[symbol].pop(0)
            candle_json = candle.to_json()
            self.candles_queue.push(candle_json)
        else:
            self.sched.remove_job(self.ids[symbol])
            if len(self.sched.get_jobs()) == 0:
                self.stop()

    def start(self):
        for symbol in self.symbols:
            self.sched.add_job(
                self.job,
                "interval",
                **self.scheduler_kwargs,
                id=self.ids[symbol],
                args=[symbol]
            )
        self.sched.start()

    def stop(self) -> None:
        self.sched.remove_all_jobs()
        if self.sched.running:
            self.sched.shutdown(wait=False)


class LiveFeed(Feed):
    def __init__(
        self,
        symbols: List[str],
        candles_queue: CandlesQueue,
        live_data_handler: LiveDataHandler,
        **scheduler_kwargs
    ):
        super().__init__(candles_queue, candles={}, **scheduler_kwargs)
        self.set_symbols(symbols)
        self.live_data_handler = live_data_handler

    def job(self, symbol):
        candles = self.live_data_handler.request_ticker_lastest_candles(
            symbol, nb_candles=2
        )
        for candle in candles:
            self.candles_queue.push(candle.to_json())

class OfflineFeed(Feed):
    def __init__(
            self,
            candles_queue: CandlesQueue,
            candles: Dict[str, List[Candle]] = {}
    ):
        super().__init__(candles_queue, candles)

    def start(self):
        for symbol in self.candles:
            while len(self.candles[symbol]) != 0:
                candle = self.candles[symbol].pop(0)
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
        candle_dataframes: List[CandleDataFrame] = []
        for symbol, csv_filename in csv_filenames.items():
            dataframe = pd.read_csv(csv_filename, dtype=dtype, sep=sep)
            candle_dataframe = CandleDataFrame.from_dataframe(dataframe, symbol)
            candle_dataframes.append(candle_dataframe)
        pandas_feed = PandasFeed(candle_dataframes, candles_queue)
        super().__init__(candles_queue, pandas_feed.candles)
