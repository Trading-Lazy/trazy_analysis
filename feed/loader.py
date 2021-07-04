from abc import abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Dict, List

import numpy as np
import pandas as pd
from pandas_market_calendars import MarketCalendar

from common.american_stock_exchange_calendar import AmericanStockExchangeCalendar
from common.types import CandleDataFrame
from db_storage.db_storage import DbStorage
from file_storage.file_storage import FileStorage
from market_data.historical.historical_data_handler import HistoricalDataHandler
from models.asset import Asset
from models.candle import Candle
from strategy.candlefetcher import CandleFetcher


class Loader:
    @abstractmethod
    def load(self):  # pragma: no cover
        raise NotImplementedError("load()")


class HistoricalDataLoader:
    def __init__(
        self,
        assets: List[str],
        historical_data_handler: HistoricalDataHandler,
        start: datetime,
        end: datetime,
    ):
        self.assets = assets
        self.historical_data_handler = historical_data_handler
        self.start = start
        self.end = end
        self.candles = {}
        self.candle_dataframes = {}

    def load(self):
        for asset in self.assets:
            self.historical_data_handler = self.historical_data_handler
            (
                candle_dataframe,
                _,
                _,
            ) = self.historical_data_handler.request_ticker_data_in_range(
                asset, self.start, self.end
            )
            self.candle_dataframes[asset] = candle_dataframe
            self.candles[asset] = self.candle_dataframes[asset].to_candles()


class CsvLoader:
    def __init__(
        self,
        csv_filenames: Dict[str, str],
        sep: str = ",",
    ):
        self.csv_filenames = csv_filenames
        self.sep = sep
        self.candles = {}
        self.candle_dataframes = {}

    def load(self):
        dtype = {
            "timestamp": str,
            "open": str,
            "high": str,
            "low": str,
            "close": str,
            "volume": float,
        }
        for asset, csv_filename in self.csv_filenames.items():
            dataframe = pd.read_csv(csv_filename, dtype=dtype, sep=self.sep)
            if dataframe.empty:
                candle_dataframe = CandleDataFrame.from_candle_list(
                    asset=asset, candles=np.array([], dtype=Candle)
                )
            else:
                candle_dataframe = CandleDataFrame.from_dataframe(dataframe, asset)
            self.candle_dataframes[asset] = candle_dataframe
            self.candles[asset] = self.candle_dataframes[asset].to_candles()


class ExternalStorageLoader:
    def __init__(
        self,
        assets: List[Asset],
        time_unit: timedelta,
        start: datetime,
        end: datetime = datetime.now(timezone.utc),
        db_storage: DbStorage = None,
        file_storage: FileStorage = None,
        market_cal: MarketCalendar = AmericanStockExchangeCalendar(),
    ):
        self.assets = assets
        self.time_unit = time_unit
        self.start = start
        self.end = end
        self.db_storage = db_storage
        self.file_storage = file_storage
        self.market_cal = market_cal
        self.candle_fetcher = CandleFetcher(
            db_storage=self.db_storage,
            file_storage=self.file_storage,
            market_cal=self.market_cal,
        )
        self.candles = {}
        self.candle_dataframes = {}

    def load(self):
        for asset in self.assets:
            self.candle_dataframes[asset] = self.candle_fetcher.fetch(
                asset, self.time_unit, self.start, self.end
            )
            self.candles[asset] = self.candle_dataframes[asset].to_candles()
