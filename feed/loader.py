from abc import abstractmethod
from datetime import datetime, timedelta
from typing import Dict, List

import numpy as np
import pandas as pd
import pytz
from pandas_market_calendars import MarketCalendar

from pandas_market_calendars.exchange_calendar_iex import IEXExchangeCalendar
from trazy_analysis.common.types import CandleDataFrame
from trazy_analysis.db_storage.db_storage import DbStorage
from trazy_analysis.file_storage.file_storage import FileStorage
from trazy_analysis.market_data.historical.historical_data_handler import (
    HistoricalDataHandler,
)
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle
from trazy_analysis.strategy.candlefetcher import CandleFetcher


class Loader:
    @abstractmethod
    def load(self):  # pragma: no cover
        raise NotImplementedError("load()")


class HistoricalDataLoader:
    def __init__(
        self,
        assets: List[Asset],
        historical_data_handlers: Dict[str, HistoricalDataHandler],
        start: datetime,
        end: datetime,
    ):
        self.assets = assets
        self.historical_data_handlers = historical_data_handlers
        self.start = start
        self.end = end
        self.candles = {}
        self.candle_dataframes = {}

    def load(self):
        for asset in self.assets:
            (
                candle_dataframe,
                _,
                _,
            ) = self.historical_data_handlers[asset.exchange.lower()].request_ticker_data_in_range(
                asset, self.start, self.end
            )
            self.candle_dataframes[asset] = candle_dataframe
            self.candles[asset] = self.candle_dataframes[asset].to_candles()


class CsvLoader:
    def __init__(
        self,
        csv_filenames: Dict[Asset, str],
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
                candle_dataframe = CandleDataFrame.from_candle_list(asset=asset, candles=np.array([], dtype=Candle))
            else:
                candle_dataframe = CandleDataFrame.from_dataframe(dataframe, asset)
            self.candle_dataframes[asset] = candle_dataframe
            self.candles[asset] = self.candle_dataframes[asset].to_candles()


class ExternalStorageLoader:
    def __init__(
        self,
        assets: List[Asset],
        start: datetime,
        end: datetime = datetime.now(pytz.UTC),
        db_storage: DbStorage = None,
        file_storage: FileStorage = None,
        market_cal: MarketCalendar = IEXExchangeCalendar(),
    ):
        self.assets = assets
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
                asset, self.start, self.end
            )
            self.candles[asset] = self.candle_dataframes[asset].to_candles()
