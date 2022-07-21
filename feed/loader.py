import abc
from abc import abstractmethod
from datetime import datetime, timedelta
from typing import Dict, List, Union, Optional

import numpy as np
import pandas as pd
import pytz
from pandas_market_calendars import MarketCalendar
from pandas_market_calendars.exchange_calendar_iex import IEXExchangeCalendar

from trazy_analysis.common.constants import MARKET_CAL
from trazy_analysis.common.helper import get_or_create_nested_dict, normalize_assets
from trazy_analysis.common.types import CandleDataFrame
from trazy_analysis.db_storage.db_storage import DbStorage
from trazy_analysis.file_storage.file_storage import FileStorage
from trazy_analysis.market_data.historical.historical_data_handler import (
    HistoricalDataHandler,
)
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle
from trazy_analysis.market_data.data_fetcher import ExternalStorageFetcher


class Loader:
    __metaclass__ = abc.ABCMeta

    @abstractmethod
    def load(self):  # pragma: no cover
        raise NotImplementedError("load()")


class HistoricalDataLoader:
    def __init__(
        self,
        assets: Dict[Asset, Union[timedelta, List[timedelta]]],
        historical_data_handlers: Dict[str, HistoricalDataHandler],
        start: datetime,
        end: datetime,
    ):
        self.assets = normalize_assets(assets)
        self.historical_data_handlers = historical_data_handlers
        self.start = start
        self.end = end
        self.candles = {}
        self.candle_dataframes: Dict[Asset, Dict[timedelta, CandleDataFrame]] = {}

    def load(self):
        # group assets by time_unit
        for asset in self.assets:
            (candle_dataframe, _, _,) = self.historical_data_handlers[
                asset.exchange.lower()
            ].request_ticker_data_in_range(
                asset,
                self.start,
                self.end,
            )

            get_or_create_nested_dict(self.candle_dataframes, asset)
            get_or_create_nested_dict(self.candles, asset)
            for time_unit in self.assets[asset]:
                if time_unit == candle_dataframe.time_unit:
                    self.candle_dataframes[asset][time_unit] = candle_dataframe
                else:
                    self.candle_dataframes[asset][time_unit] = candle_dataframe.rescale(
                        time_unit, MARKET_CAL[asset.exchange.lower()]
                    )
                self.candles[asset][time_unit] = self.candle_dataframes[asset][
                    time_unit
                ].to_candles()


class CsvLoader(Loader):
    def __init__(
        self,
        asset: Optional[Asset] = None,
        time_unit: timedelta = timedelta(minutes=1),
        csv_filename: Optional[str] = None,
        csv_filenames: Optional[Dict[Asset, Dict[timedelta, str]]] = None,
        sep: str = ",",
    ):
        if asset is not None:
            if time_unit is None or csv_filename is None:
                raise Exception(
                    "time_unit or csv_filename is None, both of them should be set"
                )
            csv_filenames = {asset: {time_unit: csv_filename}}
        elif csv_filenames is None:
            raise Exception("At least one of asset or csv_filenames should not be none")
        self.csv_filenames = csv_filenames
        self.sep = sep
        self.candles = {}
        self.candle_dataframes: Dict[Asset, Dict[timedelta, CandleDataFrame]] = {}

    def load(self):
        dtype = {
            "timestamp": str,
            "open": str,
            "high": str,
            "low": str,
            "close": str,
            "volume": float,
        }
        for asset in self.csv_filenames:
            get_or_create_nested_dict(self.candle_dataframes, asset)
            get_or_create_nested_dict(self.candles, asset)
            for time_unit, csv_filename in self.csv_filenames[asset].items():
                dataframe = pd.read_csv(csv_filename, dtype=dtype, sep=self.sep)
                if dataframe.empty:
                    candle_dataframe = CandleDataFrame.from_candle_list(
                        asset=asset, candles=np.array([], dtype=Candle)
                    )
                else:
                    candle_dataframe = CandleDataFrame.from_dataframe(
                        dataframe, asset, time_unit
                    )
                self.candle_dataframes[asset][time_unit] = candle_dataframe
                self.candles[asset][time_unit] = self.candle_dataframes[asset][
                    time_unit
                ].to_candles()


class ExternalStorageLoader:
    def __init__(
        self,
        assets: Dict[Asset, Union[timedelta, List[timedelta]]],
        start: datetime,
        end: datetime = datetime.now(pytz.UTC),
        db_storage: DbStorage = None,
        file_storage: FileStorage = None,
        market_cal: MarketCalendar = IEXExchangeCalendar(),
    ):
        self.assets = normalize_assets(assets)
        self.start = start
        self.end = end
        self.db_storage = db_storage
        self.file_storage = file_storage
        self.market_cal = market_cal
        self.candle_fetcher = ExternalStorageFetcher(
            db_storage=self.db_storage,
            file_storage=self.file_storage,
            market_cal=self.market_cal,
        )
        self.candles = {}
        self.candle_dataframes: Dict[Asset, Dict[timedelta, CandleDataFrame]] = {}

    def load(self):
        for asset in self.assets:
            candle_dataframe = self.candle_fetcher.fetch(
                asset, timedelta(minutes=1), self.start, self.end
            )
            get_or_create_nested_dict(self.candle_dataframes, asset)
            get_or_create_nested_dict(self.candles, asset)
            for time_unit in self.assets[asset]:
                self.candle_dataframes[asset][time_unit] = candle_dataframe.rescale(
                    time_unit, MARKET_CAL[asset.exchange.lower()]
                )
                self.candles[asset][time_unit] = self.candle_dataframes[asset][
                    time_unit
                ].to_candles()
