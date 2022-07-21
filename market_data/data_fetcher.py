import io
from datetime import datetime, timedelta
from typing import List, Dict, Union, Tuple

import ccxt
import pandas as pd
import pytz
from memoization import CachingAlgorithmFlag, cached
from pandas_market_calendars import MarketCalendar

from trazy_analysis.broker.fee_model import FeeModel
from trazy_analysis.common.ccxt_connector import CcxtConnector
from trazy_analysis.common.constants import DATE_DIR_FORMAT, NONE_API_KEYS
from trazy_analysis.common.helper import normalize_assets
from trazy_analysis.common.types import CandleDataFrame
from trazy_analysis.db_storage.db_storage import DbStorage
from trazy_analysis.file_storage.common import DATASETS_DIR, DONE_DIR
from trazy_analysis.file_storage.file_storage import FileStorage
from trazy_analysis.market_data.historical.ccxt_historical_data_handler import (
    CcxtHistoricalDataHandler,
)
from trazy_analysis.market_data.historical.tiingo_historical_data_handler import (
    TiingoHistoricalDataHandler,
)
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle


class ExternalStorageFetcher:
    def __init__(
        self,
        db_storage: DbStorage,
        file_storage: FileStorage,
        market_cal: MarketCalendar = None,
    ):
        self.db_storage = db_storage
        self.file_storage = file_storage
        self.market_cal = market_cal

    def query_candles(
        self,
        asset: Asset,
        time_unit: timedelta,
        start: datetime,
        end: datetime = datetime.now(pytz.UTC),
    ) -> List[Candle]:
        candles = self.db_storage.get_candles_in_range(
            asset=asset, time_unit=time_unit, start=start, end=end
        )
        return candles

    def fetch_candle_db_data(
        self,
        asset: Asset,
        time_unit: timedelta,
        start: datetime,
        end: datetime = datetime.now(pytz.UTC),
    ) -> CandleDataFrame:
        if self.db_storage is None:
            return CandleDataFrame(asset=asset, time_unit=time_unit)
        candles = self.query_candles(asset, time_unit, start, end)
        df = CandleDataFrame.from_candle_list(asset=asset, candles=candles)
        return df

    @cached(max_size=128, algorithm=CachingAlgorithmFlag.LFU)
    def fetch_candle_historical_data(
        self,
        asset: Asset,
        start: datetime,
        end: datetime = datetime.now(pytz.UTC),
    ) -> CandleDataFrame:
        if self.file_storage is None:
            return CandleDataFrame(asset=asset)
        start_date = start.date()
        end_date = end.date()
        contents: List[CandleDataFrame] = []
        for i in range(0, (end_date - start_date).days + 1):
            date = start_date + timedelta(days=i)
            date_str = date.strftime(DATE_DIR_FORMAT)
            content = self.file_storage.get_file_content(
                "{}/{}/{}/{}_{}.csv".format(
                    DATASETS_DIR, date_str, DONE_DIR, asset.key(), date_str
                )
            )
            if not content:
                continue

            df = pd.read_csv(
                io.StringIO(content),
                sep=",",
                parse_dates=True,
                usecols=CandleDataFrame.ALL_COLUMNS,
                index_col=0,
                dtype={
                    "open": str,
                    "high": str,
                    "low": str,
                    "close": str,
                    "volume": int,
                },
            )
            contents.append(CandleDataFrame.from_dataframe(df, asset))

        if not contents:
            return CandleDataFrame(asset=asset)

        merged_df = CandleDataFrame.concat(contents, asset)
        start_str = start.strftime("%Y-%m-%d %H:%M:%S%z")
        end_str = end.strftime("%Y-%m-%d %H:%M:%S%z")
        return merged_df.loc[start_str:end_str]

    def fetch(
        self,
        asset: Asset,
        time_unit: timedelta,
        start: datetime,
        end: datetime = datetime.now(pytz.UTC),
    ) -> CandleDataFrame:
        df = self.fetch_candle_db_data(asset, time_unit, start, end)
        if df.empty or start <= df.iloc[0].name:
            if df.empty:
                historical_df_end = end
            else:
                historical_df_end = df.iloc[0].name - timedelta(minutes=1)
            historical_df = self.fetch_candle_historical_data(
                asset, start, historical_df_end
            )
            if not historical_df.empty:
                df = CandleDataFrame.concat([historical_df, df], asset)
        return df


class AssetDataFetcher:
    @staticmethod
    def fetch(
        assets: Dict[Asset, Union[timedelta, List[timedelta]]],
        start: datetime,
        end: datetime = datetime.now(pytz.UTC),
        exchanges_api_keys: Dict[str, str] = None,
    ) -> Tuple["Feed", Dict[Asset, FeeModel]]:
        assets = normalize_assets(assets)
        from trazy_analysis.feed.feed import HistoricalFeed
        exchanges_api_keys = (
            exchanges_api_keys if exchanges_api_keys is not None else {}
        )
        exchanges = {asset.exchange.lower() for asset in assets}
        fee_models = {}
        historical_data_handlers = {}
        exchanges_api_keys = {
            exchange.lower(): api_key
            for exchange, api_key in exchanges_api_keys.items()
        }

        # Crypto currency exchanges
        ccxt_exchanges_api_keys = {}
        ccxt_exchanges = ccxt.exchanges
        other_exchanges = []
        for exchange in exchanges:
            if exchange in ccxt_exchanges:
                if exchange in exchanges_api_keys:
                    ccxt_exchanges_api_keys[exchange] = exchanges_api_keys[exchange]
                else:
                    ccxt_exchanges_api_keys[exchange] = NONE_API_KEYS
            else:
                other_exchanges.append(exchange)
        if len(ccxt_exchanges_api_keys) != 0:
            ccxt_connector = CcxtConnector(exchanges_api_keys=ccxt_exchanges_api_keys)
            ccxt_historical_data_handler = CcxtHistoricalDataHandler(ccxt_connector)
            for exchange in ccxt_exchanges_api_keys:
                historical_data_handlers[exchange] = ccxt_historical_data_handler

            ccxt_assets = [
                asset for asset in assets if asset.exchange in ccxt_exchanges
            ]
            fetched_exchanges = set()
            for asset in ccxt_assets:
                if asset.exchange in fetched_exchanges:
                    continue
                fetched_exchanges.add(asset.exchange)
                fee_models.update(ccxt_connector.fetch_fees(asset.exchange))

        # Other exchanges
        for exchange in other_exchanges:
            if exchange == "iex":
                historical_data_handlers["iex"] = TiingoHistoricalDataHandler()
            else:
                all_exchanges = ccxt_exchanges
                all_exchanges.append("iex")
                raise Exception(
                    f"Exchange {exchange} is not supported for now. The list of supported exchanges is: {all_exchanges}"
                )

        feed = HistoricalFeed(
            assets=assets,
            historical_data_handlers=historical_data_handlers,
            start=start,
            end=end,
        )

        return feed, fee_models
