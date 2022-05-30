import os
import traceback
from datetime import timedelta
from typing import List, Union

import numpy as np

import trazy_analysis.logger
import trazy_analysis.settings
from trazy_analysis.common.ccxt_connector import CcxtConnector
from trazy_analysis.common.constants import CONNECTION_ERROR_MESSAGE
from trazy_analysis.common.crypto_exchange_calendar import CryptoExchangeCalendar
from trazy_analysis.common.helper import fill_missing_datetimes
from trazy_analysis.common.types import CandleDataFrame
from trazy_analysis.market_data.common import datetime_from_epoch
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
)


class CcxtDataHandler:
    TICKER_DATA_RESPONSE_USED_COLUMNS = [
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    TICKER_DATA_RESPONSE_USED_COLUMNS_DTYPE = {
        "open": str,
        "high": str,
        "low": str,
        "close": str,
        "volume": int,
    }
    TICKER_DATA_TIMEZONE = "UTC"
    MARKET_CAL = CryptoExchangeCalendar()

    def __init__(self, ccxt_connector: CcxtConnector):
        self.ccxt_connector = ccxt_connector

    @classmethod
    def ticker_data_is_none(cls, raw_candles: List[List[Union[float, int]]]) -> bool:
        return raw_candles == []

    @classmethod
    def ticker_data_to_dataframe(
        cls, asset: Asset, raw_candles: List[List[Union[float, int]]]
    ) -> CandleDataFrame:
        candles = np.array(
            [
                Candle(asset=asset, open=float(raw_candle[1]), high=float(raw_candle[2]), low=float(raw_candle[3]),
                       close=float(raw_candle[4]), volume=float(raw_candle[5]),
                       timestamp=datetime_from_epoch(raw_candle[0]))
                for raw_candle in raw_candles
            ]
        )
        candle_df = CandleDataFrame.from_candle_list(asset=asset, candles=candles)
        candle_df = fill_missing_datetimes(df=candle_df, time_unit=timedelta(minutes=1))
        candle_df = CandleDataFrame.from_dataframe(candle_df, asset)
        return candle_df

    def get_tickers_list(self, exchange: str) -> np.array:  # [str]
        try:
            exchange_to_lower = exchange.lower()
            exchange_instance = self.ccxt_connector.get_exchange_instance(
                exchange_to_lower
            )
            if (
                "fetchTickers" not in exchange_instance.has
                or not exchange_instance.has["fetchTickers"]
            ):
                return np.empty([], dtype="U20")
            tickers_dict = exchange_instance.fetchTickers()
        except Exception as e:
            LOG.error(
                CONNECTION_ERROR_MESSAGE,
                str(e),
                traceback.format_exc(),
            )
            return np.empty([], dtype="U20")
        return np.array(list(tickers_dict.keys()), dtype="U20")
