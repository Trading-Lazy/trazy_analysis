from datetime import datetime
from typing import List

from django.db.models.query import QuerySet
from django.forms import model_to_dict
from pandas import DataFrame
from pymongo import MongoClient

import pandas as pd
import numpy as np
import settings
from common.utils import validate_dataframe_columns
from strategy.constants import DATE_FORMAT
from actionsapi.models import Candle

m_client = MongoClient(settings.DB_CONN, tlsAllowInvalidCertificates=True)
db = m_client["djongo_connection"]
candles_collection = db["candles"]


class CandleFetcher:
    @staticmethod
    def get_candles_from_db(
        symbol: str, start: datetime, end: datetime = datetime.now()
    ) -> QuerySet:
        candles = Candle.objects.all().filter(
            symbol=symbol, timestamp__gte=start, timestamp__lte=end
        )
        return candles

    @staticmethod
    def resample_candle_data(
        df: DataFrame, timeframe: pd.offsets.DateOffset
    ) -> DataFrame:
        required_columns = [
            "timestamp",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
        validate_dataframe_columns(df, required_columns)
        df.index = df.timestamp

        df[["open", "high", "low", "close"]] = df[
            ["open", "high", "low", "close"]
        ].astype(float)
        df[["volume"]] = df[["volume"]].astype(int)

        df = (
            df.resample(timeframe, label="right", closed="right")
            .agg(
                {
                    "symbol": "first",
                    "open": "first",
                    "high": np.max,
                    "low": np.min,
                    "close": "last",
                    "volume": np.sum,
                }
            )
            .fillna(method="ffill")
        )
        return df

    @staticmethod
    def fetch(
        symbol: str,
        timeframe: pd.offsets.DateOffset,
        start: datetime,
        end: datetime = datetime.now(),
    ) -> DataFrame:
        candles = CandleFetcher.get_candles_from_db(symbol, start, end)
        candles_list = []
        for candle in candles:
            candles_list.append(model_to_dict(candle))
        df = pd.DataFrame(
            candles_list,
            columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"],
        )
        df = CandleFetcher.resample_candle_data(df, timeframe)
        return df
