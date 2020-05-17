from datetime import datetime
from pandas import DataFrame
from pymongo import MongoClient
from pymongo import ASCENDING

import pandas as pd
import numpy as np
import settings
from strategy.constants import DATE_FORMAT
from common.utils import validate_dataframe_columns

m_client = MongoClient(settings.DB_CONN, tlsAllowInvalidCertificates=True)
db = m_client['djongo_connection']
candles_collection = db['candles']


class CandleFetcher:
    @staticmethod
    def get_data_from_db(symbol: str, start: datetime,
                         end: datetime = datetime.now()) -> DataFrame:
        query = {
            "symbol": symbol,
            "$and": [
                {"timestamp": {"$gte": start}},
                {"timestamp": {"$lt": end}}
            ]
        }
        cursor = candles_collection.find(query).sort("timestamp", ASCENDING)
        df = pd.DataFrame(list(cursor))
        return df

    @staticmethod
    def resample_candle_data(df: DataFrame, timeframe: pd.offsets.DateOffset) -> DataFrame:
        required_columns = ['_id', 'timestamp', 'symbol', 'interval', 'open', 'high', 'low', 'close', 'volume']
        validate_dataframe_columns(df, required_columns)
        df.index = pd.to_datetime(df.timestamp, format=DATE_FORMAT)
        df = df.drop(['_id', 'interval'], axis=1)

        df[['open', 'high', 'low', 'close']] = df[['open', 'high', 'low', 'close']].astype(float)
        df[['volume']] = df[['volume']].astype(int)

        df = df.resample(timeframe, label='right', closed='right').agg({
            'symbol': 'first',
            'open': 'first',
            'high': np.max,
            'low': np.min,
            'close': 'last',
            'volume': np.sum
        }).fillna(method='ffill')
        return df

    @staticmethod
    def fetch(symbol: str, timeframe: pd.offsets.DateOffset, start: datetime,
              end: datetime = datetime.now()) -> DataFrame:
        df = CandleFetcher.get_data_from_db(symbol, start, end)
        df = CandleFetcher.resample_candle_data(df, timeframe)
        return df
