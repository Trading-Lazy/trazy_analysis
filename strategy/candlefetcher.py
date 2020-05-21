from datetime import datetime
from pandas import DataFrame
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo import ASCENDING

import pandas as pd
from pandasql import sqldf
import numpy as np
import settings
from strategy.constants import DATE_FORMAT
from pandas_market_calendars import MarketCalendar
from common.exchange_calendar_euronext import EuronextExchangeCalendar

m_client = MongoClient(settings.DB_CONN, tlsAllowInvalidCertificates=True)
db = m_client['djongo_connection']
candles_collection = db['candles']
euronext_cal = EuronextExchangeCalendar()


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
    def resample_candle_data(df: DataFrame, timeframe: pd.offsets.DateOffset, business_cal: DataFrame) -> DataFrame:

        required_columns = ['_id', 'timestamp', 'symbol', 'interval', 'open', 'high', 'low', 'close', 'volume'].sort()
        sorted_columns = list(df.columns).sort()
        if sorted_columns != required_columns:
            raise Exception(
                'The input dataframe is malformed. It must contain only columns: {}'.format(required_columns))
        df.index = pd.to_datetime(df.timestamp, format=DATE_FORMAT)
        df = df.drop(['_id', 'interval'], axis=1)

        df[['open', 'high', 'low', 'close']] = df[['open', 'high', 'low', 'close']].astype(float)
        df[['volume']] = df[['volume']].astype(int)

        resample_label = 'right'
        if timeframe >= pd.offsets.Day(1):
            resample_label = 'left'
        df = df.resample(timeframe, label=resample_label, closed='right').agg({
            'symbol': 'first',
            'open': 'first',
            'high': np.max,
            'low': np.min,
            'close': 'last',
            'volume': np.sum
        }).fillna(method='ffill')

        if timeframe >= pd.offsets.Day(1):
            df_resampled = df.reindex(business_cal.index)
        else:
            s = """
                select
                    d.timestamp, d.symbol, d.open, d.high, d.low, d.close, d.volume
                from
                    df d join business_cal b on (d.timestamp >= b.market_open and d.timestamp <= b.market_close)
            """
            df_resampled = sqldf(s, locals())
            df_resampled = df_resampled.set_index('timestamp')
        return df_resampled

    @staticmethod
    def fetch(symbol: str,
              timeframe: pd.offsets.DateOffset,
              business_cal: MarketCalendar,
              start: datetime,
              end: datetime = datetime.now()) -> DataFrame:
        df = CandleFetcher.get_data_from_db(symbol, start, end)

        if not df.empty:
            business_cal = business_cal.schedule(start_date=start.strftime("%Y-%m-%d"), end_date=end.strftime("%Y-%m-%d"))
            df = CandleFetcher.resample_candle_data(df, timeframe, business_cal)
        return df
