from datetime import datetime

from django.db.models.query import QuerySet
from django.forms import model_to_dict
from pandas import DataFrame
from pymongo import MongoClient

import pandas as pd
from pandasql import sqldf
import numpy as np
import settings
from common.utils import validate_dataframe_columns, candles_to_dict
from strategy.constants import DATE_FORMAT
from actionsapi.models import Candle
from pandas_market_calendars import MarketCalendar
from common.exchange_calendar_euronext import EuronextExchangeCalendar
from pytz import utc

EURONEXT_CAL = EuronextExchangeCalendar()

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
    def resample_candle_data(df: DataFrame, timeframe: pd.offsets.DateOffset, business_cal: DataFrame) -> DataFrame:
        required_columns = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume']
        validate_dataframe_columns(df, required_columns)
        df.index = pd.to_datetime(df.timestamp, format=DATE_FORMAT)

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
            df_resampled = df.reindex(business_cal.index.tz_localize(tz='UTC'))
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
              business_cal_df: MarketCalendar,
              start: datetime,
              end: datetime = datetime.now(utc)) -> DataFrame:
        candles = CandleFetcher.get_candles_from_db(symbol, start, end)
        candles_list = candles_to_dict(candles)
        df = pd.DataFrame(
            candles_list,
            columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"],
        )
        if not df.empty:
            business_cal_df = business_cal_df.schedule(start_date=start.strftime("%Y-%m-%d"), end_date=end.strftime("%Y-%m-%d"))
            df = CandleFetcher.resample_candle_data(df, timeframe, business_cal_df)
        return df
