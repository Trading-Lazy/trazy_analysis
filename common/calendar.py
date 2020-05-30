from pandas_market_calendars import MarketCalendar
from datetime import datetime
import pandas as pd


def is_business_day(dt: datetime, business_calendar:MarketCalendar=None, df_business_calendar: pd.DataFrame=None):
    if business_calendar is not None:
        business_day = business_calendar.schedule(start_date=dt.strftime('%Y-%m-%d'),end_date=dt.strftime('%Y-%m-%d'))
        return not business_day.empty

    if not df_business_calendar.empty:
        return dt.date() in df_business_calendar.index.date


def is_business_hour(dt: datetime, business_calendar:MarketCalendar=None, df_business_calendar: pd.DataFrame=None):
    if not business_calendar is None:
        business_day = business_calendar.schedule(start_date=dt.strftime('%Y-%m-%d'), end_date=dt.strftime('%Y-%m-%d'))
        if business_day.empty:
            return False
        market_open = business_day.iloc[0]['market_open']
        market_close = business_day.iloc[0]['market_close']
        return market_open <= dt <= market_close

    if not df_business_calendar.empty:
        df_temp = df_business_calendar.loc[(df_business_calendar['market_open'] <= dt) &
                                           (df_business_calendar['market_close'] >= dt)]
        return not df_temp.empty



