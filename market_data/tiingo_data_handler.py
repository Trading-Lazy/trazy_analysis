import io
import json
from datetime import timedelta
from typing import List

import pandas as pd
from pandas_market_calendars.exchange_calendar_iex import IEXExchangeCalendar

from trazy_analysis.common.helper import resample_candle_data
from trazy_analysis.common.types import CandleDataFrame
from trazy_analysis.common.utils import lists_equal
from trazy_analysis.market_data.data_handler import DataHandler
from trazy_analysis.models.asset import Asset
from trazy_analysis.settings import TIINGO_API_TOKEN


class TiingoDataHandler(DataHandler):
    BASE_URL_API = "https://api.tiingo.com/iex"
    BASE_URL_GET_TICKERS_LIST = BASE_URL_API + "?token={}"
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
    API_TOKEN = TIINGO_API_TOKEN
    MAX_CALLS = 500
    PERIOD = 3600
    TICKER_DATA_TIMEZONE = "UTC"
    MARKET_CAL = IEXExchangeCalendar()

    @classmethod
    def ticker_data_is_none(cls, data: str) -> bool:
        columns_line = data.partition("\n")[0]
        columns = list(columns_line.split(","))
        return not lists_equal(columns, cls.TICKER_DATA_RESPONSE_USED_COLUMNS)

    @classmethod
    def parse_get_tickers_response(cls, tickers_response: str) -> List[str]:
        raw_tickers = json.loads(tickers_response)
        tickers = [raw_ticker["ticker"] for raw_ticker in raw_tickers]
        return tickers

    @classmethod
    def ticker_data_to_dataframe(cls, asset: Asset, data: str) -> CandleDataFrame:
        df = pd.read_csv(
            io.StringIO(data.strip()),
            sep=",",
            parse_dates=True,
            usecols=cls.TICKER_DATA_RESPONSE_USED_COLUMNS,
            dtype=cls.TICKER_DATA_RESPONSE_USED_COLUMNS_DTYPE,
            index_col="date",
        )
        df.index = pd.to_datetime(df.index, utc=True)
        candle_df = CandleDataFrame.from_dataframe(df, asset)
        if candle_df.shape[0] > 1:
            start_timestamp = candle_df.get_candle(0).timestamp
            end_timestamp = candle_df.get_candle(-1).timestamp
            market_cal_df = cls.MARKET_CAL.schedule(
                start_date=start_timestamp.strftime("%Y-%m-%d"),
                end_date=end_timestamp.strftime("%Y-%m-%d"),
            )
            candle_df = resample_candle_data(
                candle_dataframe=candle_df,
                time_unit=timedelta(minutes=1),
                market_cal_df=market_cal_df,
                remove_incomplete_head=True,
            )
        return candle_df
