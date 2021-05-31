import abc
import os
import traceback
from typing import List

from requests.models import Response

import logger
import settings
from common.constants import ENCODING, CONNECTION_ERROR_MESSAGE
from common.helper import request
from common.meta import RateLimitedSingletonMeta
from market_data.data_handler import DataHandler
from models.candle import Candle

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)


class LiveDataHandler(DataHandler, metaclass=RateLimitedSingletonMeta):
    @property
    @classmethod
    @abc.abstractmethod
    def BASE_URL_TICKER_LATEST_DATA(cls) -> str:  # pragma: no cover
        pass

    # methods
    @classmethod
    @abc.abstractmethod
    def parse_ticker_latest_data(
        cls, symbol: str, data: str
    ) -> List[Candle]:  # pragma: no cover
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def generate_ticker_latest_data_url(cls, ticker: str) -> str:  # pragma: no cover
        raise NotImplementedError

    @classmethod
    def request_ticker_latest_data(cls, ticker: str) -> Response:
        latest_data_points_url = cls.generate_ticker_latest_data_url(ticker)
        LOG.info("Url for %s: %s", ticker, latest_data_points_url)
        return request(latest_data_points_url)

    @classmethod
    def request_ticker_lastest_candle(cls, ticker: str) -> Candle:
        latest_candles = cls.request_ticker_lastest_candles(ticker)
        if len(latest_candles) == 0:
            return None
        return cls.request_ticker_lastest_candles(ticker)[-1]

    @classmethod
    def request_ticker_lastest_candles(
        cls, ticker: str, nb_candles: int = 1
    ) -> List[Candle]:
        try:
            response = cls.request_ticker_latest_data(ticker)
        except Exception as e:
            LOG.error(
                CONNECTION_ERROR_MESSAGE,
                str(e),
                traceback.format_exc(),
            )
            return []
        data: str = response.content.decode(ENCODING)
        if response:
            if not cls.ticker_data_is_none(data):
                latest_candles = cls.parse_ticker_latest_data(ticker, data)
                start = max(-nb_candles, -len(latest_candles))
                return latest_candles[start:]
            else:
                LOG.info("No available data for ticker %s latest candles", ticker)
                return []
        else:
            LOG.info(
                "Ticker %s latest candles request error status_code = %s, message = %s",
                ticker,
                response.status_code,
                data,
            )
        return []
