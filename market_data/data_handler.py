import abc
import os
import traceback
from typing import List

import numpy as np

import trazy_analysis.settings
from trazy_analysis.common.constants import CONNECTION_ERROR_MESSAGE, ENCODING
from trazy_analysis.common.meta import RateLimitedSingleton
from trazy_analysis.common.types import CandleDataFrame
from trazy_analysis.logger import logger

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
)


class DataHandler(metaclass=RateLimitedSingleton):
    # properties
    @property
    @classmethod
    @abc.abstractmethod
    def BASE_URL_API(cls) -> str:  # pragma: no cover
        pass

    @property
    @classmethod
    @abc.abstractmethod
    def BASE_URL_GET_TICKERS_LIST(cls) -> str:  # pragma: no cover
        pass

    @property
    @classmethod
    @abc.abstractmethod
    def TICKER_DATA_RESPONSE_USED_COLUMNS(cls) -> List[str]:  # pragma: no cover
        pass

    @property
    @classmethod
    @abc.abstractmethod
    def API_TOKEN(cls) -> str:  # pragma: no cover
        pass

    @property
    @classmethod
    @abc.abstractmethod
    def MAX_CALLS(cls) -> int:  # pragma: no cover
        pass

    @property
    @classmethod
    @abc.abstractmethod
    def PERIOD(cls) -> int:  # pragma: no cover
        pass

    # methods
    @classmethod
    @abc.abstractmethod
    def parse_get_tickers_response(
        cls, tickers_response: str
    ) -> List[str]:  # pragma: no cover
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def ticker_data_is_none(cls, data: str) -> bool:  # pragma: no cover
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def ticker_data_to_dataframe(
        cls, symbol: str, data: str
    ) -> CandleDataFrame:  # pragma: no cover
        raise NotImplementedError

    @classmethod
    def get_tickers_list(cls) -> np.array:  # [str]
        try:
            response = cls.request(cls.BASE_URL_GET_TICKERS_LIST.format(cls.API_TOKEN))
        except Exception as e:
            LOG.error(
                CONNECTION_ERROR_MESSAGE,
                str(e),
                traceback.format_exc(),
            )
            return np.empty([], dtype="U6")
        if response:
            tickers_response = response.content.decode(ENCODING)
            return cls.parse_get_tickers_response(tickers_response)
        else:
            return np.empty([], dtype="U6")
