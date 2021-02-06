import abc
from typing import List

from common.constants import ENCODING
from common.meta import RateLimitedSingletonMeta
from common.types import CandleDataFrame


class DataHandler(metaclass=RateLimitedSingletonMeta):
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
    def get_tickers_list(cls) -> List[str]:
        response = cls.request(cls.BASE_URL_GET_TICKERS_LIST.format(cls.API_TOKEN))
        if response:
            tickers_response = response.content.decode(ENCODING)
            return cls.parse_get_tickers_response(tickers_response)
        else:
            return []
