import abc
from datetime import datetime
from typing import List

from models.asset import Asset
from models.candle import Candle
from models.order import Order


class DbStorage:
    def __init__(self, database_name: str, database_url: str):
        self.database_name = database_name
        self.database_url = database_url

    @abc.abstractmethod
    def add_document(self, doc: dict, collection_name: str) -> str:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def get_document(self, id: str, collection_name: str) -> dict:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def get_all_documents(self, collection_name: str) -> List[dict]:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def clean_all_documents(self, collection_name: str) -> int:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def delete_one(self, id: str, collection_name) -> int:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, id: str, collection_name) -> int:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def add_candle(self, candle: Candle) -> str:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def get_candle(self, id: str) -> Candle:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def candle_with_id_exists(self, id: str) -> bool:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def get_all_candles(self) -> List[Candle]:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def get_candle_by_identifier(
        self, symbol: str, timestamp: datetime
    ) -> Candle:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def candle_with_identifier_exists(
        self, symbol: str, timestamp: datetime
    ) -> bool:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def get_candles_in_range(
        self, asset: Asset, start: datetime, end: datetime
    ) -> List[Candle]:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def clean_all_candles(self) -> int:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def add_signal(self, action: Order) -> str:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def get_signal(self, id: str) -> Candle:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def get_signal_by_identifier(
        self, symbol: str, strategy: str, candle_timestamp: datetime
    ) -> Candle:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def get_all_signals(self) -> List[Order]:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def clean_all_signals(self) -> int:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def add_order(self, action: Order) -> str:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def get_order(self, id: str) -> Candle:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def get_order_by_identifier(
        self, symbol: str, strategy: str, candle_timestamp: datetime
    ) -> Candle:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def get_all_orders(self) -> List[Order]:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def clean_all_orders(self) -> int:  # pragma: no cover
        raise NotImplementedError
