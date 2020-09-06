import abc
from typing import List

import pandas as pd

from models.action import Action
from models.candle import Candle


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
        self, symbol: str, timestamp: pd.Timestamp
    ) -> Candle:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def candle_with_identifier_exists(
        self, symbol: str, timestamp: pd.Timestamp
    ) -> bool:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def get_candles_in_range(
        self, symbol: str, start: pd.Timestamp, end: pd.Timestamp
    ) -> List[Candle]:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def clean_all_candles(self) -> int:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def add_action(self, action: Action) -> str:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def get_action(self, id: str) -> Candle:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def get_all_actions(self) -> List[Action]:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def clean_all_actions(self) -> int:  # pragma: no cover
        raise NotImplementedError
