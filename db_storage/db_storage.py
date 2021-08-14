import abc
from datetime import datetime
from typing import List

from trazy_analysis.common.types import CandleDataFrame
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.order import Order


class DbStorage:
    def __init__(
        self, database_name: str, database_url: str, table_analogous_name: str = "table"
    ):
        self.database_name = database_name
        self.database_url = database_url
        self.table_analogous_name = table_analogous_name

        # check that the instance is genuine
        # check db name
        dbnames = self.get_db_names()
        if database_name not in dbnames:
            raise Exception(
                f"Database name {database_name} is not one of the existing database names: {dbnames}"
            )

    @abc.abstractmethod
    def get_table_names(self) -> List[str]:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def get_db_names(self) -> List[str]:  # pragma: no cover
        raise NotImplementedError

    def check_table(self, table_name: str) -> None:
        tnames = self.get_table_names()
        if table_name not in tnames:
            raise Exception(
                f"{self.table_analogous_name.title()} {table_name} should be one of the existing "
                f"{self.table_analogous_name.lower()}s ({tnames}) of database {self.database_name}"
            )

    @abc.abstractmethod
    def add_candle(self, candle: Candle) -> str:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def get_all_candles(self) -> List[Candle]:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def get_candle_by_identifier(
        self, symbol: str, timestamp: datetime
    ) -> Candle:  # pragma: no cover
        raise NotImplementedError

    def candle_with_identifier_exists(self, asset: Asset, timestamp: datetime) -> bool:
        return self.get_candle_by_identifier(asset, timestamp) is not None

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
    def get_order_by_identifier(
        self, signal_id: str, generation_time: datetime
    ) -> Order:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def get_all_orders(self) -> List[Order]:  # pragma: no cover
        raise NotImplementedError

    @abc.abstractmethod
    def clean_all_orders(self) -> int:  # pragma: no cover
        raise NotImplementedError

    def add_candle_dataframe(
        self, candle_dataframe: CandleDataFrame
    ) -> None:  # pragma: no cover
        raise NotImplementedError

    def count(self, table_name: str) -> int:  # pragma: no cover
        raise NotImplementedError

    def close(self, table) -> None:  # pragma: no cover
        raise NotImplementedError
