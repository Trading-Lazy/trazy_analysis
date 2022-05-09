import os
from collections.abc import MutableMapping
from datetime import datetime
from typing import Callable, List
from urllib.parse import urlparse

import numpy as np
from influxdb import DataFrameClient, InfluxDBClient

import trazy_analysis.settings
from trazy_analysis.common.helper import datetime_to_epoch
from trazy_analysis.common.types import CandleDataFrame
from trazy_analysis.common.utils import timestamp_to_utc
from trazy_analysis.db_storage.db_storage import DbStorage
from trazy_analysis.logger import logger
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.order import Order
from trazy_analysis.models.signal import Signal
from trazy_analysis.settings import (
    CANDLES_COLLECTION_NAME,
    DATABASE_NAME,
    INFLUXDB_URL,
    ORDERS_COLLECTION_NAME,
    SIGNALS_COLLECTION_NAME,
)

LOG = trazy_analysis.logger.get_root_logger(
    __name__, filename=os.path.join(trazy_analysis.settings.ROOT_PATH, "output.log")
)


class InfluxDbStorage(DbStorage):
    def __init__(
        self,
        database_name: str = DATABASE_NAME,
        database_url: str = INFLUXDB_URL,
    ):
        parsed_url = urlparse(database_url)
        host = parsed_url.hostname
        port = parsed_url.port
        username = parsed_url.username
        password = parsed_url.password
        self.client = InfluxDBClient(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database_name,
        )
        super().__init__(
            database_name, database_url, table_analogous_name="measurement"
        )
        self.df_client = DataFrameClient(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database_name,
        )

    def get_table_names(self) -> List[str]:
        measurements = self.client.get_list_measurements()
        return [measurement.get("name") for measurement in measurements]

    def get_db_names(self) -> List[str]:
        dbs = self.client.get_list_database()
        return [db.get("name") for db in dbs]

    def format_candle_dict(self, candle_dict: dict):
        candle_dict["timestamp"] = datetime.strptime(
            candle_dict["time"], "%Y-%m-%dT%H:%M:%SZ"
        )
        candle_dict["asset"] = {
            "symbol": candle_dict["symbol"],
            "exchange": candle_dict["exchange"],
        }

    def format_signal_dict(self, signal_dict: dict):
        signal_dict["generation_time"] = timestamp_to_utc(
            datetime.strptime(
                signal_dict["time"],
                "%Y-%m-%dT%H:%M:%SZ",
            )
        )
        signal_dict["root_candle_timestamp"] = datetime.strptime(
            "".join(signal_dict["root_candle_timestamp"].rsplit(":", 1)),
            "%Y-%m-%d %H:%M:%S%z",
        )
        signal_dict["asset"] = {
            "symbol": signal_dict["symbol"],
            "exchange": signal_dict["exchange"],
        }
        parameters_dict = {}
        for key in signal_dict.keys():
            if key.startswith("parameters."):
                parameters_dict[key] = signal_dict[key]
        parameters = self.rebuild_parameters(parameters_dict)
        signal_dict["parameters"] = parameters

    def format_order_dict(self, order_dict: dict):
        order_dict["generation_time"] = timestamp_to_utc(
            datetime.strptime(order_dict["time"], "%Y-%m-%dT%H:%M:%SZ")
        )
        order_dict["asset"] = {
            "symbol": order_dict["symbol"],
            "exchange": order_dict["exchange"],
        }

    def execute_query(
        self,
        query: str,
        format_function: Callable[[dict], None],
        serializable_class: type,
    ) -> np.array:  # [serializables]
        result = self.client.query(query)
        points = list(result.get_points())
        if len(points) == 0:
            return np.empty(shape=len(points), dtype=Candle)
        serializables = np.empty(shape=len(points), dtype=Candle)
        for index, serializable_dict in enumerate(points):
            format_function(serializable_dict)
            serializable = serializable_class.from_serializable_dict(serializable_dict)
            serializables[index] = serializable
        return serializables

    def execute_candle_query(self, candle_query: str) -> np.array:  # [Candle]
        return self.execute_query(
            query=candle_query,
            format_function=self.format_candle_dict,
            serializable_class=Candle,
        )

    def execute_signal_query(self, signal_query: str) -> np.array:  # [Signal]
        return self.execute_query(
            query=signal_query,
            format_function=self.format_signal_dict,
            serializable_class=Signal,
        )

    def execute_order_query(self, order_query: str) -> np.array:  # [Order]
        return self.execute_query(
            query=order_query,
            format_function=self.format_order_dict,
            serializable_class=Order,
        )

    def add_candle(self, candle: Candle) -> str:
        json_body = [
            {
                "measurement": CANDLES_COLLECTION_NAME,
                "time": candle.timestamp,
                "tags": {
                    "symbol": candle.asset.symbol,
                    "exchange": candle.asset.exchange,
                },
                "fields": {
                    "open": str(candle.open),
                    "high": str(candle.high),
                    "low": str(candle.low),
                    "close": str(candle.close),
                    "volume": candle.volume,
                },
            }
        ]
        self.client.write_points(json_body)

    def get_candle_by_identifier(self, asset: Asset, timestamp: datetime) -> Candle:
        query = (
            f"select * from {CANDLES_COLLECTION_NAME} where time={int(timestamp.timestamp()) * 1000000000} and "
            f"symbol='{asset.symbol}' and exchange='{asset.exchange}'"
        )
        candles = self.execute_candle_query(query)
        if len(candles) == 0:
            return None
        return candles[0]

    def candle_with_identifier_exists(self, asset: Asset, timestamp: datetime) -> bool:
        return self.get_candle_by_identifier(asset, timestamp) is not None

    def get_candles_in_range(
        self, asset: Asset, start: datetime, end: datetime
    ) -> np.array:  # [Candle]
        query = (
            f"select * from {CANDLES_COLLECTION_NAME} where time >= {datetime_to_epoch(start, 1000000000)} "
            f"and time <= {datetime_to_epoch(end, 1000000000)} and "
            f"symbol='{asset.symbol}' and exchange='{asset.exchange}'"
        )
        return self.execute_candle_query(query)

    def get_all_candles(self) -> np.array:  # [Candle]
        query = f"select * from {CANDLES_COLLECTION_NAME}"
        return self.execute_candle_query(query)

    def clean_all_candles(self) -> None:
        query = f"delete from {CANDLES_COLLECTION_NAME}"
        self.client.query(query)

    def flatten(self, dict_to_flatten: dict, parent_key="", sep="."):
        items = []
        for k, v in dict_to_flatten.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, MutableMapping):
                items.extend(self.flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def add_signal(self, signal: Signal) -> str:
        flatten_parameters = self.flatten({"parameters": signal.parameters})
        json_body = [
            {
                "measurement": SIGNALS_COLLECTION_NAME,
                "time": signal.generation_time,
                "tags": {
                    "symbol": signal.asset.symbol,
                    "exchange": signal.asset.exchange,
                    "strategy": signal.strategy,
                    "root_candle_timestamp": signal.root_candle_timestamp,
                },
                "fields": {
                    "action": signal.action.name,
                    "direction": signal.direction.name,
                    "confidence_level": signal.confidence_level,
                    "time_in_force": str(signal.time_in_force),
                    **flatten_parameters,
                },
            }
        ]
        self.client.write_points(json_body)

    def rebuild_parameters(self, parameters_dict: dict, sep="."):
        parameters = {}
        for flattened_parameter in parameters_dict:
            nested_keys = flattened_parameter.split(sep)
            nested_keys_len = len(nested_keys)
            _parameters = parameters
            for i in range(1, nested_keys_len - 1):
                key = nested_keys[i]
                try:
                    _parameters = _parameters[key]
                except KeyError:
                    _parameters[key] = {}
                    _parameters = _parameters[key]
            parameters[nested_keys[-1]] = parameters_dict[flattened_parameter]
        return parameters

    def get_signal_by_identifier(
        self, asset: Asset, strategy: str, root_candle_timestamp: datetime
    ) -> Signal:
        query = (
            f"select * from {SIGNALS_COLLECTION_NAME} "
            f"where root_candle_timestamp='{str(root_candle_timestamp)}' and "
            f"symbol='{asset.symbol}' and exchange='{asset.exchange}'"
        )
        signals = self.execute_signal_query(query)
        if len(signals) == 0:
            return None
        return signals[0]

    def get_all_signals(self) -> np.array:  # [Signal]
        query = f"select * from {SIGNALS_COLLECTION_NAME}"
        return self.execute_signal_query(query)

    def clean_all_signals(self) -> int:
        query = f"delete from {SIGNALS_COLLECTION_NAME}"
        self.client.query(query)

    def add_order(self, order: Order) -> str:
        json_body = [
            {
                "measurement": ORDERS_COLLECTION_NAME,
                "time": order.generation_time,
                "tags": {
                    "signal_id": order.signal_id,
                },
                "fields": {
                    "symbol": order.asset.symbol,
                    "exchange": order.asset.exchange,
                    "action": order.action.name,
                    "direction": order.direction.name,
                    "size": order.size,
                    "time_in_force": str(order.time_in_force),
                    "status": order.status.name,
                    "type": order.type.name,
                    "condition": order.condition.name,
                },
            }
        ]
        self.client.write_points(json_body)

    def get_order_by_identifier(
        self, signal_id: str, generation_time: datetime
    ) -> Order:
        query = (
            f"select * from {ORDERS_COLLECTION_NAME} "
            f"where signal_id='{signal_id}' and "
            f"time={datetime_to_epoch(generation_time, 1000000000)}"
        )
        orders = self.execute_order_query(query)
        if len(orders) == 0:
            return None
        return orders[0]

    def get_all_orders(self) -> np.array:  # [Order]
        query = f"select * from {ORDERS_COLLECTION_NAME}"
        return self.execute_order_query(query)

    def clean_all_orders(self) -> int:
        query = f"delete from {ORDERS_COLLECTION_NAME}"
        self.client.query(query)

    def add_candle_dataframe(self, candle_dataframe: CandleDataFrame) -> None:
        candle_dataframe["symbol"] = candle_dataframe.asset.symbol
        candle_dataframe["exchange"] = candle_dataframe.asset.exchange
        self.df_client.write_points(
            candle_dataframe,
            CANDLES_COLLECTION_NAME,
            tag_columns=["symbol", "exchange"],
            field_columns=["open", "high", "low", "close", "volume"],
            database=DATABASE_NAME,
            protocol="line",
        )

    def count(self, table_name: str) -> int:
        select_all_query = f"select count(*) from {table_name}"
        result = self.client.query(select_all_query)
        points = list(result.get_points())
        if len(points) == 0:
            return 0
        point = points[0]
        keys = list(point.keys())
        count_keys = [key for key in keys if key.startswith("count_")]
        if len(count_keys) == 0:
            LOG.warning(
                f"There is no tag for measurement {table_name}, it's not possible to make a count"
            )
        count_key = count_keys[0]
        return int(point[count_key])

    def close(self, table) -> None:  # pragma: no cover
        self.client.close()
