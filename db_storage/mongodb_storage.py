import os
from datetime import datetime
from typing import List

import numpy as np
import pandas as pd
import pymongo
from bson import ObjectId
from pymongo.cursor import Cursor
from pymongo.results import DeleteResult, InsertOneResult

import settings
from common.types import CandleDataFrame
from db_storage.db_storage import DbStorage
from logger import logger
from models.asset import Asset
from models.candle import Candle
from models.order import Order
from models.signal import Signal
from settings import (
    CANDLES_COLLECTION_NAME,
    DATABASE_NAME,
    DOCUMENTS_COLLECTION_NAME,
    MONGODB_URL,
    ORDERS_COLLECTION_NAME,
    SIGNALS_COLLECTION_NAME,
)

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)


class MongoDbStorage(DbStorage):
    def __init__(
        self, database_name: str = DATABASE_NAME, database_url: str = MONGODB_URL
    ):
        self.client = pymongo.MongoClient(
            host=database_url, tz_aware=True, serverSelectionTimeoutMS=2000
        )
        super().__init__(database_name, database_url, table_analogous_name="collection")

        self.db: pymongo.database.Database = self.client[database_name]

        self.collections_cache = {}

        self.check_table(CANDLES_COLLECTION_NAME)
        self.collections_cache[CANDLES_COLLECTION_NAME] = self.db[
            CANDLES_COLLECTION_NAME
        ]

        self.check_table(ORDERS_COLLECTION_NAME)
        self.collections_cache[ORDERS_COLLECTION_NAME] = self.db[ORDERS_COLLECTION_NAME]

        self.check_table(SIGNALS_COLLECTION_NAME)
        self.collections_cache[SIGNALS_COLLECTION_NAME] = self.db[
            SIGNALS_COLLECTION_NAME
        ]

        self.check_table(DOCUMENTS_COLLECTION_NAME)
        self.collections_cache[DOCUMENTS_COLLECTION_NAME] = self.db[
            DOCUMENTS_COLLECTION_NAME
        ]

    def get_table_names(self) -> List[str]:
        return self.db.list_collection_names()

    def get_db_names(self) -> List[str]:
        return list(self.client.list_database_names())

    def get_collection(self, collection_name: str) -> pymongo.collection.Collection:
        if collection_name in self.collections_cache:
            return self.collections_cache[collection_name]
        self.check_table(collection_name)
        return self.db[collection_name]

    def add_document(self, doc: dict, collection_name: str) -> str:
        collection = self.get_collection(collection_name)
        insert_one_result: InsertOneResult = collection.insert_one(doc)
        object_id = insert_one_result.inserted_id
        return str(object_id)

    def get_document(self, id: str, collection_name: str) -> dict:
        doc = self.find_one({"_id": ObjectId(id)}, collection_name)
        return doc

    def get_all_documents(self, collection_name: str) -> Cursor:
        return self.find({}, collection_name)

    def clean_all_documents(self, collection_name: str) -> int:
        collection = self.get_collection(collection_name)
        delete_result: DeleteResult = collection.delete_many({})
        return delete_result.deleted_count

    def find_one(self, query: dict, collection_name: str) -> dict:
        collection = self.get_collection(collection_name)
        return collection.find_one(query)

    def find(self, query: dict, collection_name: str) -> List[dict]:
        collection = self.get_collection(collection_name)
        return collection.find(query)

    def delete_one(self, query: dict, collection_name: str) -> int:
        collection = self.get_collection(collection_name)
        delete_result: pymongo.results.DeleteResult = collection.delete_one(query)
        return delete_result.deleted_count

    def delete(self, query: dict, collection_name: str) -> int:
        collection = self.get_collection(collection_name)
        delete_result: pymongo.results.DeleteResult = collection.delete_many(query)
        return delete_result.deleted_count

    def add_candle(self, candle: Candle) -> str:
        serializable_candle = candle.to_serializable_dict()
        serializable_candle["timestamp"] = pd.Timestamp(
            serializable_candle["timestamp"]
        )
        return self.add_document(serializable_candle, CANDLES_COLLECTION_NAME)

    def get_candle(self, id: str) -> Candle:
        candle_dict: dict = self.get_document(id, CANDLES_COLLECTION_NAME)
        if candle_dict is None:
            return None
        return Candle.from_serializable_dict(candle_dict)

    def candle_with_id_exists(self, id: str) -> bool:
        return self.get_candle(id) is not None

    def get_candle_by_identifier(self, asset: Asset, timestamp: datetime) -> Candle:
        query = {"asset": asset.to_dict(), "timestamp": timestamp}
        candle_dict = self.find_one(query, CANDLES_COLLECTION_NAME)
        if candle_dict is None:
            return None
        return Candle.from_serializable_dict(candle_dict)

    def candle_with_identifier_exists(self, asset: Asset, timestamp: datetime) -> bool:
        return self.get_candle_by_identifier(asset, timestamp) is not None

    def get_candles_in_range(
        self, asset: Asset, start: datetime, end: datetime
    ) -> np.array:  # [Candle]
        query = {
            "asset": asset.to_dict(),
            "$and": [{"timestamp": {"$gte": start}}, {"timestamp": {"$lte": end}}],
        }
        cursor = list(
            self.find(query, CANDLES_COLLECTION_NAME).sort(
                [("timestamp", pymongo.ASCENDING)]
            )
        )
        candles = np.empty(shape=len(cursor), dtype=Candle)
        for index, candle_dict in enumerate(cursor):
            candle = Candle.from_serializable_dict(candle_dict)
            candles[index] = candle
        return candles

    def get_all_candles(self) -> np.array:  # [Candle]
        candles_in_dict = list(
            self.get_all_documents(CANDLES_COLLECTION_NAME).sort(
                "timestamp", pymongo.ASCENDING
            )
        )
        candles: np.array = np.empty(shape=len(candles_in_dict), dtype=Candle)
        for index, candle_dict in enumerate(candles_in_dict):
            candle = Candle.from_serializable_dict(candle_dict)
            candles[index] = candle
        return candles

    def clean_all_candles(self) -> int:
        return self.clean_all_documents(CANDLES_COLLECTION_NAME)

    def add_signal(self, signal: Signal) -> str:
        serializable_signal = signal.to_serializable_dict()
        serializable_signal["root_candle_timestamp"] = pd.Timestamp(
            serializable_signal["root_candle_timestamp"]
        )
        serializable_signal["generation_time"] = pd.Timestamp(
            serializable_signal["generation_time"]
        )
        return self.add_document(serializable_signal, SIGNALS_COLLECTION_NAME)

    def get_signal(self, id: str) -> Signal:
        signal_dict: dict = self.get_document(id, SIGNALS_COLLECTION_NAME)
        if signal_dict is None:
            return None
        return Signal.from_serializable_dict(signal_dict)

    def get_signal_by_identifier(
        self, asset: Asset, strategy: str, root_candle_timestamp: datetime
    ) -> Signal:
        query = {
            "asset": asset.to_dict(),
            "strategy": strategy,
            "root_candle_timestamp": root_candle_timestamp,
        }
        signal_dict = self.find_one(query, SIGNALS_COLLECTION_NAME)
        if signal_dict is None:
            return None
        return Signal.from_serializable_dict(signal_dict)

    def get_all_signals(self) -> np.array:  # [Signal]
        signals_in_dict = list(
            self.get_all_documents(SIGNALS_COLLECTION_NAME).sort(
                "generation_time", pymongo.ASCENDING
            )
        )
        signals = np.empty(shape=len(signals_in_dict), dtype=Signal)
        for index, signal_dict in enumerate(signals_in_dict):
            signal = Signal.from_serializable_dict(signal_dict)
            signals[index] = signal
        return signals

    def clean_all_signals(self) -> int:
        return self.clean_all_documents(SIGNALS_COLLECTION_NAME)

    def add_order(self, order: Order) -> str:
        serializable_order = order.to_serializable_dict()
        serializable_order["generation_time"] = pd.Timestamp(
            serializable_order["generation_time"]
        )
        return self.add_document(serializable_order, ORDERS_COLLECTION_NAME)

    def get_order(self, id: str) -> Order:
        order_dict: dict = self.get_document(id, ORDERS_COLLECTION_NAME)
        if order_dict is None:
            return None
        return Order.from_serializable_dict(order_dict)

    def get_order_by_identifier(
        self, signal_id: str, generation_time: datetime
    ) -> Order:
        query = {
            "signal_id": signal_id,
            "generation_time": generation_time,
        }
        order_dict = self.find_one(query, ORDERS_COLLECTION_NAME)
        if order_dict is None:
            return None
        return Order.from_serializable_dict(order_dict)

    def get_all_orders(self) -> np.array:  # [Order]
        orders_in_dict = list(
            self.get_all_documents(ORDERS_COLLECTION_NAME).sort(
                "generation_time", pymongo.ASCENDING
            )
        )
        orders = np.empty(shape=len(orders_in_dict), dtype=Order)
        for index, order_dict in enumerate(orders_in_dict):
            order = Order.from_serializable_dict(order_dict)
            orders[index] = order
        return orders

    def clean_all_orders(self) -> int:
        return self.clean_all_documents(ORDERS_COLLECTION_NAME)

    def add_candle_dataframe(self, candle_dataframe: CandleDataFrame) -> None:
        candles = candle_dataframe.to_candles()
        for candle in candles:
            self.add_candle(candle)

    def count(self, table_name: str) -> int:
        collection = self.get_collection(table_name)
        return collection.count_documents({})

    def close(self):
        self.client.close()
