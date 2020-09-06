import os
from typing import List

import pandas as pd
import pymongo
from bson import ObjectId
from pymongo.results import DeleteResult, InsertOneResult

import settings
from db_storage.db_storage import DbStorage
from logger import logger
from models.action import Action
from models.candle import Candle
from settings import (
    ACTIONS_COLLECTION_NAME,
    CANDLES_COLLECTION_NAME,
    DATABASE_NAME,
    DATABASE_URL,
)

LOG = logger.get_root_logger(
    __name__, filename=os.path.join(settings.ROOT_PATH, "output.log")
)


class MongoDbStorage(DbStorage):
    def check_collection_name(self, collection_name):
        cnames = self.db.list_collection_names()
        if collection_name not in cnames:
            raise Exception(
                "Collection {} should be one of the existing collections ({}) of database {}".format(
                    collection_name, cnames, self.db
                )
            )

    def __init__(
        self, database_name: str = DATABASE_NAME, database_url: str = DATABASE_URL
    ):
        super().__init__(database_name, database_url)
        self.client = pymongo.MongoClient(
            host=database_url, tz_aware=True, serverSelectionTimeoutMS=2000
        )
        # check that the instance is genuine
        dbnames = self.client.list_database_names()

        # check db name
        if database_name not in list(dbnames):
            raise Exception(
                "Database name {} is not one of the existing database names: {}".format(
                    database_name, list(dbnames)
                )
            )
        self.db: pymongo.database.Database = self.client[database_name]

        self.collections_cache = {}

        self.check_collection_name(CANDLES_COLLECTION_NAME)
        self.collections_cache[CANDLES_COLLECTION_NAME] = self.db[
            CANDLES_COLLECTION_NAME
        ]
        self.check_collection_name(ACTIONS_COLLECTION_NAME)
        self.collections_cache[ACTIONS_COLLECTION_NAME] = self.db[
            ACTIONS_COLLECTION_NAME
        ]

    def get_collection(self, collection_name: str) -> pymongo.collection.Collection:
        if collection_name in self.collections_cache:
            return self.collections_cache[collection_name]
        self.check_collection_name(collection_name)
        return self.db[collection_name]

    def add_document(self, doc: dict, collection_name: str) -> str:
        collection = self.get_collection(collection_name)
        insert_one_result: InsertOneResult = collection.insert_one(doc)
        object_id = insert_one_result.inserted_id
        return str(object_id)

    def get_document(self, id: str, collection_name: str) -> dict:
        doc = self.find_one({"_id": ObjectId(id)}, collection_name)
        return doc

    def get_all_documents(self, collection_name: str) -> List[dict]:
        return list(self.find({}, collection_name))

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

    def get_candle_by_identifier(self, symbol: str, timestamp: pd.Timestamp) -> Candle:
        query = {"symbol": symbol, "timestamp": timestamp}
        candle_dict = self.find_one(query, CANDLES_COLLECTION_NAME)
        if candle_dict is None:
            return None
        return Candle.from_serializable_dict(candle_dict)

    def candle_with_identifier_exists(
        self, symbol: str, timestamp: pd.Timestamp
    ) -> bool:
        return self.get_candle_by_identifier(symbol, timestamp) is not None

    def get_candles_in_range(
        self, symbol: str, start: pd.Timestamp, end: pd.Timestamp
    ) -> Candle:
        query = {
            "symbol": symbol,
            "$and": [{"timestamp": {"$gte": start}}, {"timestamp": {"$lte": end}}],
        }
        cursor = self.find(query, CANDLES_COLLECTION_NAME).sort(
            [("timestamp", pymongo.ASCENDING)]
        )
        candles = []
        for candle_dict in cursor:
            candle = Candle.from_serializable_dict(candle_dict)
            candles.append(candle)
        return candles

    def get_all_candles(self) -> List[Candle]:
        candles_in_dict = self.get_all_documents(CANDLES_COLLECTION_NAME)
        candles: List[Candle] = []
        for candle_dict in candles_in_dict:
            candle = Candle.from_serializable_dict(candle_dict)
            candles.append(candle)
        return candles

    def clean_all_candles(self) -> int:
        return self.clean_all_documents(CANDLES_COLLECTION_NAME)

    def add_action(self, action: Action) -> str:
        serializable_action = action.to_serializable_dict()
        serializable_action["candle_timestamp"] = pd.Timestamp(
            serializable_action["candle_timestamp"]
        )
        serializable_action["timestamp"] = pd.Timestamp(
            serializable_action["timestamp"]
        )
        return self.add_document(serializable_action, ACTIONS_COLLECTION_NAME)

    def get_action(self, id: str) -> Action:
        action_dict: dict = self.get_document(id, ACTIONS_COLLECTION_NAME)
        if action_dict is None:
            return None
        return Action.from_serializable_dict(action_dict)

    def get_action_by_identifier(
        self, symbol: str, strategy: str, candle_timestamp: pd.Timestamp
    ) -> Action:
        query = {
            "symbol": symbol,
            "strategy": strategy,
            "candle_timestamp": candle_timestamp,
        }
        action_dict = self.find_one(query, ACTIONS_COLLECTION_NAME)
        if action_dict is None:
            return None
        return Action.from_serializable_dict(action_dict)

    def get_all_actions(self) -> List[Action]:
        actions_in_dict = self.get_all_documents(ACTIONS_COLLECTION_NAME)
        actions = []
        for action_dict in actions_in_dict:
            action = Action.from_serializable_dict(action_dict)
            actions.append(action)
        return actions

    def clean_all_actions(self) -> int:
        return self.clean_all_documents(ACTIONS_COLLECTION_NAME)
