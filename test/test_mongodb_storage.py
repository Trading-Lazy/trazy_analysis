import time
from decimal import Decimal
from typing import List
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest
from bson import ObjectId
from pymongo.results import InsertOneResult

from db_storage.mongodb_storage import MongoDbStorage
from models.action import Action
from models.candle import Candle
from models.enums import ActionType, PositionType
from settings import (
    ACTIONS_COLLECTION_NAME,
    CANDLES_COLLECTION_NAME,
    DATABASE_NAME,
    DATABASE_URL,
    DOCUMENTS_COLLECTION_NAME,
)
from test.tools.tools import compare_actions_list, compare_candles_list

DOC1_KEY = "key1"
DOC1_VALUE = "value1"
DOC1 = {DOC1_KEY: DOC1_VALUE}

DOC2_KEY = "key2"
DOC2_VALUE = "value2"
DOC2 = {DOC2_KEY: DOC2_VALUE}

DOC3_KEY = "key2"
DOC3_VALUE = "value2"
DOC3 = {DOC3_KEY: DOC3_VALUE, DOC1_KEY: DOC1_VALUE}

CANDLE1: Candle = Candle(
    symbol="AAPL",
    open=Decimal("10.5"),
    high=Decimal("10.9"),
    low=Decimal("10.3"),
    close=Decimal("10.6"),
    volume=100,
    timestamp=pd.Timestamp("2020-05-08 14:17:00", tz="UTC"),
)

CANDLE2: Candle = Candle(
    symbol="AAPL",
    open=Decimal("10.4"),
    high=Decimal("10.8"),
    low=Decimal("10.4"),
    close=Decimal("10.5"),
    volume=80,
    timestamp=pd.Timestamp("2020-05-08 14:16:00", tz="UTC"),
)

CANDLE3: Candle = Candle(
    symbol="AAPL",
    open=Decimal("10.8"),
    high=Decimal("11.0"),
    low=Decimal("10.7"),
    close=Decimal("11.1"),
    volume=110,
    timestamp=pd.Timestamp("2020-05-08 14:37:00", tz="UTC"),
)

ACTION1: Action = Action(
    action_type=ActionType.BUY,
    position_type=PositionType.LONG,
    size=100,
    confidence_level=Decimal("0.05"),
    strategy="SmaCrossover",
    symbol="AAPL",
    candle_timestamp=pd.Timestamp("2020-05-08 14:16:00", tz="UTC"),
    parameters={},
    timestamp=pd.Timestamp("2020-05-08 14:17:00", tz="UTC"),
)

ACTION2: Action = Action(
    action_type=ActionType.SELL,
    position_type=PositionType.LONG,
    size=100,
    confidence_level=Decimal("0.05"),
    strategy="SmaCrossover",
    symbol="AAPL",
    candle_timestamp=pd.Timestamp("2020-05-08 14:17:00", tz="UTC"),
    parameters={},
    timestamp=pd.Timestamp("2020-05-08 15:19:00", tz="UTC"),
)

MONGODB_STORAGE = MongoDbStorage(DATABASE_NAME, DATABASE_URL)


@patch("pymongo.MongoClient")
def test_check_collection_name_success(mongo_client_mocked):
    client_return_value = mongo_client_mocked.return_value
    client_return_value.list_database_names.return_value = [DATABASE_NAME]

    client_return_value.__getitem__.return_value = MagicMock()
    db = client_return_value.__getitem__.return_value
    db.list_collection_names.return_value = [
        CANDLES_COLLECTION_NAME,
        ACTIONS_COLLECTION_NAME,
    ]

    mongodb_storage = MongoDbStorage()
    mongodb_storage.check_collection_name(CANDLES_COLLECTION_NAME)


@patch("pymongo.MongoClient")
def test_check_collection_name_ko(mongo_client_mocked):
    client_return_value = mongo_client_mocked.return_value
    client_return_value.list_database_names.return_value = [DATABASE_NAME]

    client_return_value.__getitem__.return_value = MagicMock()
    db = client_return_value.__getitem__.return_value
    db.list_collection_names.return_value = [
        CANDLES_COLLECTION_NAME,
        ACTIONS_COLLECTION_NAME,
    ]

    mongodb_storage = MongoDbStorage()
    collection_name = "collection"

    with pytest.raises(Exception):
        mongodb_storage.check_collection_name(collection_name)


@patch("pymongo.MongoClient")
def test_init_default_args(mongo_client_mocked):
    client_return_value = mongo_client_mocked.return_value
    client_return_value.list_database_names.return_value = [DATABASE_NAME]

    client_return_value.__getitem__.return_value = MagicMock()
    db = client_return_value.__getitem__.return_value
    db.list_collection_names.return_value = [
        CANDLES_COLLECTION_NAME,
        ACTIONS_COLLECTION_NAME,
    ]

    mongodb_storage = MongoDbStorage()
    assert mongodb_storage.database_name == DATABASE_NAME
    assert mongodb_storage.database_url == DATABASE_URL


@patch("pymongo.MongoClient")
def test_init(mongo_client_mocked):
    database_name = "database_name"
    database_url = "database_url"
    client_return_value = mongo_client_mocked.return_value
    client_return_value.list_database_names.return_value = [database_name]

    client_return_value.__getitem__.return_value = MagicMock()
    db = client_return_value.__getitem__.return_value
    db.list_collection_names.return_value = [
        CANDLES_COLLECTION_NAME,
        ACTIONS_COLLECTION_NAME,
    ]

    mongodb_storage = MongoDbStorage(database_name, database_url)
    assert mongodb_storage.database_name == database_name
    assert mongodb_storage.database_url == database_url

    mongo_client_calls = [
        call(host=database_url, tz_aware=True, serverSelectionTimeoutMS=2000)
    ]
    mongo_client_mocked.assert_has_calls(mongo_client_calls)


def test_init_non_existing_database_url():
    database_url = "database_url"
    with pytest.raises(Exception):
        mongodb_storage = MongoDbStorage(database_url=database_url)


@patch("pymongo.MongoClient")
def test_init_non_existing_database_name(mongo_client_mocked):
    database_name1 = "database_name1"
    database_name2 = "database_name2"
    database_url = "database_url"
    client_return_value = mongo_client_mocked.return_value
    client_return_value.list_database_names.return_value = [database_name2]

    client_return_value.__getitem__.return_value = MagicMock()
    db = client_return_value.__getitem__.return_value
    db.list_collection_names.return_value = [
        CANDLES_COLLECTION_NAME,
        ACTIONS_COLLECTION_NAME,
    ]

    with pytest.raises(Exception):
        mongodb_storage = MongoDbStorage(
            database_url=database_url, database_name=database_name1
        )

    mongo_client_calls = [
        call(host=database_url, tz_aware=True, serverSelectionTimeoutMS=2000)
    ]
    mongo_client_mocked.assert_has_calls(mongo_client_calls)


@patch("pymongo.MongoClient")
def test_get_collection_in_cache(mongo_client_mocked):
    client_return_value = mongo_client_mocked.return_value
    client_return_value.list_database_names.return_value = [DATABASE_NAME]

    client_return_value.__getitem__.return_value = MagicMock()
    db = client_return_value.__getitem__.return_value
    db.list_collection_names.return_value = [
        CANDLES_COLLECTION_NAME,
        ACTIONS_COLLECTION_NAME,
    ]

    mongodb_storage = MongoDbStorage()
    db.__getitem__ = MagicMock()
    mongodb_storage.get_collection(CANDLES_COLLECTION_NAME)

    db.__getitem__.assert_not_called()


@patch("pymongo.MongoClient")
def test_get_collection_not_in_cache_and_in_db(mongo_client_mocked):
    client_return_value = mongo_client_mocked.return_value
    client_return_value.list_database_names.return_value = [DATABASE_NAME]

    client_return_value.__getitem__.return_value = MagicMock()
    db = client_return_value.__getitem__.return_value
    collection_name = "collection"
    db.list_collection_names.return_value = [
        CANDLES_COLLECTION_NAME,
        ACTIONS_COLLECTION_NAME,
        collection_name,
    ]

    mongodb_storage = MongoDbStorage()
    db.__getitem__ = MagicMock()
    mongodb_storage.get_collection(collection_name)

    db_getitem_calls = [call(collection_name)]
    db.__getitem__.assert_has_calls(db_getitem_calls)


@patch("pymongo.MongoClient")
def test_get_collection_not_in_cache_and_not_in_db(mongo_client_mocked):
    client_return_value = mongo_client_mocked.return_value
    client_return_value.list_database_names.return_value = [DATABASE_NAME]

    client_return_value.__getitem__.return_value = MagicMock()
    db = client_return_value.__getitem__.return_value

    collection1_name = "collection1"
    collection2_name = "collection2"
    db.list_collection_names.return_value = [
        CANDLES_COLLECTION_NAME,
        ACTIONS_COLLECTION_NAME,
        collection2_name,
    ]

    with pytest.raises(Exception):
        MONGODB_STORAGE.get_collection(collection1_name)


@patch("pymongo.MongoClient")
def test_add_document(mongo_client_mocked):
    client_return_value = mongo_client_mocked.return_value
    client_return_value.list_database_names.return_value = [DATABASE_NAME]

    # mock db
    client_return_value.__getitem__.return_value = MagicMock()
    db = client_return_value.__getitem__.return_value

    collection_name = "collection"
    db.list_collection_names.return_value = [
        CANDLES_COLLECTION_NAME,
        ACTIONS_COLLECTION_NAME,
        collection_name,
    ]
    db.__getitem__.return_value = MagicMock()
    collection = db.__getitem__.return_value

    mongodb_storage = MongoDbStorage()

    collection.insert_one.return_value = InsertOneResult(DOC1_KEY, True)

    mongodb_storage.get_collection(collection_name)
    assert DOC1_KEY == mongodb_storage.add_document(DOC1, collection_name)

    insert_one_calls = [call(DOC1)]
    collection.insert_one.assert_has_calls(insert_one_calls)


def test_clean_all_documents():

    MONGODB_STORAGE.add_document(DOC1, DOCUMENTS_COLLECTION_NAME)
    collection = MONGODB_STORAGE.get_collection(DOCUMENTS_COLLECTION_NAME)
    assert collection.count_documents({}) > 0
    MONGODB_STORAGE.clean_all_documents(DOCUMENTS_COLLECTION_NAME)
    assert collection.count_documents({}) == 0


def test_find_one():
    MONGODB_STORAGE.clean_all_documents(DOCUMENTS_COLLECTION_NAME)

    id1 = MONGODB_STORAGE.add_document(DOC1, DOCUMENTS_COLLECTION_NAME)
    MONGODB_STORAGE.add_document(DOC2, DOCUMENTS_COLLECTION_NAME)

    expected_doc1 = DOC1.copy()
    expected_doc1["_id"] = ObjectId(id1)

    results = MONGODB_STORAGE.find_one(
        {DOC1_KEY: DOC1_VALUE}, DOCUMENTS_COLLECTION_NAME
    )

    assert results == expected_doc1
    MONGODB_STORAGE.clean_all_documents(DOCUMENTS_COLLECTION_NAME)


def test_find():

    MONGODB_STORAGE.clean_all_documents(DOCUMENTS_COLLECTION_NAME)

    id1 = MONGODB_STORAGE.add_document(DOC1, DOCUMENTS_COLLECTION_NAME)
    id3 = MONGODB_STORAGE.add_document(DOC3, DOCUMENTS_COLLECTION_NAME)

    expected_doc1 = DOC1.copy()
    expected_doc1["_id"] = ObjectId(id1)

    expected_doc3 = DOC3.copy()
    expected_doc3["_id"] = ObjectId(id3)

    results = list(
        MONGODB_STORAGE.find({DOC1_KEY: DOC1_VALUE}, DOCUMENTS_COLLECTION_NAME)
    )

    assert len(results) == 2
    assert results == [expected_doc1, expected_doc3]
    MONGODB_STORAGE.clean_all_documents(DOCUMENTS_COLLECTION_NAME)


def test_delete_one():

    MONGODB_STORAGE.clean_all_documents(DOCUMENTS_COLLECTION_NAME)

    MONGODB_STORAGE.add_document(DOC1, DOCUMENTS_COLLECTION_NAME)
    MONGODB_STORAGE.add_document(DOC3, DOCUMENTS_COLLECTION_NAME)

    deleted_count = MONGODB_STORAGE.delete_one(
        {DOC1_KEY: DOC1_VALUE}, DOCUMENTS_COLLECTION_NAME
    )

    assert deleted_count == 1
    MONGODB_STORAGE.clean_all_documents(DOCUMENTS_COLLECTION_NAME)


def test_delete():

    MONGODB_STORAGE.clean_all_documents(DOCUMENTS_COLLECTION_NAME)

    MONGODB_STORAGE.add_document(DOC1, DOCUMENTS_COLLECTION_NAME)
    MONGODB_STORAGE.add_document(DOC2, DOCUMENTS_COLLECTION_NAME)
    MONGODB_STORAGE.add_document(DOC3, DOCUMENTS_COLLECTION_NAME)

    deleted_count = MONGODB_STORAGE.delete(
        {DOC1_KEY: DOC1_VALUE}, DOCUMENTS_COLLECTION_NAME
    )

    assert deleted_count == 2
    MONGODB_STORAGE.clean_all_documents(DOCUMENTS_COLLECTION_NAME)


def test_get_all_documents():

    MONGODB_STORAGE.clean_all_documents(DOCUMENTS_COLLECTION_NAME)

    MONGODB_STORAGE.add_document(DOC1, DOCUMENTS_COLLECTION_NAME)
    MONGODB_STORAGE.add_document(DOC2, DOCUMENTS_COLLECTION_NAME)

    docs = MONGODB_STORAGE.get_all_documents(DOCUMENTS_COLLECTION_NAME)
    assert len(docs) == 2

    MONGODB_STORAGE.clean_all_documents(DOCUMENTS_COLLECTION_NAME)


def test_get_document_by_id():

    MONGODB_STORAGE.clean_all_documents(CANDLES_COLLECTION_NAME)

    id = MONGODB_STORAGE.add_document(DOC1, CANDLES_COLLECTION_NAME)
    doc = MONGODB_STORAGE.get_document(id, CANDLES_COLLECTION_NAME)
    expected_doc = DOC1.copy()
    expected_doc["_id"] = ObjectId(id)
    assert doc == expected_doc
    MONGODB_STORAGE.clean_all_documents(CANDLES_COLLECTION_NAME)


def test_get_document_by_id_non_existing_id():

    MONGODB_STORAGE.clean_all_documents(CANDLES_COLLECTION_NAME)

    id = "5f262523794d9a0a2816645b"
    doc = MONGODB_STORAGE.get_document(id, CANDLES_COLLECTION_NAME)
    assert doc is None


@patch("db_storage.mongodb_storage.MongoDbStorage.add_document")
def test_add_candle(add_document_mocked):
    mongodb_storage = MongoDbStorage()

    mongodb_storage.add_candle(CANDLE1)

    serializable_candle = CANDLE1.to_serializable_dict()
    serializable_candle["timestamp"] = pd.Timestamp(serializable_candle["timestamp"])
    add_document_calls = [call(serializable_candle, CANDLES_COLLECTION_NAME)]
    add_document_mocked.assert_has_calls(add_document_calls)


def test_clean_all_candles():

    MONGODB_STORAGE.add_candle(CANDLE1)
    collection = MONGODB_STORAGE.get_collection(CANDLES_COLLECTION_NAME)
    assert collection.count_documents({}) > 0
    MONGODB_STORAGE.clean_all_candles()
    assert collection.count_documents({}) == 0


def test_get_candle():

    MONGODB_STORAGE.clean_all_candles()

    id = MONGODB_STORAGE.add_candle(CANDLE1)
    candle: Candle = MONGODB_STORAGE.get_candle(id)
    assert candle == CANDLE1

    MONGODB_STORAGE.clean_all_candles()


def test_get_candle_non_existing_id():

    MONGODB_STORAGE.clean_all_candles()

    id = "5f262523794d9a0a2816645b"
    candle: Candle = MONGODB_STORAGE.get_candle(id)
    assert candle is None


def test_candle_with_id_exists_true():

    MONGODB_STORAGE.clean_all_candles()

    id = MONGODB_STORAGE.add_candle(CANDLE1)
    assert MONGODB_STORAGE.candle_with_id_exists(id)

    MONGODB_STORAGE.clean_all_candles()


def test_candle_with_id_exists_false():

    MONGODB_STORAGE.clean_all_candles()

    id = "5f262523794d9a0a2816645b"
    assert not MONGODB_STORAGE.candle_with_id_exists(id)


def test_get_candle_by_identifier():

    MONGODB_STORAGE.clean_all_candles()

    MONGODB_STORAGE.add_candle(CANDLE1)
    candle: Candle = MONGODB_STORAGE.get_candle_by_identifier(
        CANDLE1.symbol, CANDLE1.timestamp
    )
    assert candle == CANDLE1

    MONGODB_STORAGE.clean_all_candles()


def test_get_candle_by_identifier_non_existing_identifier_non_existing_candle():

    MONGODB_STORAGE.clean_all_candles()

    candle: Candle = MONGODB_STORAGE.get_candle_by_identifier(
        CANDLE1.symbol, CANDLE1.timestamp
    )
    assert candle is None


def test_candle_with_identifier_exists_true():

    MONGODB_STORAGE.clean_all_candles()

    MONGODB_STORAGE.add_candle(CANDLE1)
    assert MONGODB_STORAGE.candle_with_identifier_exists(
        CANDLE1.symbol, CANDLE1.timestamp
    )

    MONGODB_STORAGE.clean_all_candles()


def test_candle_with_identifier_exists_false():

    MONGODB_STORAGE.clean_all_candles()

    assert not MONGODB_STORAGE.candle_with_identifier_exists(
        CANDLE1.symbol, CANDLE1.timestamp
    )


def test_get_candles_in_range():

    MONGODB_STORAGE.clean_all_candles()

    MONGODB_STORAGE.add_candle(CANDLE1)
    MONGODB_STORAGE.add_candle(CANDLE2)
    MONGODB_STORAGE.add_candle(CANDLE3)
    start = time.time()
    candles: List[Candle] = MONGODB_STORAGE.get_candles_in_range(
        CANDLE1.symbol,
        CANDLE1.timestamp - pd.offsets.Minute(1),
        CANDLE1.timestamp + pd.offsets.Minute(1),
    )
    end = time.time()
    assert compare_candles_list(candles, [CANDLE2, CANDLE1])

    MONGODB_STORAGE.clean_all_candles()


def test_get_all_candles():

    MONGODB_STORAGE.clean_all_candles()

    MONGODB_STORAGE.add_candle(CANDLE1)
    MONGODB_STORAGE.add_candle(CANDLE2)

    candles = MONGODB_STORAGE.get_all_candles()
    assert compare_candles_list(candles, [CANDLE1, CANDLE2])

    MONGODB_STORAGE.clean_all_candles()


@patch("db_storage.mongodb_storage.MongoDbStorage.add_document")
def test_add_action(add_document_mocked):
    mongodb_storage = MongoDbStorage()

    mongodb_storage.add_action(ACTION1)

    serializable_action = ACTION1.to_serializable_dict()
    serializable_action["candle_timestamp"] = pd.Timestamp(
        serializable_action["candle_timestamp"]
    )
    serializable_action["timestamp"] = pd.Timestamp(serializable_action["timestamp"])
    add_document_calls = [call(serializable_action, ACTIONS_COLLECTION_NAME)]
    add_document_mocked.assert_has_calls(add_document_calls)


def test_clean_all_actions():

    MONGODB_STORAGE.add_action(ACTION1)
    collection = MONGODB_STORAGE.get_collection(ACTIONS_COLLECTION_NAME)
    assert collection.count_documents({}) > 0
    MONGODB_STORAGE.clean_all_actions()
    assert collection.count_documents({}) == 0


def test_get_action():

    MONGODB_STORAGE.clean_all_actions()

    id = MONGODB_STORAGE.add_action(ACTION1)
    action: Action = MONGODB_STORAGE.get_action(id)
    assert action == ACTION1

    MONGODB_STORAGE.clean_all_actions()


def test_get_action_by_identifier():

    MONGODB_STORAGE.clean_all_actions()

    MONGODB_STORAGE.add_action(ACTION1)
    action: Action = MONGODB_STORAGE.get_action_by_identifier(
        ACTION1.symbol, ACTION1.strategy, ACTION1.candle_timestamp
    )
    assert action == ACTION1

    MONGODB_STORAGE.clean_all_actions()


def test_get_action_by_identifier_non_existing_action():

    MONGODB_STORAGE.clean_all_actions()

    action: Action = MONGODB_STORAGE.get_action_by_identifier(
        ACTION1.symbol, ACTION1.strategy, ACTION1.candle_timestamp
    )
    assert action is None


def test_get_all_actions():

    MONGODB_STORAGE.clean_all_actions()

    MONGODB_STORAGE.add_action(ACTION1)
    MONGODB_STORAGE.add_action(ACTION2)

    actions = MONGODB_STORAGE.get_all_actions()
    assert compare_actions_list(actions, [ACTION1, ACTION2])

    MONGODB_STORAGE.clean_all_actions()


def test_get_action_non_existing_id():

    MONGODB_STORAGE.clean_all_actions()

    id = "5f262523794d9a0a2816645b"
    action: Action = MONGODB_STORAGE.get_action(id)
    assert action is None
