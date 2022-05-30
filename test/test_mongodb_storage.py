from datetime import datetime, timedelta
from typing import List
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest
from bson import ObjectId
from pymongo.results import InsertOneResult

from trazy_analysis.common.clock import SimulatedClock
from trazy_analysis.common.constants import DATE_FORMAT
from trazy_analysis.common.types import CandleDataFrame
from trazy_analysis.db_storage.mongodb_storage import MongoDbStorage
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.enums import Action, Direction
from trazy_analysis.models.order import Order
from trazy_analysis.models.signal import Signal
from trazy_analysis.settings import (
    CANDLES_COLLECTION_NAME,
    DATABASE_NAME,
    DOCUMENTS_COLLECTION_NAME,
    MONGODB_URL,
    ORDERS_COLLECTION_NAME,
    SIGNALS_COLLECTION_NAME,
)
from trazy_analysis.test.tools.tools import (
    compare_candles_list,
    compare_orders_list,
    compare_signals_list,
)

DOC1_KEY = "key1"
DOC1_VALUE = "value1"
DOC1 = {DOC1_KEY: DOC1_VALUE}

DOC2_KEY = "key2"
DOC2_VALUE = "value2"
DOC2 = {DOC2_KEY: DOC2_VALUE}

DOC3_KEY = "key2"
DOC3_VALUE = "value2"
DOC3 = {DOC3_KEY: DOC3_VALUE, DOC1_KEY: DOC1_VALUE}

AAPL_SYMBOL = "AAPL"
AAPL_ASSET = Asset(symbol=AAPL_SYMBOL, exchange="IEX")

CANDLE1: Candle = Candle(asset=AAPL_ASSET, open=10.5, high=10.9, low=10.3, close=10.6, volume=100,
                         timestamp=datetime.strptime("2020-05-08 14:17:00+0000", "%Y-%m-%d %H:%M:%S%z"))

CANDLE2: Candle = Candle(asset=AAPL_ASSET, open=10.4, high=10.8, low=10.4, close=10.5, volume=80,
                         timestamp=datetime.strptime("2020-05-08 14:16:00+0000", "%Y-%m-%d %H:%M:%S%z"))

CANDLE3: Candle = Candle(asset=AAPL_ASSET, open=10.8, high=11.0, low=10.7, close=11.1, volume=110,
                         timestamp=datetime.strptime("2020-05-08 14:37:00+0000", "%Y-%m-%d %H:%M:%S%z"))

clock = SimulatedClock()

clock.update_time(datetime.strptime("2020-05-08 14:17:00+0000", "%Y-%m-%d %H:%M:%S%z"))
SIGNAL1: Signal = Signal(
    asset=AAPL_ASSET,
    action=Action.BUY,
    direction=Direction.LONG,
    confidence_level=0.05,
    strategy="SmaCrossover",
    root_candle_timestamp=datetime.strptime(
        "2020-05-08 14:16:00+0000", "%Y-%m-%d %H:%M:%S%z"
    ),
    parameters={},
    clock=clock,
)

clock = SimulatedClock()
clock.update_time(datetime.strptime("2020-05-08 15:19:00+0000", "%Y-%m-%d %H:%M:%S%z"))
SIGNAL2: Signal = Signal(
    asset=AAPL_ASSET,
    action=Action.SELL,
    direction=Direction.LONG,
    confidence_level=0.05,
    strategy="SmaCrossover",
    root_candle_timestamp=datetime.strptime(
        "2020-05-08 14:17:00+0000", "%Y-%m-%d %H:%M:%S%z"
    ),
    parameters={},
    clock=clock,
)

clock.update_time(datetime.strptime("2020-05-08 14:17:00+0000", "%Y-%m-%d %H:%M:%S%z"))
ORDER1: Order = Order(
    asset=AAPL_ASSET,
    action=Action.BUY,
    direction=Direction.LONG,
    size=100,
    signal_id="1",
    clock=clock,
)

clock = SimulatedClock()
clock.update_time(datetime.strptime("2020-05-08 15:19:00+0000", "%Y-%m-%d %H:%M:%S%z"))
ORDER2: Order = Order(
    asset=AAPL_ASSET,
    action=Action.SELL,
    direction=Direction.LONG,
    size=100,
    signal_id="2",
    clock=clock,
)

MONGODB_STORAGE = MongoDbStorage(DATABASE_NAME, MONGODB_URL)


@patch("pymongo.MongoClient")
def test_check_collection_name_success(mongo_client_mocked):
    client_return_value = mongo_client_mocked.return_value
    client_return_value.list_database_names.return_value = [DATABASE_NAME]

    client_return_value.__getitem__.return_value = MagicMock()
    db = client_return_value.__getitem__.return_value
    db.list_collection_names.return_value = [
        CANDLES_COLLECTION_NAME,
        SIGNALS_COLLECTION_NAME,
        DOCUMENTS_COLLECTION_NAME,
        ORDERS_COLLECTION_NAME,
    ]

    mongodb_storage = MongoDbStorage()
    mongodb_storage.check_table(CANDLES_COLLECTION_NAME)


@patch("pymongo.MongoClient")
def test_check_collection_name_ko(mongo_client_mocked):
    client_return_value = mongo_client_mocked.return_value
    client_return_value.list_database_names.return_value = [DATABASE_NAME]

    client_return_value.__getitem__.return_value = MagicMock()
    db = client_return_value.__getitem__.return_value
    db.list_collection_names.return_value = [
        CANDLES_COLLECTION_NAME,
        SIGNALS_COLLECTION_NAME,
        DOCUMENTS_COLLECTION_NAME,
        ORDERS_COLLECTION_NAME,
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
        SIGNALS_COLLECTION_NAME,
        DOCUMENTS_COLLECTION_NAME,
        ORDERS_COLLECTION_NAME,
    ]

    mongodb_storage = MongoDbStorage()
    assert mongodb_storage.database_name == DATABASE_NAME
    assert mongodb_storage.database_url == MONGODB_URL


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
        SIGNALS_COLLECTION_NAME,
        DOCUMENTS_COLLECTION_NAME,
        ORDERS_COLLECTION_NAME,
    ]

    mongodb_storage = MongoDbStorage(database_name, database_url)
    assert mongodb_storage.database_name == database_name
    assert mongodb_storage.database_url == database_url

    mongo_client_calls = [
        call(
            connect=False,
            host=database_url,
            tz_aware=True,
            serverSelectionTimeoutMS=2000,
        )
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
        SIGNALS_COLLECTION_NAME,
        DOCUMENTS_COLLECTION_NAME,
        ORDERS_COLLECTION_NAME,
    ]

    with pytest.raises(Exception):
        MongoDbStorage(database_url=database_url, database_name=database_name1)

    mongo_client_calls = [
        call(
            connect=False,
            host=database_url,
            tz_aware=True,
            serverSelectionTimeoutMS=2000,
        )
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
        SIGNALS_COLLECTION_NAME,
        DOCUMENTS_COLLECTION_NAME,
        ORDERS_COLLECTION_NAME,
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
        SIGNALS_COLLECTION_NAME,
        DOCUMENTS_COLLECTION_NAME,
        ORDERS_COLLECTION_NAME,
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
        SIGNALS_COLLECTION_NAME,
        DOCUMENTS_COLLECTION_NAME,
        ORDERS_COLLECTION_NAME,
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
        SIGNALS_COLLECTION_NAME,
        DOCUMENTS_COLLECTION_NAME,
        ORDERS_COLLECTION_NAME,
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
    assert MONGODB_STORAGE.count(DOCUMENTS_COLLECTION_NAME) > 0
    MONGODB_STORAGE.clean_all_documents(DOCUMENTS_COLLECTION_NAME)
    assert MONGODB_STORAGE.count(DOCUMENTS_COLLECTION_NAME) == 0


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
    assert len(list(docs)) == 2

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


@patch("trazy_analysis.db_storage.mongodb_storage.MongoDbStorage.add_document")
def test_add_candle(add_document_mocked):
    mongodb_storage = MongoDbStorage()

    mongodb_storage.add_candle(CANDLE1)

    serializable_candle = CANDLE1.to_serializable_dict()
    serializable_candle["timestamp"] = pd.Timestamp(serializable_candle["timestamp"])
    add_document_calls = [call(serializable_candle, CANDLES_COLLECTION_NAME)]
    add_document_mocked.assert_has_calls(add_document_calls)


def test_clean_all_candles():
    MONGODB_STORAGE.add_candle(CANDLE1)
    MONGODB_STORAGE.add_candle(CANDLE2)
    assert MONGODB_STORAGE.count(CANDLES_COLLECTION_NAME) > 0
    MONGODB_STORAGE.clean_all_candles()
    assert MONGODB_STORAGE.count(CANDLES_COLLECTION_NAME) == 0


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
    candle: Candle = MONGODB_STORAGE.get_candle_by_identifier(CANDLE1.asset, CANDLE1.timestamp)
    assert candle == CANDLE1

    MONGODB_STORAGE.clean_all_candles()


def test_get_candle_by_identifier_non_existing_identifier_non_existing_candle():
    MONGODB_STORAGE.clean_all_candles()

    candle: Candle = MONGODB_STORAGE.get_candle_by_identifier(CANDLE1.asset, CANDLE1.timestamp)
    assert candle is None


def test_candle_with_identifier_exists_true():
    MONGODB_STORAGE.clean_all_candles()

    MONGODB_STORAGE.add_candle(CANDLE1)
    assert MONGODB_STORAGE.candle_with_identifier_exists(
        CANDLE1.asset, CANDLE1.timestamp
    )

    MONGODB_STORAGE.clean_all_candles()


def test_candle_with_identifier_exists_false():
    MONGODB_STORAGE.clean_all_candles()

    assert not MONGODB_STORAGE.candle_with_identifier_exists(
        CANDLE1.asset, CANDLE1.timestamp
    )


def test_get_candles_in_range():
    MONGODB_STORAGE.clean_all_candles()

    MONGODB_STORAGE.add_candle(CANDLE1)
    MONGODB_STORAGE.add_candle(CANDLE2)
    MONGODB_STORAGE.add_candle(CANDLE3)
    candles: List[Candle] = MONGODB_STORAGE.get_candles_in_range(CANDLE1.asset,
                                                                 CANDLE1.timestamp - timedelta(minutes=1),
                                                                 CANDLE1.timestamp + timedelta(minutes=1))
    assert compare_candles_list(candles, [CANDLE2, CANDLE1])

    MONGODB_STORAGE.clean_all_candles()


def test_get_all_candles():
    MONGODB_STORAGE.clean_all_candles()

    MONGODB_STORAGE.add_candle(CANDLE1)
    MONGODB_STORAGE.add_candle(CANDLE2)

    candles = MONGODB_STORAGE.get_all_candles()
    assert compare_candles_list(candles, [CANDLE2, CANDLE1])

    MONGODB_STORAGE.clean_all_candles()


@patch("trazy_analysis.db_storage.mongodb_storage.MongoDbStorage.add_document")
def test_add_signal(add_document_mocked):
    mongodb_storage = MongoDbStorage()

    mongodb_storage.add_signal(SIGNAL1)

    serializable_signal = SIGNAL1.to_serializable_dict()
    serializable_signal["root_candle_timestamp"] = pd.Timestamp(
        serializable_signal["root_candle_timestamp"]
    )
    serializable_signal["generation_time"] = pd.Timestamp(
        serializable_signal["generation_time"]
    )
    add_document_calls = [call(serializable_signal, SIGNALS_COLLECTION_NAME)]
    add_document_mocked.assert_has_calls(add_document_calls)


def test_clean_all_signals():
    MONGODB_STORAGE.add_signal(SIGNAL1)
    assert MONGODB_STORAGE.count(SIGNALS_COLLECTION_NAME) > 0
    MONGODB_STORAGE.clean_all_signals()
    assert MONGODB_STORAGE.count(SIGNALS_COLLECTION_NAME) == 0


def test_get_signal():
    MONGODB_STORAGE.clean_all_signals()

    id = MONGODB_STORAGE.add_signal(SIGNAL1)
    signal: Signal = MONGODB_STORAGE.get_signal(id)
    assert signal == SIGNAL1

    MONGODB_STORAGE.clean_all_signals()


def test_get_signal_by_identifier():
    MONGODB_STORAGE.clean_all_signals()

    MONGODB_STORAGE.add_signal(SIGNAL1)
    signal: Signal = MONGODB_STORAGE.get_signal_by_identifier(SIGNAL1.asset, SIGNAL1.strategy,
                                                              SIGNAL1.root_candle_timestamp)
    assert signal == SIGNAL1

    MONGODB_STORAGE.clean_all_signals()


def test_get_signal_by_identifier_non_existing_signal():
    MONGODB_STORAGE.clean_all_signals()

    signal: Signal = MONGODB_STORAGE.get_signal_by_identifier(SIGNAL1.asset, SIGNAL1.strategy,
                                                              SIGNAL1.root_candle_timestamp)
    assert signal is None


def test_get_all_signals():
    MONGODB_STORAGE.clean_all_signals()

    MONGODB_STORAGE.add_signal(SIGNAL1)
    MONGODB_STORAGE.add_signal(SIGNAL2)

    signals = MONGODB_STORAGE.get_all_signals()
    assert compare_signals_list(signals, [SIGNAL1, SIGNAL2])

    MONGODB_STORAGE.clean_all_signals()


def test_get_signal_non_existing_id():
    MONGODB_STORAGE.clean_all_signals()

    id = "5f262523794d9a0a2816645b"
    signal: Signal = MONGODB_STORAGE.get_signal(id)
    assert signal is None


@patch("trazy_analysis.db_storage.mongodb_storage.MongoDbStorage.add_document")
def test_add_order(add_document_mocked):
    mongodb_storage = MongoDbStorage()

    mongodb_storage.add_order(ORDER1)

    serializable_order = ORDER1.to_serializable_dict()
    serializable_order["generation_time"] = pd.Timestamp(
        serializable_order["generation_time"]
    )
    add_document_calls = [call(serializable_order, ORDERS_COLLECTION_NAME)]
    add_document_mocked.assert_has_calls(add_document_calls)


def test_clean_all_orders():
    MONGODB_STORAGE.add_order(ORDER1)
    assert MONGODB_STORAGE.count(ORDERS_COLLECTION_NAME) > 0
    MONGODB_STORAGE.clean_all_orders()
    assert MONGODB_STORAGE.count(ORDERS_COLLECTION_NAME) == 0


def test_get_order():
    MONGODB_STORAGE.clean_all_orders()

    id = MONGODB_STORAGE.add_order(ORDER1)
    order: Order = MONGODB_STORAGE.get_order(id)
    assert order == ORDER1

    MONGODB_STORAGE.clean_all_orders()


def test_get_order_by_identifier():
    MONGODB_STORAGE.clean_all_orders()

    MONGODB_STORAGE.add_order(ORDER1)
    order: Order = MONGODB_STORAGE.get_order_by_identifier(
        ORDER1.signal_id, ORDER1.generation_time
    )
    assert order == ORDER1

    MONGODB_STORAGE.clean_all_orders()


def test_get_order_by_identifier_non_existing_order():
    MONGODB_STORAGE.clean_all_orders()

    order: Order = MONGODB_STORAGE.get_order_by_identifier(
        ORDER1.signal_id, ORDER1.generation_time
    )
    assert order is None


def test_get_all_orders():
    MONGODB_STORAGE.clean_all_orders()

    MONGODB_STORAGE.add_order(ORDER1)
    MONGODB_STORAGE.add_order(ORDER2)

    orders = MONGODB_STORAGE.get_all_orders()
    assert compare_orders_list(orders, [ORDER1, ORDER2])

    MONGODB_STORAGE.clean_all_orders()


def test_get_order_non_existing_id():
    MONGODB_STORAGE.clean_all_orders()

    id = "5f262523794d9a0a2816645b"
    order: Order = MONGODB_STORAGE.get_order(id)
    assert order is None


def test_add_candle_dataframe():
    df_candles = {
        "timestamp": [
            "2020-05-07 14:24:00+00:00",
            "2020-05-07 14:25:00+00:00",
            "2020-05-07 14:26:00+00:00",
            "2020-05-07 14:31:00+00:00",
        ],
        "open": ["323.69", "323.81", "324.10", "323.93"],
        "high": [
            "323.81",
            "324.21",
            "324.10",
            "323.95",
        ],
        "low": ["323.67", "323.81", "323.97", "323.83"],
        "close": [
            "323.81",
            "324.10",
            "324.03",
            "323.88",
        ],
        "volume": [500, 700, 400, 300],
    }
    df = pd.DataFrame(
        df_candles,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    df.index = pd.to_datetime(df.timestamp, format=DATE_FORMAT)
    df = df.drop(["timestamp"], axis=1)

    MONGODB_STORAGE.clean_all_candles()
    candle_dataframe = CandleDataFrame.from_dataframe(df, Asset("AAPL", exchange="IEX"))
    MONGODB_STORAGE.add_candle_dataframe(candle_dataframe)

    expected_candles = candle_dataframe.to_candles()
    candles = MONGODB_STORAGE.get_all_candles()

    assert (candles == expected_candles).all(axis=None)

    MONGODB_STORAGE.clean_all_candles()


def test_count():
    MONGODB_STORAGE.clean_all_candles()

    MONGODB_STORAGE.add_candle(CANDLE1)
    MONGODB_STORAGE.add_candle(CANDLE2)
    MONGODB_STORAGE.add_candle(CANDLE3)

    assert MONGODB_STORAGE.count(CANDLES_COLLECTION_NAME) == 3

    MONGODB_STORAGE.clean_all_candles()
