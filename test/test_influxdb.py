from datetime import datetime, timedelta
from typing import List
from unittest.mock import call, patch

import pandas as pd
import pytest

from trazy_analysis.common.clock import SimulatedClock
from trazy_analysis.common.constants import DATE_FORMAT
from trazy_analysis.common.types import CandleDataFrame
from trazy_analysis.db_storage.influxdb_storage import InfluxDbStorage
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.enums import Action, Direction
from trazy_analysis.models.order import Order
from trazy_analysis.models.signal import Signal
from trazy_analysis.settings import (
    CANDLES_COLLECTION_NAME,
    DATABASE_NAME,
    INFLUXDB_URL,
    ORDERS_COLLECTION_NAME,
    SIGNALS_COLLECTION_NAME,
)
from trazy_analysis.test.tools.tools import (
    compare_candles_list,
    compare_orders_list,
    compare_signals_list,
)

AAPL_SYMBOL = "AAPL"
AAPL_ASSET = Asset(symbol=AAPL_SYMBOL, exchange="IEX")

CANDLE1: Candle = Candle(
    asset=AAPL_ASSET,
    open=10.5,
    high=10.9,
    low=10.3,
    close=10.6,
    volume=100,
    timestamp=datetime.strptime("2020-05-08 14:17:00+0000", "%Y-%m-%d %H:%M:%S%z"),
)

CANDLE2: Candle = Candle(
    asset=AAPL_ASSET,
    open=10.4,
    high=10.8,
    low=10.4,
    close=10.5,
    volume=80,
    timestamp=datetime.strptime("2020-05-08 14:16:00+0000", "%Y-%m-%d %H:%M:%S%z"),
)

CANDLE3: Candle = Candle(
    asset=AAPL_ASSET,
    open=10.8,
    high=11.0,
    low=10.7,
    close=11.1,
    volume=110,
    timestamp=datetime.strptime("2020-05-08 14:37:00+0000", "%Y-%m-%d %H:%M:%S%z"),
)

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

INFLUXDB_STORAGE = InfluxDbStorage(DATABASE_NAME, INFLUXDB_URL)


@patch("influxdb.client.InfluxDBClient.get_list_measurements")
@patch("influxdb.client.InfluxDBClient.get_list_database")
@patch("influxdb.client.InfluxDBClient.__init__")
def test_check_table_success(
    influx_client_init_mocked, get_list_database_mocked, get_list_measurements_mocked
):
    influx_client_init_mocked.return_value = None
    get_list_database_mocked.return_value = [{"name": DATABASE_NAME}]

    get_list_measurements_mocked.return_value = [
        {"name": CANDLES_COLLECTION_NAME},
        {"name": SIGNALS_COLLECTION_NAME},
        {"name": ORDERS_COLLECTION_NAME},
    ]

    influxdb_storage = InfluxDbStorage()
    influxdb_storage.check_table(CANDLES_COLLECTION_NAME)


@patch("influxdb.client.InfluxDBClient.get_list_measurements")
@patch("influxdb.client.InfluxDBClient.get_list_database")
@patch("influxdb.client.InfluxDBClient.__init__")
def test_check_table_ko(
    influx_client_init_mocked, get_list_database_mocked, get_list_measurements_mocked
):
    influx_client_init_mocked.return_value = None
    get_list_database_mocked.return_value = [{"name": DATABASE_NAME}]

    get_list_measurements_mocked.return_value = [
        {"name": CANDLES_COLLECTION_NAME},
        {"name": SIGNALS_COLLECTION_NAME},
        {"name": ORDERS_COLLECTION_NAME},
    ]

    influxdb_storage = InfluxDbStorage()
    collection_name = "collection"

    with pytest.raises(Exception):
        influxdb_storage.check_table(collection_name)


@patch("influxdb.client.InfluxDBClient.get_list_measurements")
@patch("influxdb.client.InfluxDBClient.get_list_database")
@patch("influxdb.client.InfluxDBClient.__init__")
def test_init_default_args(
    influx_client_init_mocked, get_list_database_mocked, get_list_measurements_mocked
):
    influx_client_init_mocked.return_value = None
    get_list_database_mocked.return_value = [{"name": DATABASE_NAME}]

    get_list_measurements_mocked.return_value = [
        {"name": CANDLES_COLLECTION_NAME},
        {"name": SIGNALS_COLLECTION_NAME},
        {"name": ORDERS_COLLECTION_NAME},
    ]

    influxdb_storage = InfluxDbStorage()
    assert influxdb_storage.database_name == DATABASE_NAME
    assert influxdb_storage.database_url == INFLUXDB_URL


@patch("influxdb.client.InfluxDBClient.get_list_measurements")
@patch("influxdb.client.InfluxDBClient.get_list_database")
@patch("influxdb.client.InfluxDBClient.__init__")
def test_init(
    influx_client_init_mocked, get_list_database_mocked, get_list_measurements_mocked
):
    influx_client_init_mocked.return_value = None
    influx_client_init_mocked.return_value = None

    database_name = "database_name"
    database_url = "http://localhost:8086"
    get_list_database_mocked.return_value = [{"name": database_name}]

    get_list_measurements_mocked.return_value = [
        {"name": CANDLES_COLLECTION_NAME},
        {"name": SIGNALS_COLLECTION_NAME},
        {"name": ORDERS_COLLECTION_NAME},
    ]

    influxdb_storage = InfluxDbStorage(database_name, database_url)
    assert influxdb_storage.database_name == database_name
    assert influxdb_storage.database_url == database_url

    influx_client_calls = [
        call(
            database="database_name",
            host="localhost",
            password=None,
            port=8086,
            username=None,
        ),
        call(
            database="database_name",
            host="localhost",
            password=None,
            port=8086,
            username=None,
        ),
    ]
    influx_client_init_mocked.assert_has_calls(influx_client_calls)


def test_init_non_existing_database_url():
    database_url = "database_url"
    with pytest.raises(Exception):
        InfluxDbStorage(database_url=database_url)


@patch("influxdb.client.InfluxDBClient.get_list_measurements")
@patch("influxdb.client.InfluxDBClient.get_list_database")
@patch("influxdb.client.InfluxDBClient.__init__")
def test_init_non_existing_database_name(
    influx_client_init_mocked, get_list_database_mocked, get_list_measurements_mocked
):
    influx_client_init_mocked.return_value = None
    database_name1 = "database_name1"
    database_name2 = "database_name2"
    database_url = "http://localhost:8086"

    get_list_database_mocked.return_value = [database_name2]

    get_list_measurements_mocked.return_value = [
        {"name": CANDLES_COLLECTION_NAME},
        {"name": SIGNALS_COLLECTION_NAME},
        {"name": ORDERS_COLLECTION_NAME},
    ]

    with pytest.raises(Exception):
        InfluxDbStorage(database_url=database_url, database_name=database_name1)

    influx_client_calls = [
        call(
            database="database_name1",
            host="localhost",
            password=None,
            port=8086,
            username=None,
        )
    ]
    influx_client_init_mocked.assert_has_calls(influx_client_calls)


@patch("influxdb.client.InfluxDBClient.write_points")
def test_add_candle(write_points_mocked):
    influxdb_storage = InfluxDbStorage()

    influxdb_storage.add_candle(CANDLE1)

    serializable_candle = CANDLE1.to_serializable_dict()
    serializable_candle["timestamp"] = pd.Timestamp(serializable_candle["timestamp"])
    expected_json_body = [
        {
            "measurement": CANDLES_COLLECTION_NAME,
            "time": CANDLE1.timestamp,
            "tags": {
                "symbol": CANDLE1.asset.symbol,
                "exchange": CANDLE1.asset.exchange,
            },
            "fields": {
                "open": str(CANDLE1.open),
                "high": str(CANDLE1.high),
                "low": str(CANDLE1.low),
                "close": str(CANDLE1.close),
                "volume": CANDLE1.volume,
            },
        }
    ]
    write_points_mocked_calls = [call(expected_json_body)]
    write_points_mocked.assert_has_calls(write_points_mocked_calls)


def test_clean_all_candles():
    INFLUXDB_STORAGE.add_candle(CANDLE1)
    INFLUXDB_STORAGE.add_candle(CANDLE2)
    assert INFLUXDB_STORAGE.count(CANDLES_COLLECTION_NAME) > 0
    INFLUXDB_STORAGE.clean_all_candles()
    assert INFLUXDB_STORAGE.count(CANDLES_COLLECTION_NAME) == 0


def test_get_candle_by_identifier():
    INFLUXDB_STORAGE.clean_all_candles()

    INFLUXDB_STORAGE.add_candle(CANDLE1)
    candle: Candle = INFLUXDB_STORAGE.get_candle_by_identifier(
        CANDLE1.asset, CANDLE1.timestamp
    )
    assert candle == CANDLE1

    INFLUXDB_STORAGE.clean_all_candles()


def test_get_candle_by_identifier_non_existing_identifier_non_existing_candle():
    INFLUXDB_STORAGE.clean_all_candles()

    candle: Candle = INFLUXDB_STORAGE.get_candle_by_identifier(
        CANDLE1.asset, CANDLE1.timestamp
    )
    assert candle is None


def test_candle_with_identifier_exists_true():
    INFLUXDB_STORAGE.clean_all_candles()

    INFLUXDB_STORAGE.add_candle(CANDLE1)
    assert INFLUXDB_STORAGE.candle_with_identifier_exists(
        CANDLE1.asset, CANDLE1.timestamp
    )

    INFLUXDB_STORAGE.clean_all_candles()


def test_candle_with_identifier_exists_false():
    INFLUXDB_STORAGE.clean_all_candles()

    assert not INFLUXDB_STORAGE.candle_with_identifier_exists(
        CANDLE1.asset, CANDLE1.timestamp
    )


def test_get_candles_in_range():
    INFLUXDB_STORAGE.clean_all_candles()

    INFLUXDB_STORAGE.add_candle(CANDLE1)
    INFLUXDB_STORAGE.add_candle(CANDLE2)
    INFLUXDB_STORAGE.add_candle(CANDLE3)
    candles: List[Candle] = INFLUXDB_STORAGE.get_candles_in_range(
        CANDLE1.asset,
        CANDLE1.timestamp - timedelta(minutes=1),
        CANDLE1.timestamp + timedelta(minutes=1),
    )
    assert compare_candles_list(candles, [CANDLE2, CANDLE1])

    INFLUXDB_STORAGE.clean_all_candles()


def test_get_all_candles():
    INFLUXDB_STORAGE.clean_all_candles()

    INFLUXDB_STORAGE.add_candle(CANDLE1)
    INFLUXDB_STORAGE.add_candle(CANDLE2)

    candles = INFLUXDB_STORAGE.get_all_candles()
    assert compare_candles_list(candles, [CANDLE2, CANDLE1])

    INFLUXDB_STORAGE.clean_all_candles()


@patch("influxdb.client.InfluxDBClient.write_points")
def test_add_signal(write_points_mocked):
    influxdb_storage = InfluxDbStorage()

    influxdb_storage.add_signal(SIGNAL1)

    serializable_signal = SIGNAL1.to_serializable_dict()
    serializable_signal["root_candle_timestamp"] = pd.Timestamp(
        serializable_signal["root_candle_timestamp"]
    )
    serializable_signal["generation_time"] = pd.Timestamp(
        serializable_signal["generation_time"]
    )
    expected_json_body = [
        {
            "measurement": "signals",
            "time": SIGNAL1.generation_time,
            "tags": {
                "symbol": SIGNAL1.asset.symbol,
                "exchange": SIGNAL1.asset.exchange,
                "strategy": SIGNAL1.strategy,
                "root_candle_timestamp": SIGNAL1.root_candle_timestamp,
            },
            "fields": {
                "action": "BUY",
                "direction": "LONG",
                "confidence_level": 0.05,
                "time_in_force": "0:05:00",
            },
        }
    ]
    write_points_calls = [call(expected_json_body)]
    write_points_mocked.assert_has_calls(write_points_calls)


def test_clean_all_signals():
    INFLUXDB_STORAGE.add_signal(SIGNAL1)
    INFLUXDB_STORAGE.add_signal(SIGNAL2)
    assert INFLUXDB_STORAGE.count(SIGNALS_COLLECTION_NAME) > 0
    INFLUXDB_STORAGE.clean_all_signals()
    assert INFLUXDB_STORAGE.count(SIGNALS_COLLECTION_NAME) == 0


def test_get_signal_by_identifier():
    INFLUXDB_STORAGE.clean_all_signals()

    INFLUXDB_STORAGE.add_signal(SIGNAL1)
    signal: Signal = INFLUXDB_STORAGE.get_signal_by_identifier(
        SIGNAL1.asset, SIGNAL1.strategy, SIGNAL1.root_candle_timestamp
    )
    assert signal == SIGNAL1

    INFLUXDB_STORAGE.clean_all_signals()


def test_get_signal_by_identifier_non_existing_signal():
    INFLUXDB_STORAGE.clean_all_signals()

    signal: Signal = INFLUXDB_STORAGE.get_signal_by_identifier(
        SIGNAL1.asset, SIGNAL1.strategy, SIGNAL1.root_candle_timestamp
    )
    assert signal is None


def test_get_all_signals():
    INFLUXDB_STORAGE.clean_all_signals()

    INFLUXDB_STORAGE.add_signal(SIGNAL1)
    INFLUXDB_STORAGE.add_signal(SIGNAL2)

    signals = INFLUXDB_STORAGE.get_all_signals()
    assert compare_signals_list(signals, [SIGNAL1, SIGNAL2])

    INFLUXDB_STORAGE.clean_all_signals()


@patch("influxdb.client.InfluxDBClient.write_points")
def test_add_order(write_points_mocked):
    influxdb_storage = InfluxDbStorage()

    influxdb_storage.add_order(ORDER1)

    serializable_order = ORDER1.to_serializable_dict()
    serializable_order["generation_time"] = pd.Timestamp(
        serializable_order["generation_time"]
    )

    expected_json_body = [
        {
            "measurement": "orders",
            "time": ORDER1.generation_time,
            "tags": {"signal_id": ORDER1.signal_id},
            "fields": {
                "symbol": ORDER1.asset.symbol,
                "exchange": ORDER1.asset.exchange,
                "action": ORDER1.action.name,
                "direction": ORDER1.direction.name,
                "size": ORDER1.size,
                "time_in_force": str(ORDER1.time_in_force),
                "status": ORDER1.status.name,
                "type": ORDER1.type.name,
                "condition": ORDER1.condition.name,
            },
        }
    ]

    write_points_calls = [call(expected_json_body)]
    write_points_mocked.assert_has_calls(write_points_calls)


def test_clean_all_orders():
    INFLUXDB_STORAGE.add_order(ORDER1)
    INFLUXDB_STORAGE.add_order(ORDER2)
    assert INFLUXDB_STORAGE.count(ORDERS_COLLECTION_NAME) > 0
    INFLUXDB_STORAGE.clean_all_orders()
    assert INFLUXDB_STORAGE.count(ORDERS_COLLECTION_NAME) == 0


def test_get_order_by_identifier():
    INFLUXDB_STORAGE.clean_all_orders()

    INFLUXDB_STORAGE.add_order(ORDER1)
    order: Order = INFLUXDB_STORAGE.get_order_by_identifier(
        ORDER1.signal_id, ORDER1.generation_time
    )
    assert order == ORDER1

    INFLUXDB_STORAGE.clean_all_orders()


def test_get_order_by_identifier_non_existing_order():
    INFLUXDB_STORAGE.clean_all_orders()

    order: Order = INFLUXDB_STORAGE.get_order_by_identifier(
        ORDER1.signal_id, ORDER1.generation_time
    )
    assert order is None


def test_get_all_orders():
    INFLUXDB_STORAGE.clean_all_orders()

    INFLUXDB_STORAGE.add_order(ORDER1)
    INFLUXDB_STORAGE.add_order(ORDER2)

    orders = INFLUXDB_STORAGE.get_all_orders()
    assert compare_orders_list(orders, [ORDER1, ORDER2])

    INFLUXDB_STORAGE.clean_all_orders()


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

    INFLUXDB_STORAGE.clean_all_candles()
    candle_dataframe = CandleDataFrame.from_dataframe(df, Asset("AAPL", exchange="IEX"))
    INFLUXDB_STORAGE.add_candle_dataframe(candle_dataframe)

    expected_candles = candle_dataframe.to_candles()
    candles = INFLUXDB_STORAGE.get_all_candles()

    assert (candles == expected_candles).all(axis=None)

    INFLUXDB_STORAGE.clean_all_candles()


def test_count():
    INFLUXDB_STORAGE.clean_all_candles()

    INFLUXDB_STORAGE.add_candle(CANDLE1)
    INFLUXDB_STORAGE.add_candle(CANDLE2)
    INFLUXDB_STORAGE.add_candle(CANDLE3)

    assert INFLUXDB_STORAGE.count(CANDLES_COLLECTION_NAME) == 3

    INFLUXDB_STORAGE.clean_all_candles()
