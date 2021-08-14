from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from trazy_analysis.common.constants import DATE_FORMAT
from trazy_analysis.common.exchange_calendar_euronext import EuronextExchangeCalendar
from trazy_analysis.common.types import CandleDataFrame
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.candle import Candle

SYMBOL = "IVV"
CANDLE1 = Candle(
    asset=Asset(symbol=SYMBOL, exchange="IEX"),
    open=323.69,
    high=323.81,
    low=323.67,
    close=323.81,
    volume=500,
    timestamp=datetime.strptime("2020-05-07 14:24:00+0000", "%Y-%m-%d %H:%M:%S%z"),
)
CANDLE2 = Candle(
    asset=Asset(symbol=SYMBOL, exchange="IEX"),
    open=323.81,
    high=324.21,
    low=323.81,
    close=324.1,
    volume=700,
    timestamp=datetime.strptime("2020-05-07 14:25:00+0000", "%Y-%m-%d %H:%M:%S%z"),
)
CANDLE3 = Candle(
    asset=Asset(symbol=SYMBOL, exchange="IEX"),
    open=324.1,
    high=324.1,
    low=323.97,
    close=324.03,
    volume=400,
    timestamp=datetime.strptime("2020-05-07 14:26:00+0000", "%Y-%m-%d %H:%M:%S%z"),
)
CANDLE4 = Candle(
    asset=Asset(symbol=SYMBOL, exchange="IEX"),
    open=323.93,
    high=323.95,
    low=323.83,
    close=323.88,
    volume=300,
    timestamp=datetime.strptime("2020-05-07 14:31:00+0000", "%Y-%m-%d %H:%M:%S%z"),
)
MARKET_CAL = EuronextExchangeCalendar()


def test_candle_dataframe():
    candle_dataframe = CandleDataFrame.from_candle_list(
        asset=Asset(symbol=SYMBOL, exchange="IEX"),
        candles=np.array([CANDLE1, CANDLE2, CANDLE3, CANDLE4], dtype=Candle),
    )

    expected_df_candles = {
        "timestamp": [
            "2020-05-07 14:24:00+00:00",
            "2020-05-07 14:25:00+00:00",
            "2020-05-07 14:26:00+00:00",
            "2020-05-07 14:31:00+00:00",
        ],
        "open": ["323.69", "323.81", "324.1", "323.93"],
        "high": [
            "323.81",
            "324.21",
            "324.1",
            "323.95",
        ],
        "low": ["323.67", "323.81", "323.97", "323.83"],
        "close": [
            "323.81",
            "324.1",
            "324.03",
            "323.88",
        ],
        "volume": [500, 700, 400, 300],
    }
    expected_df = pd.DataFrame(
        expected_df_candles,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    expected_df.index = pd.to_datetime(expected_df.timestamp, format=DATE_FORMAT)
    expected_df = expected_df.drop(["timestamp"], axis=1)

    assert (candle_dataframe == expected_df).all(axis=None)
    assert candle_dataframe.asset == Asset(symbol=SYMBOL, exchange="IEX")


def test_candle_dataframe_duplicate_index_in_init():
    with pytest.raises(ValueError):
        CandleDataFrame.from_candle_list(
            Asset(symbol=SYMBOL, exchange="IEX"),
            candles=np.array(
                [CANDLE1, CANDLE2, CANDLE3, CANDLE4, CANDLE4], dtype=Candle
            ),
        )


def test_candle_dataframe_add_candle():
    candle_dataframe = CandleDataFrame.from_candle_list(
        Asset(symbol=SYMBOL, exchange="IEX"),
        candles=np.array([CANDLE1, CANDLE2, CANDLE3], dtype=Candle),
    )
    expected_df_candles = {
        "timestamp": [
            "2020-05-07 14:24:00+00:00",
            "2020-05-07 14:25:00+00:00",
            "2020-05-07 14:26:00+00:00",
        ],
        "open": ["323.69", "323.81", "324.1"],
        "high": ["323.81", "324.21", "324.1"],
        "low": ["323.67", "323.81", "323.97"],
        "close": ["323.81", "324.1", "324.03"],
        "volume": [500, 700, 400],
    }
    expected_df = pd.DataFrame(
        expected_df_candles,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    expected_df.index = pd.to_datetime(expected_df.timestamp, format=DATE_FORMAT)
    expected_df = expected_df.drop(["timestamp"], axis=1)
    assert (candle_dataframe == expected_df).all(axis=None)

    candle_dataframe.add_candle(CANDLE4)
    expected_df_candles = {
        "timestamp": [
            "2020-05-07 14:24:00+00:00",
            "2020-05-07 14:25:00+00:00",
            "2020-05-07 14:26:00+00:00",
            "2020-05-07 14:31:00+00:00",
        ],
        "open": ["323.69", "323.81", "324.1", "323.93"],
        "high": [
            "323.81",
            "324.21",
            "324.1",
            "323.95",
        ],
        "low": ["323.67", "323.81", "323.97", "323.83"],
        "close": [
            "323.81",
            "324.1",
            "324.03",
            "323.88",
        ],
        "volume": [500, 700, 400, 300],
    }
    expected_df = pd.DataFrame(
        expected_df_candles,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    expected_df.index = pd.to_datetime(expected_df.timestamp, format=DATE_FORMAT)
    expected_df = expected_df.drop(["timestamp"], axis=1)
    assert (candle_dataframe == expected_df).all(axis=None)


def test_candle_dataframe_duplicate_index_in_add_candle():
    candle_dataframe = CandleDataFrame.from_candle_list(
        Asset(symbol=SYMBOL, exchange="IEX"),
        candles=np.array([CANDLE1, CANDLE2, CANDLE3, CANDLE4], dtype=Candle),
    )
    with pytest.raises(ValueError):
        candle_dataframe.add_candle(CANDLE4)


def test_get_candle():
    candle_dataframe = CandleDataFrame.from_candle_list(
        Asset(symbol=SYMBOL, exchange="IEX"),
        candles=np.array([CANDLE1, CANDLE2, CANDLE3, CANDLE4], dtype=Candle),
    )
    assert candle_dataframe.get_candle(0) == CANDLE1
    assert candle_dataframe.get_candle(1) == CANDLE2
    assert candle_dataframe.get_candle(2) == CANDLE3
    assert candle_dataframe.get_candle(3) == CANDLE4


def test_get_candle_symbol_not_set():
    candles = [CANDLE1, CANDLE2, CANDLE3, CANDLE4]
    candles_data = [candle.to_serializable_dict() for candle in candles]
    candle_dataframe = CandleDataFrame(candles_data=candles_data)
    with pytest.raises(Exception):
        candle_dataframe.get_candle(2)


def test_to_candles():
    candle_dataframe = CandleDataFrame.from_candle_list(
        Asset(symbol=SYMBOL, exchange="IEX"),
        candles=np.array([CANDLE1, CANDLE2, CANDLE3, CANDLE4], dtype=Candle),
    )
    assert (
        candle_dataframe.to_candles()
        == np.array([CANDLE1, CANDLE2, CANDLE3, CANDLE4], dtype=Candle)
    ).all()


def test_to_candles_symbol_not_set():
    candles = [CANDLE1, CANDLE2, CANDLE3, CANDLE4]
    candles_data = [candle.to_serializable_dict() for candle in candles]
    candle_dataframe = CandleDataFrame(candles_data=candles_data)
    with pytest.raises(Exception):
        candle_dataframe.to_candles()


def test_append():
    candle_dataframe1 = CandleDataFrame.from_candle_list(
        Asset(symbol=SYMBOL, exchange="IEX"),
        candles=np.array([CANDLE1, CANDLE2], dtype=Candle),
    )
    candle_dataframe2 = CandleDataFrame.from_candle_list(
        Asset(symbol=SYMBOL, exchange="IEX"),
        candles=np.array([CANDLE3, CANDLE4], dtype=Candle),
    )
    concatenated_candle_dataframe = candle_dataframe1.append(candle_dataframe2)
    assert (
        concatenated_candle_dataframe.to_candles()
        == np.array(
            [
                CANDLE1,
                CANDLE2,
                CANDLE3,
                CANDLE4,
            ],
            dtype=Candle,
        )
    ).all()
    assert concatenated_candle_dataframe.asset == Asset(symbol=SYMBOL, exchange="IEX")


def test_append_duplicate_index():
    candle_dataframe1 = CandleDataFrame.from_candle_list(
        Asset(symbol=SYMBOL, exchange="IEX"),
        candles=np.array([CANDLE1, CANDLE2], dtype=Candle),
    )
    candle_dataframe2 = CandleDataFrame.from_candle_list(
        Asset(symbol=SYMBOL, exchange="IEX"),
        candles=np.array([CANDLE1, CANDLE3, CANDLE4], dtype=Candle),
    )
    with pytest.raises(Exception):
        candle_dataframe1.append(candle_dataframe2)


def test_from_candle_list_empty_candles_list():
    candle_dataframe = CandleDataFrame.from_candle_list(
        Asset(symbol=SYMBOL, exchange="IEX"), candles=np.array([], dtype=Candle)
    )
    assert (candle_dataframe.to_candles() == np.array([], dtype=Candle)).all()


def test_from_candle_list():
    candles = np.array([CANDLE1, CANDLE2, CANDLE3, CANDLE4], dtype=Candle)
    candle_dataframe = CandleDataFrame.from_candle_list(
        asset=Asset(SYMBOL, "IEX"), candles=candles
    )
    assert (candle_dataframe.to_candles() == candles).all()


def test_from_dataframe_index_is_set():
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

    candle_dataframe = CandleDataFrame.from_dataframe(df, Asset(SYMBOL, exchange="IEX"))
    assert (
        candle_dataframe.to_candles()
        == np.array([CANDLE1, CANDLE2, CANDLE3, CANDLE4], dtype=Candle)
    ).all()


def test_from_dataframe_index_is_not_set():
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

    candle_dataframe = CandleDataFrame.from_dataframe(df, Asset(SYMBOL, exchange="IEX"))
    assert (
        candle_dataframe.to_candles()
        == np.array([CANDLE1, CANDLE2, CANDLE3, CANDLE4], dtype=Candle)
    ).all()


def test_concat():
    candles1 = [CANDLE1]
    candles_data1 = [candle.to_serializable_dict() for candle in candles1]
    candle_dataframe1 = CandleDataFrame(candles_data=candles_data1)

    candles2 = [CANDLE2, CANDLE3]
    candles_data2 = [candle.to_serializable_dict() for candle in candles2]
    candle_dataframe2 = CandleDataFrame(candles_data=candles_data2)

    candles3 = [CANDLE4]
    candles_data3 = [candle.to_serializable_dict() for candle in candles3]
    candle_dataframe3 = CandleDataFrame(candles_data=candles_data3)

    concatenated_candle_dataframe = CandleDataFrame.concat(
        [candle_dataframe1, candle_dataframe2, candle_dataframe3],
        Asset(SYMBOL, exchange="IEX"),
    )
    assert (
        concatenated_candle_dataframe.to_candles()
        == np.array(
            [
                CANDLE1,
                CANDLE2,
                CANDLE3,
                CANDLE4,
            ],
            dtype=Candle,
        )
    ).all()
    assert concatenated_candle_dataframe.asset == Asset(SYMBOL, exchange="IEX")


def test_concat_duplicate_index():
    candles1 = np.array([CANDLE1], dtype=Candle)
    candles_data1 = [candle.to_serializable_dict() for candle in candles1]
    candle_dataframe1 = CandleDataFrame(candles_data=candles_data1)

    candles2 = np.array([CANDLE2, CANDLE3], dtype=Candle)
    candles_data2 = [candle.to_serializable_dict() for candle in candles2]
    candle_dataframe2 = CandleDataFrame(candles_data=candles_data2)

    candles3 = np.array([CANDLE1, CANDLE4], dtype=Candle)
    candles_data3 = [candle.to_serializable_dict() for candle in candles3]
    candle_dataframe3 = CandleDataFrame(candles_data=candles_data3)

    with pytest.raises(Exception):
        CandleDataFrame.concat(
            [candle_dataframe1, candle_dataframe2, candle_dataframe3], SYMBOL
        )


def test_aggregate():
    candle_dataframe = CandleDataFrame.from_candle_list(
        asset=Asset(symbol=SYMBOL, exchange="IEX"),
        candles=np.array([CANDLE1, CANDLE2, CANDLE3, CANDLE4], dtype=Candle),
    )
    aggregated_candle_dataframe = candle_dataframe.aggregate(
        timedelta(minutes=5), MARKET_CAL
    )
    assert len(aggregated_candle_dataframe) == 3
    assert aggregated_candle_dataframe.get_candle(0) == Candle(
        asset=Asset(symbol=SYMBOL, exchange="IEX"),
        open=323.69,
        high=324.21,
        low=323.67,
        close=324.10,
        volume=1200,
        timestamp=datetime.strptime("2020-05-07 14:25:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    )
    assert aggregated_candle_dataframe.get_candle(1) == Candle(
        asset=Asset(symbol=SYMBOL, exchange="IEX"),
        open=324.10,
        high=324.10,
        low=323.97,
        close=324.03,
        volume=400,
        timestamp=datetime.strptime("2020-05-07 14:30:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    )
    assert aggregated_candle_dataframe.get_candle(2) == Candle(
        asset=Asset(symbol=SYMBOL, exchange="IEX"),
        open=323.93,
        high=323.95,
        low=323.83,
        close=323.88,
        volume=300,
        timestamp=datetime.strptime("2020-05-07 14:35:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    )
    assert aggregated_candle_dataframe.asset == Asset(symbol=SYMBOL, exchange="IEX")


def test_aggregate_empty_candle_dataframe():
    candle_dataframe = CandleDataFrame.from_candle_list(
        Asset(symbol=SYMBOL, exchange="IEX"), candles=np.array([], dtype=Candle)
    )
    aggregated_candle_dataframe = candle_dataframe.aggregate(
        timedelta(minutes=1), MARKET_CAL
    )
    assert (candle_dataframe == aggregated_candle_dataframe).all(axis=None)
    assert aggregated_candle_dataframe.asset == Asset(symbol=SYMBOL, exchange="IEX")
