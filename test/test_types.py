from decimal import Decimal

import pandas as pd
import pytest

from common.constants import DATE_FORMAT
from common.exchange_calendar_euronext import EuronextExchangeCalendar
from common.types import CandleDataFrame
from models.candle import Candle

SYMBOL = "IVV"
CANDLE1 = Candle(
    symbol=SYMBOL,
    open=Decimal("323.69"),
    high=Decimal("323.81"),
    low=Decimal("323.67"),
    close=Decimal("323.81"),
    volume=500,
    timestamp=pd.Timestamp("2020-05-07 14:24:00", tz="UTC"),
)
CANDLE2 = Candle(
    symbol=SYMBOL,
    open=Decimal("323.81"),
    high=Decimal("324.21"),
    low=Decimal("323.81"),
    close=Decimal("324.10"),
    volume=700,
    timestamp=pd.Timestamp("2020-05-07 14:25:00", tz="UTC"),
)
CANDLE3 = Candle(
    symbol=SYMBOL,
    open=Decimal("324.10"),
    high=Decimal("324.10"),
    low=Decimal("323.97"),
    close=Decimal("324.03"),
    volume=400,
    timestamp=pd.Timestamp("2020-05-07 14:26:00", tz="UTC"),
)
CANDLE4 = Candle(
    symbol=SYMBOL,
    open=Decimal("323.93"),
    high=Decimal("323.95"),
    low=Decimal("323.83"),
    close=Decimal("323.88"),
    volume=300,
    timestamp=pd.Timestamp("2020-05-07 14:31:00", tz="UTC"),
)
MARKET_CAL = EuronextExchangeCalendar()


def test_candle_dataframe():
    candle_dataframe = CandleDataFrame(
        symbol=SYMBOL, candles=[CANDLE1, CANDLE2, CANDLE3, CANDLE4]
    )

    expected_df_candles = {
        "timestamp": [
            "2020-05-07 14:24:00+00:00",
            "2020-05-07 14:25:00+00:00",
            "2020-05-07 14:26:00+00:00",
            "2020-05-07 14:31:00+00:00",
        ],
        "open": ["323.69", "323.81", "324.10", "323.93"],
        "high": ["323.81", "324.21", "324.10", "323.95",],
        "low": ["323.67", "323.81", "323.97", "323.83"],
        "close": ["323.81", "324.10", "324.03", "323.88",],
        "volume": [500, 700, 400, 300],
    }
    expected_df = pd.DataFrame(
        expected_df_candles,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    expected_df.index = pd.to_datetime(expected_df.timestamp, format=DATE_FORMAT)
    expected_df = expected_df.drop(["timestamp"], axis=1)

    assert (candle_dataframe == expected_df).all(axis=None)
    assert candle_dataframe.symbol == SYMBOL


def test_candle_dataframe_duplicate_index_in_init():
    with pytest.raises(ValueError):
        CandleDataFrame(
            symbol=SYMBOL, candles=[CANDLE1, CANDLE2, CANDLE3, CANDLE4, CANDLE4]
        )


def test_candle_dataframe_add_candle():
    candle_dataframe = CandleDataFrame(
        symbol=SYMBOL, candles=[CANDLE1, CANDLE2, CANDLE3]
    )
    expected_df_candles = {
        "timestamp": [
            "2020-05-07 14:24:00+00:00",
            "2020-05-07 14:25:00+00:00",
            "2020-05-07 14:26:00+00:00",
        ],
        "open": ["323.69", "323.81", "324.10"],
        "high": ["323.81", "324.21", "324.10"],
        "low": ["323.67", "323.81", "323.97"],
        "close": ["323.81", "324.10", "324.03"],
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
        "open": ["323.69", "323.81", "324.10", "323.93"],
        "high": ["323.81", "324.21", "324.10", "323.95",],
        "low": ["323.67", "323.81", "323.97", "323.83"],
        "close": ["323.81", "324.10", "324.03", "323.88",],
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
    candle_dataframe = CandleDataFrame(
        symbol=SYMBOL, candles=[CANDLE1, CANDLE2, CANDLE3, CANDLE4]
    )
    with pytest.raises(ValueError):
        candle_dataframe.add_candle(CANDLE4)


def test_get_candle():
    candle_dataframe = CandleDataFrame(
        symbol=SYMBOL, candles=[CANDLE1, CANDLE2, CANDLE3, CANDLE4]
    )
    assert candle_dataframe.get_candle(0) == CANDLE1
    assert candle_dataframe.get_candle(1) == CANDLE2
    assert candle_dataframe.get_candle(2) == CANDLE3
    assert candle_dataframe.get_candle(3) == CANDLE4


def test_get_candle_symbol_not_set():
    candle_dataframe = CandleDataFrame(candles=[CANDLE1, CANDLE2, CANDLE3, CANDLE4])
    with pytest.raises(Exception):
        candle_dataframe.get_candle(2)


def test_to_candles():
    candle_dataframe = CandleDataFrame(
        symbol=SYMBOL, candles=[CANDLE1, CANDLE2, CANDLE3, CANDLE4]
    )
    assert candle_dataframe.to_candles() == [CANDLE1, CANDLE2, CANDLE3, CANDLE4]


def test_to_candles_symbol_not_set():
    candle_dataframe = CandleDataFrame(candles=[CANDLE1, CANDLE2, CANDLE3, CANDLE4])
    with pytest.raises(Exception):
        candle_dataframe.to_candles()


def test_append():
    candle_dataframe1 = CandleDataFrame(symbol=SYMBOL, candles=[CANDLE1, CANDLE2])
    candle_dataframe2 = CandleDataFrame(symbol=SYMBOL, candles=[CANDLE3, CANDLE4])
    concatenated_candle_dataframe = candle_dataframe1.append(candle_dataframe2)
    assert concatenated_candle_dataframe.to_candles() == [
        CANDLE1,
        CANDLE2,
        CANDLE3,
        CANDLE4,
    ]
    assert concatenated_candle_dataframe.symbol == SYMBOL


def test_append_duplicate_index():
    candle_dataframe1 = CandleDataFrame(symbol=SYMBOL, candles=[CANDLE1, CANDLE2])
    candle_dataframe2 = CandleDataFrame(
        symbol=SYMBOL, candles=[CANDLE1, CANDLE3, CANDLE4]
    )
    with pytest.raises(Exception):
        candle_dataframe1.append(candle_dataframe2)


def test_from_dataframe_index_is_set():
    df_candles = {
        "timestamp": [
            "2020-05-07 14:24:00+00:00",
            "2020-05-07 14:25:00+00:00",
            "2020-05-07 14:26:00+00:00",
            "2020-05-07 14:31:00+00:00",
        ],
        "open": ["323.69", "323.81", "324.10", "323.93"],
        "high": ["323.81", "324.21", "324.10", "323.95",],
        "low": ["323.67", "323.81", "323.97", "323.83"],
        "close": ["323.81", "324.10", "324.03", "323.88",],
        "volume": [500, 700, 400, 300],
    }
    df = pd.DataFrame(
        df_candles, columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    df.index = pd.to_datetime(df.timestamp, format=DATE_FORMAT)
    df = df.drop(["timestamp"], axis=1)

    candle_dataframe = CandleDataFrame.from_dataframe(df, SYMBOL)
    assert candle_dataframe.to_candles() == [CANDLE1, CANDLE2, CANDLE3, CANDLE4]


def test_from_dataframe_index_is_not_set():
    df_candles = {
        "timestamp": [
            "2020-05-07 14:24:00+00:00",
            "2020-05-07 14:25:00+00:00",
            "2020-05-07 14:26:00+00:00",
            "2020-05-07 14:31:00+00:00",
        ],
        "open": ["323.69", "323.81", "324.10", "323.93"],
        "high": ["323.81", "324.21", "324.10", "323.95",],
        "low": ["323.67", "323.81", "323.97", "323.83"],
        "close": ["323.81", "324.10", "324.03", "323.88",],
        "volume": [500, 700, 400, 300],
    }
    df = pd.DataFrame(
        df_candles, columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    df.index = pd.to_datetime(df.timestamp, format=DATE_FORMAT)
    df = df.drop(["timestamp"], axis=1)

    candle_dataframe = CandleDataFrame.from_dataframe(df, SYMBOL)
    assert candle_dataframe.to_candles() == [CANDLE1, CANDLE2, CANDLE3, CANDLE4]


def test_concat():
    candle_dataframe1 = CandleDataFrame(candles=[CANDLE1])
    candle_dataframe2 = CandleDataFrame(candles=[CANDLE2, CANDLE3])
    candle_dataframe3 = CandleDataFrame(candles=[CANDLE4])
    concatenated_candle_dataframe = CandleDataFrame.concat(
        [candle_dataframe1, candle_dataframe2, candle_dataframe3], SYMBOL
    )
    assert concatenated_candle_dataframe.to_candles() == [
        CANDLE1,
        CANDLE2,
        CANDLE3,
        CANDLE4,
    ]
    assert concatenated_candle_dataframe.symbol == SYMBOL


def test_concat_duplicate_index():
    candle_dataframe1 = CandleDataFrame(candles=[CANDLE1])
    candle_dataframe2 = CandleDataFrame(candles=[CANDLE2, CANDLE3])
    candle_dataframe3 = CandleDataFrame(candles=[CANDLE1, CANDLE4])
    with pytest.raises(Exception):
        CandleDataFrame.concat(
            [candle_dataframe1, candle_dataframe2, candle_dataframe3], SYMBOL
        )


def test_aggregate():
    candle_dataframe = CandleDataFrame(
        symbol=SYMBOL, candles=[CANDLE1, CANDLE2, CANDLE3, CANDLE4]
    )
    aggregated_candle_dataframe = candle_dataframe.aggregate(
        pd.offsets.Minute(5), MARKET_CAL
    )
    assert len(aggregated_candle_dataframe) == 3
    assert aggregated_candle_dataframe.get_candle(0) == Candle(
        symbol=SYMBOL,
        open=Decimal("323.69"),
        high=Decimal("324.21"),
        low=Decimal("323.67"),
        close=Decimal("324.10"),
        volume=1200,
        timestamp=pd.Timestamp("2020-05-07 14:25:00+00:00"),
    )
    assert aggregated_candle_dataframe.get_candle(1) == Candle(
        symbol=SYMBOL,
        open=Decimal("324.10"),
        high=Decimal("324.10"),
        low=Decimal("323.97"),
        close=Decimal("324.03"),
        volume=400,
        timestamp=pd.Timestamp("2020-05-07 14:30:00+00:00"),
    )
    assert aggregated_candle_dataframe.get_candle(2) == Candle(
        symbol=SYMBOL,
        open=Decimal("323.93"),
        high=Decimal("323.95"),
        low=Decimal("323.83"),
        close=Decimal("323.88"),
        volume=300,
        timestamp=pd.Timestamp("2020-05-07 14:35:00+00:00"),
    )
    assert aggregated_candle_dataframe.symbol == SYMBOL


def test_aggregate_empty_candle_dataframe():
    candle_dataframe = CandleDataFrame(symbol=SYMBOL, candles=[])
    aggregated_candle_dataframe = candle_dataframe.aggregate(
        pd.offsets.Minute(1), MARKET_CAL
    )
    assert (candle_dataframe == aggregated_candle_dataframe).all(axis=None)
    assert aggregated_candle_dataframe.symbol == SYMBOL
