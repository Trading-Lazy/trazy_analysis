from datetime import datetime, timezone

import pandas as pd

from common.clock import SimulatedClock
from models.enums import Action, Direction
from models.signal import Signal

clock = SimulatedClock()
clock.update_time(
    "IVV", datetime.strptime("2020-05-08 14:17:00+0000", "%Y-%m-%d %H:%M:%S%z")
)
SIGNAL1 = Signal(
    symbol="IVV",
    action=Action.BUY,
    direction=Direction.LONG,
    confidence_level="0.05",
    strategy="SmaCrossoverStrategy",
    root_candle_timestamp=datetime.strptime(
        "2020-05-08 14:16:00+0000", "%Y-%m-%d %H:%M:%S%z"
    ),
    parameters={},
    clock=clock,
)
SIGNAL1_DICT = {
    "symbol": "IVV",
    "action": "BUY",
    "direction": "LONG",
    "confidence_level": "0.05",
    "strategy": "SmaCrossoverStrategy",
    "root_candle_timestamp": "2020-05-08 14:16:00+00:00",
    "parameters": {},
    "generation_time": "2020-05-08 14:17:00+00:00",
    "time_in_force": "0:05:00",
}

SIGNAL2 = Signal(
    symbol="IVV",
    action=Action.BUY,
    direction=Direction.LONG,
    confidence_level="0.05",
    strategy="SmaCrossoverStrategy",
    root_candle_timestamp=datetime.strptime(
        "2020-05-08 14:16:00+0000", "%Y-%m-%d %H:%M:%S%z"
    ),
    parameters={},
    clock=clock,
)

SIGNAL3 = Signal(
    symbol="GOOGL",
    action=Action.BUY,
    direction=Direction.LONG,
    confidence_level="0.05",
    strategy="SmaCrossoverStrategy",
    root_candle_timestamp=datetime.strptime(
        "2020-05-08 14:16:00+0000", "%Y-%m-%d %H:%M:%S%z"
    ),
    parameters={},
    clock=clock,
)


def test_no_clock_no_generation_time():
    signal = Signal(
        symbol="GOOGL",
        action=Action.BUY,
        direction=Direction.LONG,
        confidence_level="0.05",
        strategy="SmaCrossoverStrategy",
        root_candle_timestamp=datetime.strptime(
            "2020-05-08 14:16:00+0000", "%Y-%m-%d %H:%M:%S%z"
        ),
        parameters={},
    )
    assert datetime.now(timezone.utc) - signal.generation_time < pd.offsets.Second(1)


def test_eq():
    assert SIGNAL1 == SIGNAL2
    assert not SIGNAL2 == SIGNAL3
    assert not SIGNAL1 == object()


def test_ne():
    assert not SIGNAL1 != SIGNAL2
    assert SIGNAL2 != SIGNAL3


def test_from_serializable_dict():
    assert Signal.from_serializable_dict(SIGNAL1_DICT) == SIGNAL1


def test_to_serializable_dict():
    assert SIGNAL1_DICT == SIGNAL1.to_serializable_dict()


def test_in_force():
    assert SIGNAL1.in_force(
        datetime.strptime("2020-05-08 14:19:00+0000", "%Y-%m-%d %H:%M:%S%z")
    )
    assert not SIGNAL1.in_force(
        datetime.strptime("2020-05-08 14:22:00+0000", "%Y-%m-%d %H:%M:%S%z")
    )

    # None timestamp
    clock.update_time(
        "IVV", datetime.strptime("2020-05-08 14:17:00+0000", "%Y-%m-%d %H:%M:%S%z")
    )
    assert SIGNAL1.in_force()
    clock.update_time(
        "IVV", datetime.strptime("2020-05-08 14:22:00+0000", "%Y-%m-%d %H:%M:%S%z")
    )
    assert not SIGNAL1.in_force()

    # No clock
    SIGNAL1.clock = None
    assert SIGNAL1.in_force()

    SIGNAL1.clock = clock


def test_signal_type():
    assert SIGNAL1.is_entry_signal
    assert not SIGNAL1.is_exit_signal
