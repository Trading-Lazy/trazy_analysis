from datetime import datetime, timedelta

import pandas as pd
import pytz

from trazy_analysis.common.clock import SimulatedClock
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.enums import Action, Direction
from trazy_analysis.models.signal import Signal

clock = SimulatedClock()
IVV_ASSET = Asset(symbol="IVV", exchange="IEX")
GOOGL_ASSET = Asset(symbol="GOOGL", exchange="IEX")
clock.update_time(datetime.strptime("2020-05-08 14:17:00+0000", "%Y-%m-%d %H:%M:%S%z"))
SIGNAL1 = Signal(
    asset=IVV_ASSET,
    time_unit=timedelta(minutes=1),
    action=Action.BUY,
    direction=Direction.LONG,
    confidence_level=0.05,
    strategy="SmaCrossoverStrategy",
    root_candle_timestamp=datetime.strptime(
        "2020-05-08 14:16:00+0000", "%Y-%m-%d %H:%M:%S%z"
    ),
    parameters={},
    clock=clock,
)
SIGNAL1_DICT = {
    "asset": {"symbol": "IVV", "exchange": "IEX"},
    "time_unit": "0:01:00",
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
    asset=IVV_ASSET,
    time_unit=timedelta(minutes=1),
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
    asset=GOOGL_ASSET,
    time_unit=timedelta(minutes=1),
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
        asset=GOOGL_ASSET,
        time_unit=timedelta(minutes=1),
        action=Action.BUY,
        direction=Direction.LONG,
        confidence_level="0.05",
        strategy="SmaCrossoverStrategy",
        root_candle_timestamp=datetime.strptime(
            "2020-05-08 14:16:00+0000", "%Y-%m-%d %H:%M:%S%z"
        ),
        parameters={},
    )
    assert datetime.now(pytz.UTC) - signal.generation_time < pd.offsets.Second(1)


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
        datetime.strptime("2020-05-08 14:17:00+0000", "%Y-%m-%d %H:%M:%S%z")
    )
    assert SIGNAL1.in_force()
    clock.update_time(
        datetime.strptime("2020-05-08 14:22:00+0000", "%Y-%m-%d %H:%M:%S%z")
    )
    assert not SIGNAL1.in_force()

    # No clock
    SIGNAL1.clock = None
    assert SIGNAL1.in_force()

    SIGNAL1.clock = clock


def test_signal_type():
    assert SIGNAL1.is_entry_signal
    assert not SIGNAL1.is_exit_signal
