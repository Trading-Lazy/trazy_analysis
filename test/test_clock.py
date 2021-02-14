from unittest.mock import call, patch

import pandas as pd
import pytz

from common.clock import Clock, LiveClock, SimulatedClock


@patch("common.clock.Clock.update_bars")
@patch("common.clock.Clock.update_time")
def test_update(update_time_mocked, update_bars_mocked):
    clock = Clock()
    symbol = "AAPL"
    timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    clock.update(symbol, timestamp)
    update_time_mocked_calls = [call(symbol, timestamp)]
    update_time_mocked.assert_has_calls(update_time_mocked_calls)
    update_bars_mocked_calls = [call(symbol)]
    update_bars_mocked.assert_has_calls(update_bars_mocked_calls)


def test_current_time_live_clock():
    clock = LiveClock()
    timestamp = pd.Timestamp.now("UTC")
    assert clock.current_time(symbol="AAPL") - timestamp < pd.offsets.Second(1)


def test_current_time_simulated_clock():
    clock = SimulatedClock()
    now = pd.Timestamp.now("UTC")
    symbol = "AAPL"
    assert clock.current_time() - now < pd.offsets.Second(1)
    assert clock.bars(symbol) == 0

    timestamp1 = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    clock.update(symbol, timestamp1)
    assert clock.current_time(symbol=symbol) == timestamp1

    timestamp2 = pd.Timestamp("2017-10-05 08:01:00", tz=pytz.UTC)
    clock.update(symbol, timestamp2)
    assert clock.bars(symbol) == 2


def test_update_time_simulated_clock():
    pass


def test_update_bars_simulated_clock():
    pass


def test_bars_simulated_clock():
    pass
