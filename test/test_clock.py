from datetime import datetime
from unittest.mock import call, patch

import pandas as pd
import pytz

from trazy_analysis.common.clock import Clock, LiveClock, SimulatedClock
from trazy_analysis.models.asset import Asset

SYMBOL = "AAPL"
EXCHANGE = "IEX"
ASSET = Asset(symbol=SYMBOL, exchange=EXCHANGE)


@patch("trazy_analysis.common.clock.Clock.update_bars")
@patch("trazy_analysis.common.clock.Clock.update_time")
def test_update(update_time_mocked, update_bars_mocked):
    clock = Clock()
    timestamp = datetime.strptime("2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    clock.update(timestamp)
    update_time_mocked_calls = [call(timestamp)]
    update_time_mocked.assert_has_calls(update_time_mocked_calls)
    update_bars_mocked_calls = [call()]
    update_bars_mocked.assert_has_calls(update_bars_mocked_calls)


def test_current_time_live_clock():
    clock = LiveClock()
    timestamp = datetime.now(pytz.UTC)
    assert clock.current_time() - timestamp < pd.offsets.Second(1)


def test_current_time_simulated_clock():
    clock = SimulatedClock()
    now = datetime.now(pytz.UTC)
    assert clock.current_time() - now < pd.offsets.Second(1)
    assert clock.current_bars() == 0

    timestamp1 = datetime.strptime("2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    clock.update(timestamp1)
    assert clock.current_time() == timestamp1

    timestamp2 = datetime.strptime("2017-10-05 08:01:00+0000", "%Y-%m-%d %H:%M:%S%z")
    clock.update(timestamp2)
    assert clock.current_bars() == 2


def test_update_time_simulated_clock():
    pass


def test_update_bars_simulated_clock():
    pass


def test_bars_simulated_clock():
    pass
