from collections import deque
from datetime import datetime, timedelta
from unittest.mock import patch

from trazy_analysis.broker.broker_manager import BrokerManager
from trazy_analysis.broker.simulated_broker import SimulatedBroker
from trazy_analysis.common.clock import SimulatedClock
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.enums import Action, Direction
from trazy_analysis.models.signal import Signal
from trazy_analysis.order_manager.order_creator import OrderCreator
from trazy_analysis.order_manager.order_manager import OrderManager
from trazy_analysis.order_manager.position_sizer import PositionSizer


@patch("trazy_analysis.broker.simulated_broker.SimulatedBroker.max_entry_order_size")
@patch("trazy_analysis.order_manager.order_creator.OrderCreator.create_order")
@patch("trazy_analysis.broker.simulated_broker.SimulatedBroker.submit_order")
def test_process_check_signals(
    submit_order_mocked, create_order_mocked, max_entry_order_size_mocked
):
    clock = SimulatedClock()
    events = deque()
    symbol = "IVV"
    exchange = "IEX"
    asset = Asset(symbol=symbol, exchange=exchange)
    timestamp = datetime.strptime("2020-05-08 14:16:00+0000", "%Y-%m-%d %H:%M:%S%z")
    clock.update(timestamp)
    broker = SimulatedBroker(clock, events, initial_funds=10000.0)
    broker.subscribe_funds_to_portfolio(10000.0)
    broker_manager = BrokerManager(brokers={exchange: broker})
    position_sizer = PositionSizer(broker_manager=broker_manager)
    order_creator = OrderCreator(broker_manager=broker_manager)
    order_manager = OrderManager(
        events, broker_manager, position_sizer, order_creator, clock
    )
    signal = Signal(asset=asset, time_unit=timedelta(minutes=1), action=Action.BUY, direction=Direction.LONG,
                    confidence_level="0.05", strategy="SmaCrossoverStrategy", root_candle_timestamp=timestamp,
                    parameters={}, clock=clock)

    order_manager.check_signal(signal)

    assert len(order_manager.pending_signals) == 1
    assert order_manager.pending_signals.popleft() == signal

    assert len(order_manager.pending_signals) == 0

    submit_order_mocked_calls = []
    submit_order_mocked.assert_has_calls(submit_order_mocked_calls)
