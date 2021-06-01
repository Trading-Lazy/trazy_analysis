from collections import deque
from datetime import datetime
from unittest.mock import patch

from broker.broker_manager import BrokerManager
from broker.simulated_broker import SimulatedBroker
from common.clock import SimulatedClock
from models.asset import Asset
from models.enums import Action, Direction
from models.signal import Signal
from order_manager.order_creator import OrderCreator
from order_manager.order_manager import OrderManager
from order_manager.position_sizer import PositionSizer


@patch("broker.simulated_broker.SimulatedBroker.max_entry_order_size")
@patch("order_manager.order_creator.OrderCreator.create_order")
@patch("broker.simulated_broker.SimulatedBroker.submit_order")
def test_process_check_signals(
    submit_order_mocked, create_order_mocked, max_entry_order_size_mocked
):
    clock = SimulatedClock()
    events = deque()
    symbol = "IVV"
    exchange = "IEX"
    asset = Asset(symbol=symbol, exchange=exchange)
    timestamp = datetime.strptime("2020-05-08 14:16:00+0000", "%Y-%m-%d %H:%M:%S%z")
    clock.update(asset, timestamp)
    broker = SimulatedBroker(clock, events, initial_funds=10000.0)
    broker.subscribe_funds_to_portfolio(10000.0)
    broker_manager = BrokerManager(brokers={exchange: broker}, clock=clock)
    position_sizer = PositionSizer(broker_manager=broker_manager)
    order_creator = OrderCreator(broker_manager=broker_manager)
    order_manager = OrderManager(events, broker_manager, position_sizer, order_creator)
    signal = Signal(
        asset=asset,
        action=Action.BUY,
        direction=Direction.LONG,
        confidence_level="0.05",
        strategy="SmaCrossoverStrategy",
        root_candle_timestamp=timestamp,
        parameters={},
        clock=clock,
    )

    order_manager.check_signal(signal)

    assert len(order_manager.pending_signals) == 1
    assert order_manager.pending_signals.popleft() == signal

    assert len(order_manager.pending_signals) == 0

    submit_order_mocked_calls = []
    submit_order_mocked.assert_has_calls(submit_order_mocked_calls)
