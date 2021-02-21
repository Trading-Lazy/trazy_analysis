from datetime import datetime
from unittest.mock import call, patch

from broker.simulated_broker import SimulatedBroker
from common.clock import SimulatedClock
from models.enums import Action, Direction
from models.order import Order
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
    symbol = "IVV"
    timestamp = datetime.strptime("2020-05-08 14:16:00+0000", "%Y-%m-%d %H:%M:%S%z")
    clock.update(symbol, timestamp)
    broker = SimulatedBroker(clock, initial_funds=10000.0)
    broker.subscribe_funds_to_portfolio(10000.0)
    position_sizer = PositionSizer(broker)
    order_creator = OrderCreator(broker=broker)
    order_manager = OrderManager(broker, position_sizer, order_creator)
    signal = Signal(
        symbol=symbol,
        action=Action.BUY,
        direction=Direction.LONG,
        confidence_level="0.05",
        strategy="SmaCrossoverStrategy",
        root_candle_timestamp=timestamp,
        parameters={},
        clock=clock,
    )

    # test with default bars delay 1
    order_manager.check_signal(signal, bars_delay=1)

    assert order_manager.pending_signals.qsize() == 1
    assert order_manager.pending_signals.get() == (signal, 1)

    assert order_manager.pending_signals.qsize() == 0

    # test with 2 bars delay
    order = Order(
        symbol=symbol,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="IVV-SmaCrossoverStrategy-2020-05-08 14:16:00+00:00",
        clock=clock,
    )
    create_order_mocked.return_value = order
    max_entry_order_size_mocked.return_value = 100
    order_manager.check_signal(signal, bars_delay=0)
    assert order_manager.pending_signals.qsize() == 0
    submit_order_mocked_calls = [call(order)]
    submit_order_mocked.assert_has_calls(submit_order_mocked_calls)
