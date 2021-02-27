from collections import deque
from datetime import datetime

from broker.simulated_broker import SimulatedBroker
from common.clock import SimulatedClock
from models.enums import Action, Direction
from models.order import Order
from models.signal import Signal
from order_manager.order_creator import OrderCreator


def test_create_order_from_signal():
    clock = SimulatedClock()
    events = deque()
    symbol = "IVV"
    timestamp = datetime.strptime("2020-05-08 14:16:00+0000", "%Y-%m-%d %H:%M:%S%z")
    clock.update(symbol, timestamp)
    broker = SimulatedBroker(clock, events)
    order_creator = OrderCreator(broker=broker)
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

    order = order_creator.create_order(signal, clock)
    expected_order = Order(
        symbol=symbol,
        action=Action.BUY,
        direction=Direction.LONG,
        size=0,
        signal_id=signal.signal_id,
        clock=clock,
    )
    assert order == expected_order
