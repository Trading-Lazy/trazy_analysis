import pandas as pd

from broker.simulated_broker import SimulatedBroker
from common.clock import SimulatedClock
from models.enums import Action, Direction
from models.order import Order
from models.signal import Signal
from order_manager.order_creator import OrderCreator


def test_create_order_from_signal():
    clock = SimulatedClock()
    symbol = "IVV"
    timestamp = pd.Timestamp("2020-05-08 14:16:00+00:00")
    clock.update(symbol, timestamp)
    broker = SimulatedBroker(clock)
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
