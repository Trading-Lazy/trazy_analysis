from collections import deque
from datetime import datetime

from trazy_analysis.broker.simulated_broker import SimulatedBroker
from trazy_analysis.common.clock import SimulatedClock
from trazy_analysis.models.asset import Asset
from trazy_analysis.models.enums import Action, Direction
from trazy_analysis.models.order import Order
from trazy_analysis.models.signal import Signal
from trazy_analysis.order_manager.order_creator import OrderCreator


def test_create_order_from_signal():
    clock = SimulatedClock()
    events = deque()
    symbol = "IVV"
    exchange = "IEX"
    asset = Asset(symbol=symbol, exchange=exchange)
    timestamp = datetime.strptime("2020-05-08 14:16:00+0000", "%Y-%m-%d %H:%M:%S%z")
    clock.update(timestamp)
    broker = SimulatedBroker(clock, events)
    order_creator = OrderCreator(broker_manager=broker)
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

    order = order_creator.create_order(signal, clock)
    expected_order = Order(
        asset=asset,
        action=Action.BUY,
        direction=Direction.LONG,
        size=0,
        signal_id=signal.signal_id,
        clock=clock,
    )
    assert order == expected_order
