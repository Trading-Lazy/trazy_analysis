import pandas as pd
import pytest
import pytz

from common.clock import SimulatedClock
from models.enums import Action, Direction, OrderStatus, OrderType
from models.multiple_order import (
    BracketOrder,
    CoverOrder,
    HomogeneousSequentialOrder,
    MultipleOrder,
    OcoOrder,
    SequentialOrder,
)
from models.order import Order


def test_multiple_order():
    clock = SimulatedClock()
    symbol1 = "AAA"
    order1 = Order(
        symbol=symbol1,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        clock=clock,
    )
    symbol2 = "BBB"
    order2 = Order(
        symbol=symbol2,
        action=Action.BUY,
        direction=Direction.LONG,
        size=150,
        signal_id="1",
        clock=clock,
    )
    orders = [order1, order2]
    multiple_order = MultipleOrder(orders=orders)
    assert order1.status == OrderStatus.CREATED
    assert order2.status == OrderStatus.CREATED

    multiple_order.submit()
    assert order1.status == OrderStatus.SUBMITTED
    assert order2.status == OrderStatus.SUBMITTED

    count1 = 1

    def on_complete_callback():
        nonlocal count1
        count1 = 5

    multiple_order.add_on_complete_callback(on_complete_callback)
    assert multiple_order.pending_orders() == orders
    assert multiple_order.completed_orders() == []

    order1.complete()
    assert count1 == 1
    assert multiple_order.pending_orders() == [order2]
    assert multiple_order.completed_orders() == [order1]
    assert order1.status == OrderStatus.COMPLETED
    assert order2.status == OrderStatus.SUBMITTED

    order2.complete()
    assert count1 == 5
    assert multiple_order.pending_orders() == []
    assert multiple_order.completed_orders() == orders
    assert order1.status == OrderStatus.COMPLETED
    assert order2.status == OrderStatus.COMPLETED

    assert multiple_order == multiple_order
    assert multiple_order != object
    multiple_order2 = MultipleOrder(orders=[order1])
    assert multiple_order != multiple_order2


def test_sequential_order():
    clock = SimulatedClock()
    symbol1 = "AAA"
    order1 = Order(
        symbol=symbol1,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        clock=clock,
    )
    symbol2 = "BBB"
    order2 = Order(
        symbol=symbol2,
        action=Action.BUY,
        direction=Direction.LONG,
        size=150,
        signal_id="1",
        clock=clock,
    )
    orders = [order1, order2]
    sequential_order = SequentialOrder(orders=orders)
    assert order1.status == OrderStatus.CREATED
    assert order2.status == OrderStatus.CREATED

    sequential_order.submit()
    assert order1.status == OrderStatus.SUBMITTED
    assert order2.status == OrderStatus.CREATED

    assert sequential_order.pending_orders() == orders
    assert sequential_order.completed_orders() == []

    assert sequential_order.get_first_order() == order1

    order1.complete()
    assert order1.status == OrderStatus.COMPLETED
    assert order2.status == OrderStatus.SUBMITTED
    assert sequential_order.pending_orders() == [order2]
    assert sequential_order.completed_orders() == [order1]

    order2.complete()
    assert order1.status == OrderStatus.COMPLETED
    assert order2.status == OrderStatus.COMPLETED
    assert sequential_order.pending_orders() == []
    assert sequential_order.completed_orders() == orders

    sequential_order = SequentialOrder(orders=[])
    with pytest.raises(Exception):
        sequential_order.get_first_order()


def test_oco_order():
    clock = SimulatedClock()
    symbol1 = "AAA"
    order1 = Order(
        symbol=symbol1,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        clock=clock,
    )
    symbol2 = "BBB"
    order2 = Order(
        symbol=symbol2,
        action=Action.BUY,
        direction=Direction.LONG,
        size=150,
        signal_id="1",
        clock=clock,
    )
    orders = [order1, order2]
    oco_order = OcoOrder(orders=orders)
    assert order1.status == OrderStatus.CREATED
    assert order2.status == OrderStatus.CREATED

    oco_order.submit()
    assert order1.status == OrderStatus.SUBMITTED
    assert order2.status == OrderStatus.SUBMITTED

    count1 = 1

    def on_complete_callback():
        nonlocal count1
        count1 = 5

    oco_order.add_on_complete_callback(on_complete_callback)

    assert oco_order.pending_orders() == orders
    assert oco_order.completed_orders() == []

    order2.complete()
    assert order1.status == OrderStatus.CANCELLED
    assert order2.status == OrderStatus.COMPLETED
    assert count1 == 5
    assert oco_order.pending_orders() == []
    assert oco_order.completed_orders() == [order2]


def test_homogeneous_sequential_order():
    clock = SimulatedClock()
    timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    symbol = "AAA"
    clock.update(symbol, timestamp)
    symbol1 = "AAA"
    order1 = Order(
        symbol=symbol1,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.MARKET,
        clock=clock,
    )
    symbol2 = "AAA"
    order2 = Order(
        symbol=symbol2,
        action=Action.SELL,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.STOP,
        clock=clock,
    )
    orders = [order1, order2]
    homogeneous_sequential_order = HomogeneousSequentialOrder(
        symbol=symbol, orders=orders, clock=clock
    )
    assert order1.status == OrderStatus.CREATED
    assert order2.status == OrderStatus.CREATED

    assert homogeneous_sequential_order.pending_orders() == orders
    assert homogeneous_sequential_order.completed_orders() == []

    assert homogeneous_sequential_order.get_first_order() == order1

    homogeneous_sequential_order.submit()
    assert order1.status == OrderStatus.SUBMITTED
    assert order2.status == OrderStatus.CREATED
    assert homogeneous_sequential_order.submission_time == timestamp

    order1.complete()
    assert order1.status == OrderStatus.COMPLETED
    assert order2.status == OrderStatus.SUBMITTED
    assert homogeneous_sequential_order.pending_orders() == [order2]
    assert homogeneous_sequential_order.completed_orders() == [order1]

    order2.complete()
    assert order2.status == OrderStatus.COMPLETED
    assert homogeneous_sequential_order.pending_orders() == []
    assert homogeneous_sequential_order.completed_orders() == orders


def test_homogeneous_sequential_order_multiple_order():
    clock = SimulatedClock()
    timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    symbol = "AAA"
    clock.update(symbol, timestamp)
    symbol1 = "AAA"
    order1 = Order(
        symbol=symbol1,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.MARKET,
        clock=clock,
    )
    symbol2 = "AAA"
    order2 = Order(
        symbol=symbol2,
        action=Action.SELL,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.STOP,
        clock=clock,
    )
    symbol3 = "AAA"
    order3 = Order(
        symbol=symbol3,
        action=Action.SELL,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.STOP,
        clock=clock,
    )
    multiple_order = MultipleOrder(orders=[order2, order3])
    orders = [order1, multiple_order]
    homogeneous_sequential_order = HomogeneousSequentialOrder(
        symbol=symbol, orders=orders, clock=clock
    )
    assert order1.status == OrderStatus.CREATED
    assert order2.status == OrderStatus.CREATED

    assert homogeneous_sequential_order.pending_orders() == orders
    assert homogeneous_sequential_order.completed_orders() == []

    assert homogeneous_sequential_order.get_first_order() == order1

    homogeneous_sequential_order.submit()
    assert order1.status == OrderStatus.SUBMITTED
    assert order2.status == OrderStatus.CREATED
    assert homogeneous_sequential_order.submission_time == timestamp

    order1.complete()
    assert order1.status == OrderStatus.COMPLETED
    assert multiple_order.status == OrderStatus.SUBMITTED
    assert homogeneous_sequential_order.pending_orders() == [multiple_order]
    assert homogeneous_sequential_order.completed_orders() == [order1]

    multiple_order.complete()
    assert multiple_order.status == OrderStatus.COMPLETED
    assert homogeneous_sequential_order.pending_orders() == []
    assert homogeneous_sequential_order.completed_orders() == orders


def test_homogeneous_sequential_order_different_symbols():
    clock = SimulatedClock()
    timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    symbol = "AAA"
    clock.update(symbol, timestamp)
    symbol1 = "BBB"
    order1 = Order(
        symbol=symbol1,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.MARKET,
        clock=clock,
    )
    symbol2 = "AAA"
    order2 = Order(
        symbol=symbol2,
        action=Action.SELL,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.STOP,
        clock=clock,
    )
    orders = [order1, order2]
    with pytest.raises(Exception):
        HomogeneousSequentialOrder(symbol=symbol, orders=orders, clock=clock)


def test_homogeneous_sequential_order_different_symbols_multiple_order():
    clock = SimulatedClock()
    timestamp = pd.Timestamp("2017-10-05 08:00:00", tz=pytz.UTC)
    symbol = "AAA"
    clock.update(symbol, timestamp)
    symbol1 = "AAA"
    order1 = Order(
        symbol=symbol1,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.MARKET,
        clock=clock,
    )
    symbol2 = "AAA"
    order2 = Order(
        symbol=symbol2,
        action=Action.SELL,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.STOP,
        clock=clock,
    )
    symbol3 = "BBB"
    order3 = Order(
        symbol=symbol3,
        action=Action.SELL,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.STOP,
        clock=clock,
    )
    multiple_order = MultipleOrder(orders=[order2, order3])
    orders = [order1, multiple_order]
    with pytest.raises(Exception):
        HomogeneousSequentialOrder(symbol=symbol, orders=orders, clock=clock)


def test_cover_order():
    clock = SimulatedClock()
    symbol1 = "AAA"
    initiation_order = Order(
        symbol=symbol1,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.MARKET,
        clock=clock,
    )
    symbol2 = "AAA"
    stop_order = Order(
        symbol=symbol2,
        action=Action.SELL,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.STOP,
        clock=clock,
    )
    orders = [initiation_order, stop_order]
    symbol = "AAA"
    cover_order = CoverOrder(
        symbol=symbol,
        initiation_order=initiation_order,
        stop_order=stop_order,
        clock=clock,
    )
    assert initiation_order.status == OrderStatus.CREATED
    assert stop_order.status == OrderStatus.CREATED

    assert cover_order.pending_orders() == orders
    assert cover_order.completed_orders() == []

    cover_order.submit()
    assert initiation_order.status == OrderStatus.SUBMITTED
    assert stop_order.status == OrderStatus.CREATED

    assert cover_order.get_first_order() == initiation_order

    initiation_order.complete()
    assert initiation_order.status == OrderStatus.COMPLETED
    assert stop_order.status == OrderStatus.SUBMITTED
    assert cover_order.pending_orders() == [stop_order]
    assert cover_order.completed_orders() == [initiation_order]

    stop_order.complete()
    assert stop_order.status == OrderStatus.COMPLETED
    assert cover_order.pending_orders() == []
    assert cover_order.completed_orders() == orders


def test_cover_order_wrong_initiation_order_type():
    clock = SimulatedClock()
    symbol1 = "AAA"
    initiation_order = Order(
        symbol=symbol1,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.STOP,
        clock=clock,
    )
    symbol2 = "AAA"
    stop_order = Order(
        symbol=symbol2,
        action=Action.SELL,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.STOP,
        clock=clock,
    )

    symbol = "AAA"
    with pytest.raises(Exception):
        CoverOrder(
            symbol=symbol,
            initiation_order=initiation_order,
            stop_order=stop_order,
            clock=clock,
        )


def test_cover_order_wrong_initiation_order_action():
    clock = SimulatedClock()
    symbol1 = "AAA"
    initiation_order = Order(
        symbol=symbol1,
        action=Action.SELL,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.MARKET,
        clock=clock,
    )
    symbol2 = "AAA"
    stop_order = Order(
        symbol=symbol2,
        action=Action.SELL,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.STOP,
        clock=clock,
    )

    symbol = "AAA"
    with pytest.raises(Exception):
        CoverOrder(
            symbol=symbol,
            initiation_order=initiation_order,
            stop_order=stop_order,
            clock=clock,
        )


def test_cover_order_wrong_stop_order_type():
    clock = SimulatedClock()
    symbol1 = "AAA"
    initiation_order = Order(
        symbol=symbol1,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.LIMIT,
        clock=clock,
    )
    symbol2 = "AAA"
    stop_order = Order(
        symbol=symbol2,
        action=Action.SELL,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.MARKET,
        clock=clock,
    )

    symbol = "AAA"
    with pytest.raises(Exception):
        CoverOrder(
            symbol=symbol,
            initiation_order=initiation_order,
            stop_order=stop_order,
            clock=clock,
        )


def test_cover_order_wrong_stop_order_action():
    clock = SimulatedClock()
    symbol1 = "AAA"
    initiation_order = Order(
        symbol=symbol1,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.LIMIT,
        clock=clock,
    )
    symbol2 = "AAA"
    stop_order = Order(
        symbol=symbol2,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.STOP,
        clock=clock,
    )

    symbol = "AAA"
    with pytest.raises(Exception):
        CoverOrder(
            symbol=symbol,
            initiation_order=initiation_order,
            stop_order=stop_order,
            clock=clock,
        )


def test_bracket_order():
    clock = SimulatedClock()
    symbol1 = "AAA"
    initiation_order = Order(
        symbol=symbol1,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.MARKET,
        clock=clock,
    )
    symbol2 = "AAA"
    target_order = Order(
        symbol=symbol2,
        action=Action.SELL,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.TARGET,
        clock=clock,
    )
    symbol3 = "AAA"
    stop_order = Order(
        symbol=symbol3,
        action=Action.SELL,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.STOP,
        clock=clock,
    )
    symbol = "AAA"
    bracket_order = BracketOrder(
        symbol=symbol,
        initiation_order=initiation_order,
        target_order=target_order,
        stop_order=stop_order,
        clock=clock,
    )
    assert initiation_order.status == OrderStatus.CREATED
    assert target_order.status == OrderStatus.CREATED
    assert stop_order.status == OrderStatus.CREATED
    assert len(bracket_order.orders) == 2
    assert isinstance(bracket_order.orders[1], OcoOrder)

    oco_order = bracket_order.orders[1]
    orders = [initiation_order, oco_order]

    assert bracket_order.pending_orders() == orders
    assert bracket_order.completed_orders() == []

    bracket_order.submit()
    assert initiation_order.status == OrderStatus.SUBMITTED
    assert target_order.status == OrderStatus.CREATED
    assert stop_order.status == OrderStatus.CREATED

    assert bracket_order.get_first_order() == initiation_order

    initiation_order.complete()
    assert initiation_order.status == OrderStatus.COMPLETED
    assert target_order.status == OrderStatus.SUBMITTED
    assert stop_order.status == OrderStatus.SUBMITTED
    assert oco_order.status == OrderStatus.SUBMITTED
    assert bracket_order.pending_orders() == [oco_order]
    assert bracket_order.completed_orders() == [initiation_order]

    stop_order.complete()
    assert target_order.status == OrderStatus.CANCELLED
    assert stop_order.status == OrderStatus.COMPLETED
    assert oco_order.status == OrderStatus.COMPLETED
    assert bracket_order.pending_orders() == []
    assert bracket_order.completed_orders() == orders


def test_bracket_order_wrong_initiation_order_type():
    clock = SimulatedClock()
    symbol1 = "AAA"
    initiation_order = Order(
        symbol=symbol1,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.STOP,
        clock=clock,
    )
    symbol2 = "AAA"
    target_order = Order(
        symbol=symbol2,
        action=Action.SELL,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.LIMIT,
        clock=clock,
    )
    symbol3 = "AAA"
    stop_order = Order(
        symbol=symbol3,
        action=Action.SELL,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.STOP,
        clock=clock,
    )
    symbol = "AAA"
    with pytest.raises(Exception):
        BracketOrder(
            symbol=symbol,
            initiation_order=initiation_order,
            target_order=target_order,
            stop_order=stop_order,
            clock=clock,
        )


def test_bracket_order_wrong_initiation_order_action():
    clock = SimulatedClock()
    symbol1 = "AAA"
    initiation_order = Order(
        symbol=symbol1,
        action=Action.SELL,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.MARKET,
        clock=clock,
    )
    symbol2 = "AAA"
    target_order = Order(
        symbol=symbol2,
        action=Action.SELL,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.TARGET,
        clock=clock,
    )
    symbol3 = "AAA"
    stop_order = Order(
        symbol=symbol3,
        action=Action.SELL,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.STOP,
        clock=clock,
    )
    symbol = "AAA"
    with pytest.raises(Exception):
        BracketOrder(
            symbol=symbol,
            initiation_order=initiation_order,
            target_order=target_order,
            stop_order=stop_order,
            clock=clock,
        )


def test_bracket_order_wrong_target_order_type():
    clock = SimulatedClock()
    symbol1 = "AAA"
    initiation_order = Order(
        symbol=symbol1,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.MARKET,
        clock=clock,
    )
    symbol2 = "AAA"
    target_order = Order(
        symbol=symbol2,
        action=Action.SELL,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.MARKET,
        clock=clock,
    )
    symbol3 = "AAA"
    stop_order = Order(
        symbol=symbol3,
        action=Action.SELL,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.STOP,
        clock=clock,
    )
    symbol = "AAA"
    with pytest.raises(Exception):
        BracketOrder(
            symbol=symbol,
            initiation_order=initiation_order,
            target_order=target_order,
            stop_order=stop_order,
            clock=clock,
        )


def test_bracket_order_wrong_target_order_action():
    clock = SimulatedClock()
    symbol1 = "AAA"
    initiation_order = Order(
        symbol=symbol1,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.MARKET,
        clock=clock,
    )
    symbol2 = "AAA"
    target_order = Order(
        symbol=symbol2,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.TARGET,
        clock=clock,
    )
    symbol3 = "AAA"
    stop_order = Order(
        symbol=symbol3,
        action=Action.SELL,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.STOP,
        clock=clock,
    )
    symbol = "AAA"
    with pytest.raises(Exception):
        BracketOrder(
            symbol=symbol,
            initiation_order=initiation_order,
            target_order=target_order,
            stop_order=stop_order,
            clock=clock,
        )


def test_bracket_order_wrong_stop_order_type():
    clock = SimulatedClock()
    symbol1 = "AAA"
    initiation_order = Order(
        symbol=symbol1,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.MARKET,
        clock=clock,
    )
    symbol2 = "AAA"
    target_order = Order(
        symbol=symbol2,
        action=Action.SELL,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.TARGET,
        clock=clock,
    )
    symbol3 = "AAA"
    stop_order = Order(
        symbol=symbol3,
        action=Action.SELL,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.MARKET,
        clock=clock,
    )
    symbol = "AAA"
    with pytest.raises(Exception):
        BracketOrder(
            symbol=symbol,
            initiation_order=initiation_order,
            target_order=target_order,
            stop_order=stop_order,
            clock=clock,
        )


def test_bracket_order_wrong_stop_order_action():
    clock = SimulatedClock()
    symbol1 = "AAA"
    initiation_order = Order(
        symbol=symbol1,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.MARKET,
        clock=clock,
    )
    symbol2 = "AAA"
    target_order = Order(
        symbol=symbol2,
        action=Action.SELL,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.TARGET,
        clock=clock,
    )
    symbol3 = "AAA"
    stop_order = Order(
        symbol=symbol3,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        type=OrderType.STOP,
        clock=clock,
    )
    symbol = "AAA"
    with pytest.raises(Exception):
        BracketOrder(
            symbol=symbol,
            initiation_order=initiation_order,
            target_order=target_order,
            stop_order=stop_order,
            clock=clock,
        )
