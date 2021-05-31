from datetime import datetime

from common.clock import SimulatedClock
from models.asset import Asset
from models.enums import Action, Direction, OrderStatus, OrderType
from models.order import Order, OrderBase


EXCHANGE = "IEX"


def test_add_on_complete_callback():
    symbol = "BBB"
    asset = Asset(symbol=symbol, exchange=EXCHANGE)
    clock = SimulatedClock()
    order = Order(
        asset=asset,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        clock=clock,
    )

    count1 = 1

    def on_complete_callback1():
        nonlocal count1
        count1 = 5

    count2 = 3

    def on_complete_callback2():
        nonlocal count2
        count2 = 7

    order.add_on_complete_callback(on_complete_callback1)
    order.add_on_complete_callback(on_complete_callback2)

    order.complete()

    assert count1 == 5
    assert count2 == 7


def test_submit_order_base():
    generation_time = datetime.strptime(
        "2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    submission_time = datetime.strptime(
        "2017-10-05 09:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    order = OrderBase(generation_time=generation_time)

    order.submit(submission_time)

    assert order.submission_time == submission_time
    assert order.status == OrderStatus.SUBMITTED


def test_submit_order():
    symbol = "BBB"
    asset = Asset(symbol=symbol, exchange=EXCHANGE)
    clock = SimulatedClock()
    timestamp = datetime.strptime("2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    clock.update(asset, timestamp)
    order = Order(
        asset=asset,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        clock=clock,
    )

    order.submit()

    assert order.submission_time == timestamp
    assert order.status == OrderStatus.SUBMITTED
    assert order.type == OrderType.MARKET


def test_cancel_order_base():
    generation_time = datetime.strptime(
        "2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    order = OrderBase(generation_time=generation_time)

    order.cancel()

    assert order.status == OrderStatus.CANCELLED


def test_make_expired_order_base():
    generation_time = datetime.strptime(
        "2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    order = OrderBase(generation_time=generation_time)

    order.disable()

    assert order.status == OrderStatus.EXPIRED


def test_time_in_force_order_base():
    generation_time = datetime.strptime(
        "2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    submission_time = datetime.strptime(
        "2017-10-05 09:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    order = OrderBase(generation_time=generation_time)
    order.submit(submission_time)

    assert order.in_force(
        datetime.strptime("2017-10-05 09:02:00+0000", "%Y-%m-%d %H:%M:%S%z")
    )
    assert not order.in_force(
        datetime.strptime("2017-10-05 09:07:00+0000", "%Y-%m-%d %H:%M:%S%z")
    )

    assert order.status == OrderStatus.EXPIRED


def test_time_in_force_order():
    symbol = "BBB"
    asset = Asset(symbol=symbol, exchange=EXCHANGE)
    clock = SimulatedClock()
    timestamp = datetime.strptime("2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    clock.update(asset, timestamp)
    order = Order(
        asset=asset,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        clock=clock,
        generation_time=timestamp,
    )

    order.submit()

    assert order.submission_time == timestamp
    assert order.status == OrderStatus.SUBMITTED
    assert order.type == OrderType.MARKET

    assert order.in_force()
    assert order.in_force(
        datetime.strptime("2017-10-05 08:03:00+0000", "%Y-%m-%d %H:%M:%S%z")
    )
    assert not order.in_force(
        datetime.strptime("2017-10-05 08:10:00+0000", "%Y-%m-%d %H:%M:%S%z")
    )

    assert order.status == OrderStatus.EXPIRED


def test_is_entry_or_exit_order():
    symbol = "BBB"
    asset = Asset(symbol=symbol, exchange=EXCHANGE)
    order = Order(
        asset=asset,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
    )

    assert order.is_entry_order
    assert not order.is_exit_order


def test_from_serializable_dict():
    serializable_dict = {
        "asset": {"symbol": "AAA", "exchange": "IEX"},
        "signal_id": "1",
        "action": "BUY",
        "direction": "LONG",
        "size": 100,
        "type": "MARKET",
        "condition": "GTC",
        "status": "SUBMITTED",
        "generation_time": "2017-10-05 08:00:00+00:00",
        "time_in_force": "0:05:00",
        "submission_time": datetime.strptime(
            "2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
        ),
    }
    order = Order.from_serializable_dict(serializable_dict)
    symbol = "AAA"
    asset = Asset(symbol=symbol, exchange=EXCHANGE)
    clock = SimulatedClock()
    timestamp = datetime.strptime("2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    clock.update(asset, timestamp)
    expected_order = Order(
        asset=asset,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        clock=clock,
        status=OrderStatus.SUBMITTED,
    )
    assert expected_order == order


def test_to_serializable_dict():
    symbol = "AAA"
    asset = Asset(symbol=symbol, exchange=EXCHANGE)
    clock = SimulatedClock()
    timestamp = datetime.strptime("2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    clock.update(symbol, timestamp)
    order = Order(
        asset=asset,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        clock=clock,
    )
    order.submit()
    expected_dict = {
        "asset": {"symbol": "AAA", "exchange": "IEX"},
        "signal_id": "1",
        "action": "BUY",
        "direction": "LONG",
        "size": 100,
        "status": "SUBMITTED",
        "generation_time": "2017-10-05 08:00:00+00:00",
        "time_in_force": "0:05:00",
        "submission_time": datetime.strptime(
            "2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
        ),
    }
    expected_dict == order.to_serializable_dict()


def test_eq_ne():
    symbol1 = "AAA"
    asset1 = Asset(symbol=symbol1, exchange=EXCHANGE)
    clock = SimulatedClock()
    timestamp = datetime.strptime("2017-10-05 08:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    clock.update(symbol1, timestamp)
    order1 = Order(
        asset=asset1,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        clock=clock,
    )
    order2 = Order(
        asset=asset1,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        clock=clock,
    )

    symbol2 = "BBB"
    asset2 = Asset(symbol=symbol2, exchange=EXCHANGE)
    order3 = Order(
        asset=asset2,
        action=Action.SELL,
        direction=Direction.SHORT,
        size=200,
        signal_id="1",
    )

    assert order1 == order2
    assert order1 != order3
    assert order1 != object()


def test_limit_order():
    symbol = "AAA"
    asset = Asset(symbol=symbol, exchange=EXCHANGE)
    clock = SimulatedClock()
    limit = 15
    limit_order = Order(
        asset=asset,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        limit=limit,
        type=OrderType.LIMIT,
        clock=clock,
    )
    assert limit_order.limit == limit
    assert limit_order.type == OrderType.LIMIT


def test_stop_order():
    symbol = "AAA"
    asset = Asset(symbol=symbol, exchange=EXCHANGE)
    clock = SimulatedClock()
    stop = 15
    stop_order = Order(
        asset=asset,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        stop=stop,
        type=OrderType.STOP,
        clock=clock,
    )
    assert stop_order.stop == stop
    assert stop_order.type == OrderType.STOP


def test_trailing_stop_order():
    symbol = "AAA"
    asset = Asset(symbol=symbol, exchange=EXCHANGE)
    clock = SimulatedClock()
    stop_pct = 0.01
    trailing_stop_order = Order(
        asset=asset,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        stop_pct=stop_pct,
        type=OrderType.TRAILING_STOP,
        clock=clock,
    )
    assert trailing_stop_order.stop_pct == stop_pct
    assert trailing_stop_order.type == OrderType.TRAILING_STOP


def test_trailing_stop_order_eq():
    symbol = "AAA"
    asset = Asset(symbol=symbol, exchange=EXCHANGE)
    clock = SimulatedClock()
    stop_pct = 0.01
    trailing_stop_order1 = Order(
        asset=asset,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        stop_pct=stop_pct,
        type=OrderType.TRAILING_STOP,
        clock=clock,
    )
    trailing_stop_order2 = Order(
        asset=asset,
        action=Action.BUY,
        direction=Direction.LONG,
        size=100,
        signal_id="1",
        stop_pct=stop_pct,
        type=OrderType.TRAILING_STOP,
        clock=clock,
    )
    assert trailing_stop_order1 == trailing_stop_order2
