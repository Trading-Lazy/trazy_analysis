from datetime import datetime

import pytest

from models.asset import Asset
from models.enums import Action, Direction
from position.position import Position
from position.transaction import Transaction

EXCHANGE = "IEX"


def test_update_price_failure_with_earlier_timestamp():
    """
    Tests that an exception is raised when an attempt to update the price is made with a timestamp earlier thant
    the current timestamp
    """
    # Initial long details
    symbol = "MSFT"
    asset = Asset(symbol=symbol, exchange=EXCHANGE)
    size = 100
    timestamp = datetime.strptime("2020-06-16 15:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    price = 193.74
    order_id = "123"
    commission = 1.0

    # Create the initial transaction and position
    transaction = Transaction(
        asset=asset,
        size=size,
        action=Action.BUY,
        direction=Direction.LONG,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position = Position.open_from_transaction(transaction)

    assert position.asset == asset
    assert position.price == price

    # Update the market price
    new_price = 192.80
    new_timestamp = datetime.strptime("2020-06-16 14:00:00+0000", "%Y-%m-%d %H:%M:%S%z")

    position.update_price(new_price, new_timestamp)
    assert position.last_price_update == new_timestamp


def test_update_price_failure_with_negative_price():
    """
    Tests that an exception is raised when an attempt to update the price is made with a negative price
    """
    # Initial long details
    symbol = "MSFT"
    asset = Asset(symbol=symbol, exchange=EXCHANGE)
    size = 100
    timestamp = datetime.strptime("2020-06-16 15:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    price = 193.74
    order_id = "123"
    commission = 1.0

    # Create the initial transaction and position
    transaction = Transaction(
        asset=asset,
        size=size,
        action=Action.BUY,
        direction=Direction.LONG,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position = Position.open_from_transaction(transaction)

    assert position.asset == asset
    assert position.price == price
    assert position.last_price_update == timestamp

    # Update the market price
    new_price = -192.80
    new_timestamp = datetime.strptime("2020-06-16 16:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    with pytest.raises(Exception):
        position.update_price(new_price, new_timestamp)


def test_basic_long_equities_position():
    """
    Tests that the properties on the Position
    are calculated for a simple long equities position.
    """
    # Initial long details
    symbol = "MSFT"
    asset = Asset(symbol=symbol, exchange=EXCHANGE)
    size = 100
    timestamp = datetime.strptime("2020-06-16 15:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    price = 193.74
    order_id = "123"
    commission = 1.0

    # Create the initial transaction and position
    transaction = Transaction(
        asset=asset,
        size=size,
        action=Action.BUY,
        direction=Direction.LONG,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position = Position.open_from_transaction(transaction)

    assert position.asset == asset
    assert position.price == price
    assert position.last_price_update == timestamp

    # Update the market price
    new_price = 192.80
    new_timestamp = datetime.strptime("2020-06-16 16:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    position.update_price(new_price, new_timestamp)

    assert position.price == new_price
    assert position.last_price_update == new_timestamp

    assert position.buy_size == 100
    assert position.sell_size == 0
    assert position.avg_bought == 193.74
    assert position.avg_sold == 0.0
    assert position.commission == 1.0

    assert position.direction == Direction.LONG
    assert position.market_value == 19280.0
    assert position.avg_price == 193.75
    assert position.net_size == 100
    assert position.total_bought == 19374.0
    assert position.total_sold == 0.0
    assert position.net_total == -19374.0
    assert position.net_incl_commission == -19375.0
    assert position.unrealised_pnl == pytest.approx(-95.0, 0.01)
    assert position.realised_pnl == 0.0
    assert position.total_pnl == pytest.approx(-95.0, 0.01)


def test_position_long_twice():
    """
    Tests that the properties on the Position
    are calculated for two consective long trades
    with differing quantities and market prices.
    """
    # Initial long details
    symbol = "MSFT"
    asset = Asset(symbol=symbol, exchange=EXCHANGE)
    size = 100
    timestamp = datetime.strptime("2020-06-16 15:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    price = 193.74
    order_id = "123"
    commission = 1.0

    # Create the initial transaction and position
    first_transaction = Transaction(
        asset=asset,
        size=size,
        action=Action.BUY,
        direction=Direction.LONG,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position = Position.open_from_transaction(first_transaction)

    assert position.asset == asset
    assert position.price == price
    assert position.last_price_update == timestamp

    # Second long
    second_size = 60
    second_timestamp = datetime.strptime(
        "2020-06-16 16:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    second_price = 193.79
    second_order_id = "234"
    second_commission = 1.0
    second_transaction = Transaction(
        asset=asset,
        size=second_size,
        action=Action.BUY,
        direction=Direction.LONG,
        price=second_price,
        order_id=second_order_id,
        commission=second_commission,
        timestamp=second_timestamp,
    )
    position.transact(second_transaction)

    assert position.price == second_price
    assert position.last_price_update == second_timestamp

    assert position.buy_size == 160
    assert position.sell_size == 0
    assert position.avg_bought == pytest.approx(193.75, 0.01)
    assert position.avg_sold == 0.0
    assert position.commission == 2.0

    assert position.direction == Direction.LONG
    assert position.market_value == pytest.approx(31006.40, 0.01)
    assert position.avg_price == pytest.approx(193.77125, 0.01)
    assert position.net_size == 160
    assert position.total_bought == pytest.approx(31001.40, 0.01)
    assert position.total_sold == 0.0
    assert position.net_total == pytest.approx(-31001.40, 0.01)
    assert position.net_incl_commission == pytest.approx(-31003.40, 0.01)
    assert position.unrealised_pnl == pytest.approx(3.0, 0.01)
    assert position.realised_pnl == 0.0
    assert position.total_pnl == pytest.approx(3.0, 0.01)


def test_position_long_close():
    """
    Tests that the properties on the Position
    are calculated for a long opening trade and
    subsequent closing trade.
    """
    # Initial long details
    symbol = "AMZN"
    asset = Asset(symbol=symbol, exchange=EXCHANGE)
    size = 100
    timestamp = datetime.strptime("2020-06-16 15:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    price = 2615.27
    order_id = "123"
    commission = 1.0

    # Create the initial transaction and position
    first_transaction = Transaction(
        asset=asset,
        size=size,
        action=Action.BUY,
        direction=Direction.LONG,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position = Position.open_from_transaction(first_transaction)

    assert position.asset == asset
    assert position.price == price
    assert position.last_price_update == timestamp

    # Closing trade
    second_size = 100
    second_timestamp = datetime.strptime(
        "2020-06-16 16:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    second_price = 2622.0
    second_order_id = "234"
    second_commission = 6.81
    second_transaction = Transaction(
        asset=asset,
        size=second_size,
        action=Action.SELL,
        direction=Direction.LONG,
        price=second_price,
        order_id=second_order_id,
        commission=second_commission,
        timestamp=second_timestamp,
    )
    position.transact(second_transaction)

    assert position.price == second_price
    assert position.last_price_update == second_timestamp

    assert position.buy_size == 100
    assert position.sell_size == 100
    assert position.avg_bought == 2615.27
    assert position.avg_sold == 2622.0
    assert position.commission == 7.81

    assert position.market_value == 0.0
    assert position.avg_price == 0.0
    assert position.net_size == 0
    assert position.total_bought == 261527.0
    assert position.total_sold == 262200.0
    assert position.net_total == 673.0
    assert position.net_incl_commission == 665.19
    assert position.unrealised_pnl == 0.0
    assert position.realised_pnl == pytest.approx(665.19, 0.01)
    assert position.total_pnl == pytest.approx(665.19, 0.01)


def test_position_long_and_short():
    """
    Tests that the properties on the Position
    are calculated for a long trade followed by
    a partial closing short trade with differing
    market prices.
    """
    # Initial long details
    symbol = "SPY"
    asset = Asset(symbol=symbol, exchange=EXCHANGE)
    size = 100
    timestamp = datetime.strptime("2020-06-16 15:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    price = 307.05
    order_id = "123"
    commission = 1.0

    # Create the initial transaction and position
    first_transaction = Transaction(
        asset=asset,
        size=size,
        action=Action.BUY,
        direction=Direction.LONG,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position = Position.open_from_transaction(first_transaction)

    assert position.asset == asset
    assert position.price == price
    assert position.last_price_update == timestamp

    # Short details and transaction
    second_size = 60
    second_timestamp = datetime.strptime(
        "2020-06-16 16:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    second_price = 314.91
    second_order_id = "234"
    second_commission = 1.42
    second_transaction = Transaction(
        asset=asset,
        size=second_size,
        action=Action.SELL,
        direction=Direction.LONG,
        price=second_price,
        order_id=second_order_id,
        commission=second_commission,
        timestamp=second_timestamp,
    )
    position.transact(second_transaction)

    assert position.price == second_price
    assert position.last_price_update == second_timestamp

    assert position.buy_size == 100
    assert position.sell_size == 60
    assert position.avg_bought == 307.05
    assert position.avg_sold == 314.91
    assert position.commission == 2.42

    assert position.direction == Direction.LONG
    assert position.market_value == pytest.approx(12596.40, 0.01)
    assert position.avg_price == pytest.approx(307.06, 0.01)
    assert position.net_size == 40
    assert position.total_bought == pytest.approx(30705.0, 0.01)
    assert position.total_sold == pytest.approx(18894.60, 0.01)
    assert position.net_total == pytest.approx(-11810.40, 0.01)
    assert position.net_incl_commission == pytest.approx(-11812.82, 0.01)
    assert position.unrealised_pnl == pytest.approx(314.0, 0.01)
    assert position.realised_pnl == pytest.approx(469.58, 0.01)
    assert position.total_pnl == pytest.approx(783.58, 0.01)


def test_position_long_short_long_short_ending_long():
    """
    Tests that the properties on the Position
    are calculated for four trades consisting
    of a long, short, long and short, net long
    after all trades with varying quantities
    and market prices.
    """
    # First trade (first long)
    symbol = "SPY"
    asset = Asset(symbol=symbol, exchange=EXCHANGE)
    size = 453
    timestamp = datetime.strptime("2020-06-16 15:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    price = 312.96
    order_id = "100"
    commission = 1.95

    # Create the initial transaction and position
    first_transaction = Transaction(
        asset=asset,
        size=size,
        action=Action.BUY,
        direction=Direction.LONG,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position = Position.open_from_transaction(first_transaction)

    # Second trade (first short)
    size = 397
    timestamp = datetime.strptime("2020-06-16 16:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    price = 315.599924
    order_id = "101"
    commission = 4.8
    transaction = Transaction(
        asset=asset,
        size=size,
        action=Action.SELL,
        direction=Direction.LONG,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position.transact(transaction)

    # Third trade (second long)
    size = 624
    timestamp = datetime.strptime("2020-06-16 17:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    price = 312.96
    order_id = "102"
    commission = 2.68
    transaction = Transaction(
        asset=asset,
        size=size,
        action=Action.BUY,
        direction=Direction.LONG,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position.transact(transaction)

    # Fourth trade (second short), now net long
    size = 519
    timestamp = datetime.strptime("2020-06-16 18:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    price = 315.78
    order_id = "103"
    commission = 6.28
    transaction = Transaction(
        asset=asset,
        size=size,
        action=Action.SELL,
        direction=Direction.LONG,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position.transact(transaction)

    assert position.asset == asset
    assert position.price == price
    assert position.last_price_update == timestamp

    assert position.buy_size == 1077
    assert position.sell_size == 916
    assert position.avg_bought == 312.96
    assert position.avg_sold == pytest.approx(315.7019, 0.0001)
    assert position.commission == pytest.approx(15.71, 0.01)

    assert position.direction == Direction.LONG
    assert position.market_value == pytest.approx(50840.58, 0.01)
    assert position.avg_price == pytest.approx(312.96, 0.01)
    assert position.net_size == 161
    assert position.total_bought == pytest.approx(337057.92, 0.01)
    assert position.total_sold == pytest.approx(289182.98, 0.01)
    assert position.net_total == pytest.approx(-47874.93, 0.01)
    assert position.net_incl_commission == pytest.approx(-47890.64, 0.01)
    assert position.unrealised_pnl == pytest.approx(453.32, 0.01)
    assert position.realised_pnl == pytest.approx(2496.61, 0.01)
    assert position.total_pnl == pytest.approx(2949.93, 0.01)


def test_basic_short_equities_position():
    """
    Tests that the properties on the Position
    are calculated for a simple short equities position.
    """
    # Initial short details
    symbol = "TLT"
    asset = Asset(symbol=symbol, exchange=EXCHANGE)
    size = 100
    timestamp = datetime.strptime("2020-06-16 15:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    price = 162.39
    order_id = "123"
    commission = 1.37

    # Create the initial transaction and position
    transaction = Transaction(
        asset=asset,
        size=size,
        action=Action.SELL,
        direction=Direction.SHORT,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position = Position.open_from_transaction(transaction)

    assert position.asset == asset
    assert position.price == price
    assert position.last_price_update == timestamp

    # Update the market price
    new_price = 159.43
    new_timestamp = datetime.strptime("2020-06-16 16:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    position.update_price(new_price, new_timestamp)

    assert position.price == new_price
    assert position.last_price_update == new_timestamp

    assert position.buy_size == 0
    assert position.sell_size == 100
    assert position.avg_bought == 0.0
    assert position.avg_sold == 162.39
    assert position.commission == 1.37

    assert position.direction == Direction.SHORT
    assert position.market_value == -15943.0
    assert position.avg_price == 162.3763
    assert position.net_size == -100
    assert position.total_bought == 0.0

    # np.isclose used for floating point precision
    assert position.total_sold == pytest.approx(16239.0, 0.01)
    assert position.net_total == pytest.approx(16239.0, 0.01)
    assert position.net_incl_commission == pytest.approx(16237.63, 0.01)
    assert position.unrealised_pnl == pytest.approx(294.63, 0.01)
    assert position.realised_pnl == 0.0
    assert position.total_pnl == pytest.approx(294.63, 0.01)


def test_position_short_twice():
    """
    Tests that the properties on the Position
    are calculated for two consective short trades
    with differing quantities and market prices.
    """
    # Initial short details
    symbol = "MSFT"
    asset = Asset(symbol=symbol, exchange=EXCHANGE)
    size = 100
    timestamp = datetime.strptime("2020-06-16 15:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    price = 194.55
    order_id = "123"
    commission = 1.44

    # Create the initial transaction and position
    first_transaction = Transaction(
        asset=asset,
        size=size,
        action=Action.SELL,
        direction=Direction.SHORT,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position = Position.open_from_transaction(first_transaction)

    assert position.asset == asset
    assert position.price == price
    assert position.last_price_update == timestamp

    # Second short
    second_size = 60
    second_timestamp = datetime.strptime(
        "2020-06-16 16:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    second_price = 194.76
    second_order_id = "234"
    second_commission = 1.27
    second_transaction = Transaction(
        asset=asset,
        size=second_size,
        action=Action.SELL,
        direction=Direction.SHORT,
        price=second_price,
        order_id=second_order_id,
        commission=second_commission,
        timestamp=second_timestamp,
    )
    position.transact(second_transaction)

    assert position.price == second_price
    assert position.last_price_update == second_timestamp

    assert position.buy_size == 0
    assert position.sell_size == 160
    assert position.avg_bought == 0.0
    assert position.avg_sold == 194.62875
    assert position.commission == 2.71

    assert position.direction == Direction.SHORT
    assert position.market_value == -31161.6
    assert position.avg_price == pytest.approx(194.611, 0.001)
    assert position.net_size == -160
    assert position.total_bought == 0.0
    assert position.total_sold == pytest.approx(31140.60, 0.01)
    assert position.net_total == 31140.6
    assert position.net_incl_commission == pytest.approx(31137.89, 0.01)
    assert position.unrealised_pnl == pytest.approx(-23.71, 0.01)
    assert position.realised_pnl == 0.0
    assert position.total_pnl == pytest.approx(-23.71, 0.01)


def test_position_short_close():
    """
    Tests that the properties on the Position
    are calculated for a short opening trade and
    subsequent closing trade.
    """
    # Initial short details
    symbol = "TSLA"
    asset = Asset(symbol=symbol, exchange=EXCHANGE)
    size = 100
    timestamp = datetime.strptime("2020-06-16 15:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    price = 982.13
    order_id = "123"
    commission = 3.18

    # Create the initial transaction and position
    first_transaction = Transaction(
        asset=asset,
        size=size,
        action=Action.SELL,
        direction=Direction.SHORT,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position = Position.open_from_transaction(first_transaction)

    assert position.asset == asset
    assert position.price == price
    assert position.last_price_update == timestamp

    # Closing trade
    second_size = 100
    second_timestamp = datetime.strptime(
        "2020-06-16 16:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    second_price = 982.13
    second_order_id = "234"
    second_commission = 1.0
    second_transaction = Transaction(
        asset=asset,
        size=second_size,
        action=Action.BUY,
        direction=Direction.SHORT,
        price=second_price,
        order_id=second_order_id,
        commission=second_commission,
        timestamp=second_timestamp,
    )
    position.transact(second_transaction)

    assert position.price == second_price
    assert position.last_price_update == second_timestamp

    assert position.buy_size == 100
    assert position.sell_size == 100
    assert position.avg_bought == 982.13
    assert position.avg_sold == 982.13
    assert position.commission == 4.18

    assert position.market_value == 0.0
    assert position.avg_price == 0.0
    assert position.net_size == 0
    assert position.total_bought == 98213.0
    assert position.total_sold == 98213.0
    assert position.net_total == 0.0
    assert position.net_incl_commission == -4.18
    assert position.unrealised_pnl == 0.0
    assert position.realised_pnl == -4.18
    assert position.total_pnl == -4.18


def test_position_short_and_long():
    """
    Tests that the properties on the Position
    are calculated for a short trade followed by
    a partial closing long trade with differing
    market prices.
    """
    # Initial short details
    symbol = "TLT"
    asset = Asset(symbol=symbol, exchange=EXCHANGE)
    size = 100
    timestamp = datetime.strptime("2020-06-16 15:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    price = 162.39
    order_id = "123"
    commission = 1.37

    # Create the initial transaction and position
    transaction = Transaction(
        asset=asset,
        size=size,
        action=Action.SELL,
        direction=Direction.SHORT,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position = Position.open_from_transaction(transaction)

    assert position.asset == asset
    assert position.price == price
    assert position.last_price_update == timestamp

    # Long details and transaction
    second_size = 60
    second_timestamp = datetime.strptime(
        "2020-06-16 16:00:00+0000", "%Y-%m-%d %H:%M:%S%z"
    )
    second_price = 159.99
    second_order_id = "234"
    second_commission = 1.0
    second_transaction = Transaction(
        asset=asset,
        size=second_size,
        action=Action.BUY,
        direction=Direction.SHORT,
        price=second_price,
        order_id=second_order_id,
        commission=second_commission,
        timestamp=second_timestamp,
    )
    position.transact(second_transaction)

    assert position.price == second_price
    assert position.last_price_update == second_timestamp

    assert position.buy_size == 60
    assert position.sell_size == 100
    assert position.avg_bought == pytest.approx(159.99, 0.01)
    assert position.avg_sold == 162.39
    assert position.commission == 2.37

    assert position.direction == Direction.SHORT
    assert position.market_value == -6399.6
    assert position.avg_price == 162.3763
    assert position.net_size == -40
    assert position.total_bought == pytest.approx(9599.40, 0.01)
    assert position.total_sold == pytest.approx(16239.0, 0.01)
    assert position.net_total == pytest.approx(6639.60, 0.01)
    assert position.net_incl_commission == pytest.approx(6637.23, 0.01)
    assert position.unrealised_pnl == pytest.approx(95.452, 0.001)
    assert position.realised_pnl == pytest.approx(142.178, 0.001)
    assert position.total_pnl == pytest.approx(237.630, 0.001)


def test_position_short_long_short_long_ending_short():
    """
    Tests that the properties on the Position
    are calculated for four trades consisting
    of a short, long, short and long ending net
    short after all trades with varying quantities
    and market prices.
    """
    # First trade (first short)
    symbol = "AGG"
    asset = Asset(symbol=symbol, exchange=EXCHANGE)
    size = 762
    timestamp = datetime.strptime("2020-06-16 15:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    price = 117.74
    order_id = "100"
    commission = 5.35
    transaction = Transaction(
        asset=asset,
        size=size,
        action=Action.SELL,
        direction=Direction.SHORT,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position = Position.open_from_transaction(transaction)

    # Second trade (second short)
    size = 477
    timestamp = datetime.strptime("2020-06-16 16:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    price = 117.875597
    order_id = "101"
    commission = 2.31
    transaction = Transaction(
        asset=asset,
        size=size,
        action=Action.BUY,
        direction=Direction.SHORT,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position.transact(transaction)

    # Third trade (third short)
    size = 595
    timestamp = datetime.strptime("2020-06-16 17:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    price = 117.74
    order_id = "102"
    commission = 4.18
    transaction = Transaction(
        asset=asset,
        size=size,
        action=Action.SELL,
        direction=Direction.SHORT,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position.transact(transaction)

    # Fourth trade (fourth short), now net short
    size = 427
    timestamp = datetime.strptime("2020-06-16 18:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    price = 117.793115
    order_id = "103"
    commission = 2.06
    transaction = Transaction(
        asset=asset,
        size=size,
        action=Action.BUY,
        direction=Direction.SHORT,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position.transact(transaction)

    assert position.asset == asset
    assert position.price == price
    assert position.last_price_update == timestamp

    assert position.buy_size == 904
    assert position.sell_size == 1357
    assert position.avg_bought == pytest.approx(117.8366, 0.00001)
    assert position.avg_sold == 117.74
    assert position.commission == pytest.approx(13.90, 0.01)

    assert position.direction == Direction.SHORT
    assert position.market_value == -53360.281095
    assert position.avg_price == 117.7329771554900515843773029
    assert position.net_size == -453
    assert position.total_bought == pytest.approx(106524.319874, 0.0000001)
    assert position.total_sold == 159773.18
    assert position.net_total == pytest.approx(53248.860126, 0.0000001)
    assert position.net_incl_commission == pytest.approx(53234.960126, 0.0000001)
    assert position.unrealised_pnl == pytest.approx(-27.2424, 0.00001)
    assert position.realised_pnl == pytest.approx(-98.0785, 0.00001)
    assert position.total_pnl == pytest.approx(-125.320969, 0.0000001)


def test_position_limit_reached_long():
    """
    Tests that the properties on the Position
    are calculated for four trades consisting
    of a short, long, short and long ending net
    short after all trades with varying quantities
    and market prices.
    """
    # First trade (first long)
    symbol = "AGG"
    asset = Asset(symbol=symbol, exchange=EXCHANGE)
    size = 762
    timestamp = datetime.strptime("2020-06-16 15:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    price = 117.74
    order_id = "100"
    commission = 5.35
    transaction = Transaction(
        asset=asset,
        size=size,
        action=Action.BUY,
        direction=Direction.LONG,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position = Position.open_from_transaction(transaction)

    # Second trade (second long)
    size = 765
    timestamp = datetime.strptime("2020-06-16 16:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    price = 117.875597
    order_id = "101"
    commission = 2.31
    transaction = Transaction(
        asset=asset,
        size=size,
        action=Action.SELL,
        direction=Direction.LONG,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position.transact(transaction)

    assert position.asset == asset
    assert position.price == price
    assert position.last_price_update == timestamp

    assert position.buy_size == 762
    assert position.sell_size == 762
    assert position.avg_bought == 117.74
    assert position.avg_sold == 117.875597
    assert position.commission == 7.66

    assert position.direction == Direction.LONG
    assert position.market_value == 0.000000
    assert position.avg_price == 0.0
    assert position.net_size == 0
    assert position.total_bought == pytest.approx(89717.88, 0.01)
    assert position.total_sold == 89821.204914
    assert position.net_total == pytest.approx(103.324914, 0.0000001)
    assert position.net_incl_commission == pytest.approx(95.664914, 0.0000001)
    assert position.unrealised_pnl == 0.000000
    assert position.realised_pnl == pytest.approx(95.664914, 0.0000001)
    assert position.total_pnl == pytest.approx(95.664914, 0.0000001)


def test_position_limit_reached_short():
    """
    Tests that the properties on the Position
    are calculated for four trades consisting
    of a short, long, short and long ending net
    short after all trades with varying quantities
    and market prices.
    """
    # First trade (first short)
    symbol = "AGG"
    asset = Asset(symbol=symbol, exchange=EXCHANGE)
    size = 762
    timestamp = datetime.strptime("2020-06-16 15:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    price = 117.74
    order_id = "100"
    commission = 5.35
    transaction = Transaction(
        asset=asset,
        size=size,
        action=Action.SELL,
        direction=Direction.SHORT,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position = Position.open_from_transaction(transaction)

    # Second trade (second short)
    size = 765
    timestamp = datetime.strptime("2020-06-16 16:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    price = 117.875597
    order_id = "101"
    commission = 2.31
    transaction = Transaction(
        asset=asset,
        size=size,
        action=Action.BUY,
        direction=Direction.SHORT,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position.transact(transaction)

    assert position.asset == asset
    assert position.price == price
    assert position.last_price_update == timestamp

    assert position.buy_size == 762
    assert position.sell_size == 762
    assert position.avg_bought == 117.875597
    assert position.avg_sold == 117.74
    assert position.commission == 7.66

    assert position.direction == Direction.SHORT
    assert position.market_value == 0.000000
    assert position.avg_price == 0.0
    assert position.net_size == 0
    assert position.total_bought == 89821.204914
    assert position.total_sold == pytest.approx(89717.88, 0.01)
    assert position.net_total == pytest.approx(-103.324914, 0.0000001)
    assert position.net_incl_commission == pytest.approx(-110.984914, 0.000001)
    assert position.unrealised_pnl == 0.000000
    assert position.realised_pnl == pytest.approx(-110.984914, 0.000001)
    assert position.total_pnl == pytest.approx(-110.984914, 0.000001)


def test_position_direction_different_from_transaction():
    """
    Tests that the properties on the Position
    are calculated for four trades consisting
    of a short, long, short and long ending net
    short after all trades with varying quantities
    and market prices.
    """
    # First trade (first short)
    symbol = "AGG"
    asset = Asset(symbol=symbol, exchange=EXCHANGE)
    size1 = 762
    timestamp1 = datetime.strptime("2020-06-16 15:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    price1 = 117.74
    order_id1 = "100"
    commission1 = 5.35
    transaction = Transaction(
        asset=asset,
        size=size1,
        action=Action.BUY,
        direction=Direction.LONG,
        price=price1,
        order_id=order_id1,
        commission=commission1,
        timestamp=timestamp1,
    )
    position = Position.open_from_transaction(transaction)

    # Second trade (second short)
    size2 = 765
    timestamp2 = datetime.strptime("2020-06-16 16:00:00+0000", "%Y-%m-%d %H:%M:%S%z")
    price2 = 117.875597
    order_id2 = "101"
    commission2 = 2.31
    transaction = Transaction(
        asset=asset,
        size=size2,
        action=Action.SELL,
        direction=Direction.SHORT,
        price=price2,
        order_id=order_id2,
        commission=commission2,
        timestamp=timestamp2,
    )
    with pytest.raises(ValueError):
        position.transact(transaction)

    assert position.asset == asset
    assert position.price == price1
    assert position.last_price_update == timestamp1

    assert position.buy_size == 762
    assert position.sell_size == 0
    assert position.avg_bought == 117.74
    assert position.avg_sold == 0.0
    assert position.commission == 5.35

    assert position.direction == Direction.LONG
    assert position.market_value == pytest.approx(89717.88, 0.01)
    assert position.avg_price == pytest.approx(117.7470209973753280839895013, 0.01)
    assert position.net_size == 762
    assert position.total_bought == pytest.approx(89717.88, 0.01)
    assert position.total_sold == 0.0
    assert position.net_total == pytest.approx(-89717.88, 0.01)
    assert position.net_incl_commission == -89723.23
    assert position.unrealised_pnl == pytest.approx(-5.35, 0.01)
    assert position.realised_pnl == 0.0
    assert position.total_pnl == pytest.approx(-5.35, 0.01)


def test_transact_for_incorrect_symbol():
    """
    Tests that the 'transact' method, when provided
    with a Transaction with a Symbol that does not
    match the position's symbol, raises an Exception.
    """
    symbol1 = "AAPL"
    asset1 = Asset(symbol=symbol1, exchange=EXCHANGE)
    symbol2 = "AMZN"
    asset2 = Asset(symbol=symbol2, exchange=EXCHANGE)

    position = Position(
        asset=asset1,
        price=950.0,
        buy_size=100,
        sell_size=0,
        direction=Direction.LONG,
        avg_bought=950.0,
        avg_sold=0.0,
        buy_commission=1.0,
        sell_commission=0.0,
        timestamp=datetime.strptime("2020-06-16 15:00:00+0000", "%Y-%m-%d %H:%M:%S%z"),
    )

    new_timestamp = datetime.strptime("2020-06-16 16:00:00", "%Y-%m-%d %H:%M:%S")
    transaction = Transaction(
        asset=asset2,
        size=50,
        action=Action.BUY,
        direction=Direction.LONG,
        price=960.0,
        order_id="123",
        commission=1.0,
        timestamp=new_timestamp,
    )

    with pytest.raises(Exception):
        position.transact(transaction)


def test_transact_with_size_zero():
    """
    Tests that the 'transact' method, when provided
    with a Transaction with size 0 doesn't alter the
    position values.
    """
    symbol = "AAPL"
    asset = Asset(symbol=symbol, exchange=EXCHANGE)
    price = 950.0
    buy_size = 100
    sell_size = 0
    avg_bought = 950.0
    avg_sold = 0.0
    buy_commission = 1.0
    sell_commission = 0.0
    timestamp = datetime.strptime("2020-06-16 15:00:00+0000", "%Y-%m-%d %H:%M:%S%z")

    position = Position(
        asset=asset,
        price=price,
        buy_size=buy_size,
        sell_size=sell_size,
        direction=Direction.LONG,
        avg_bought=avg_bought,
        avg_sold=avg_sold,
        buy_commission=buy_commission,
        sell_commission=sell_commission,
        timestamp=timestamp,
    )

    new_timestamp = datetime.strptime("2020-06-16 16:00:00", "%Y-%m-%d %H:%M:%S")
    transaction = Transaction(
        asset=asset,
        size=0,
        action=Action.BUY,
        direction=Direction.LONG,
        price=960.0,
        order_id="123",
        commission=1.0,
        timestamp=new_timestamp,
    )

    position.transact(transaction)

    assert position.asset == asset
    assert position.price == price
    assert position.last_price_update == timestamp

    assert position.buy_size == buy_size
    assert position.sell_size == sell_size
    assert position.avg_bought == avg_bought
    assert position.avg_sold == avg_sold
    assert position.commission == buy_commission

    assert position.direction == Direction.LONG
    assert position.market_value == 95000.0
    assert position.avg_price == 950.01
    assert position.net_size == 100
    assert position.total_bought == 95000.0
    assert position.total_sold == 0.0
    assert position.net_total == -95000.0
    assert position.net_incl_commission == -95001.0
    assert position.unrealised_pnl == pytest.approx(-1.00, 0.01)
    assert position.realised_pnl == 0.0
    assert position.total_pnl == pytest.approx(-1.00, 0.01)


def test_neq_different_type():
    symbol1 = "AAPL"
    price1 = 950.0
    buy_size1 = 100
    sell_size1 = 0
    avg_bought1 = 950.0
    avg_sold1 = 0.0
    buy_commission1 = 1.0
    sell_commission1 = 0.0
    timestamp1 = datetime.strptime("2020-06-16 15:00:00+0000", "%Y-%m-%d %H:%M:%S%z")

    position1 = Position(
        symbol1,
        price=price1,
        buy_size=buy_size1,
        sell_size=sell_size1,
        direction=Direction.LONG,
        avg_bought=avg_bought1,
        avg_sold=avg_sold1,
        buy_commission=buy_commission1,
        sell_commission=sell_commission1,
        timestamp=timestamp1,
    )

    position2 = Position(
        symbol1,
        price=price1,
        buy_size=buy_size1,
        sell_size=sell_size1,
        direction=Direction.LONG,
        avg_bought=avg_bought1,
        avg_sold=avg_sold1,
        buy_commission=buy_commission1,
        sell_commission=sell_commission1,
        timestamp=timestamp1,
    )

    symbol1 = "GOOGL"
    price1 = 850.0
    buy_size1 = 150
    sell_size1 = 2
    avg_bought1 = 850.0
    avg_sold1 = 0.0
    buy_commission1 = 0.5
    sell_commission1 = 0.0
    position3 = Position(
        symbol1,
        price=price1,
        buy_size=buy_size1,
        sell_size=sell_size1,
        direction=Direction.LONG,
        avg_bought=avg_bought1,
        avg_sold=avg_sold1,
        buy_commission=buy_commission1,
        sell_commission=sell_commission1,
        timestamp=timestamp1,
    )
    assert position1 == position2
    assert position1 != position3
    assert position1 != object()
