from decimal import Decimal

import pandas as pd
import pytest
import pytz

from models.enums import Direction, Action
from position.position import Position
from position.transaction import Transaction


def test_update_price_failure_with_earlier_timestamp():
    """
    Tests that an exception is raised when an attempt to update the price is made with a timestamp earlier thant
    the current timestamp
    """
    # Initial long details
    symbol = "MSFT"
    size = 100
    timestamp = pd.Timestamp("2020-06-16 15:00:00", tz=pytz.UTC)
    price = Decimal("193.74")
    order_id = "123"
    commission = Decimal("1.0")

    # Create the initial transaction and position
    transaction = Transaction(
        symbol,
        size=size,
        action=Action.BUY,
        direction=Direction.LONG,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position = Position.open_from_transaction(transaction)

    assert position.symbol == symbol
    assert position.price == price
    assert position.timestamp == timestamp

    # Update the market price
    new_price = Decimal("192.80")
    new_timestamp = pd.Timestamp("2020-06-16 14:00:00", tz=pytz.UTC)
    with pytest.raises(Exception):
        position.update_price(new_price, new_timestamp)


def test_update_price_failure_with_negative_price():
    """
    Tests that an exception is raised when an attempt to update the price is made with a negative price
    """
    # Initial long details
    symbol = "MSFT"
    size = 100
    timestamp = pd.Timestamp("2020-06-16 15:00:00", tz=pytz.UTC)
    price = Decimal("193.74")
    order_id = "123"
    commission = Decimal("1.0")

    # Create the initial transaction and position
    transaction = Transaction(
        symbol,
        size=size,
        action=Action.BUY,
        direction=Direction.LONG,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position = Position.open_from_transaction(transaction)

    assert position.symbol == symbol
    assert position.price == price
    assert position.timestamp == timestamp

    # Update the market price
    new_price = Decimal("-192.80")
    new_timestamp = pd.Timestamp("2020-06-16 16:00:00", tz=pytz.UTC)
    with pytest.raises(Exception):
        position.update_price(new_price, new_timestamp)


def test_basic_long_equities_position():
    """
    Tests that the properties on the Position
    are calculated for a simple long equities position.
    """
    # Initial long details
    symbol = "MSFT"
    size = 100
    timestamp = pd.Timestamp("2020-06-16 15:00:00", tz=pytz.UTC)
    price = Decimal("193.74")
    order_id = "123"
    commission = Decimal("1.0")

    # Create the initial transaction and position
    transaction = Transaction(
        symbol,
        size=size,
        action=Action.BUY,
        direction=Direction.LONG,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position = Position.open_from_transaction(transaction)

    assert position.symbol == symbol
    assert position.price == price
    assert position.timestamp == timestamp

    # Update the market price
    new_price = Decimal("192.80")
    new_timestamp = pd.Timestamp("2020-06-16 16:00:00", tz=pytz.UTC)
    position.update_price(new_price, new_timestamp)

    assert position.price == new_price
    assert position.timestamp == new_timestamp

    assert position.buy_size == 100
    assert position.sell_size == 0
    assert position.avg_bought == Decimal("193.74")
    assert position.avg_sold == Decimal("0.0")
    assert position.commission == Decimal("1.0")

    assert position.direction == Direction.LONG
    assert position.market_value == Decimal("19280.0")
    assert position.avg_price == Decimal("193.75")
    assert position.net_size == 100
    assert position.total_bought == Decimal("19374.0")
    assert position.total_sold == Decimal("0.0")
    assert position.net_total == Decimal("-19374.0")
    assert position.net_incl_commission == Decimal("-19375.0")
    assert position.unrealised_pnl == Decimal("-95.0")
    assert position.realised_pnl == Decimal("0.0")
    assert position.total_pnl == Decimal("-95.0")


def test_position_long_twice():
    """
    Tests that the properties on the Position
    are calculated for two consective long trades
    with differing quantities and market prices.
    """
    # Initial long details
    symbol = "MSFT"
    size = 100
    timestamp = pd.Timestamp("2020-06-16 15:00:00", tz=pytz.UTC)
    price = Decimal("193.74")
    order_id = "123"
    commission = Decimal("1.0")

    # Create the initial transaction and position
    first_transaction = Transaction(
        symbol,
        size=size,
        action=Action.BUY,
        direction=Direction.LONG,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position = Position.open_from_transaction(first_transaction)

    assert position.symbol == symbol
    assert position.price == price
    assert position.timestamp == timestamp

    # Second long
    second_size = 60
    second_timestamp = pd.Timestamp("2020-06-16 16:00:00", tz=pytz.UTC)
    second_price = Decimal("193.79")
    second_order_id = "234"
    second_commission = Decimal("1.0")
    second_transaction = Transaction(
        symbol,
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
    assert position.timestamp == second_timestamp

    assert position.buy_size == 160
    assert position.sell_size == 0
    assert position.avg_bought == Decimal("193.75875")
    assert position.avg_sold == Decimal("0.0")
    assert position.commission == Decimal("2.0")

    assert position.direction == Direction.LONG
    assert position.market_value == Decimal("31006.40")
    assert position.avg_price == Decimal("193.77125")
    assert position.net_size == 160
    assert position.total_bought == Decimal("31001.40")
    assert position.total_sold == Decimal("0.0")
    assert position.net_total == Decimal("-31001.40")
    assert position.net_incl_commission == Decimal("-31003.40")
    assert position.unrealised_pnl == Decimal("3.0")
    assert position.realised_pnl == Decimal("0.0")
    assert position.total_pnl == Decimal("3.0")


def test_position_long_close():
    """
    Tests that the properties on the Position
    are calculated for a long opening trade and
    subsequent closing trade.
    """
    # Initial long details
    symbol = "AMZN"
    size = 100
    timestamp = pd.Timestamp("2020-06-16 15:00:00", tz=pytz.UTC)
    price = Decimal("2615.27")
    order_id = "123"
    commission = Decimal("1.0")

    # Create the initial transaction and position
    first_transaction = Transaction(
        symbol,
        size=size,
        action=Action.BUY,
        direction=Direction.LONG,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position = Position.open_from_transaction(first_transaction)

    assert position.symbol == symbol
    assert position.price == price
    assert position.timestamp == timestamp

    # Closing trade
    second_size = 100
    second_timestamp = pd.Timestamp("2020-06-16 16:00:00", tz=pytz.UTC)
    second_price = Decimal("2622.0")
    second_order_id = "234"
    second_commission = Decimal("6.81")
    second_transaction = Transaction(
        symbol,
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
    assert position.timestamp == second_timestamp

    assert position.buy_size == 100
    assert position.sell_size == 100
    assert position.avg_bought == Decimal("2615.27")
    assert position.avg_sold == Decimal("2622.0")
    assert position.commission == Decimal("7.81")

    assert position.market_value == Decimal("0.0")
    assert position.avg_price == Decimal("0.0")
    assert position.net_size == 0
    assert position.total_bought == Decimal("261527.0")
    assert position.total_sold == Decimal("262200.0")
    assert position.net_total == Decimal("673.0")
    assert position.net_incl_commission == Decimal("665.19")
    assert position.unrealised_pnl == Decimal("0.0")
    assert position.realised_pnl == Decimal("665.19")
    assert position.total_pnl == Decimal("665.19")


def test_position_long_and_short():
    """
    Tests that the properties on the Position
    are calculated for a long trade followed by
    a partial closing short trade with differing
    market prices.
    """
    # Initial long details
    symbol = "SPY"
    size = 100
    timestamp = pd.Timestamp("2020-06-16 15:00:00", tz=pytz.UTC)
    price = Decimal("307.05")
    order_id = "123"
    commission = Decimal("1.0")

    # Create the initial transaction and position
    first_transaction = Transaction(
        symbol,
        size=size,
        action=Action.BUY,
        direction=Direction.LONG,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position = Position.open_from_transaction(first_transaction)

    assert position.symbol == symbol
    assert position.price == price
    assert position.timestamp == timestamp

    # Short details and transaction
    second_size = 60
    second_timestamp = pd.Timestamp("2020-06-16 16:00:00", tz=pytz.UTC)
    second_price = Decimal("314.91")
    second_order_id = "234"
    second_commission = Decimal("1.42")
    second_transaction = Transaction(
        symbol,
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
    assert position.timestamp == second_timestamp

    assert position.buy_size == 100
    assert position.sell_size == 60
    assert position.avg_bought == Decimal("307.05")
    assert position.avg_sold == Decimal("314.91")
    assert position.commission == Decimal("2.42")

    assert position.direction == Direction.LONG
    assert position.market_value == Decimal("12596.40")
    assert position.avg_price == Decimal("307.06")
    assert position.net_size == 40
    assert position.total_bought == Decimal("30705.0")
    assert position.total_sold == Decimal("18894.60")
    assert position.net_total == Decimal("-11810.40")
    assert position.net_incl_commission == Decimal("-11812.82")
    assert position.unrealised_pnl == Decimal("314.0")
    assert position.realised_pnl == Decimal("469.58")
    assert position.total_pnl == Decimal("783.58")


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
    size = 453
    timestamp = pd.Timestamp("2020-06-16 15:00:00", tz=pytz.UTC)
    price = Decimal("312.96")
    order_id = "100"
    commission = Decimal("1.95")

    # Create the initial transaction and position
    first_transaction = Transaction(
        symbol,
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
    timestamp = pd.Timestamp("2020-06-16 16:00:00", tz=pytz.UTC)
    price = Decimal("315.599924")
    order_id = "101"
    commission = Decimal("4.8")
    transaction = Transaction(
        symbol,
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
    timestamp = pd.Timestamp("2020-06-16 17:00:00", tz=pytz.UTC)
    price = Decimal("312.96")
    order_id = "102"
    commission = Decimal("2.68")
    transaction = Transaction(
        symbol,
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
    timestamp = pd.Timestamp("2020-06-16 18:00:00", tz=pytz.UTC)
    price = Decimal("315.78")
    order_id = "103"
    commission = Decimal("6.28")
    transaction = Transaction(
        symbol,
        size=size,
        action=Action.SELL,
        direction=Direction.LONG,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position.transact(transaction)

    assert position.symbol == symbol
    assert position.price == price
    assert position.timestamp == timestamp

    assert position.buy_size == 1077
    assert position.sell_size == 916
    assert position.avg_bought == Decimal("312.96")
    assert position.avg_sold == Decimal("315.7019539606986899563318777")
    assert position.commission == Decimal("15.71")

    assert position.direction == Direction.LONG
    assert position.market_value == Decimal("50840.58")
    assert position.avg_price == Decimal("312.9642989786443825441039926")
    assert position.net_size == 161
    assert position.total_bought == Decimal("337057.92")
    assert position.total_sold == Decimal("289182.9898280000000000000000")
    assert position.net_total == Decimal("-47874.9301720000000000000000")
    assert position.net_incl_commission == Decimal("-47890.6401720000000000000000")
    assert position.unrealised_pnl == Decimal("453.3278644382544103992571914")
    assert position.realised_pnl == Decimal("2496.611963561745589600742777")
    assert position.total_pnl == Decimal("2949.939827999999999999999968")


def test_basic_short_equities_position():
    """
    Tests that the properties on the Position
    are calculated for a simple short equities position.
    """
    # Initial short details
    symbol = "TLT"
    size = 100
    timestamp = pd.Timestamp("2020-06-16 15:00:00", tz=pytz.UTC)
    price = Decimal("162.39")
    order_id = "123"
    commission = Decimal("1.37")

    # Create the initial transaction and position
    transaction = Transaction(
        symbol,
        size=size,
        action=Action.SELL,
        direction=Direction.SHORT,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position = Position.open_from_transaction(transaction)

    assert position.symbol == symbol
    assert position.price == price
    assert position.timestamp == timestamp

    # Update the market price
    new_price = Decimal("159.43")
    new_timestamp = pd.Timestamp("2020-06-16 16:00:00", tz=pytz.UTC)
    position.update_price(new_price, new_timestamp)

    assert position.price == new_price
    assert position.timestamp == new_timestamp

    assert position.buy_size == 0
    assert position.sell_size == 100
    assert position.avg_bought == Decimal("0.0")
    assert position.avg_sold == Decimal("162.39")
    assert position.commission == Decimal("1.37")

    assert position.direction == Direction.SHORT
    assert position.market_value == Decimal("-15943.0")
    assert position.avg_price == Decimal("162.3763")
    assert position.net_size == -100
    assert position.total_bought == Decimal("0.0")

    # np.isclose used for floating point precision
    assert position.total_sold == Decimal("16239.0")
    assert position.net_total == Decimal("16239.0")
    assert position.net_incl_commission == Decimal("16237.63")
    assert position.unrealised_pnl == Decimal("294.63")
    assert position.realised_pnl == Decimal("0.0")
    assert position.total_pnl == Decimal("294.63")


def test_position_short_twice():
    """
    Tests that the properties on the Position
    are calculated for two consective short trades
    with differing quantities and market prices.
    """
    # Initial short details
    symbol = "MSFT"
    size = 100
    timestamp = pd.Timestamp("2020-06-16 15:00:00", tz=pytz.UTC)
    price = Decimal("194.55")
    order_id = "123"
    commission = Decimal("1.44")

    # Create the initial transaction and position
    first_transaction = Transaction(
        symbol,
        size=size,
        action=Action.SELL,
        direction=Direction.SHORT,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position = Position.open_from_transaction(first_transaction)

    assert position.symbol == symbol
    assert position.price == price
    assert position.timestamp == timestamp

    # Second short
    second_size = 60
    second_timestamp = pd.Timestamp("2020-06-16 16:00:00", tz=pytz.UTC)
    second_price = Decimal("194.76")
    second_order_id = "234"
    second_commission = Decimal("1.27")
    second_transaction = Transaction(
        symbol,
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
    assert position.timestamp == second_timestamp

    assert position.buy_size == 0
    assert position.sell_size == 160
    assert position.avg_bought == Decimal("0.0")
    assert position.avg_sold == Decimal("194.62875")
    assert position.commission == Decimal("2.71")

    assert position.direction == Direction.SHORT
    assert position.market_value == Decimal("-31161.6")
    assert position.avg_price == Decimal("194.6118125")
    assert position.net_size == -160
    assert position.total_bought == Decimal("0.0")
    assert position.total_sold == Decimal("31140.60")
    assert position.net_total == Decimal("31140.6")
    assert position.net_incl_commission == Decimal("31137.89")
    assert position.unrealised_pnl == Decimal("-23.71")
    assert position.realised_pnl == Decimal("0.0")
    assert position.total_pnl == Decimal("-23.71")


def test_position_short_close():
    """
    Tests that the properties on the Position
    are calculated for a short opening trade and
    subsequent closing trade.
    """
    # Initial short details
    symbol = "TSLA"
    size = 100
    timestamp = pd.Timestamp("2020-06-16 15:00:00", tz=pytz.UTC)
    price = Decimal("982.13")
    order_id = "123"
    commission = Decimal("3.18")

    # Create the initial transaction and position
    first_transaction = Transaction(
        symbol,
        size=size,
        action=Action.SELL,
        direction=Direction.SHORT,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position = Position.open_from_transaction(first_transaction)

    assert position.symbol == symbol
    assert position.price == price
    assert position.timestamp == timestamp

    # Closing trade
    second_size = 100
    second_timestamp = pd.Timestamp("2020-06-16 16:00:00", tz=pytz.UTC)
    second_price = Decimal("982.13")
    second_order_id = "234"
    second_commission = Decimal("1.0")
    second_transaction = Transaction(
        symbol,
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
    assert position.timestamp == second_timestamp

    assert position.buy_size == 100
    assert position.sell_size == 100
    assert position.avg_bought == Decimal("982.13")
    assert position.avg_sold == Decimal("982.13")
    assert position.commission == Decimal("4.18")

    assert position.market_value == Decimal("0.0")
    assert position.avg_price == Decimal("0.0")
    assert position.net_size == 0
    assert position.total_bought == Decimal("98213.0")
    assert position.total_sold == Decimal("98213.0")
    assert position.net_total == Decimal("0.0")
    assert position.net_incl_commission == Decimal("-4.18")
    assert position.unrealised_pnl == Decimal("0.0")
    assert position.realised_pnl == Decimal("-4.18")
    assert position.total_pnl == Decimal("-4.18")


def test_position_short_and_long():
    """
    Tests that the properties on the Position
    are calculated for a short trade followed by
    a partial closing long trade with differing
    market prices.
    """
    # Initial short details
    symbol = "TLT"
    size = 100
    timestamp = pd.Timestamp("2020-06-16 15:00:00", tz=pytz.UTC)
    price = Decimal("162.39")
    order_id = "123"
    commission = Decimal("1.37")

    # Create the initial transaction and position
    transaction = Transaction(
        symbol,
        size=size,
        action=Action.SELL,
        direction=Direction.SHORT,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position = Position.open_from_transaction(transaction)

    assert position.symbol == symbol
    assert position.price == price
    assert position.timestamp == timestamp

    # Long details and transaction
    second_size = 60
    second_timestamp = pd.Timestamp("2020-06-16 16:00:00", tz=pytz.UTC)
    second_price = Decimal("159.99")
    second_order_id = "234"
    second_commission = Decimal("1.0")
    second_transaction = Transaction(
        symbol,
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
    assert position.timestamp == second_timestamp

    assert position.buy_size == 60
    assert position.sell_size == 100
    assert position.avg_bought == Decimal("159.99")
    assert position.avg_sold == Decimal("162.39")
    assert position.commission == Decimal("2.37")

    assert position.direction == Direction.SHORT
    assert position.market_value == Decimal("-6399.6")
    assert position.avg_price == Decimal("162.3763")
    assert position.net_size == -40
    assert position.total_bought == Decimal("9599.40")
    assert position.total_sold == Decimal("16239.0")
    assert position.net_total == Decimal("6639.60")
    assert position.net_incl_commission == Decimal("6637.23")
    assert position.unrealised_pnl == Decimal("95.452")
    assert position.realised_pnl == Decimal("142.178")
    assert position.total_pnl == Decimal("237.630")


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
    size = 762
    timestamp = pd.Timestamp("2020-06-16 15:00:00", tz=pytz.UTC)
    price = Decimal("117.74")
    order_id = "100"
    commission = Decimal("5.35")
    transaction = Transaction(
        symbol,
        size=size,
        action=Action.SELL,
        direction=Direction.SHORT,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position = Position.open_from_transaction(transaction)

    # Second trade (first long)
    size = 477
    timestamp = pd.Timestamp("2020-06-16 16:00:00", tz=pytz.UTC)
    price = Decimal("117.875597")
    order_id = "101"
    commission = Decimal("2.31")
    transaction = Transaction(
        symbol,
        size=size,
        action=Action.BUY,
        direction=Direction.SHORT,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position.transact(transaction)

    # Third trade (second short)
    size = 595
    timestamp = pd.Timestamp("2020-06-16 17:00:00", tz=pytz.UTC)
    price = Decimal("117.74")
    order_id = "102"
    commission = Decimal("4.18")
    transaction = Transaction(
        symbol,
        size=size,
        action=Action.SELL,
        direction=Direction.SHORT,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position.transact(transaction)

    # Fourth trade (second long), now net short
    size = 427
    timestamp = pd.Timestamp("2020-06-16 18:00:00", tz=pytz.UTC)
    price = Decimal("117.793115")
    order_id = "103"
    commission = Decimal("2.06")
    transaction = Transaction(
        symbol,
        size=size,
        action=Action.BUY,
        direction=Direction.SHORT,
        price=price,
        order_id=order_id,
        commission=commission,
        timestamp=timestamp,
    )
    position.transact(transaction)

    assert position.symbol == symbol
    assert position.price == price
    assert position.timestamp == timestamp

    assert position.buy_size == 904
    assert position.sell_size == 1357
    assert position.avg_bought == Decimal("117.8366370287610619469026549")
    assert position.avg_sold == Decimal("117.74")
    assert position.commission == Decimal("13.90")

    assert position.direction == Direction.SHORT
    assert position.market_value == Decimal("-53360.281095")
    assert position.avg_price == Decimal("117.7329771554900515843773029")
    assert position.net_size == -453
    assert position.total_bought == Decimal("106524.3198740000000000000000")
    assert position.total_sold == Decimal("159773.18")
    assert position.net_total == Decimal("53248.8601260000000000000000")
    assert position.net_incl_commission == Decimal("53234.9601260000000000000000")
    assert position.unrealised_pnl == Decimal("-27.2424435630066322770817863")
    assert position.realised_pnl == Decimal("-98.07852543699336772291823152")
    assert position.total_pnl == Decimal("-125.3209690000000000000000178")


def test_transact_for_incorrect_symbol():
    """
    Tests that the 'transact' method, when provided
    with a Transaction with a Symbol that does not
    match the position's symbol, raises an Exception.
    """
    symbol1 = "AAPL"
    symbol2 = "AMZN"

    position = Position(symbol1, price=Decimal("950.0"), buy_size=100, sell_size=0, direction=Direction.LONG,
                        avg_bought=Decimal("950.0"), avg_sold=Decimal("0.0"), buy_commission=Decimal("1.0"),
                        sell_commission=Decimal("0.0"), timestamp=pd.Timestamp("2020-06-16 15:00:00", tz=pytz.UTC))

    new_timestamp = pd.Timestamp("2020-06-16 16:00:00")
    transaction = Transaction(
        symbol2,
        size=50,
        action=Action.BUY,
        direction=Direction.LONG,
        price=Decimal("960.0"),
        order_id="123",
        commission=Decimal("1.0"),
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
    price = Decimal("950.0")
    buy_size = 100
    sell_size = 0
    avg_bought = Decimal("950.0")
    avg_sold = Decimal("0.0")
    buy_commission = Decimal("1.0")
    sell_commission = Decimal("0.0")
    timestamp = pd.Timestamp("2020-06-16 15:00:00", tz=pytz.UTC)

    position = Position(symbol, price=price, buy_size=buy_size, sell_size=sell_size, direction=Direction.LONG,
                        avg_bought=avg_bought, avg_sold=avg_sold, buy_commission=buy_commission,
                        sell_commission=sell_commission, timestamp=timestamp)

    new_timestamp = pd.Timestamp("2020-06-16 16:00:00")
    transaction = Transaction(
        symbol,
        size=0,
        action=Action.BUY,
        direction=Direction.LONG,
        price=Decimal("960.0"),
        order_id="123",
        commission=Decimal("1.0"),
        timestamp=new_timestamp,
    )

    position.transact(transaction)

    assert position.symbol == symbol
    assert position.price == price
    assert position.timestamp == timestamp

    assert position.buy_size == buy_size
    assert position.sell_size == sell_size
    assert position.avg_bought == avg_bought
    assert position.avg_sold == avg_sold
    assert position.commission == buy_commission

    assert position.direction == Direction.LONG
    assert position.market_value == Decimal("95000.0")
    assert position.avg_price == Decimal("950.01")
    assert position.net_size == 100
    assert position.total_bought == Decimal("95000.0")
    assert position.total_sold == Decimal("0.0")
    assert position.net_total == Decimal("-95000.0")
    assert position.net_incl_commission == Decimal("-95001.0")
    assert position.unrealised_pnl == Decimal("-1.00")
    assert position.realised_pnl == Decimal("0.0")
    assert position.total_pnl == Decimal("-1.00")
