from typing import List
from contextlib import contextmanager

import pytest

from db_storage.db_storage import DbStorage
from models.order import Order
from models.candle import Candle


def clean_candles_in_db(db_storage: DbStorage):
    db_storage.clean_all_candles()


def clean_actions_in_db(db_storage: DbStorage):
    db_storage.clean_all_orders()


def compare_candles_list(
    candles_list1: List[Candle], candles_list2: List[Candle]
) -> bool:
    if len(candles_list1) != len(candles_list2):
        return False
    length = len(candles_list1)
    for i in range(0, length):
        if candles_list1[i] != candles_list2[i]:
            return False
    return True


def compare_orders_list(
    orders_list1: List[Order], orders_list2: List[Order]
) -> bool:
    if len(orders_list1) != len(orders_list2):
        return False
    length = len(orders_list1)
    for i in range(0, length):
        if orders_list1[i] != orders_list2[i]:
            return False
    return True


@contextmanager
def not_raises(exception):
    try:
        yield
    except exception:
        raise pytest.fail("DID RAISE {0}".format(exception))
