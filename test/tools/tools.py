from contextlib import contextmanager
from typing import List

import pytest

from trazy_analysis.db_storage.db_storage import DbStorage
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.order import Order
from trazy_analysis.models.signal import Signal


def clean_candles_in_db(db_storage: DbStorage):
    db_storage.clean_all_candles()


def clean_signals_in_db(db_storage: DbStorage):
    db_storage.clean_all_orders()


def clean_orders_in_db(db_storage: DbStorage):
    db_storage.clean_all_orders()


def compare_list(list1: List[object], list2: List[object]):
    if len(list1) != len(list2):
        return False
    length = len(list1)
    for i in range(0, length):
        if list1[i] != list2[i]:
            return False
    return True


def compare_candles_list(
    candles_list1: List[Candle], candles_list2: List[Candle]
) -> bool:
    return compare_list(candles_list1, candles_list2)


def compare_signals_list(
    signals_list1: List[Signal], signals_list2: List[Signal]
) -> bool:
    return compare_list(signals_list1, signals_list2)


def compare_orders_list(orders_list1: List[Order], orders_list2: List[Order]) -> bool:
    return compare_list(orders_list1, orders_list2)


@contextmanager
def not_raises(exception):
    try:
        yield
    except exception:
        raise pytest.fail("DID RAISE {0}".format(exception))
