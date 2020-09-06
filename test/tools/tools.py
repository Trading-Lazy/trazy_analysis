from typing import List
from contextlib import contextmanager

import pytest

from db_storage.db_storage import DbStorage
from models.action import Action
from models.candle import Candle


def clean_candles_in_db(db_storage: DbStorage):
    db_storage.clean_all_candles()


def clean_actions_in_db(db_storage: DbStorage):
    db_storage.clean_all_actions()


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


def compare_actions_list(
    actions_list1: List[Action], actions_list2: List[Action]
) -> bool:
    if len(actions_list1) != len(actions_list2):
        return False
    length = len(actions_list1)
    for i in range(0, length):
        if actions_list1[i] != actions_list2[i]:
            return False
    return True


@contextmanager
def not_raises(exception):
    try:
        yield
    except exception:
        raise pytest.fail("DID RAISE {0}".format(exception))
