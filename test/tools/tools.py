from decimal import Decimal
from typing import List

from actionsapi.models import Candle, Action


def clean_candles_in_db():
    for candle in Candle.objects.all():
        candle.delete()


def clean_actions_in_db():
    for action in Action.objects.all():
        action.delete()


def compare_candle(candle1: Candle, candle2: Candle) -> bool:
    return (
        candle1.symbol == candle2.symbol
        and Decimal(candle1.open).normalize() == Decimal(candle2.open).normalize()
        and Decimal(candle1.high).normalize() == Decimal(candle2.high).normalize()
        and Decimal(candle1.low).normalize() == Decimal(candle2.low).normalize()
        and Decimal(candle1.close).normalize() == Decimal(candle2.close).normalize()
        and candle1.volume == candle2.volume
    )


def compare_candles_list(
    candles_list1: List[Candle], candles_list2: List[Candle]
) -> bool:
    if len(candles_list1) != len(candles_list2):
        return False
    length = len(candles_list1)
    for i in range(0, length):
        if not compare_candle(candles_list1[i], candles_list2[i]):
            return False
    return True


def compare_action(action1: Action, action2: Action) -> bool:
    return (
        action1.action_type == action2.action_type
        and action1.position_type == action2.position_type
        and action1.amount == action2.amount
        and Decimal(action1.confidence_level) == Decimal(action2.confidence_level)
        and action1.strategy == action2.strategy
        and action1.symbol == action2.symbol
        and action1.candle_id == action2.candle_id
        and action1.parameters == action2.parameters
    )


def compare_actions_list(
    actions_list1: List[Action], actions_list2: List[Action]
) -> bool:
    if len(actions_list1) != len(actions_list2):
        return False
    length = len(actions_list1)
    for i in range(0, length):
        if not compare_action(actions_list1[i], actions_list2[i]):
            return False
    return True
