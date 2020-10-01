from decimal import Decimal
from enum import Enum, auto


class PriceType(Enum):
    OPEN = auto()
    HIGH = auto()
    LOW = auto()
    CLOSE = auto()
    LAST = auto()


def get_or_create_nested_dict(nested_dict: dict, *keys) -> None:
    """
    Check if *keys (nested) exists in `element` (dict).
    """
    if not isinstance(nested_dict, dict):
        raise AttributeError("keys_exists() expects dict as first argument.")
    if len(keys) == 0:
        raise AttributeError("keys_exists() expects at least two arguments, one given.")

    _nested_dict = nested_dict
    for key in keys:
        try:
            _nested_dict = _nested_dict[key]
        except KeyError:
            _nested_dict[key] = {}
            _nested_dict = _nested_dict[key]


def get_state(data: Decimal) -> "CrossoverState":
    from indicators.crossover import CrossoverState

    if data is None or data == Decimal("0"):
        return CrossoverState.IDLE
    return CrossoverState.POS if data > Decimal("0") else CrossoverState.NEG
