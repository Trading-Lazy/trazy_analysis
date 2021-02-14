from decimal import Decimal
from enum import Enum, auto


class PriceType(Enum):
    OPEN = auto()
    HIGH = auto()
    LOW = auto()
    CLOSE = auto()
    LAST = auto()


def get_state(data: Decimal) -> "CrossoverState":
    from indicators.crossover import CrossoverState

    if data is None or data == Decimal("0"):
        return CrossoverState.IDLE
    return CrossoverState.POS if data > Decimal("0") else CrossoverState.NEG
