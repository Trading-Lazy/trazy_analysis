from enum import Enum, auto


class PriceType(Enum):
    OPEN = auto()
    HIGH = auto()
    LOW = auto()
    CLOSE = auto()
    BODY_HIGH = auto()
    BODY_LOW = auto()
    LAST = auto()


def get_state(data: float) -> "CrossoverState":
    from trazy_analysis.indicators.crossover import CrossoverState

    if data is None or data == 0:
        return CrossoverState.IDLE
    return CrossoverState.POS if data > 0 else CrossoverState.NEG
