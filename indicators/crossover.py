from decimal import Decimal
from enum import Enum

from rx import Observable

from indicators.common import get_state
from indicators.stream import StreamData, check_data_type


class CrossoverState(Enum):
    IDLE = Decimal("0")
    IDLE_POS_TREND = Decimal("0.5")
    IDLE_NEG_TREND = Decimal("-0.5")
    POS = Decimal("1")
    NEG = Decimal("-1")


class Crossover(StreamData):
    def __init__(
        self, source_stream_data1: Observable, source_stream_data2: Observable
    ):
        self.sign_stream = source_stream_data1 - source_stream_data2
        super().__init__(source_data=self.sign_stream)
        check_data_type(source_stream_data1.data, [int, float, Decimal])
        check_data_type(source_stream_data2.data, [int, float, Decimal])
        self.state = CrossoverState.IDLE
        self.data = Decimal("0")

    def _handle_new_data(self, new_data: Decimal) -> None:
        new_state = get_state(new_data)
        if (
            self.state == CrossoverState.NEG
            or self.state == CrossoverState.IDLE_NEG_TREND
        ) and (new_state == CrossoverState.POS or new_state == CrossoverState.IDLE):
            self.state = CrossoverState.POS
        elif (
            self.state == CrossoverState.NEG
            or self.state == CrossoverState.IDLE_NEG_TREND
            or self.state == CrossoverState.IDLE
        ) and new_state == CrossoverState.NEG:
            self.state = CrossoverState.IDLE_NEG_TREND
        elif (
            self.state == CrossoverState.POS
            or self.state == CrossoverState.IDLE_POS_TREND
            or self.state == CrossoverState.IDLE
        ) and (new_state == CrossoverState.IDLE or new_state == CrossoverState.POS):
            self.state = CrossoverState.IDLE_POS_TREND
        elif (
            self.state == CrossoverState.POS
            or self.state == CrossoverState.IDLE_POS_TREND
        ) and new_state == CrossoverState.NEG:
            self.state = CrossoverState.NEG
        self.data = round(self.state.value)
        self.on_next(self.data)
