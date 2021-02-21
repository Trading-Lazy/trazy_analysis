from decimal import Decimal
from enum import Enum

from common.helper import check_type
from indicators.common import get_state
from indicators.stream import StreamData


class CrossoverState(Enum):
    IDLE = 0
    IDLE_POS_TREND = 0.5
    IDLE_NEG_TREND = -0.5
    POS = 1
    NEG = -1


class Crossover(StreamData):
    count = 0
    instances = 0

    def __init__(
        self, source_stream_data1: StreamData, source_stream_data2: StreamData
    ):
        Crossover.instances += 1
        self.sign_stream = source_stream_data1.sub(source_stream_data2)
        super().__init__(source_data=self.sign_stream)
        check_type(source_stream_data1.data, [int, float, Decimal])
        check_type(source_stream_data2.data, [int, float, Decimal])
        self.state = CrossoverState.IDLE
        self.data = 0

    def handle_new_data(self, new_data: float) -> None:
        Crossover.count += 1
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
