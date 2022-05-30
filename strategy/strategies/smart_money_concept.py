from collections import deque
from datetime import timedelta
from typing import Dict, List

import numpy as np
from intervaltree import IntervalTree

from trazy_analysis.common.clock import Clock
from trazy_analysis.indicators.indicators_manager import IndicatorsManager
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.enums import Action, Direction, CandleDirection
from trazy_analysis.models.signal import Signal
from trazy_analysis.order_manager.order_manager import OrderManager
from trazy_analysis.strategy.context import Context
from trazy_analysis.strategy.strategy import LOG, Strategy


class SmartMoneyConcept(Strategy):
    DEFAULT_PARAMETERS = {
        "comparator": np.greater,
        "order": 2,
        "method": "local_extrema",
        "extrema_base": "body",
        "breakout_base": "body",
    }

    def __init__(
        self,
        context: Context,
        order_manager: OrderManager,
        events: deque,
        parameters: Dict[str, float],
        indicators_manager: IndicatorsManager = IndicatorsManager(),
    ):
        super().__init__(context, order_manager, events, parameters, indicators_manager)

        self.poi_touch = {
            asset: self.indicators_manager.PoiTouch(
                asset,
                time_unit=timedelta(minutes=5),
                comparator=self.parameters["comparator"],
                order=self.parameters["order"],
                method=self.parameters["method"],
                extrema_base=self.parameters["extrema_base"],
                breakout_base=self.parameters["breakout_base"],
            )
            for asset in context.candles
        }
        self.pending_bos = False
        self.pois = []
        self.min_value = float("inf")
        self.min_value_total = float("inf")
        self.bos_happened = False
        self.previous_extrema_val = None
        self.current_extrema_val = None
        self.pois_touch = IntervalTree()

    def generate_signals(self, context: Context, clock: Clock) -> List[Signal]:
        signals = []
        signal = None
        for candle in context.get_last_candles():
            if self.poi_touch[candle.asset].data:
                LOG.info("Time to buy")
                signal = Signal(
                    action=Action.BUY,
                    direction=Direction.LONG,
                    confidence_level=1.0,
                    strategy=self.name,
                    asset=candle.asset,
                    root_candle_timestamp=context.current_timestamp,
                    parameters={},
                    clock=clock,
                )

            if signal is not None:
                signals.append(signal)
            return signals
