from collections import deque
from datetime import timedelta
from typing import Dict, List

import numpy as np
import telegram_send
from intervaltree import IntervalTree

from trazy_analysis.common.clock import Clock
from trazy_analysis.indicators.indicators_manager import IndicatorsManager
from trazy_analysis.models.enums import Action, Direction
from trazy_analysis.models.parameter import Discrete, Choice, Static
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
        "timeframe": timedelta(minutes=5),
    }

    DEFAULT_PARAMETERS_SPACE = {
        "comparator": Static(np.greater),
        "order": Discrete([1, 5]),
        "method": Choice(["local_extrema", "fractal"]),
        "extrema_base": Choice(["body", "candle"]),
        "breakout_base": Choice(["body", "candle"]),
        "timeframe": Choice(
            [
                timedelta(minutes=1),
                timedelta(minutes=5),
                timedelta(minutes=15),
                timedelta(minutes=30),
                timedelta(hours=1),
                timedelta(hours=4),
            ]
        ),
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
            if self.parameters["timeframe"] != candle.asset.time_unit:
                continue
            if self.poi_touch[candle.asset].data:
                LOG.info(f"Time to buy: {candle.asset.time_unit}")
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
                telegram_send.send(
                    messages=[
                        f"Signal generated!",
                        f"BUY LONG {str(candle.asset)}",
                        f"{candle.asset.exchange}: Strategy Smart money concepts results so far: "
                        f"cash = {self.order_manager.broker_manager.get_broker(candle.asset.exchange).get_portfolio_cash_balance()}, "
                        f"portfolio = {self.order_manager.broker_manager.get_broker(candle.asset.exchange).get_portfolio_as_dict()}, "
                        f"total_equity = {self.order_manager.broker_manager.get_broker(candle.asset.exchange).get_portfolio_total_equity()}",
                    ]
                )

        if signal is not None:
            signals.append(signal)
        return signals
