from datetime import timedelta
from typing import Dict, List

import numpy as np
import telegram_send
from intervaltree import IntervalTree

from trazy_analysis.indicators.bos import PoiTouch
from trazy_analysis.indicators.indicator import CandleIndicator
from trazy_analysis.models.candle import Candle
from trazy_analysis.models.enums import Action, Direction
from trazy_analysis.models.parameter import Discrete, Choice, Static
from trazy_analysis.models.signal import Signal
from trazy_analysis.strategy.context import Context
from trazy_analysis.strategy.strategy import LOG, StrategyBase


class SmartMoneyConcept(StrategyBase):
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
        self, context: Context, data: CandleIndicator, parameters: Dict[str, float]
    ):
        super().__init__(data, parameters)

        self.poi_touch = {
            asset: PoiTouch(
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

    def current(self, candle: Candle) -> List[Signal]:
        if self.poi_touch[candle.asset].data:
            LOG.info(f"Time to buy: {candle.time_unit}")
            self.add_signal(
                Signal(asset=candle.asset, time_unit=timedelta(minutes=1), action=Action.BUY, direction=Direction.LONG)
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
